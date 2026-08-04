package broker

import (
	"sync/atomic"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
)

// staleCaptureStorage poisons exactly one storage.GetQueue result, by running a
// side effect AFTER the inner read has returned and then handing back the
// pre-side-effect record. The embedded interface supplies every other method,
// so this stays valid if interfaces.Storage grows.
//
// It is a value-injecting decorator, NOT a blocking double. Nothing in this
// file asserts that a goroutine fails to complete within a deadline, and there
// are no goroutines, sleeps or timeouts in it at all. That distinction is the
// reason this fixture is admissible where the storage double rejected during
// the Step 6 design debate was not.
type staleCaptureStorage struct {
	interfaces.Storage
	armedName string
	armed     atomic.Bool
	onFire    func()
}

func (s *staleCaptureStorage) GetQueue(name string) (*protocol.Queue, error) {
	// The inner read completes and releases the metadata mutex BEFORE the side
	// effect runs, so the re-entrant DeleteQueue/DeclareQueue inside onFire
	// cannot deadlock against it.
	rec, err := s.Storage.GetQueue(name)
	if err == nil && name == s.armedName && s.armed.CompareAndSwap(true, false) {
		s.onFire()
	}
	// Deliberately returns the PRE-side-effect record: that is exactly what the
	// racing interleaving hands the caller.
	return rec, err
}

// TestLastConsumerAutoDeleteCannotDeleteSuccessor is the B-7 regression. The
// last-consumer auto-delete in UnregisterConsumer must tear down the
// incarnation whose AutoDelete flag it read, never whatever currently answers
// to the name.
//
// WHY THIS NEEDS A SEAM WHERE reaper_incarnation_test.go DOES NOT. reapSweep
// RECEIVES its identity as a parameter, so a stale one can simply be handed to
// it. UnregisterConsumer DERIVES its identity internally, one line before
// acting on it — the sole storage.GetQueue in that function is the auto-delete
// capture itself. Racing those two adjacent calls does not work, and this
// fixture does not attempt it: measured with the fix reverted, a swept-stagger
// delete+redeclare scored 0 detections in 200 iterations under -race and 200
// without. The window is unreachable by TIMING and trivially reachable by
// VALUE. Derived-not-received means untestable as written, not ungatable —
// look one level down for the seam.
func TestLastConsumerAutoDeleteCannotDeleteSuccessor(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	const q = "auto-del-q"
	if _, err := b.DeclareQueue(q, true, true, false, nil); err != nil {
		t.Fatalf("DeclareQueue(%s): %v", q, err)
	}
	if err := b.BindQueue(q, "", q, nil); err != nil {
		t.Fatalf("BindQueue(%s): %v", q, err)
	}
	ord1 := b.getOrCreateQueueState(q).Ordinal()

	var qs2 *QueueState
	dec := &staleCaptureStorage{
		Storage:   b.storage,
		armedName: q,
		onFire: func() {
			// The gap: another client destroys and re-creates this name while
			// UnregisterConsumer sits between its capture and its act.
			if _, err := b.DeleteQueue(q, false, false); err != nil {
				t.Errorf("in-gap DeleteQueue: %v", err)
			}
			if _, err := b.DeclareQueue(q, true, true, false, nil); err != nil {
				t.Errorf("in-gap DeclareQueue: %v", err)
			}
			qs2 = b.getOrCreateQueueState(q)
			for i := 0; i < 3; i++ {
				if err := b.PublishMessage("", q, &protocol.Message{
					Body: []byte("payload"), DeliveryMode: 2,
				}); err != nil {
					t.Errorf("in-gap PublishMessage #%d: %v", i, err)
				}
			}
		},
	}

	// Installed BEFORE any consumer exists, and never reassigned afterwards.
	//
	// b.storage is a plain interface field with no synchronization, and
	// deliverMessage reads it on the consumerPollLoop goroutine that
	// RegisterConsumer starts. Assigning it while a consumer is live is an
	// unsynchronized write against a live reader; it happens not to trip -race
	// in this arrangement only because the queue is empty and that loop stays
	// parked, which is not a property worth depending on. Writing before the
	// reader exists removes it rather than relying on it being asleep.
	//
	// Arming is therefore separate from installation, and atomic: the flag is
	// read on whichever goroutine calls GetQueue. It must not be armed during
	// registration, because RegisterConsumer itself calls GetQueue.
	b.storage = dec

	consumer := &protocol.Consumer{
		Tag:           "c1",
		Queue:         q,
		PrefetchCount: 10,
		Messages:      make(chan *protocol.Delivery, 10),
		Cancel:        make(chan struct{}, 1),
	}
	if err := registerConsumer(b, q, "c1", consumer); err != nil {
		t.Fatalf("registerConsumer: %v", err)
	}

	dec.armed.Store(true)
	if err := b.UnregisterConsumer("c1"); err != nil {
		t.Fatalf("UnregisterConsumer(c1): %v", err)
	}

	if dec.armed.Load() {
		t.Fatalf("the decorator never fired: UnregisterConsumer made no GetQueue(%s) call, "+
			"so this fixture proved nothing", q)
	}
	if qs2 == nil {
		t.Fatalf("the successor was never created; fixture premise broken")
	}
	if qs2.Ordinal() == ord1 {
		t.Fatalf("redeclare reused ordinal %d; ordinals must never repeat within a broker run", ord1)
	}
	// PREMISE, not outcome. It guards against a vacuous run in which nothing
	// was published, and it is invariant under the fix by construction:
	// deleteQueueIncarnation never writes qs.waiting, and QueueState.Close()
	// touches only closed/stopCh/wake. So it reads 3 on a fixed tree AND on a
	// broken one — which is exactly what keeps it a premise instead of a
	// circular assertion. It is deliberately NOT re-asserted after the call:
	// there it could not distinguish the two trees, and an assertion no
	// mutation can reach is not evidence.
	if got := qs2.WaitingCount(); got != 3 {
		t.Fatalf("successor waiting = %d, want 3; the assertions below would be vacuous", got)
	}

	// The load-bearing observables: the successor's record survives, carrying
	// the successor's own ordinal, and its QueueState is still open.
	rec, err := b.storage.GetQueue(q)
	if err != nil {
		t.Fatalf("GetQueue(%s) after the last-consumer auto-delete = %v, want the SUCCESSOR's "+
			"record to still exist: the auto-delete decided on incarnation %d and must not "+
			"destroy incarnation %d", q, err, ord1, qs2.Ordinal())
	}
	if rec.Ordinal != qs2.Ordinal() {
		t.Errorf("GetQueue(%s).Ordinal = %d, want the successor's %d", q, rec.Ordinal, qs2.Ordinal())
	}
	select {
	case <-qs2.StopCh():
		t.Errorf("successor's QueueState was closed by an auto-delete that judged incarnation %d", ord1)
	default:
	}
}
