package broker

import (
	"testing"
	"time"
)

// TestReapSweepCannotDeleteSuccessorOfTheQueueItJudgedIdle is the B-2
// delete-path regression.
//
// reapSweep reads its idle inputs -- LastActivityMilli and queueHasConsumers --
// from the QueueState it was handed, and only then issues the deletion. Every
// input is therefore stale by the time the delete runs. A queue.declare landing
// in that gap used to make the reaper destroy the SUCCESSOR by name: its
// record, its ring and its bindings, on the strength of the PREDECESSOR's idle
// clock, after the broker had already confirmed the successor's durable
// publishes.
//
// The race is made deterministic by calling reapSweep directly with the
// predecessor's QueueState after the redeclare has already completed -- the
// exact state the racing interleaving produces, with no goroutines and no
// timing assertion.
func TestReapSweepCannotDeleteSuccessorOfTheQueueItJudgedIdle(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	const expiresMs = 1000
	args := map[string]interface{}{"x-expires": int64(expiresMs)}

	if _, err := b.DeclareQueue("q", true, false, false, args); err != nil {
		t.Fatalf("DeclareQueue(q): %v", err)
	}
	qs1 := b.getOrCreateQueueState("q")
	ord1 := qs1.Ordinal()

	// The predecessor's policy is what drives the sweep. Assert it resolved,
	// otherwise reapSweep returns early and the test proves nothing.
	if p := qs1.Policy(); p == nil || !p.HasQueueExpires {
		t.Fatalf("qs1.Policy() = %+v, want HasQueueExpires; the sweep below would be a no-op", p)
	}

	// Drive the predecessor's idle clock past its expiry window, so the sweep
	// WILL decide to delete. Without this the test would pass vacuously.
	qs1.MarkActivity(b.ttlNowMillis() - 10*expiresMs)

	if _, err := b.DeleteQueue("q", false, false); err != nil {
		t.Fatalf("DeleteQueue(q): %v", err)
	}
	if _, err := b.DeclareQueue("q", true, false, false, args); err != nil {
		t.Fatalf("DeclareQueue(q) redeclare: %v", err)
	}
	qs2 := b.getOrCreateQueueState("q")
	ord2 := qs2.Ordinal()
	if ord2 == ord1 {
		t.Fatalf("redeclare reused ordinal %d; ordinals must never repeat within a broker run", ord1)
	}

	// The successor is fresh, so its own idle clock must NOT be expired --
	// otherwise a correct reaper would legitimately delete it and the
	// assertions below could not distinguish the defect from correct behaviour.
	qs2.MarkActivity(b.ttlNowMillis())

	const n = 3
	publishConfirmedDurable(t, b, "q", n)
	if got := qs2.WaitingCount(); got != n {
		t.Fatalf("successor waiting = %d, want %d; the assertions below would be vacuous", got, n)
	}

	// The stale sweep: an idle decision about incarnation ord1, arriving after
	// ord2 has taken the name.
	if next := b.reapSweep("q", qs1); next <= 0 {
		t.Errorf("reapSweep returned next = %v, want a positive interval", next)
	}

	rec, err := b.storage.GetQueue("q")
	if err != nil {
		t.Fatalf("GetQueue(q) after the stale sweep = %v, want the successor's record to still exist", err)
	}
	if rec.Ordinal != ord2 {
		t.Errorf("GetQueue(q).Ordinal = %d, want successor's %d", rec.Ordinal, ord2)
	}
	if got := qs2.WaitingCount(); got != n {
		t.Errorf("successor WaitingCount = %d, want %d; the reaper stranded confirmed durable "+
			"messages of incarnation %d using incarnation %d's idle clock", got, n, ord2, ord1)
	}
	select {
	case <-qs2.StopCh():
		t.Errorf("successor's QueueState was closed by a sweep that judged incarnation %d idle", ord1)
	default:
	}
}

// TestReapSweepStillDeletesTheIncarnationItJudgedIdle is the positive control
// for the test above: the incarnation-scoped delete must remain a working
// delete, not a universal refusal. Without this, replacing the reaper's call
// with a no-op would pass the regression test.
func TestReapSweepStillDeletesTheIncarnationItJudgedIdle(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	const expiresMs = 1000
	args := map[string]interface{}{"x-expires": int64(expiresMs)}

	if _, err := b.DeclareQueue("q", true, false, false, args); err != nil {
		t.Fatalf("DeclareQueue(q): %v", err)
	}
	qs := b.getOrCreateQueueState("q")
	qs.MarkActivity(b.ttlNowMillis() - 10*expiresMs)

	if next := b.reapSweep("q", qs); next != reaperMaxInterval {
		t.Errorf("reapSweep returned next = %v, want reaperMaxInterval (%v) after an idle delete",
			next, time.Duration(reaperMaxInterval))
	}
	if _, err := b.storage.GetQueue("q"); err == nil {
		t.Errorf("GetQueue(q) succeeded after the reaper judged it idle; the idle queue was not deleted")
	}
}
