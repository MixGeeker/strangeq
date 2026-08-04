package broker

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// bindStaggerSpread is the number of distinct per-iteration delays swept across
// the racing bind. Delay n is n*100us, so the sweep covers 0..29.9ms -- wide
// enough to straddle a full teardown on this machine in both -race and
// non--race builds, which differ by an order of magnitude. See the use site.
const bindStaggerSpread = 300

// TestBindQueueRacingDeleteLeavesNoGhostBinding is the B-5 regression.
//
// BindQueue validated the queue's existence and then stored the binding with
// nothing serializing the two. deleteQueueIncarnation's binding sweep runs
// inside the queue's create mutex, so a bind whose check passed just before the
// sweep, and whose store landed just after it, wrote a binding row addressed to
// a queue that no longer exists. No later delete of that name removes it -- the
// teardown that would have swept it has already run. (DeleteExchange and an
// explicit queue.unbind still remove it; nothing on the queue's own path does.)
//
// The consequence is worse than a leaked row. A publish is all-or-nothing
// across its routed target set: PublishMessage returns ErrQueueClosed for the
// WHOLE publish as soon as any one routed target's QueueState is closed. So a
// single ghost row on an (exchange, routing key) permanently poisons that key
// for every healthy queue bound to it. B-5 is an availability defect, not
// accounting residue.
//
// Unlike the purge and delete paths, a binding row carries no identity to
// revalidate against, so the fix is mutual exclusion and the detector is
// necessarily probabilistic (canon: probabilistic-detector).
//
// WHAT IS ASSERTED AT RUNTIME, and why it is not the obvious thing. The
// ghost-producing window -- bindings already swept, record not yet deleted --
// cannot be observed from outside; the fix makes every bind that would have
// entered it block instead. So the loop asserts the sweep BRACKETED the
// teardown: some binds completed ahead of it and some landed after it. Both
// counts must be non-zero, which is what shows the swept delays crossed the
// window rather than falling entirely on one side of it. Shrinking the loop, or
// narrowing the spread, fails the test rather than quietly making it prove
// nothing.
//
// DETECTION IS A RATE, NOT A GUARANTEE. Measured by removing the create mutex
// from BindQueue and counting:
//
//	non--race   3 of 3 runs red, first ghost at iterations 2, 3, 3
//	-race       9 of 12 runs red, first ghost at iterations 61-71
//
// So a single green -race run is NOT evidence that BindQueue's locking is
// intact: roughly one run in four misses. If you are changing that locking, run
// this test several times, and prefer a non--race run -- it reddens sooner and
// more reliably, because the race detector stretches the teardown and pushes
// the ghost window out to a later stagger.
//
// The -race clustering at iterations 61-71 is the window opening at a real
// delay of ~6.1-7.1ms, not scatter, which is why bindStaggerSpread must stay
// wide enough to reach it. A narrower sweep measured 0 detections and is what
// made an earlier version of this comment claim, wrongly, that -race gated
// nothing at all.
//
// The ghost row itself is real and observable independently of any race: after
// a delete followed by a late StoreBinding, GetQueueBindings returns the row
// with no error. Only the interleaving is probabilistic, not the assertion.
//
// Deliberately NOT addressed by making this deterministic: B-5's guarantee is
// mutual exclusion, which a sequential test cannot exercise, and forcing the
// interleaving would mean shipping a scheduling hook in production code.
func TestBindQueueRacingDeleteLeavesNoGhostBinding(t *testing.T) {
	const iterations = 300

	b, cleanup := createTestBroker(t)
	defer cleanup()

	if err := b.DeclareExchange("ex", "direct", true, false, false, nil); err != nil {
		t.Fatalf("DeclareExchange(ex): %v", err)
	}

	beforeTeardown, afterTeardown := 0, 0
	for i := 0; i < iterations; i++ {
		name := fmt.Sprintf("bind-q-%d", i)
		if _, err := b.DeclareQueue(name, true, false, false, nil); err != nil {
			t.Fatalf("DeclareQueue(%s): %v", name, err)
		}

		var (
			bindErr error
			wg      sync.WaitGroup
		)
		wg.Add(2)
		go func() {
			defer wg.Done()
			// Staggered deliberately. deleteQueueIncarnation deletes the
			// metadata record LAST, so a lock-free BindQueue's existence check
			// keeps passing well into the teardown; the interesting window
			// opens only once the teardown is underway, and firing both
			// goroutines together lands the bind ahead of it essentially every
			// time (measured: 0 ghosts in 300 unstaggered iterations).
			//
			// Every fourth iteration runs UNSTAGGERED, on purpose: those are
			// what keep the "ran ahead of the teardown" side of the bracket
			// assertion reliably non-zero. Leaving that side to the swept
			// delays alone yielded 1 of 300 under -race, which would flake.
			// The swept iterations supply the other side.
			//
			// The stagger changes only the hit RATE. Every assertion below is
			// on an outcome, never on a duration.
			if i%4 != 0 {
				time.Sleep(time.Duration((i%bindStaggerSpread)*100) * time.Microsecond)
			}
			bindErr = b.BindQueue(name, "ex", "rk", nil)
		}()
		go func() {
			defer wg.Done()
			b.DeleteQueue(name, false, false)
		}()
		wg.Wait()

		if _, err := b.storage.GetQueue(name); err == nil {
			// The delete lost the race outright; the queue is alive and its
			// binding is legitimate. Nothing to check.
			continue
		}

		if bindErr != nil {
			// Refused: the bind reached the existence check after the teardown
			// had removed the record, or (with the fix) blocked on the create
			// mutex until it had.
			afterTeardown++
		} else {
			// Accepted, yet the queue is gone: the bind ran ahead of the
			// teardown that then removed it.
			beforeTeardown++
		}

		bindings, err := b.storage.GetQueueBindings(name)
		if err != nil {
			continue
		}
		if len(bindings) > 0 {
			t.Fatalf("iteration %d: queue %s is deleted but %d binding row(s) survive it "+
				"(first: exchange=%s key=%s); a bind that raced the teardown wrote a row "+
				"after the binding sweep had already passed, and no later delete of this "+
				"name will remove it", i, name, len(bindings),
				bindings[0].ExchangeName, bindings[0].RoutingKey)
		}
	}

	if beforeTeardown == 0 || afterTeardown == 0 {
		t.Fatalf("the swept stagger did not straddle the teardown: %d bind(s) ran ahead of it and "+
			"%d landed after it, across %d iterations. Both must be non-zero, because that is what "+
			"shows the sweep crossed the window this test exists to cover -- bindings already "+
			"swept, record not yet deleted -- which cannot be observed directly. Raise the "+
			"iteration count or bindStaggerSpread rather than trusting this run",
			beforeTeardown, afterTeardown, iterations)
	}
	t.Logf("binds ahead of the teardown: %d; binds after it: %d; of %d iterations",
		beforeTeardown, afterTeardown, iterations)
}
