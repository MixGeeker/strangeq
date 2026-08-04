package broker

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// TestTransientRace_NoStrandUnderConcurrentPublishers proves that concurrent
// transient publishers to the same queue do not strand messages: every
// published message must be delivered, and WaitingCount must reconcile to 0
// after full drain. Without always-reserve-at-mint, a neighbor transient's
// casMaxHead can advance head past a still-unstored lower tag, causing a
// consumer gap-skip that strands the message and leaks the waiting counter.
func TestTransientRace_NoStrandUnderConcurrentPublishers(t *testing.T) {
	const numQueues = 100
	const publishersPerQueue = 8
	const msgsPerPublisher = 5

	var totalLeakedWaiting atomic.Int64
	var totalLeakedInflight atomic.Int64

	for q := 0; q < numQueues; q++ {
		b, cleanup := createTestBroker(t)

		const qname = "q"
		_, err := b.DeclareQueue(qname, false, false, false, nil)
		require.NoError(t, err)
		qs := b.getOrCreateQueueState(qname)

		msgs := make(chan *protocol.Delivery, 256)
		var delivered atomic.Int64
		drainerDone := make(chan struct{})
		go func() {
			defer close(drainerDone)
			for range msgs {
				delivered.Add(1)
			}
		}()
		cons := &protocol.Consumer{Tag: "c", Queue: qname, NoAck: true, Messages: msgs}
		require.NoError(t, registerConsumer(b, qname, "c", cons))

		var wg sync.WaitGroup
		start := make(chan struct{})
		for p := 0; p < publishersPerQueue; p++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < msgsPerPublisher; i++ {
					m := &protocol.Message{RoutingKey: qname, Body: []byte("x"), DeliveryMode: 1}
					_ = b.PublishMessage("", qname, m)
				}
			}()
		}
		close(start)
		wg.Wait()

		// Wait for every message to be HANDED OFF, not merely claimed.
		//
		// This predicate was `qs.tail.Load() >= qs.head.Load()` and was unsound
		// from the day it was written (it failed ~12/100 here and ~3/100 on
		// pristine main — same mechanism, different timing luck). Claim's CAS
		// advances tail BEFORE deliverMessage runs, so tail >= head certifies
		// "all tags CLAIMED", not "all messages DELIVERED". It goes true while
		// the last delivery is still in flight; UnregisterConsumer then closes
		// stopCh, deliverMessage's select has both cases ready, Go picks at
		// random, and when stopCh wins the message is requeued — which is
		// CORRECT basic.cancel semantics (AMQP 0-9-1 §1.8.3.6). WaitingCount()
		// == 1 is then the true ready depth, not a leak: the counters are
		// right and the old predicate was asserting something the code never
		// promised.
		//
		// delivered == total is the contract this test's own doc comment
		// already claims ("every published message must be delivered") but
		// never asserted. It is deterministic: once all 40 are delivered AND
		// drained, the requeue branch above is unreachable, so waiting and
		// inflight are provably 0 by the time they are sampled.
		//
		// Evidence, kept separate from the reasoning:
		//   MEASURED here — 0 failures in 1,000 iterations (-count=10 x 100
		//   queues) in isolation, against a ~12.5% failure rate for the old
		//   predicate; and mutation-proven — deleting the reserve-at-mint
		//   line in FrontierReserve (the exact "without always-reserve-at-mint"
		//   condition this test's header names) fails it loudly with
		//   "consumer did not deliver all 40 messages".
		//   ANALYTICAL, not measured — that this is strictly stronger than
		//   tail >= head because a gap-skipped strand still satisfies the old
		//   predicate. Under the mutation above the old predicate's run also
		//   failed, but via the WaitingCount assertion (-3970) rather than the
		//   drain check, so that mutation does not by itself separate the two.
		//
		// Approved by team-lead on the Fable root-cause analysis; a settle
		// window or retry was explicitly refused and is NOT what this is.
		const wantDelivered = publishersPerQueue * msgsPerPublisher
		require.Eventually(t, func() bool {
			return delivered.Load() == int64(wantDelivered)
		}, 10*time.Second, 100*time.Microsecond,
			"consumer did not deliver all %d messages", wantDelivered)

		require.NoError(t, b.UnregisterConsumer("c"))
		close(msgs)
		<-drainerDone

		totalLeakedWaiting.Add(qs.WaitingCount())
		totalLeakedInflight.Add(qs.InflightCount())

		cleanup()
	}

	require.Zero(t, totalLeakedWaiting.Load(), "transient publish must not leak WaitingCount")
	require.Zero(t, totalLeakedInflight.Load(), "transient publish must not leak InflightCount")
}
