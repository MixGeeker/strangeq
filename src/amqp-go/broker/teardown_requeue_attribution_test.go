package broker

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

// teardown_requeue_attribution_test.go — the attribution fixture for the leak
// shape {leakedWaiting=1, leakedInflight=0} that frontier_flip_strand_test.go
// asserts against.
//
// That shape has exactly two producers (see the writer enumeration at the top of
// frontier_flip_strand_test.go): a real strand, or a cancelled in-flight
// delivery returning to the ready set. The two are indistinguishable from the
// leak counters alone, which is what made an intermittent failure there
// unattributable. This file pins the SECOND producer deterministically — no
// scheduling race, no repetition count — so the shape can be attributed instead
// of argued about, and so the discriminator (RequeueDepth) is itself gated.
//
// It is also a production contract in its own right: cancelling a consumer with
// a delivery still parked in deliverMessage's hand-off select must return that
// message to the ready set exactly once, leaving inflight balanced at zero. A
// broker that dropped it instead would lose the message silently.

// TestTeardownRequeue_ProducesTheStrandLeakShape drives a no-ack consumer whose
// Messages channel is UNBUFFERED and never read. The poll loop therefore blocks
// in deliverMessage's {send | <-stopCh} select — the exact window that
// tail>=head does not cover, because Claim advances tail by CAS before
// deliverMessage runs.
//
// With the send unable to complete, closing stopCh leaves the select exactly
// one ready arm, so this reproduces at 1/1 rather than at the ~50% a race would
// give: the arm runs settle() then finishNack(requeue=true) -> Requeue, which is
// waiting+1 / inflight-1.
func TestTeardownRequeue_ProducesTheStrandLeakShape(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	const qname = "teardown-attrib"
	_, err := b.DeclareQueue(qname, true, false, false, nil)
	require.NoError(t, err)
	qs := b.getOrCreateQueueState(qname)

	// Unbuffered and deliberately unread: this is the whole fixture.
	msgs := make(chan *protocol.Delivery)
	cons := &protocol.Consumer{Tag: "c", Queue: qname, NoAck: true, Messages: msgs}
	require.NoError(t, registerConsumer(b, qname, "c", cons))

	require.NoError(t, b.PublishMessage("", qname, &protocol.Message{
		RoutingKey:   qname,
		Body:         []byte("x"),
		DeliveryMode: 1,
	}))

	// The premise, asserted rather than assumed: the delivery must actually be
	// parked in the hand-off select before the cancel. deliver() has run
	// (ClaimInflight: waiting-1, inflight+1) and the send cannot complete, so
	// {waiting=0, inflight=1} is precisely "parked in the select". Without this
	// wait the cancel could land before the claim and the test would assert
	// nothing.
	require.Eventually(t, func() bool {
		return qs.InflightCount() == 1 && qs.WaitingCount() == 0
	}, 10*time.Second, 100*time.Microsecond,
		"delivery never reached deliverMessage's hand-off select")
	require.EqualValues(t, 0, qs.RequeueDepth(), "nothing may be in the requeue ring before the cancel")

	require.NoError(t, b.UnregisterConsumer("c"))

	// The shape. leakedWaiting=1 / leakedInflight=0 is byte-for-byte what a
	// strand reports; requeueDepth=1 is what a strand does NOT report, and is
	// the only thing that separates them.
	require.EqualValues(t, 1, qs.WaitingCount(),
		"a cancelled in-flight delivery must return to the ready set")
	require.EqualValues(t, 0, qs.InflightCount(),
		"the requeue must balance inflight back to zero, not leave it negative or held")
	require.EqualValues(t, 1, qs.RequeueDepth(),
		"the requeued message must be in the requeue ring — this is the discriminator "+
			"that tells a teardown requeue apart from a strand, which leaves it at 0")
}

// TestTeardownRequeue_QuiescedConsumerCancelLeaksNothing is the other half of
// the attribution, and the reason frontier_flip_strand_test.go waits on the
// counters before cancelling: once no delivery is in flight, the same cancel
// leaks nothing at all.
//
// It shares the fixture above's shape except that the delivery is allowed to
// complete first, so the poll loop is back in Claim/park when stopCh closes and
// there is no select arm to lose. If this ever goes red while the fixture above
// stays green, the quiescence predicate has stopped implying "no delivery in
// flight" and the harness's wait is no longer sound.
func TestTeardownRequeue_QuiescedConsumerCancelLeaksNothing(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	const qname = "teardown-attrib-quiesced"
	_, err := b.DeclareQueue(qname, true, false, false, nil)
	require.NoError(t, err)
	qs := b.getOrCreateQueueState(qname)

	// Buffered and drained, so the hand-off always completes.
	msgs := make(chan *protocol.Delivery, 8)
	received := make(chan struct{}, 8)
	drainerDone := make(chan struct{})
	go func() {
		defer close(drainerDone)
		for range msgs {
			received <- struct{}{}
		}
	}()
	cons := &protocol.Consumer{Tag: "c", Queue: qname, NoAck: true, Messages: msgs}
	require.NoError(t, registerConsumer(b, qname, "c", cons))

	require.NoError(t, b.PublishMessage("", qname, &protocol.Message{
		RoutingKey:   qname,
		Body:         []byte("x"),
		DeliveryMode: 1,
	}))

	select {
	case <-received:
	case <-time.After(10 * time.Second):
		t.Fatal("consumer never received the message")
	}

	// The same predicate the harness uses.
	require.Eventually(t, func() bool {
		return qs.WaitingCount() == 0 && qs.InflightCount() == 0 && qs.RequeueDepth() == 0
	}, 10*time.Second, 100*time.Microsecond, "queue never quiesced after delivery")

	require.NoError(t, b.UnregisterConsumer("c"))
	close(msgs)
	<-drainerDone

	require.EqualValues(t, 0, qs.WaitingCount(), "a quiesced cancel must not re-enter anything into the ready set")
	require.EqualValues(t, 0, qs.InflightCount(), "a quiesced cancel must not move the inflight counter")
	require.EqualValues(t, 0, qs.RequeueDepth(), "a quiesced cancel must not requeue anything")
}
