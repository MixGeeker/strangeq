package broker

import (
	"fmt"
	"sync"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
)

// ============================================================================
// P0 Correctness fixes: spec-driven tests
//
// C3: deliveryIndex global key collision — two queues with same per-queue msgID
//     must not collide in the global deliveryIndex. Fixed by using a global
//     atomic counter for delivery tags.
//
// C7: queueConsumers non-atomic RMW — concurrent RegisterConsumer calls on
//     the same queue must not lose consumers. Fixed with per-queue mutex.
// ============================================================================

// TestDeliveryIndexNoCollision verifies that messages published to two
// different queues get globally unique delivery tags, so ACK routing via
// deliveryIndex doesn't collide (C3 fix).
func TestDeliveryIndexNoCollision(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	// Declare two queues
	broker.DeclareQueue("queue-a", false, false, false, nil)
	broker.DeclareQueue("queue-b", false, false, false, nil)

	// Publish one message to each queue — both would get msgID=1 with
	// per-queue counters, causing deliveryIndex collision
	msg1 := &protocol.Message{Body: []byte("msg1"), Exchange: "", RoutingKey: "queue-a"}
	msg2 := &protocol.Message{Body: []byte("msg2"), Exchange: "", RoutingKey: "queue-b"}

	if err := broker.PublishMessage("", "queue-a", msg1); err != nil {
		t.Fatalf("Publish to queue-a failed: %v", err)
	}
	if err := broker.PublishMessage("", "queue-b", msg2); err != nil {
		t.Fatalf("Publish to queue-b failed: %v", err)
	}

	// Delivery tags must be globally unique (not both = 1)
	if msg1.DeliveryTag == msg2.DeliveryTag {
		t.Errorf("delivery tags collide: both = %d (should be globally unique)", msg1.DeliveryTag)
	}

	// Verify both can be found in deliveryIndex via GetConsumerForDelivery
	// (This test verifies the fix at the storage broker level — we register
	// consumers and deliver messages to populate the index)
}

// TestQueueConsumersConcurrentRegister verifies that concurrent
// RegisterConsumer calls on the same queue don't lose consumers (C7 fix).
func TestQueueConsumersConcurrentRegister(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()
	broker.DeclareQueue("test-queue", false, false, false, nil)

	const numConsumers = 20
	var wg sync.WaitGroup
	wg.Add(numConsumers)

	// Register N consumers concurrently
	for i := 0; i < numConsumers; i++ {
		go func(idx int) {
			defer wg.Done()
			tag := fmt.Sprintf("consumer-%d", idx)
			consumer := &protocol.Consumer{
				Tag:      tag,
				Queue:    "test-queue",
				Messages: make(chan *protocol.Delivery, 1),
			}
			if err := broker.RegisterConsumer("test-queue", tag, consumer); err != nil {
				t.Errorf("RegisterConsumer %d failed: %v", idx, err)
			}
		}(i)
	}

	wg.Wait()

	// Verify all consumers are in activeConsumers
	count := 0
	broker.activeConsumers.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	if count != numConsumers {
		t.Errorf("expected %d active consumers, got %d", numConsumers, count)
	}

	// Verify all consumers are in queueConsumers list
	val, ok := broker.queueConsumers.Load("test-queue")
	if !ok {
		t.Fatal("queueConsumers entry not found")
	}
	consumers := val.([]*ConsumerState)
	if len(consumers) != numConsumers {
		t.Errorf("expected %d consumers in queue list, got %d (lost %d during concurrent register)",
			numConsumers, len(consumers), numConsumers-len(consumers))
	}

	// Clean up
	for i := 0; i < numConsumers; i++ {
		broker.UnregisterConsumer(fmt.Sprintf("consumer-%d", i))
	}
}

// TestQueueConsumersConcurrentUnregister verifies that concurrent
// UnregisterConsumer calls don't corrupt the consumer list (C7 fix).
func TestQueueConsumersConcurrentUnregister(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()
	broker.DeclareQueue("test-queue", false, false, false, nil)

	const numConsumers = 20
	// Register all consumers first
	for i := 0; i < numConsumers; i++ {
		tag := fmt.Sprintf("consumer-%d", i)
		consumer := &protocol.Consumer{
			Tag:      tag,
			Queue:    "test-queue",
			Messages: make(chan *protocol.Delivery, 1),
		}
		if err := broker.RegisterConsumer("test-queue", tag, consumer); err != nil {
			t.Fatalf("RegisterConsumer %d failed: %v", i, err)
		}
	}

	// Unregister all concurrently
	var wg sync.WaitGroup
	wg.Add(numConsumers)
	for i := 0; i < numConsumers; i++ {
		go func(idx int) {
			defer wg.Done()
			tag := fmt.Sprintf("consumer-%d", idx)
			if err := broker.UnregisterConsumer(tag); err != nil {
				t.Errorf("UnregisterConsumer %d failed: %v", idx, err)
			}
		}(i)
	}

	wg.Wait()

	// All consumers should be gone
	count := 0
	broker.activeConsumers.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	if count != 0 {
		t.Errorf("expected 0 active consumers after unregister, got %d", count)
	}

	// queueConsumers should be empty
	val, ok := broker.queueConsumers.Load("test-queue")
	if ok {
		consumers := val.([]*ConsumerState)
		if len(consumers) > 0 {
			t.Errorf("expected empty queueConsumers list, got %d remaining", len(consumers))
		}
	}
}

// TestDeliveryTagsGloballyUniquePerQueueMonotonic verifies the two properties
// the C3 fix actually protects, publishing alternately to two queues.
//
// This supersedes TestGlobalDeliveryTagMonotonic, which asserted that delivery
// tags strictly increase ACROSS queues. That was never a required property — it
// was an artifact of minting every tag from one shared counter. Tags are now
// composed per queue, so interleaved publishes to different queues legitimately
// produce descending values, and the old assertion is false by construction.
//
// The property that IS load-bearing is global UNIQUENESS, which is what that
// test's own doc comment named as its purpose. It is load-bearing because the
// broker has ONE shared WAL whose offsetIndex, ackBitmap and currentFileOffsets
// are keyed by a bare uint64 with no queue discriminator, as is the broker's
// deliveryIndex — so two queues ever minting the same tag would mean an ack on
// one queue reclaiming another queue's message: silent, durable, cross-queue
// data loss.
//
// The second property, per-queue monotonicity, is what the dispatch plane
// depends on (Claim walks a queue's tags in ascending order) and the old test
// never checked it at all. So this is strictly stronger coverage than what it
// replaces, on both dimensions that matter.
func TestDeliveryTagsGloballyUniquePerQueueMonotonic(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("queue-a", false, false, false, nil)
	broker.DeclareQueue("queue-b", false, false, false, nil)

	seen := make(map[uint64]string, 100)
	prevPerQueue := make(map[string]uint64, 2)

	// Publish to both queues alternately
	for i := 0; i < 100; i++ {
		qn := "queue-a"
		if i%2 == 1 {
			qn = "queue-b"
		}
		msg := &protocol.Message{Body: []byte("x"), Exchange: "", RoutingKey: qn}
		if err := broker.PublishMessage("", qn, msg); err != nil {
			t.Fatalf("Publish %d to %s failed: %v", i, qn, err)
		}

		// (1) Global uniqueness across queues.
		if owner, dup := seen[msg.DeliveryTag]; dup {
			t.Fatalf("delivery tag %d reused at iteration %d: already issued to %s, now to %s — "+
				"tags key the shared WAL and the broker delivery index, so a collision is "+
				"cross-queue data loss", msg.DeliveryTag, i, owner, qn)
		}
		seen[msg.DeliveryTag] = qn

		// (2) Strictly increasing WITHIN each queue (what Claim's ascending walk needs).
		if prev, ok := prevPerQueue[qn]; ok && msg.DeliveryTag <= prev {
			t.Errorf("delivery tag %d not increasing within %s (prev=%d) at iteration %d",
				msg.DeliveryTag, qn, prev, i)
		}
		prevPerQueue[qn] = msg.DeliveryTag
	}

	if len(seen) != 100 {
		t.Errorf("expected 100 distinct delivery tags, got %d", len(seen))
	}
}

// TestStopChIdempotentClose verifies that close(stopCh) via sync.Once
// doesn't panic when called twice (C7 fix — stopOnce prevents double-close).
func TestStopChIdempotentClose(t *testing.T) {
	broker, cleanup := createTestBroker(t)
	defer cleanup()

	broker.DeclareQueue("test-queue", false, false, false, nil)
	consumer := &protocol.Consumer{
		Tag:      "test-consumer",
		Queue:    "test-queue",
		Messages: make(chan *protocol.Delivery, 1),
	}
	if err := broker.RegisterConsumer("test-queue", "test-consumer", consumer); err != nil {
		t.Fatalf("RegisterConsumer failed: %v", err)
	}

	// Get the consumer state
	val, ok := broker.activeConsumers.Load("test-consumer")
	if !ok {
		t.Fatal("consumer not found")
	}
	state := val.(*ConsumerState)

	// Close stopCh twice — should not panic
	state.stopOnce.Do(func() { close(state.stopCh) })
	state.stopOnce.Do(func() { close(state.stopCh) }) // would panic without sync.Once

	// Cleanup
	broker.UnregisterConsumer("test-consumer")
}
