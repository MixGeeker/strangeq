package broker

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWaitForCapacity_ReleasedByAckSignal attributes the wake directly: the
// safety fallback is moved beyond the test's generous never-happened bound, so
// only AckAdvance -> NotifyNewMessage can release the registered producer.
func TestWaitForCapacity_ReleasedByAckSignal(t *testing.T) {
	const unreachableFallback = 10 * time.Minute

	qs := NewQueueState(2)
	defer qs.Close()
	qs.SetCapacityFallback(unreachableFallback)
	require.Equal(t, unreachableFallback, qs.capacityFallback(),
		"fixture premise broken: capacity fallback was not moved out of reach")

	stop, cancel := makeStop()
	defer cancel()

	qs.FrontierComplete(qs.FrontierReserve(), true)
	qs.FrontierComplete(qs.FrontierReserve(), true)
	timer := testTimer(qs)
	defer timer.Stop()
	t0, _, ok := qs.Claim(stop, timer)
	require.True(t, ok, "first capacity-fixture claim failed")
	t1, _, ok := qs.Claim(stop, timer)
	require.True(t, ok, "second capacity-fixture claim failed")
	qs.ClaimInflight(t0)
	qs.ClaimInflight(t1)
	require.Equal(t, uint64(2), qs.Depth(),
		"fixture premise broken: queue depth is not the configured high-water mark")
	require.True(t, qs.AtHighWaterMark(),
		"fixture premise broken: producer would not park below the high-water mark")

	released := make(chan bool, 1)
	go func() { released <- qs.WaitForCapacity(stop) }()
	requireCapacityWaitParked(t, qs, released)

	qs.AckAdvance(t0)
	select {
	case ok := <-released:
		require.True(t, ok, "WaitForCapacity refused instead of accepting capacity released by ack")
	case <-time.After(5 * time.Second):
		t.Fatal("WaitForCapacity never observed the AckAdvance signal; the 10-minute fallback was unreachable")
	}
}

// requireCapacityWaitParked observes the registration NotifyNewMessage relies
// on. Its clock is only a generous outer bound for "registration never
// happened"; it does not distinguish fast from slow execution.
func requireCapacityWaitParked(t *testing.T, qs *QueueState, returned <-chan bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for qs.producerParked.Load() == 0 {
		select {
		case ok := <-returned:
			t.Fatalf("WaitForCapacity returned (%t) without registering a producer park at the high-water mark", ok)
		case <-deadline.C:
			t.Fatal("WaitForCapacity never registered its producer park; an unregistered park cannot be signaled by NotifyNewMessage")
		default:
			runtime.Gosched()
		}
	}
}

func TestWaitForCapacity_CapacityFallbackDefault(t *testing.T) {
	qs := NewQueueState(1)
	defer qs.Close()

	require.Equal(t, 10*time.Millisecond, qs.capacityFallback(),
		"WaitForCapacity production fallback must remain 10ms")
}

func TestNotifyNewMessage_SendsWhenProducerParked(t *testing.T) {
	qs := NewQueueState(1)
	defer qs.Close()

	qs.producerParked.Store(1)
	qs.NotifyNewMessage()

	select {
	case <-qs.wake:
	default:
		t.Fatal("NotifyNewMessage did not send to wake channel when producerParked > 0")
	}
}
