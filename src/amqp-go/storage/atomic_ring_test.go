package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

func msgAt(tag uint64) *protocol.Message {
	return &protocol.Message{
		DeliveryTag: tag,
		Body:        []byte(fmt.Sprintf("msg-%d", tag)),
	}
}

func scanNonNil(r *AtomicRing) int {
	n := 0
	for i := range r.slots {
		if r.slots[i].Load() != nil {
			n++
		}
	}
	return n
}

func TestAtomicRing_StoreAndLoadByTag(t *testing.T) {
	r := NewAtomicRing(256)
	for tag := uint64(1); tag <= 5; tag++ {
		_, spilled, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
		require.False(t, spilled)
	}
	require.Equal(t, uint64(5), r.Count())
	for tag := uint64(1); tag <= 5; tag++ {
		got, ok := r.LoadByTag(tag)
		require.True(t, ok)
		require.Equal(t, tag, got.DeliveryTag)
		require.Equal(t, []byte(fmt.Sprintf("msg-%d", tag)), got.Body)
	}
}

func TestAtomicRing_StoreSpilledWhenSlotOccupied(t *testing.T) {
	r := NewAtomicRing(4)
	for tag := uint64(0); tag < 4; tag++ {
		_, spilled, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
		require.False(t, spilled)
	}
	require.Equal(t, uint64(4), r.Count())

	seq, spilled, err := r.Store(4, msgAt(4))
	require.NoError(t, err)
	require.True(t, spilled)
	// The returned seq is the SLOT the tag addresses (tag & mask), not a
	// monotonic store counter. Slots are now addressed arithmetically from the
	// tag, so tag 4 in a 4-slot ring maps back onto slot 0 — which is exactly
	// why this store spills. No production caller reads this value (every one
	// of them discards it and reads only `spilled`), so it is asserted here
	// purely to pin the addressing rule the spill behaviour follows from.
	require.Equal(t, uint64(4)&r.mask, seq)
	require.Equal(t, uint64(4), r.Count())

	got, ok := r.LoadByTag(0)
	require.True(t, ok)
	require.Equal(t, uint64(0), got.DeliveryTag)
}

func TestAtomicRing_LoadByTagReturnsFalseAfterDelete(t *testing.T) {
	r := NewAtomicRing(256)
	_, _, err := r.Store(7, msgAt(7))
	require.NoError(t, err)

	got, ok := r.LoadByTag(7)
	require.True(t, ok)
	require.NotNil(t, got)

	require.True(t, r.Delete(7))
	got, ok = r.LoadByTag(7)
	require.False(t, ok)
	require.Nil(t, got)
}

func TestAtomicRing_LoadByTagReturnsFalseAfterWraparound(t *testing.T) {
	r := NewAtomicRing(4)
	_, _, err := r.Store(0, msgAt(0))
	require.NoError(t, err)
	require.True(t, r.Delete(0))

	for tag := uint64(1); tag < 4; tag++ {
		_, _, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
	}
	_, _, err = r.Store(4, msgAt(4))
	require.NoError(t, err)

	got, ok := r.LoadByTag(0)
	require.False(t, ok)
	require.Nil(t, got)

	got, ok = r.LoadByTag(4)
	require.True(t, ok)
	require.Equal(t, uint64(4), got.DeliveryTag)
}

func TestAtomicRing_DeleteIdempotent_Sequential(t *testing.T) {
	r := NewAtomicRing(256)
	_, _, err := r.Store(10, msgAt(10))
	require.NoError(t, err)
	require.Equal(t, uint64(1), r.Count())

	require.True(t, r.Delete(10))
	require.Equal(t, uint64(0), r.Count())

	require.False(t, r.Delete(10))
	require.Equal(t, uint64(0), r.Count())
}

func TestAtomicRing_DeleteIdempotent_Concurrent(t *testing.T) {
	r := NewAtomicRing(256)
	for tag := uint64(0); tag < 100; tag++ {
		_, _, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
	}
	require.Equal(t, uint64(100), r.Count())

	var wg sync.WaitGroup
	var successes atomic.Int64
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if r.Delete(42) {
				successes.Add(1)
			}
		}()
	}
	wg.Wait()

	require.Equal(t, int64(1), successes.Load())
	require.Equal(t, uint64(99), r.Count())
}

// TestAtomicRing_DeleteRejectsWrappedOccupant is the successor to the old
// stale-tagToSeq guard. That test poisoned the tag->slot map to prove Delete
// would not trust a stale mapping; slots are now addressed arithmetically as
// tag&mask, so there is no map left to poison and that exact failure mode is
// structurally impossible.
//
// The equivalent hazard in the arithmetic design is WRAPAROUND: two tags one
// full lap apart share a slot, so a slot can hold an occupant that is not the
// tag being asked about. Delete must refuse in that case — deleting the current
// occupant on behalf of a stale tag from a previous lap would discard a live,
// unacked message and silently corrupt the queue's depth accounting, since
// Delete's CompareAndSwap result is the arbitration point
// DeleteMessageIfPresent relies on for exactly-once accounting.
//
// This is the same invariant the old test protected — Delete never acts on a
// tag it cannot positively identify in the slot — expressed against the
// mechanism that replaced the map.
func TestAtomicRing_DeleteRejectsWrappedOccupant(t *testing.T) {
	const ringSize = 4
	r := NewAtomicRing(ringSize)

	// tag 1 and tag 1+ringSize are exactly one lap apart, so they share a slot.
	// The ring never overwrites an occupied slot, so the lap is simulated the
	// only way it can actually occur: the first occupant is removed, then the
	// next lap's tag takes the slot.
	_, spilled, err := r.Store(1, msgAt(1))
	require.NoError(t, err)
	require.False(t, spilled)
	require.True(t, r.Delete(1))

	const wrapped = uint64(1 + ringSize)
	_, spilled, err = r.Store(wrapped, msgAt(wrapped))
	require.NoError(t, err)
	require.False(t, spilled)

	// The stale tag from the previous lap must not be able to evict the current
	// occupant, even though both address the same slot.
	require.False(t, r.Delete(1),
		"Delete(1) must refuse: slot %d now holds tag %d from the next lap, not tag 1",
		wrapped&r.mask, wrapped)

	got, ok := r.LoadByTag(wrapped)
	require.True(t, ok, "the wrapped occupant must still be resident after the refused delete")
	require.Equal(t, wrapped, got.DeliveryTag)
	require.Equal(t, uint64(1), r.Count())

	// And the stale tag must read as a miss rather than returning its lap-mate.
	_, ok = r.LoadByTag(1)
	require.False(t, ok, "tag 1 must read as a miss: its slot belongs to tag %d now", wrapped)
}

func TestAtomicRing_MessageCountAccurate(t *testing.T) {
	r := NewAtomicRing(256)
	const N = 50
	for tag := uint64(1); tag <= N; tag++ {
		_, _, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
	}
	require.Equal(t, uint64(N), r.Count())

	const M = 20
	for tag := uint64(1); tag <= M; tag++ {
		require.True(t, r.Delete(tag))
	}
	require.Equal(t, uint64(N-M), r.Count())
}

func TestAtomicRing_PurgeClearsAll(t *testing.T) {
	r := NewAtomicRing(256)
	for tag := uint64(1); tag <= 10; tag++ {
		_, _, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
	}
	require.Equal(t, uint64(10), r.Count())

	removed := r.Purge()
	require.Equal(t, 10, removed)
	require.Equal(t, uint64(0), r.Count())
	for i := range r.slots {
		require.Nil(t, r.slots[i].Load())
	}
}

func TestAtomicRing_PowerOfTwoValidation(t *testing.T) {
	require.Panics(t, func() { NewAtomicRing(3) })
}

func TestAtomicRing_ConcurrentStoreDifferentTags(t *testing.T) {
	r := NewAtomicRing(1 << 16)
	const G = 100
	var wg sync.WaitGroup
	for g := 1; g <= G; g++ {
		wg.Add(1)
		go func(tag uint64) {
			defer wg.Done()
			_, _, _ = r.Store(tag, msgAt(tag))
		}(uint64(g))
	}
	wg.Wait()

	require.Equal(t, uint64(G), r.Count())
	for g := 1; g <= G; g++ {
		got, ok := r.LoadByTag(uint64(g))
		require.True(t, ok, "tag %d should be present", g)
		require.Equal(t, uint64(g), got.DeliveryTag)
	}
}

func TestAtomicRing_ConcurrentStoreDeleteMixed(t *testing.T) {
	r := NewAtomicRing(256)
	for tag := uint64(0); tag < 50; tag++ {
		_, _, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
	}
	require.Equal(t, uint64(50), r.Count())

	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(2)
		go func(tag uint64) {
			defer wg.Done()
			_, _, _ = r.Store(50+tag, msgAt(50+tag))
		}(uint64(g))
		go func(tag uint64) {
			defer wg.Done()
			r.Delete(tag)
		}(uint64(g))
	}
	wg.Wait()

	scanned := scanNonNil(r)
	require.Equal(t, uint64(scanned), r.Count())
}

func TestAtomicRing_GetAll(t *testing.T) {
	r := NewAtomicRing(256)
	for tag := uint64(1); tag <= 5; tag++ {
		_, _, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
	}
	all := r.GetAll()
	require.Len(t, all, 5)

	tags := make(map[uint64]bool)
	for _, m := range all {
		tags[m.DeliveryTag] = true
	}
	for tag := uint64(1); tag <= 5; tag++ {
		require.True(t, tags[tag])
	}
}

func TestAtomicRing_GetRange(t *testing.T) {
	r := NewAtomicRing(256)
	for tag := uint64(1); tag <= 10; tag++ {
		_, _, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
	}
	got := r.GetRange(3, 7)
	require.Len(t, got, 5)
	for _, m := range got {
		require.GreaterOrEqual(t, m.DeliveryTag, uint64(3))
		require.LessOrEqual(t, m.DeliveryTag, uint64(7))
	}
}

func TestAtomicRing_DeleteRange(t *testing.T) {
	r := NewAtomicRing(256)
	for tag := uint64(1); tag <= 10; tag++ {
		_, _, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
	}
	require.Equal(t, uint64(10), r.Count())

	removed := r.DeleteRange(3, 7)
	require.Equal(t, 5, removed)
	require.Equal(t, uint64(5), r.Count())

	tags := make(map[uint64]bool)
	for _, m := range r.GetAll() {
		tags[m.DeliveryTag] = true
	}
	for tag := uint64(3); tag <= 7; tag++ {
		require.False(t, tags[tag])
	}
	require.True(t, tags[1])
	require.True(t, tags[10])
}

// TestAtomicRing_StoreReturnsAddressableSlot replaces the old LoadBySeq test.
// The property it protected — a stored message is retrievable at the
// coordinate Store handed back — still holds; what changed is that the
// coordinate is now the tag's slot (tag & mask) rather than a separate
// monotonic counter, so the same message is reachable by its tag directly.
func TestAtomicRing_StoreReturnsAddressableSlot(t *testing.T) {
	r := NewAtomicRing(256)
	seq, _, err := r.Store(42, msgAt(42))
	require.NoError(t, err)
	require.Equal(t, uint64(42)&r.mask, seq, "the returned coordinate is the tag's slot")

	got, ok := r.LoadByTag(42)
	require.True(t, ok)
	require.Equal(t, uint64(42), got.DeliveryTag)

	// A tag that was never stored must miss, including one whose slot is free.
	_, ok = r.LoadByTag(999)
	require.False(t, ok)
}

// TestAtomicRing_LoadRejectsWrappedOccupant is the load-side counterpart to
// TestAtomicRing_DeleteRejectsWrappedOccupant, and the successor to the old
// LoadBySeq-after-wraparound test.
//
// Under arithmetic slot addressing, tags one full lap apart collide in the same
// slot. The DeliveryTag identity check inside LoadByTag is the ONLY thing
// preventing a lookup for a long-gone tag from returning its lap-mate — which
// would deliver the wrong message body to a consumer under the wrong tag. This
// pins that check.
func TestAtomicRing_LoadRejectsWrappedOccupant(t *testing.T) {
	const ringSize = 4
	r := NewAtomicRing(ringSize)
	for tag := uint64(1); tag <= 4; tag++ {
		_, spilled, err := r.Store(tag, msgAt(tag))
		require.NoError(t, err)
		require.False(t, spilled)
	}

	// Free tag 1's slot, then let the next lap's tag take it.
	require.True(t, r.Delete(1))
	const wrapped = uint64(1 + ringSize)
	_, spilled, err := r.Store(wrapped, msgAt(wrapped))
	require.NoError(t, err)
	require.False(t, spilled)

	// The departed tag must miss even though its slot is occupied again...
	_, ok := r.LoadByTag(1)
	require.False(t, ok, "tag 1 is gone; its slot now belongs to tag %d", wrapped)

	// ...and the current occupant must be returned under its OWN tag only.
	got, ok := r.LoadByTag(wrapped)
	require.True(t, ok)
	require.Equal(t, wrapped, got.DeliveryTag)
}

func TestAtomicRing_StoreNilMessageReturnsError(t *testing.T) {
	r := NewAtomicRing(256)
	_, _, err := r.Store(1, nil)
	require.Error(t, err)
	require.Equal(t, uint64(0), r.Count())
}

func TestAtomicRing_StoreAfterCloseReturnsError(t *testing.T) {
	r := NewAtomicRing(256)
	r.Close()
	_, _, err := r.Store(1, msgAt(1))
	require.Error(t, err)
	require.Equal(t, uint64(0), r.Count())
}

func TestAtomicRing_StoreZeroAllocs(t *testing.T) {
	r := NewAtomicRing(1 << 16)
	msg := msgAt(1)
	_, _, _ = r.Store(1, msg)

	allocs := testing.AllocsPerRun(200, func() {
		_, _, _ = r.Store(1, msg)
	})
	require.Equal(t, float64(0), allocs)
}

func TestAtomicRing_DeleteZeroAllocs(t *testing.T) {
	r := NewAtomicRing(1 << 16)
	for i := 0; i < 256; i++ {
		_, _, _ = r.Store(uint64(i), msgAt(uint64(i)))
	}

	var i uint64
	allocs := testing.AllocsPerRun(200, func() {
		r.Delete(i)
		i++
	})
	require.Equal(t, float64(0), allocs)
}

func TestAtomicRing_LoadZeroAllocs(t *testing.T) {
	r := NewAtomicRing(1 << 16)
	_, _, _ = r.Store(1, msgAt(1))

	allocs := testing.AllocsPerRun(200, func() {
		_, _ = r.LoadByTag(1)
	})
	require.Equal(t, float64(0), allocs)
}
