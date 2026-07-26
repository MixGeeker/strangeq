package storage

import (
	"fmt"
	"sync/atomic"

	"github.com/maxpert/amqp-go/protocol"
)

const DefaultAtomicRingSize = 1 << 16

// AtomicRing is a fixed-size, lock-free ring buffer of in-flight messages
// keyed by delivery tag.
//
// Slot addressing: a message with delivery tag `tag` always lives at
// `r.slots[tag & r.mask]` (mask == size-1, size a power of two). There is no
// separate seq counter and no tag->slot map: the slot index is a pure
// function of the tag, so Store/LoadByTag/Delete never need to look anything
// up — they compute the slot directly from the tag they were given.
//
// Wraparound safety: because the tag space is far larger than the ring
// (see broker/tag_packing.go: up to 2^44 per-queue sequence values against a
// ring of 2^16), multiple tags alias to the same slot over the life of a
// queue. The ring tells a live occupant from a stale one purely by comparing
// the stored message's own DeliveryTag against the tag being looked up
// (LoadByTag) or deleted (Delete). If they don't match, the slot holds a
// message from a different lap around the ring (or is empty), and the
// operation is a miss. This identity check is the ONLY thing that makes
// wraparound safe — never remove it.
type AtomicRing struct {
	slots        []atomic.Pointer[protocol.Message]
	size         int
	mask         uint64
	messageCount atomic.Uint64
	closed       atomic.Bool
}

func NewAtomicRing(size int) *AtomicRing {
	if size <= 0 {
		size = DefaultAtomicRingSize
	}
	if size&(size-1) != 0 {
		panic("AtomicRing: size must be a power of two")
	}
	r := &AtomicRing{
		size: size,
		mask: uint64(size - 1),
	}
	r.slots = make([]atomic.Pointer[protocol.Message], size)
	return r
}

// Store places msg at the slot addressed by deliveryTag (slot = tag & mask).
// The returned seq IS the slot index. If the slot is already occupied
// (spilled == true), the ring is full at that index and the caller is
// expected to fall back to durable storage; messageCount is not incremented
// in that case.
func (r *AtomicRing) Store(deliveryTag uint64, msg *protocol.Message) (seq uint64, spilled bool, err error) {
	if msg == nil {
		return 0, false, fmt.Errorf("message is nil")
	}
	if r.closed.Load() {
		return 0, false, fmt.Errorf("ring is closed")
	}
	seq = deliveryTag & r.mask
	// Every caller passes deliveryTag == msg.DeliveryTag (the tag is minted and
	// stamped on the message before it is stored), so this is normally a no-op.
	// Guard it: an UNCONDITIONAL write here races a concurrent reader of the SAME
	// message object during a durable fan-out — the publish loop hands the i==0
	// copy to the async store, then reads `*message` again to build the i>=1 tail
	// copies, while this completion (on the WAL batch-writer goroutine) would
	// re-stamp the tag. The values are identical, so the guarded write never fires
	// in practice and the (benign) data race is removed without changing behavior.
	if msg.DeliveryTag != deliveryTag {
		msg.DeliveryTag = deliveryTag
	}
	if r.slots[seq].CompareAndSwap(nil, msg) {
		r.messageCount.Add(1)
		return seq, false, nil
	}
	return seq, true, nil
}

// LoadByTag returns the message stored under deliveryTag, if it is still
// resident. The DeliveryTag identity check is what makes ring wraparound
// safe: an occupant left over from a previous lap around the ring has a
// different tag and must read as a miss rather than being returned.
func (r *AtomicRing) LoadByTag(deliveryTag uint64) (*protocol.Message, bool) {
	msg := r.slots[deliveryTag&r.mask].Load()
	if msg != nil && msg.DeliveryTag == deliveryTag {
		return msg, true
	}
	return nil, false
}

// Delete removes the message under deliveryTag, if present. The
// CompareAndSwap result is the arbitration point: exactly one concurrent
// caller for the same tag observes true, which is what
// DeleteMessageIfPresent relies on for exactly-once depth accounting.
func (r *AtomicRing) Delete(deliveryTag uint64) bool {
	slot := &r.slots[deliveryTag&r.mask]
	msg := slot.Load()
	if msg == nil || msg.DeliveryTag != deliveryTag {
		return false
	}
	if slot.CompareAndSwap(msg, nil) {
		r.messageCount.Add(^uint64(0))
		return true
	}
	return false
}

func (r *AtomicRing) Purge() int {
	removed := 0
	for i := range r.slots {
		if r.slots[i].Swap(nil) != nil {
			removed++
		}
	}
	for {
		cur := r.messageCount.Load()
		if cur < uint64(removed) {
			if r.messageCount.CompareAndSwap(cur, 0) {
				break
			}
			continue
		}
		if r.messageCount.CompareAndSwap(cur, cur-uint64(removed)) {
			break
		}
	}
	return removed
}

func (r *AtomicRing) Count() uint64 {
	return r.messageCount.Load()
}

func (r *AtomicRing) Size() int {
	return r.size
}

func (r *AtomicRing) GetAll() []*protocol.Message {
	result := make([]*protocol.Message, 0, int(r.messageCount.Load()))
	for i := range r.slots {
		if msg := r.slots[i].Load(); msg != nil {
			result = append(result, msg)
		}
	}
	return result
}

func (r *AtomicRing) GetRange(startTag, endTag uint64) []*protocol.Message {
	if startTag > endTag {
		return nil
	}
	result := make([]*protocol.Message, 0)
	for i := range r.slots {
		msg := r.slots[i].Load()
		if msg != nil && msg.DeliveryTag >= startTag && msg.DeliveryTag <= endTag {
			result = append(result, msg)
		}
	}
	return result
}

func (r *AtomicRing) DeleteRange(startTag, endTag uint64) int {
	if startTag > endTag {
		return 0
	}
	removed := 0
	for i := range r.slots {
		msg := r.slots[i].Load()
		if msg == nil || msg.DeliveryTag < startTag || msg.DeliveryTag > endTag {
			continue
		}
		if r.slots[i].CompareAndSwap(msg, nil) {
			r.messageCount.Add(^uint64(0))
			removed++
		}
	}
	return removed
}

func (r *AtomicRing) Close() {
	r.closed.Store(true)
}
