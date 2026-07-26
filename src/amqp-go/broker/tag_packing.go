package broker

// Composite packed delivery tag.
//
// THE PROBLEM: internal delivery tags used to be minted from a single
// broker-global counter. With 2+ queues publishing concurrently, tags
// interleave across queues, so each queue's dispatch plane (which assumes its
// own tag space is DENSE — Claim walks tail one integer at a time) sees a
// permanent hole for every tag minted by another queue. Each hole is only
// discovered after a full cold-read miss, so consumers fall permanently
// behind and delivery collapses.
//
// THE FIX: mint tags as `(queueOrdinal << OrdinalShift) | perQueueSeq`. Each
// queue owns a private, contiguous, zero-based sequence (perQueueSeq), so
// Claim's walk only ever visits tags this queue itself minted — dense again.
// Distinct queues occupy distinct high bits, so tags stay globally unique
// across the whole broker: this is MANDATORY, not an optimization. The
// broker's single shared WAL (offsetIndex, ackBitmap, currentFileOffsets) and
// the broker's deliveryIndex sync.Map are ALL keyed by a bare uint64 with NO
// queue discriminator — if two queues ever produced the same tag, that would
// be silent cross-queue data loss.
//
// WHY 20/44: a queue ordinal is allocated once per queue LIFETIME (declare to
// delete), not per message, so it can be a small field. 20 bits gives
// 2^20 - 1 = 1,048,575 queue lifetimes — far beyond what any deployment
// creates/destroys over its life. The remaining 44 bits give each queue a
// private sequence of 2^44 - 1 ≈ 17.6e12 messages; at a sustained 110,000
// msg/s (well above this broker's current throughput target) that is
// enough for ≈ 5,000 years before a single queue could exhaust its space.
const (
	// OrdinalShift is the bit position where the queue ordinal begins. The
	// low OrdinalShift bits are the queue-private, zero-based sequence.
	OrdinalShift = 44

	// SeqMask isolates the per-queue sequence bits of a packed tag.
	SeqMask = (uint64(1) << OrdinalShift) - 1

	// MaxQueueOrdinal is the largest queue ordinal that fits above SeqMask
	// (20 bits: 0..1,048,575). Ordinal 0 is reserved to mean "unassigned" at
	// the protocol.Queue persistence layer, so allocation starts at 1.
	MaxQueueOrdinal = (uint64(1) << 20) - 1
)

// PackTag combines a queue ordinal and a per-queue sequence number into a
// single composite delivery tag. Callers must ensure seq <= SeqMask and
// ordinal <= MaxQueueOrdinal — this function does not validate (the hot path
// callers already guarantee it structurally; see QueueState.SetOrdinal and
// QueueState.FrontierReserve).
func PackTag(ordinal, seq uint64) uint64 {
	return (ordinal << OrdinalShift) | seq
}

// TagOrdinal extracts the queue ordinal from a composite delivery tag.
func TagOrdinal(tag uint64) uint64 {
	return tag >> OrdinalShift
}

// TagSeq extracts the per-queue sequence number from a composite delivery tag.
func TagSeq(tag uint64) uint64 {
	return tag & SeqMask
}
