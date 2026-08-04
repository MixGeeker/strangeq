package broker

import (
	"testing"
)

// TestDeleteQueueRefusesSupersededIncarnation is T6 from spec-teardown (B-6).
// A caller holding an ordinal captured from BEFORE a delete+redeclare race
// must be refused when it finally reaches deleteQueueIncarnation, not applied
// to whatever successor now answers to the name.
//
// Residency alone cannot observe this defect class: storage.GetMessage falls
// back readAhead -> WAL -> segments, so a durable message stays retrievable
// even after the queue that owned it is destroyed. The load-bearing
// observables are the successor's record existence, its dispatch cursors and
// its WaitingCount -- what B-6 actually moves.
func TestDeleteQueueRefusesSupersededIncarnation(t *testing.T) {
	b, cleanup := createTestBroker(t)
	defer cleanup()

	if _, err := b.DeclareQueue("q", true, false, false, nil); err != nil {
		t.Fatalf("DeclareQueue(q): %v", err)
	}
	qs1 := b.getOrCreateQueueState("q")
	ord1 := qs1.Ordinal()

	if _, err := b.DeleteQueue("q", false, false); err != nil {
		t.Fatalf("DeleteQueue(q): %v", err)
	}
	if _, err := b.DeclareQueue("q", true, false, false, nil); err != nil {
		t.Fatalf("DeclareQueue(q) redeclare: %v", err)
	}
	qs2 := b.getOrCreateQueueState("q")
	ord2 := qs2.Ordinal()

	// PREMISE of the whole design: ordinals are never reused within a run.
	// Assert it at runtime rather than assuming it (a runtime premise
	// assertion, not a comment).
	if ord2 == ord1 {
		t.Fatalf("redeclare reused ordinal %d from the deleted incarnation; ordinals must never repeat within a broker run", ord1)
	}

	const n = 3
	publishConfirmedDurable(t, b, "q", n)

	preTail := qs2.Tail()
	preHead := qs2.Head()
	preWaiting := qs2.WaitingCount()
	if preWaiting != int64(n) {
		t.Fatalf("successor waiting=%d before the call, want %d; the cursor assertions below would be vacuous", preWaiting, n)
	}
	minSucc, _ := qs2.TagBand()

	// The stale caller: an ordinal captured before the redeclare, exactly the
	// shape a concurrent queue.delete or a racing reaper sweep would carry.
	count, err := b.deleteQueueIncarnation("q", ord1, false, false)
	if err != nil {
		t.Errorf("deleteQueueIncarnation(q, ord1=%d) unexpected error: %v", ord1, err)
	}
	if count != 0 {
		t.Errorf("deleteQueueIncarnation(q, ord1=%d) count = %d, want 0", ord1, count)
	}

	// (1) The successor's record must still exist, with the successor's own
	// ordinal -- not annihilated by a call that named it but meant its
	// predecessor.
	rec, gerr := b.storage.GetQueue("q")
	if gerr != nil {
		t.Fatalf("GetQueue(q) after stale delete = %v, want the successor's record to still exist", gerr)
	}
	if rec.Ordinal != ord2 {
		t.Errorf("GetQueue(q).Ordinal = %d, want successor's ordinal %d", rec.Ordinal, ord2)
	}

	// (2) The successor's dispatch cursors and WaitingCount are unchanged --
	// the observable a residency-only check would miss entirely.
	if got := qs2.Tail(); got != preTail {
		t.Errorf("successor tail changed: %d -> %d; a stale delete stranded the successor's ready set", preTail, got)
	}
	if got := qs2.Head(); got != preHead {
		t.Errorf("successor head changed: %d -> %d, want unchanged", preHead, got)
	}
	if got := qs2.WaitingCount(); got != preWaiting {
		t.Errorf("successor WaitingCount changed: %d -> %d; its confirmed durable messages are no longer claimable", preWaiting, got)
	}

	// (3) Belt-and-suspenders: messages are still resident too (not the
	// load-bearing assertion on its own, but should hold given (1) and (2)).
	for i := uint64(0); i < uint64(n); i++ {
		tag := minSucc + i
		if _, err := b.storage.GetMessage("q", tag); err != nil {
			t.Errorf("GetMessage(q, tag=%d) = %v, want the successor's message still resident", tag, err)
		}
	}
}
