package storage

// Hot-path field layout of QueueSegments and SegmentFile, locked at runtime.
//
// WHY THIS EXISTS. SegmentManager.Acknowledge touches QueueSegments per ack
// (ackChan, and on the fallback path ackBitmap + bitmapMutex), and GetMessage's
// last tier reads SegmentFile (file, index, mutex). step4.md recorded those
// offsets in a one-off unsafe.Offsetof run and wrote the numbers into a report —
// which is a measurement nobody re-runs. The benchmark gate on this machine
// produced 7 false regressions on an unchanged tree, so layout claims about
// these two structs are settled by offsets rather than by timings; that
// settlement belongs in the suite, not in a note (canon rule 11).
//
// WHAT A FAILURE MEANS. Not "you broke the broker". It means a field was added,
// removed or resized ABOVE one of the hot groups, so the layout no longer matches
// the table below. For QueueSegments that table IS the layout the zero-regression
// baseline was taken on. For SegmentFile it is not — see the provenance note on
// segmentHotFieldOffsets before you read a failure there as a regression. Either
// move the new field below the group, or re-derive the table the way that note
// documents and replace it.

import (
	"testing"
	"unsafe"
)

// segmentHotFieldOffsets is the layout at d5c3f85. It is one table with two
// different provenances, and conflating them is how the previous version of this
// comment came to assert a verification that had never been run: it claimed all
// 18 entries were "verified equal to HEAD 3d20384", which is false for 8 of them.
// Both halves below were re-measured with unsafe.Offsetof compiled against each
// commit's own source, not copied out of a report.
//
// QueueSegments (9 entries) — a true BEFORE == AFTER pin. Every offset is
// byte-identical at 3d20384 and at d5c3f85. The struct itself did change
// (lastCompaction removed; loadFaults, loadFaultsMutex, closed and logger added;
// sizeof 248 -> 280), but every edit lands BELOW ackBatchSize, so the per-ack
// group is untouched. Caveat for the "every field above keeps the byte offset it
// had at HEAD 3d20384" note at the tail of QueueSegments in segment_manager.go:
// that holds only as far down as ackBatchSize. compactionMux (200 -> 176),
// metrics (208 -> 224), stopChan (224 -> 240) and wg (232 -> 248) all moved. None
// of them is on the ack path, so nothing is wrong — the note just over-claims.
//
// SegmentFile (9 entries) — NOT a before/after pin. d5c3f85 deleted the
// indexPath string field, which sat above every field the cold-read tier touches,
// so eight of the nine entries record a DELIBERATE 16-byte shift:
//
//	field          3d20384  d5c3f85
//	segmentNum           0        0
//	path                 8        8
//	file                40       24
//	minOffset           48       32
//	maxOffset           56       40
//	messageCount        64       48
//	deletedCount        72       56
//	fileSize            80       64
//	index               88       72
//	mutex               96       80
//	sizeof             120      112
//
// Pinning that shift proves the layout is stable from d5c3f85 forward. It cannot
// prove the shift was free, so here is why it is — reasoned to a verdict of
// NEUTRAL, and measured on darwin/arm64 go1.26:
//
//   - The sealed-segment cold read touches minOffset+maxOffset per candidate
//     segment, then file, index and the RWMutex's readerCount word for the hit.
//     At 3d20384 that set spanned exactly 2 cache lines, always: 120 bytes lands
//     in the 128-byte size class, whose objects are 128-byte aligned, so the
//     object base is always 64-byte aligned. 112 is its own size class with a
//     16-byte minimum alignment (73 objects per 8 KiB span), so the base is
//     equally likely to be 0, 16, 32 or 48 mod 64 — 4096 allocations came out
//     1052/996/994/1054. Three of those four give the same 2-line span; the
//     mod-32 case gives 3. Expected footprint 2.0 -> 2.25 lines. No co-accessed
//     pair was separated in the common case, and no atomic word straddles a line
//     in any case.
//   - That 0.25 of a line-fill is compared against a read that performs two
//     pread(2) calls, two heap allocations and a CRC32 pass. It is three orders
//     of magnitude below the noise floor of the syscall pair.
//   - The 16-byte saving is per SEGMENT, not per read: a SegmentFile is
//     allocated once per segment file, and DefaultSegmentSize is 1 GiB. It is
//     16 bytes per gigabyte of cold data.
//
// So: not a win worth citing, not a regression worth reverting.
var segmentHotFieldOffsets = []struct {
	name string
	want uintptr
}{
	// QueueSegments — the per-ack struct. BEFORE == AFTER, 9/9.
	{"qs.currentSegment", 72},
	{"qs.currentIndex", 80},
	{"qs.mutex", 88},
	{"qs.sealedSegments", 96},
	{"qs.sealedMutex", 104},
	{"qs.ackBitmap", 128},
	{"qs.bitmapMutex", 136},
	{"qs.ackChan", 160},
	{"qs.ackBatchSize", 168},
	// SegmentFile — the cold-read tier. d5c3f85's CHANGED layout; 8 of these 9
	// sit 16 bytes lower than they did at 3d20384 (indexPath deleted).
	{"sf.path", 8},
	{"sf.file", 24},
	{"sf.minOffset", 32},
	{"sf.maxOffset", 40},
	{"sf.messageCount", 48},
	{"sf.deletedCount", 56},
	{"sf.fileSize", 64},
	{"sf.index", 72},
	{"sf.mutex", 80},
}

func TestSegmentHotFieldLayoutIsPinned(t *testing.T) {
	var qs QueueSegments
	var sf SegmentFile

	if unsafe.Sizeof(uintptr(0)) != 8 || unsafe.Sizeof(qs.mutex) != 8 {
		t.Skipf("layout table is stated for 64-bit with an 8-byte sync.Mutex; this build has "+
			"pointer=%d mutex=%d", unsafe.Sizeof(uintptr(0)), unsafe.Sizeof(qs.mutex))
	}

	got := map[string]uintptr{
		"qs.currentSegment": unsafe.Offsetof(qs.currentSegment),
		"qs.currentIndex":   unsafe.Offsetof(qs.currentIndex),
		"qs.mutex":          unsafe.Offsetof(qs.mutex),
		"qs.sealedSegments": unsafe.Offsetof(qs.sealedSegments),
		"qs.sealedMutex":    unsafe.Offsetof(qs.sealedMutex),
		"qs.ackBitmap":      unsafe.Offsetof(qs.ackBitmap),
		"qs.bitmapMutex":    unsafe.Offsetof(qs.bitmapMutex),
		"qs.ackChan":        unsafe.Offsetof(qs.ackChan),
		"qs.ackBatchSize":   unsafe.Offsetof(qs.ackBatchSize),
		"sf.path":           unsafe.Offsetof(sf.path),
		"sf.file":           unsafe.Offsetof(sf.file),
		"sf.minOffset":      unsafe.Offsetof(sf.minOffset),
		"sf.maxOffset":      unsafe.Offsetof(sf.maxOffset),
		"sf.messageCount":   unsafe.Offsetof(sf.messageCount),
		"sf.deletedCount":   unsafe.Offsetof(sf.deletedCount),
		"sf.fileSize":       unsafe.Offsetof(sf.fileSize),
		"sf.index":          unsafe.Offsetof(sf.index),
		"sf.mutex":          unsafe.Offsetof(sf.mutex),
	}

	for _, f := range segmentHotFieldOffsets {
		if got[f.name] != f.want {
			t.Errorf("%s moved: offset %d, baseline %d. A field above the hot group changed size or "+
				"position; new fields must go BELOW it (see the comment at the end of each struct).",
				f.name, got[f.name], f.want)
		}
	}

	// Sizes are reported, not asserted: both structs document "new fields go
	// below the hot group", and appending there legitimately changes sizeof. The
	// 3d20384 figures are here so a future reader can see which direction a size
	// moved without re-archiving the commit.
	t.Logf("QueueSegments sizeof=%d (248 at 3d20384)  SegmentFile sizeof=%d (120 at 3d20384)",
		unsafe.Sizeof(qs), unsafe.Sizeof(sf))
}
