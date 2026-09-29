//go:build !windows

package storage

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
)

// A write failure inside compactSegment, driven by a REAL kernel-enforced
// condition (RLIMIT_FSIZE -> EFBIG on write), not by a production hook. The
// limit is process-wide, so the experiment runs in a re-exec of this same test
// binary; the parent only asserts the child's exit status.
func TestSegmentCompaction_WriteFailureMustNotDestroyRecords(t *testing.T) {
	if os.Getenv("SQ_SEGMENT_WRITE_FAULT_CHILD") == "1" {
		segmentWriteFaultChild(t)
		return
	}
	cmd := exec.Command(os.Args[0],
		"-test.run=^TestSegmentCompaction_WriteFailureMustNotDestroyRecords$",
		"-test.v", "-test.count=1")
	cmd.Env = append(os.Environ(), "SQ_SEGMENT_WRITE_FAULT_CHILD=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("child process (RLIMIT_FSIZE write-fault arm) failed: %v\n%s", err, out)
	}
}

func segmentWriteFaultChild(t *testing.T) {
	dataDir := t.TempDir()
	sm, err := NewSegmentManagerWithConfig(dataDir, segTestConfig())
	require.NoError(t, err)
	defer sm.Close()

	const count = 12
	const queueName = "write-fault-queue"
	qs, seg := buildSealedSegment(t, sm, queueName, count)
	_, unacked := ackMost(t, qs, seg, count)

	before := readFileBytes(t, seg.path)

	// Cap any file at 32 bytes. Every segment record is larger than that, so the
	// FIRST write into the compaction temp file returns EFBIG from the kernel.
	var saved syscall.Rlimit
	require.NoError(t, syscall.Getrlimit(syscall.RLIMIT_FSIZE, &saved))
	limited := syscall.Rlimit{Cur: 32, Max: saved.Max}
	require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_FSIZE, &limited))

	// PREMISE: the limit must really be in force, or this fixture is vacuous.
	probe := filepath.Join(dataDir, "rlimit-probe")
	perr := os.WriteFile(probe, make([]byte, 4096), 0o644)
	_ = os.Remove(probe)

	cerr := qs.compactSegment(seg)

	require.NoError(t, syscall.Setrlimit(syscall.RLIMIT_FSIZE, &saved))
	require.Error(t, perr, "PREMISE: RLIMIT_FSIZE did not take effect; the write-fault arm would be vacuous")
	assert.Error(t, cerr,
		"compactSegment must FAIL when a record cannot be written: it is about to rename a truncated file over the only copy of the data")

	after := readFileBytes(t, seg.path)
	assert.Equal(t, before, after,
		"the segment file must be byte-identical after a failed compaction")
	for _, o := range unacked {
		assert.Contains(t, string(after), string(segBodyMarker(o)),
			"DESTROYED: unacked record %d is gone after a write failure during compaction", o)
		m, err := qs.readMessage(o)
		if !assert.NoError(t, err, "record %d must survive an aborted compaction", o) {
			continue
		}
		assert.Equal(t, o, m.DeliveryTag)
	}

	orphans, err := filepath.Glob(filepath.Join(qs.dataDir, "*"+segmentCompactSuffix))
	require.NoError(t, err)
	require.Empty(t, orphans, "an aborted compaction must not leave its temp file behind")
}
