package server

import (
	"path/filepath"
	"testing"
)

func TestWindowsResourceSamplers(t *testing.T) {
	root := t.TempDir()
	if bytes, ok := readDiskFreeBytes(root); !ok || bytes == 0 {
		t.Fatalf("disk capacity unavailable: bytes=%d ok=%v", bytes, ok)
	}
	if _, ok := readDiskFreeBytes(filepath.Join(root, "absent")); ok {
		t.Fatal("missing volume path must not report a successful sample")
	}
	if _, ok := readDiskFreeBytes("invalid\x00path"); ok {
		t.Fatal("invalid path must not report a successful sample")
	}
	if bytes, ok := readProcessRSSBytes(); !ok || bytes == 0 {
		t.Fatalf("process working set unavailable: bytes=%d ok=%v", bytes, ok)
	}
	if bytes, ok := readTotalMemoryBytes(); !ok || bytes == 0 {
		t.Fatalf("physical memory unavailable: bytes=%d ok=%v", bytes, ok)
	}
}
