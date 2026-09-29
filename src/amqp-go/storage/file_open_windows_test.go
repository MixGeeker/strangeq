package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

func TestWindowsStorageReplaceRetainsReaders(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "数据", strings.Repeat("long-path-", 18))
	require.NoError(t, os.MkdirAll(dir, 0755))
	oldPath, newPath := filepath.Join(dir, "segment"), filepath.Join(dir, "segment.compact")
	require.NoError(t, os.WriteFile(oldPath, []byte("before"), 0644))
	reader, err := openStorageFile(oldPath, os.O_RDONLY, 0)
	require.NoError(t, err)
	defer reader.Close()
	writer, err := openStorageFile(newPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	require.NoError(t, err)
	_, err = writer.Write([]byte("after"))
	require.NoError(t, err)
	require.NoError(t, writer.Sync())
	require.NoError(t, writer.Close())
	require.NoError(t, replaceStorageFile(newPath, oldPath))
	previous := make([]byte, 6)
	_, err = reader.ReadAt(previous, 0)
	require.NoError(t, err)
	require.Equal(t, "before", string(previous))
	current, err := os.ReadFile(oldPath)
	require.NoError(t, err)
	require.Equal(t, "after", string(current))
}

func TestWindowsStorageNamesSurviveRestartWithoutAliases(t *testing.T) {
	dir := t.TempDir()
	names := []string{"orders", "Orders", "ORDERS", "con", "nul.txt", "lpt1", "aux", "con .txt", "COM¹", "lpt²", "orders.", "orders ",
		"a/b", `a\b`, "x:y", "x*y", "x?y", "x|y", `x"y`, "x<y>", "é", "É", "数据", "a\x01b", "%2f", "/"}
	pm, err := NewPersistentMetadataStore(dir)
	require.NoError(t, err)
	sm, err := NewSegmentManagerWithConfig(dir, segTestConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sm.Close()) })
	for i, name := range names {
		t.Logf("persist queue and binding %q", name)
		require.NoError(t, pm.StoreQueue(&protocol.Queue{Name: name, Durable: true, Ordinal: uint64(i + 1)}), name)
		require.NoError(t, pm.StoreBinding(name, "events", name, nil), name)
		require.NoError(t, sm.CheckpointBatch(name, segTestMessages(name, []uint64{uint64(i + 1)})), name)
	}
	require.NoError(t, sm.Close())
	fresh, err := NewPersistentMetadataStore(dir)
	require.NoError(t, err)
	queues, err := fresh.ListQueues()
	require.NoError(t, err)
	require.Len(t, queues, len(names))
	bindings, err := fresh.ListBindings()
	require.NoError(t, err)
	require.Len(t, bindings, len(names))
	recovered, err := reopenAndRecover(t, dir)
	require.NoError(t, err)
	require.Len(t, recovered, len(names))
	for i, name := range names {
		queue, err := fresh.GetQueue(name)
		require.NoError(t, err, name)
		require.Equal(t, uint64(i+1), queue.Ordinal, name)
		require.Equal(t, []uint64{uint64(i + 1)}, recoveredOffsets(recovered[name]), name)
	}
}
