package storage

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newReadAheadTestStorage(t *testing.T, tmpDir string, ringSize int) *DisruptorStorage {
	t.Helper()
	ec := interfaces.EngineConfig{
		RingBufferSize:        ringSize,
		SpillThresholdPercent: 80,
		WALSyncDisabled:       true,
	}
	storage, err := NewDisruptorStorageWithEngineConfig(tmpDir, ec)
	require.NoError(t, err)
	return storage
}

func publishDurableMessages(t *testing.T, storage *DisruptorStorage, queueName string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		msg := &protocol.Message{
			Body:         []byte(fmt.Sprintf("msg-%d", i)),
			DeliveryMode: 2,
			DeliveryTag:  uint64(i),
		}
		require.NoError(t, storage.StoreMessage(queueName, msg))
	}
}

func clearRingForWALFallback(storage *DisruptorStorage, queueName string, n int) {
	ring := storage.getQueueRing(queueName)
	for i := 0; i < n; i++ {
		ring.ring.Delete(uint64(i))
	}
}

func TestReadAhead_PopulatesOnFirstMiss(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newReadAheadTestStorage(t, tmpDir, 64)
	defer storage.Close()

	require.NoError(t, storage.StoreQueue(&protocol.Queue{Name: "q", Durable: true}))
	publishDurableMessages(t, storage, "q", 200)
	clearRingForWALFallback(storage, "q", 200)

	ring := storage.getQueueRing("q")

	msg, err := storage.GetMessage("q", 0)
	require.NoError(t, err)
	require.NotNil(t, msg)
	assert.Equal(t, []byte("msg-0"), msg.Body)

	assert.Equal(t, 1, ring.readAhead.batchReadCount())
	assert.Equal(t, readAheadMaxEntries, ring.readAhead.len())

	for i := 1; i < readAheadMaxEntries; i++ {
		assert.True(t, ring.readAhead.has(uint64(i)),
			"tag %d should be in read-ahead buffer", i)
	}

	for i := 1; i < readAheadMaxEntries; i++ {
		msg, err := storage.GetMessage("q", uint64(i))
		require.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("msg-%d", i)), msg.Body)
	}

	assert.Equal(t, 1, ring.readAhead.batchReadCount(),
		"subsequent reads should be served from buffer, no new batch reads")
}

func TestReadAhead_RingHitSkipsReadAhead(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newReadAheadTestStorage(t, tmpDir, 1024)
	defer storage.Close()

	require.NoError(t, storage.StoreQueue(&protocol.Queue{Name: "q", Durable: true}))
	publishDurableMessages(t, storage, "q", 200)

	ring := storage.getQueueRing("q")

	for i := 0; i < 200; i++ {
		msg, err := storage.GetMessage("q", uint64(i))
		require.NoError(t, err)
		assert.Equal(t, []byte(fmt.Sprintf("msg-%d", i)), msg.Body)
	}

	assert.Equal(t, 0, ring.readAhead.batchReadCount(),
		"read-ahead should never fire when all messages are ring-resident")
	assert.Equal(t, 0, ring.readAhead.len())
}

func TestReadAhead_CorrectMessageContent(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newReadAheadTestStorage(t, tmpDir, 64)
	defer storage.Close()

	require.NoError(t, storage.StoreQueue(&protocol.Queue{Name: "q", Durable: true}))

	for i := 0; i < 200; i++ {
		msg := &protocol.Message{
			Body:         []byte(fmt.Sprintf("unique-body-%d", i)),
			DeliveryMode: 2,
			DeliveryTag:  uint64(i),
		}
		require.NoError(t, storage.StoreMessage("q", msg))
	}
	clearRingForWALFallback(storage, "q", 200)

	for i := 0; i < 200; i++ {
		msg, err := storage.GetMessage("q", uint64(i))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("unique-body-%d", i), string(msg.Body),
			"body mismatch for tag %d", i)
	}
}

func TestReadAhead_EvictionOnNonSequential(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newReadAheadTestStorage(t, tmpDir, 64)
	defer storage.Close()

	require.NoError(t, storage.StoreQueue(&protocol.Queue{Name: "q", Durable: true}))
	publishDurableMessages(t, storage, "q", 200)
	clearRingForWALFallback(storage, "q", 200)

	ring := storage.getQueueRing("q")

	_, err := storage.GetMessage("q", 0)
	require.NoError(t, err)
	assert.Equal(t, 1, ring.readAhead.batchReadCount())
	assert.True(t, ring.readAhead.has(0))

	msg, err := storage.GetMessage("q", 150)
	require.NoError(t, err)
	assert.Equal(t, []byte("msg-150"), msg.Body)
	assert.Equal(t, 2, ring.readAhead.batchReadCount(),
		"non-sequential access should trigger a new batch read")

	assert.False(t, ring.readAhead.has(0),
		"old buffer should be evicted by the new batch")
	assert.True(t, ring.readAhead.has(150),
		"new buffer should contain the requested tag")
}

func TestReadAhead_SequentialConsumeAmortisesBatchReads(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newReadAheadTestStorage(t, tmpDir, 64)
	defer storage.Close()

	require.NoError(t, storage.StoreQueue(&protocol.Queue{Name: "q", Durable: true}))

	const numMsgs = 2000
	maxRecordBytes := 0
	for i := 0; i < numMsgs; i++ {
		msg := &protocol.Message{
			Body:         []byte(fmt.Sprintf("msg-%d", i)),
			DeliveryMode: 2,
			DeliveryTag:  uint64(i),
		}
		record, err := appendMessageRecord(nil, "q", msg, uint64(i), false)
		require.NoError(t, err)
		maxRecordBytes = max(maxRecordBytes, len(record))
		require.NoError(t, storage.StoreMessage("q", msg))
	}
	ring := storage.getQueueRing("q")
	require.NotNil(t, ring.readAhead,
		"fixture premise broken: the queue has no read-ahead buffer")
	clearRingForWALFallback(storage, "q", numMsgs)
	require.Zero(t, ring.ring.Count(),
		"fixture premise broken: clearing the ring left %d resident messages, so GetMessage can bypass read-ahead",
		ring.ring.Count())

	maxEntryBatchBytes := readAheadMaxEntries * maxRecordBytes
	require.Less(t, maxEntryBatchBytes, readAheadChunkSize,
		"fixture premise broken: %d entries can occupy %d bytes at the fixture's largest serialized record size (%d), so the %d-byte chunk bound can bind before the entry-count bound",
		readAheadMaxEntries, maxEntryBatchBytes, maxRecordBytes, readAheadChunkSize)

	wal := storage.wal.sharedWAL
	wal.offsetIndexMutex.RLock()
	walFiles := make(map[uint64]struct{})
	missingOffsets := 0
	for i := 0; i < numMsgs; i++ {
		location, ok := wal.offsetIndex[uint64(i)]
		if !ok {
			missingOffsets++
			continue
		}
		walFiles[location.fileNum] = struct{}{}
	}
	wal.offsetIndexMutex.RUnlock()
	require.Zero(t, missingOffsets,
		"fixture premise broken: %d fixture messages are missing from the WAL index",
		missingOffsets)
	require.Len(t, walFiles, 1,
		"fixture premise broken: %d messages span %d WAL files, so ceil(%d/%d) cannot be the batch count",
		numMsgs, len(walFiles), numMsgs, readAheadMaxEntries)

	wantBatches := (numMsgs + readAheadMaxEntries - 1) / readAheadMaxEntries
	require.Less(t, wantBatches, numMsgs,
		"fixture premise broken: readAheadMaxEntries=%d allows no amortisation; the exact count would self-consistently accept one WAL read per message",
		readAheadMaxEntries)

	first, err := storage.GetMessage("q", 0)
	require.NoError(t, err)
	require.NotNil(t, first)
	require.Equal(t, []byte("msg-0"), first.Body)

	firstBatchEntries := ring.readAhead.len()
	require.Equal(t, readAheadMaxEntries, firstBatchEntries,
		"fixture premise broken: the entry-count bound did not bind before the %d-byte chunk bound; the first batch buffered %d entries, want %d",
		readAheadChunkSize, firstBatchEntries, readAheadMaxEntries)

	for i := 1; i < numMsgs; i++ {
		msg, err := storage.GetMessage("q", uint64(i))
		require.NoError(t, err)
		require.NotNil(t, msg)
		require.Equal(t, []byte(fmt.Sprintf("msg-%d", i)), msg.Body)
	}

	gotBatches := ring.readAhead.batchReadCount()
	require.Equal(t, wantBatches, gotBatches,
		"read-ahead did not amortise WAL reads: %d sequential GetMessage calls used %d batches; want ceil(%d/%d)=%d",
		numMsgs, gotBatches, numMsgs, readAheadMaxEntries, wantBatches)
}

// TestReadAhead_ResolvesSharedBodyRefs exercises the ITER5 shared-body path
// through the read-ahead buffer: messages are written as fused BodyBlock +
// reference-record units (one body copy on disk, N refs), then read back
// through GetMessage after the ring is cleared. The read-ahead batch reader
// must resolve each BodyRef via readBodyBlockAt and materialize the body.
//
// Delivery tags are globally unique across queues (the shared WAL's offset
// index keys on tag alone), so each queue gets a non-overlapping tag range.
func TestReadAhead_ResolvesSharedBodyRefs(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newReadAheadTestStorage(t, tmpDir, 64)
	defer storage.Close()

	queues := []string{"qa", "qb", "qc"}
	base := map[string]uint64{"qa": 0, "qb": 1000, "qc": 2000}
	for _, q := range queues {
		require.NoError(t, storage.StoreQueue(&protocol.Queue{Name: q, Durable: true}))
	}

	body := sharedTestBody(4096)
	const numUnits = 10
	for b := 0; b < numUnits; b++ {
		var wg sync.WaitGroup
		wg.Add(len(queues))
		batchErrs := make([]error, len(queues))
		subs := make([]interfaces.SharedDurableSub, len(queues))
		for i, q := range queues {
			i := i
			msg := &protocol.Message{
				Body:         body,
				DeliveryMode: 2,
				DeliveryTag:  base[q] + uint64(b),
			}
			subs[i] = interfaces.SharedDurableSub{
				QueueName: q,
				Message:   msg,
				OnDurable: func(err error) {
					batchErrs[i] = err
					wg.Done()
				},
			}
		}
		require.NoError(t, storage.StoreSharedAsync(subs))
		wg.Wait()
		for _, e := range batchErrs {
			require.NoError(t, e)
		}
	}

	for _, q := range queues {
		ring := storage.getQueueRing(q)
		for b := 0; b < numUnits; b++ {
			ring.ring.Delete(base[q] + uint64(b))
		}
	}

	ring := storage.getQueueRing("qa")
	startBatchReads := ring.readAhead.batchReadCount()

	msg, err := storage.GetMessage("qa", base["qa"])
	require.NoError(t, err)
	assert.Equal(t, body, msg.Body, "shared body must be resolved from BodyBlock")
	assert.Nil(t, msg.BodyRef, "BodyRef must be cleared after resolution")

	assert.Equal(t, startBatchReads+1, ring.readAhead.batchReadCount(),
		"first miss should trigger a read-ahead batch read")

	for b := 1; b < numUnits; b++ {
		msg, err := storage.GetMessage("qa", base["qa"]+uint64(b))
		require.NoError(t, err, "tag %d", b)
		assert.Equal(t, body, msg.Body, "tag %d body must resolve from shared BodyBlock", b)
		assert.Nil(t, msg.BodyRef, "tag %d BodyRef must be cleared", b)
	}
}

// corruptFirstBodyBlock flips a byte inside the first WAL record's data region
// (the ITER5 BodyBlock, which precedes its reference records per the co-location
// invariant). The corruption causes a CRC mismatch in readBodyBlockAt.
func corruptFirstBodyBlock(t *testing.T, dataDir string) {
	t.Helper()
	path := firstWALFile(t, dataDir)
	f, err := os.OpenFile(path, os.O_RDWR, 0644)
	require.NoError(t, err)
	defer f.Close()

	hdr := make([]byte, 8)
	_, err = f.ReadAt(hdr, int64(WALHeaderSize))
	require.NoError(t, err)
	dataLen := binary.BigEndian.Uint32(hdr[4:8])

	data := make([]byte, dataLen)
	_, err = f.ReadAt(data, int64(WALHeaderSize)+8)
	require.NoError(t, err)

	recType, _ := recordTypeAndPayload(data)
	require.Equal(t, WALRecordTypeBodyBlock, recType, "first record must be a BodyBlock")

	corruptOffset := int64(WALHeaderSize) + 8 + 9
	oneByte := make([]byte, 1)
	_, err = f.ReadAt(oneByte, corruptOffset)
	require.NoError(t, err)
	oneByte[0] ^= 0xFF
	_, err = f.WriteAt(oneByte, corruptOffset)
	require.NoError(t, err)
}

// TestReadAhead_CorruptBodyBlockSkipsMessage verifies the C1 fix: when
// readBodyBlockAt fails (here, CRC corruption), the message is SKIPPED in
// readMessageBatch rather than stored with Body==nil. The caller falls through
// to wal.Read, which also errors, so GetMessage returns an error instead of a
// nil-body message (silent data loss).
func TestReadAhead_CorruptBodyBlockSkipsMessage(t *testing.T) {
	tmpDir := t.TempDir()
	storage := newReadAheadTestStorage(t, tmpDir, 64)
	defer storage.Close()

	queues := []string{"qa", "qb"}
	for _, q := range queues {
		require.NoError(t, storage.StoreQueue(&protocol.Queue{Name: q, Durable: true}))
	}

	body := sharedTestBody(4096)
	var wg sync.WaitGroup
	wg.Add(len(queues))
	batchErrs := make([]error, len(queues))
	subs := make([]interfaces.SharedDurableSub, len(queues))
	for i, q := range queues {
		i := i
		msg := &protocol.Message{
			Body:         body,
			DeliveryMode: 2,
			DeliveryTag:  uint64(i),
		}
		subs[i] = interfaces.SharedDurableSub{
			QueueName: q,
			Message:   msg,
			OnDurable: func(err error) {
				batchErrs[i] = err
				wg.Done()
			},
		}
	}
	require.NoError(t, storage.StoreSharedAsync(subs))
	wg.Wait()
	for _, e := range batchErrs {
		require.NoError(t, e)
	}

	corruptFirstBodyBlock(t, tmpDir)

	for i, q := range queues {
		storage.getQueueRing(q).ring.Delete(uint64(i))
	}

	msg, err := storage.GetMessage("qa", 0)
	assert.Error(t, err, "corrupt body block must surface as an error, not a nil-body message")
	assert.Nil(t, msg, "no message should be returned for a corrupt body block")
}
