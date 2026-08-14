package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/maxpert/amqp-go/protocol"
)

const (
	readAheadChunkSize  = 256 * 1024
	readAheadMaxEntries = 64
)

type readAheadBuffer struct {
	mu         sync.Mutex
	messages   map[uint64]*protocol.Message
	batchReads int
}

func newReadAheadBuffer() *readAheadBuffer {
	return &readAheadBuffer{
		messages: make(map[uint64]*protocol.Message),
	}
}

func (b *readAheadBuffer) get(tag uint64) (*protocol.Message, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	msg, ok := b.messages[tag]
	return msg, ok
}

// put takes OWNERSHIP of messages. Once handed over, the caller must not read or
// write that map again except through this type's methods — remove() mutates it
// in place on every acknowledgement, so an unsynchronised access by the caller is
// a data race.
func (b *readAheadBuffer) put(messages map[uint64]*protocol.Message) {
	b.mu.Lock()
	b.messages = messages
	b.batchReads++
	b.mu.Unlock()
}

// remove drops a single tag from the buffer.
//
// This is the cache's ack invalidation and it is load-bearing.
// DisruptorStorage.GetMessage consults this buffer BEFORE the WAL, and a
// readMessageBatch fills it with up to readAheadMaxEntries tags at once — so
// without this an acknowledged message keeps being served from here, bypassing
// the WAL's own ack gate entirely. broker/queue_reaper.go reapTTLSweep probes
// acked tags on every sweep and relies on getting nothing back; a cache hit
// there re-expires and re-dead-letters the same record without bound.
func (b *readAheadBuffer) remove(tag uint64) {
	b.mu.Lock()
	delete(b.messages, tag)
	b.mu.Unlock()
}

func (b *readAheadBuffer) len() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.messages)
}

func (b *readAheadBuffer) has(tag uint64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.messages[tag]
	return ok
}

func (b *readAheadBuffer) batchReadCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.batchReads
}

func (wm *WALManager) ReadBatch(queueName string, startOffset uint64) (map[uint64]*protocol.Message, error) {
	wm.mu.RLock()
	sw := wm.sharedWAL
	wm.mu.RUnlock()
	if sw == nil {
		return nil, fmt.Errorf("shared WAL not initialized")
	}
	return sw.readMessageBatch(queueName, startOffset)
}

func (qw *QueueWAL) readMessageBatch(queueName string, startOffset uint64) (map[uint64]*protocol.Message, error) {
	qw.offsetIndexMutex.RLock()
	location, ok := qw.offsetIndex[startOffset]
	qw.offsetIndexMutex.RUnlock()
	if !ok {
		return nil, fmt.Errorf("offset %d not in WAL index", startOffset)
	}

	var messages map[uint64]*protocol.Message
	collect := func(file *os.File) error {
		var cerr error
		messages, cerr = qw.collectBatchFrom(file, queueName, location.filePosition)
		return cerr
	}

	qw.fileMutex.Lock()
	currentFileNum := qw.fileNum.Load()
	if location.fileNum == currentFileNum {
		file := qw.currentReadFile
		qw.fileMutex.Unlock()
		if file == nil {
			return nil, fmt.Errorf("WAL current read file not open for fileNum %d", location.fileNum)
		}
		if err := collect(file); err != nil {
			return nil, err
		}
	} else {
		qw.fileMutex.Unlock()
		// The old-file handle comes from a cache that no longer holds its lock
		// across this read, so the handle can be closed under us. That surfaces
		// as os.ErrClosed and withOldFileHandle retries it against a fresh one.
		if err := qw.withOldFileHandle(location.fileNum, collect); err != nil {
			return nil, err
		}
	}

	// Drop acknowledged records before they are returned OR cached. The caller
	// (DisruptorStorage.GetMessage) both answers the probe from this map and feeds
	// it straight into the read-ahead buffer, so an acked record surviving here is
	// served immediately AND re-poisons the cache for every later sweep. The
	// offsetIndex entry that admitted us above is deleted asynchronously, so it is
	// still present for a moment after the ack and cannot be relied on to filter.
	qw.bitmapMutex.RLock()
	for offset := range messages {
		if qw.ackBitmap.Contains(offset) {
			delete(messages, offset)
		}
	}
	qw.bitmapMutex.RUnlock()

	if len(messages) == 0 {
		return nil, fmt.Errorf("no messages found in read-ahead for offset %d", startOffset)
	}

	return messages, nil
}

// collectBatchFrom reads one read-ahead chunk from file at filePosition and
// decodes every whole record in it belonging to queueName. It is split out of
// readMessageBatch so the retrying old-file arm and the current-file arm share
// one decoder, and so the retry has a single unit to re-run: everything that
// touches the file handle is inside here, and everything that does not — the
// acknowledgement filter, the empty check — is outside it.
//
// A short read at end of file is not an error: io.EOF simply bounds the chunk.
func (qw *QueueWAL) collectBatchFrom(file *os.File, queueName string, filePosition int64) (map[uint64]*protocol.Message, error) {
	chunk := make([]byte, readAheadChunkSize)
	n, err := file.ReadAt(chunk, filePosition)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	chunk = chunk[:n]

	messages := make(map[uint64]*protocol.Message)
	pos := 0
	for pos < len(chunk) {
		if pos+8 > len(chunk) {
			break
		}
		crc := binary.BigEndian.Uint32(chunk[pos : pos+4])
		dataLen := binary.BigEndian.Uint32(chunk[pos+4 : pos+8])

		recordEnd := pos + 8 + int(dataLen)
		if recordEnd > len(chunk) {
			break
		}

		if crc != crc32.ChecksumIEEE(chunk[pos+4:recordEnd]) {
			break
		}

		data := chunk[pos+8 : recordEnd]
		recType, payload := recordTypeAndPayload(data)
		if recType == WALRecordTypeMessage {
			rm, ok := deserializeMessagePayload(payload)
			if ok && rm.QueueName == queueName {
				if len(rm.Message.BodyRef) == 8 {
					blockOffset := int64(binary.BigEndian.Uint64(rm.Message.BodyRef))
					body, berr := readBodyBlockAt(file, blockOffset)
					if berr != nil {
						// A body block that cannot be resolved because the
						// handle was closed under us is not a corrupt record —
						// it is the retryable case, and it must reach
						// withOldFileHandle rather than being skipped here.
						if errors.Is(berr, os.ErrClosed) {
							return nil, berr
						}
						pos = recordEnd
						continue
					}
					rm.Message.Body = body
					rm.Message.BodyRef = nil
				}
				rm.Message.Redelivered = false
				messages[rm.Offset] = rm.Message
				if len(messages) >= readAheadMaxEntries {
					break
				}
			}
		}

		pos = recordEnd
	}

	return messages, nil
}
