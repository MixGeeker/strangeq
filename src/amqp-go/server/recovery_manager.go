package server

import (
	"errors"
	"fmt"
	"time"

	"github.com/maxpert/amqp-go/broker"
	"github.com/maxpert/amqp-go/interfaces"
	"github.com/maxpert/amqp-go/protocol"
	"go.uber.org/zap"
)

// ErrLegacyDataDirectory is returned by PerformRecovery when a queue has
// recoverable durable messages but no composite-tag ordinal
// (broker/tag_packing.go) was ever persisted for it — protocol.Queue.Ordinal
// reads 0 as this restart's recovery begins. That means the data directory
// predates per-queue delivery-tag sequencing: its recovered delivery tags are
// raw, unpacked legacy values whose high bits are not a valid queue ordinal
// at all. There is no safe in-place migration. server/builder.go treats this
// as fatal (the server refuses to start) rather than silently discarding the
// directory's data the way most other recovery errors are handled — see the
// comment there.
var ErrLegacyDataDirectory = errors.New("data directory predates per-queue delivery-tag sequencing; drain and recreate the affected queue(s), or the data directory, before restarting on this build")

// ErrOrdinalMismatch is returned by PerformRecovery when a queue's recovered
// delivery tags carry a composite-tag ordinal (broker/tag_packing.go)
// different from the ordinal assigned to that queue. Since two queues must
// never mint the same delivery tag (the shared WAL's offsetIndex/ackBitmap
// and the broker's deliveryIndex are keyed by a bare uint64 with no queue
// discriminator), a mismatch here means recovery is about to hand this
// queue's consumers tags that collide with another queue's tag space —
// corruption, or a legacy directory ErrLegacyDataDirectory's own check
// didn't already catch. Fatal for the same reason as ErrLegacyDataDirectory.
var ErrOrdinalMismatch = errors.New("recovered delivery tag ordinal does not match the queue's assigned ordinal")

// RecoveryManager handles server startup recovery of durable entities
type RecoveryManager struct {
	storage interfaces.Storage
	broker  UnifiedBroker
	logger  *zap.Logger

	// queueOrdinalsAtDeclare captures each durable queue's persisted
	// composite-tag ordinal (broker/tag_packing.go) as read from metadata
	// BEFORE recoverDurableQueues (re)declares it. DeclareQueue ->
	// resolveQueueOrdinal (broker/storage_broker.go) silently allocates and
	// persists a fresh ordinal for any queue whose stored record still has
	// Ordinal==0, so by the time recoverPersistentMessages runs the
	// persisted value is never 0 anymore for a queue that went through that
	// path. This map is recovery's only remaining visibility into the
	// PRE-recovery value, which is exactly what legacy-directory detection
	// needs (see ErrLegacyDataDirectory).
	queueOrdinalsAtDeclare map[string]uint64
}

// NewRecoveryManager creates a new recovery manager
func NewRecoveryManager(storage interfaces.Storage, broker UnifiedBroker, logger *zap.Logger) *RecoveryManager {
	return &RecoveryManager{
		storage:                storage,
		broker:                 broker,
		logger:                 logger,
		queueOrdinalsAtDeclare: make(map[string]uint64),
	}
}

// PerformRecovery performs complete server startup recovery
func (r *RecoveryManager) PerformRecovery() (*protocol.RecoveryStats, error) {
	startTime := time.Now()

	r.logger.Info("Starting server recovery process...")

	// Step 1: Validate storage integrity and repair if needed
	stats, err := r.validateAndRepairStorage()
	if err != nil {
		return stats, fmt.Errorf("storage validation failed: %w", err)
	}

	// Step 2: Recover durable exchanges
	if err := r.recoverDurableExchanges(stats); err != nil {
		return stats, fmt.Errorf("exchange recovery failed: %w", err)
	}

	// Step 3: Recover durable queues
	if err := r.recoverDurableQueues(stats); err != nil {
		return stats, fmt.Errorf("queue recovery failed: %w", err)
	}

	// Step 4: Recover bindings
	if err := r.recoverBindings(stats); err != nil {
		return stats, fmt.Errorf("binding recovery failed: %w", err)
	}

	// Step 5: Recover persistent messages
	if err := r.recoverPersistentMessages(stats); err != nil {
		return stats, fmt.Errorf("message recovery failed: %w", err)
	}

	// Step 6: Recover pending acknowledgments
	if err := r.recoverPendingAcknowledgments(stats); err != nil {
		return stats, fmt.Errorf("acknowledgment recovery failed: %w", err)
	}

	stats.RecoveryDuration = time.Since(startTime)

	// Step 7: Mark recovery as complete
	if err := r.storage.MarkRecoveryComplete(stats); err != nil {
		r.logger.Warn("Failed to mark recovery complete", zap.Error(err))
	}

	r.logger.Info("Server recovery completed",
		zap.Duration("duration", stats.RecoveryDuration),
		zap.Int("exchanges_recovered", stats.DurableExchangesRecovered),
		zap.Int("queues_recovered", stats.DurableQueuesRecovered),
		zap.Int("bindings_recovered", stats.BindingsRecovered),
		zap.Int("messages_recovered", stats.PersistentMessagesRecovered),
		zap.Int("pending_acks_recovered", stats.PendingAcksRecovered),
		zap.Int("corrupted_entries_repaired", stats.CorruptedEntriesRepaired),
		zap.Strings("validation_errors", stats.ValidationErrors),
	)

	return stats, nil
}

// validateAndRepairStorage validates storage integrity and repairs corruption
func (r *RecoveryManager) validateAndRepairStorage() (*protocol.RecoveryStats, error) {
	r.logger.Info("Validating storage integrity...")

	// First validate storage integrity
	stats, err := r.storage.ValidateStorageIntegrity()
	if err != nil {
		return stats, err
	}

	// If validation errors were found, attempt auto-repair
	if len(stats.ValidationErrors) > 0 {
		r.logger.Warn("Storage validation errors found, attempting auto-repair",
			zap.Int("error_count", len(stats.ValidationErrors)))

		repairStats, err := r.storage.RepairCorruption(true) // auto-repair enabled
		if err != nil {
			return stats, err
		}

		// Merge repair stats
		stats.CorruptedEntriesRepaired = repairStats.CorruptedEntriesRepaired
		if len(repairStats.ValidationErrors) > 0 {
			stats.ValidationErrors = append(stats.ValidationErrors, repairStats.ValidationErrors...)
		}
	}

	return stats, nil
}

// recoverDurableExchanges recovers all durable exchanges from storage
func (r *RecoveryManager) recoverDurableExchanges(stats *protocol.RecoveryStats) error {
	r.logger.Info("Recovering durable exchanges...")

	metadata, err := r.storage.GetDurableEntityMetadata()
	if err != nil {
		return err
	}

	for i := range metadata.Exchanges {
		exchange := metadata.Exchanges[i]
		if exchange.Durable {
			err := r.broker.DeclareExchange(
				exchange.Name,
				exchange.Kind,
				exchange.Durable,
				exchange.AutoDelete,
				exchange.Internal,
				exchange.Arguments,
			)
			if err != nil {
				r.logger.Error("Failed to recover durable exchange",
					zap.String("exchange", exchange.Name),
					zap.Error(err))
				stats.ValidationErrors = append(stats.ValidationErrors,
					fmt.Sprintf("Failed to recover exchange %s: %v", exchange.Name, err))
			} else {
				stats.DurableExchangesRecovered++
				r.logger.Debug("Recovered durable exchange",
					zap.String("exchange", exchange.Name),
					zap.String("type", exchange.Kind))
			}
		}
	}

	return nil
}

// recoverDurableQueues recovers all durable queues from storage
func (r *RecoveryManager) recoverDurableQueues(stats *protocol.RecoveryStats) error {
	r.logger.Info("Recovering durable queues...")

	metadata, err := r.storage.GetDurableEntityMetadata()
	if err != nil {
		return err
	}

	for i := range metadata.Queues {
		queue := metadata.Queues[i]
		if queue.Durable {
			// Capture the ordinal EXACTLY as persisted, before DeclareQueue's
			// resolveQueueOrdinal cold path can silently allocate and persist a
			// fresh one for a record that still reads 0 — see
			// queueOrdinalsAtDeclare's field doc.
			r.queueOrdinalsAtDeclare[queue.Name] = queue.Ordinal

			_, err := r.broker.DeclareQueue(
				queue.Name,
				queue.Durable,
				queue.AutoDelete,
				queue.Exclusive,
				queue.Arguments,
			)
			if err != nil {
				r.logger.Error("Failed to recover durable queue",
					zap.String("queue", queue.Name),
					zap.Error(err))
				stats.ValidationErrors = append(stats.ValidationErrors,
					fmt.Sprintf("Failed to recover queue %s: %v", queue.Name, err))
			} else {
				stats.DurableQueuesRecovered++
				r.logger.Debug("Recovered durable queue",
					zap.String("queue", queue.Name))
			}
		}
	}

	return nil
}

// recoverBindings recovers all bindings from storage
func (r *RecoveryManager) recoverBindings(stats *protocol.RecoveryStats) error {
	r.logger.Info("Recovering bindings...")

	metadata, err := r.storage.GetDurableEntityMetadata()
	if err != nil {
		return err
	}

	for _, binding := range metadata.Bindings {
		err := r.broker.BindQueue(
			binding.Queue,
			binding.Exchange,
			binding.RoutingKey,
			binding.Arguments,
		)
		if err != nil {
			r.logger.Error("Failed to recover binding",
				zap.String("queue", binding.Queue),
				zap.String("exchange", binding.Exchange),
				zap.String("routing_key", binding.RoutingKey),
				zap.Error(err))
			stats.ValidationErrors = append(stats.ValidationErrors,
				fmt.Sprintf("Failed to recover binding %s->%s: %v", binding.Exchange, binding.Queue, err))
		} else {
			stats.BindingsRecovered++
			r.logger.Debug("Recovered binding",
				zap.String("queue", binding.Queue),
				zap.String("exchange", binding.Exchange),
				zap.String("routing_key", binding.RoutingKey))
		}
	}

	return nil
}

// recoverPersistentMessages recovers all persistent messages from storage.
//
// The shared WAL is never purged when a queue is deleted (DeleteQueue only
// tears down the in-memory ring), and recoverable records are gathered by
// queue NAME. So records from several past "incarnations" of the same name —
// each minted under a different ordinal — can all surface here, and records
// can survive for a queue that no longer exists at all. The ordinal and the
// existence of a metadata record for queueName MUST be resolved BEFORE any
// message is loaded into the ring, so records belonging to dead incarnations
// or deleted queues are never loaded in the first place — loading them and
// then discarding would still let the ordinal-mismatch panic below trip on
// perfectly ordinary dead-incarnation records.
func (r *RecoveryManager) recoverPersistentMessages(stats *protocol.RecoveryStats) error {
	r.logger.Info("Recovering persistent messages...")

	recoverableMessages, err := r.storage.GetRecoverableMessages()
	if err != nil {
		return err
	}

	for queueName, messages := range recoverableMessages {
		queueOrdinal, hasRecord, oerr := r.resolveRecoveredQueueOrdinal(queueName)
		if oerr != nil {
			return fmt.Errorf("failed to resolve ordinal for queue %q: %w", queueName, oerr)
		}

		if !hasRecord {
			// Case C: no metadata record exists for this queue name at all —
			// it was deleted (queue.delete purges the in-memory ring but
			// never the shared WAL). AMQP 0-9-1 §1.7.2.10: queue.delete
			// removes the queue and all its messages, enforced here at
			// recovery time. Discard every record; do not refuse to boot.
			var lo, hi uint64
			for i, message := range messages {
				if message.DeliveryMode != 2 {
					continue
				}
				if i == 0 || message.DeliveryTag < lo {
					lo = message.DeliveryTag
				}
				if message.DeliveryTag > hi {
					hi = message.DeliveryTag
				}
			}
			r.logger.Warn("discarding recovered records for deleted queue",
				zap.String("queue", queueName),
				zap.Int("discarded", len(messages)),
				zap.Uint64("ordinal_span_min", broker.TagOrdinal(lo)),
				zap.Uint64("ordinal_span_max", broker.TagOrdinal(hi)))
			continue
		}

		if queueOrdinal == 0 {
			// Case D: metadata record exists but carries Ordinal==0 while
			// records exist for it — a genuine pre-packing data directory.
			// This is the ONLY case allowed to report ErrLegacyDataDirectory.
			var count uint64
			var minTag, maxTag uint64
			has := false
			for _, message := range messages {
				if message.DeliveryMode != 2 {
					continue
				}
				if !has || message.DeliveryTag < minTag {
					minTag = message.DeliveryTag
				}
				if message.DeliveryTag > maxTag {
					maxTag = message.DeliveryTag
				}
				has = true
				count++
			}
			if has {
				return fmt.Errorf(
					"%w: queue %q has %d recovered durable message(s) (tags %d..%d) but no delivery-tag ordinal was ever persisted for it",
					ErrLegacyDataDirectory, queueName, count, minTag, maxTag)
			}
			// No DeliveryMode==2 records at all — nothing to recover, nothing
			// to refuse over.
			continue
		}

		// Cases A and B: a real ordinal is assigned to this queue name.
		// Partition records into ones that belong to this incarnation
		// (ordinal == queueOrdinal, case A) versus dead older incarnations
		// of the same name (ordinal < queueOrdinal, case B) versus records
		// whose ordinal is GREATER than the assigned one — never a dead
		// incarnation, always metadata corruption (the persisted ordinal
		// regressed) — which stays fatal via ErrOrdinalMismatch.
		var minTag, maxTag uint64
		var queueCount uint64
		hasRecovered := false
		var discardedCount int
		var discardedLo, discardedHi uint64
		hasDiscarded := false
		for _, message := range messages {
			if message.DeliveryMode != 2 {
				continue
			}
			tagOrdinal := broker.TagOrdinal(message.DeliveryTag)
			if tagOrdinal > queueOrdinal {
				return fmt.Errorf(
					"%w: queue %q is assigned ordinal %d, but a recovered tag carries ordinal %d (tag=%d)",
					ErrOrdinalMismatch, queueName, queueOrdinal, tagOrdinal, message.DeliveryTag)
			}
			if tagOrdinal < queueOrdinal {
				// Case B: a dead incarnation's record. Discard — do not load,
				// do not count.
				discardedCount++
				if !hasDiscarded || message.DeliveryTag < discardedLo {
					discardedLo = message.DeliveryTag
				}
				if message.DeliveryTag > discardedHi {
					discardedHi = message.DeliveryTag
				}
				hasDiscarded = true
				continue
			}

			// tagOrdinal == queueOrdinal: belongs to the current incarnation.
			err := r.storage.LoadMessageFromRecovery(queueName, message)
			if err != nil {
				r.logger.Error("Failed to load recovered message into ring buffer",
					zap.String("queue", queueName),
					zap.Uint64("delivery_tag", message.DeliveryTag),
					zap.Error(err))
				stats.ValidationErrors = append(stats.ValidationErrors,
					fmt.Sprintf("Failed to load message in queue %s: %v", queueName, err))
				continue
			}
			stats.PersistentMessagesRecovered++
			if !hasRecovered || message.DeliveryTag < minTag {
				minTag = message.DeliveryTag
			}
			if message.DeliveryTag > maxTag {
				maxTag = message.DeliveryTag
			}
			hasRecovered = true
			queueCount++
			r.logger.Debug("Loaded recovered message into ring buffer",
				zap.String("queue", queueName),
				zap.Uint64("delivery_tag", message.DeliveryTag))
		}

		if hasDiscarded {
			r.logger.Warn("discarding records from a dead incarnation of a queue",
				zap.String("queue", queueName),
				zap.Int("discarded", discardedCount),
				zap.Uint64("ordinal_span_min", broker.TagOrdinal(discardedLo)),
				zap.Uint64("ordinal_span_max", broker.TagOrdinal(discardedHi)))
		}

		if hasRecovered {
			r.broker.RecoverQueue(queueName, minTag, maxTag, queueCount)
		}
	}

	return nil
}

// resolveRecoveredQueueOrdinal returns the composite-tag ordinal
// (broker/tag_packing.go) recovery should expect a queue's recovered
// delivery tags to carry, and whether a metadata record exists for
// queueName at all. The two are reported separately (tri-state) because
// they demand opposite responses at recovery time:
//
//   - hasRecord == false means no metadata record exists for this name —
//     the queue was deleted (queue.delete purges only the in-memory ring,
//     never the shared WAL) — and its leftover records must simply be
//     discarded, never treated as a reason to refuse to boot.
//   - hasRecord == true, ordinal == 0 means a metadata record exists but
//     never had a composite-tag ordinal assigned: a genuine pre-packing
//     data directory, which IS fatal (ErrLegacyDataDirectory).
//
// The ordinal comes from the value captured before recoverDurableQueues
// (re)declared the queue (the normal case for any durable queue — see
// queueOrdinalsAtDeclare's field doc), or from the currently persisted
// record for a queue that path never touched, e.g. a non-durable queue that
// nonetheless holds recoverable persistent messages.
func (r *RecoveryManager) resolveRecoveredQueueOrdinal(queueName string) (ordinal uint64, hasRecord bool, err error) {
	if ordinal, ok := r.queueOrdinalsAtDeclare[queueName]; ok {
		return ordinal, true, nil
	}
	q, err := r.storage.GetQueue(queueName)
	if err != nil {
		if errors.Is(err, interfaces.ErrQueueNotFound) {
			return 0, false, nil
		}
		return 0, false, err
	}
	if q == nil {
		return 0, false, nil
	}
	return q.Ordinal, true, nil
}

// recoverPendingAcknowledgments recovers all pending acknowledgments from storage
func (r *RecoveryManager) recoverPendingAcknowledgments(stats *protocol.RecoveryStats) error {
	r.logger.Info("Recovering pending acknowledgments...")

	pendingAcks, err := r.storage.GetAllPendingAcks()
	if err != nil {
		return err
	}

	// Clean up expired acknowledgments (older than 1 hour)
	expiredCutoff := 1 * time.Hour
	err = r.storage.CleanupExpiredAcks(expiredCutoff)
	if err != nil {
		r.logger.Warn("Failed to cleanup expired acknowledgments", zap.Error(err))
	}

	// CRITICAL FIX: Rebuild global delivery index during crash recovery
	// This ensures ACKs can be routed correctly after server restart
	for _, pendingAck := range pendingAcks {
		if !pendingAck.DeliveredAt.IsZero() {
			age := time.Since(pendingAck.DeliveredAt)
			if age > expiredCutoff {
				err := r.storage.DeletePendingAck(pendingAck.QueueName, pendingAck.DeliveryTag)
				if err != nil {
					r.logger.Warn("Failed to delete expired pending ack",
						zap.String("queue", pendingAck.QueueName),
						zap.Uint64("delivery_tag", pendingAck.DeliveryTag),
						zap.Error(err))
				}
				continue
			}
		}

		r.broker.RebuildDeliveryIndex(pendingAck.DeliveryTag, pendingAck.ConsumerTag)

		stats.PendingAcksRecovered++
		r.logger.Debug("Recovered pending acknowledgment",
			zap.String("queue", pendingAck.QueueName),
			zap.String("consumer", pendingAck.ConsumerTag),
			zap.Uint64("delivery_tag", pendingAck.DeliveryTag))
	}

	return nil
}

// UpdateDurableEntityMetadata updates the durable entity metadata in storage
func (r *RecoveryManager) UpdateDurableEntityMetadata(exchanges map[string]*protocol.Exchange, queues map[string]*protocol.Queue) error {
	metadata := &protocol.DurableEntityMetadata{
		Exchanges:   []*protocol.Exchange{},
		Queues:      []*protocol.Queue{},
		Bindings:    []protocol.Binding{},
		LastUpdated: time.Now(),
	}

	// Collect durable exchanges
	for _, exchange := range exchanges {
		if exchange.Durable {
			exchangeCopy := exchange.Copy()
			metadata.Exchanges = append(metadata.Exchanges, &exchangeCopy)

			// Collect bindings for this exchange
			for _, binding := range exchange.Bindings {
				metadata.Bindings = append(metadata.Bindings, *binding)
			}
		}
	}

	// Collect durable queues
	for _, queue := range queues {
		if queue.Durable {
			metadata.Queues = append(metadata.Queues, queue)
		}
	}

	return r.storage.StoreDurableEntityMetadata(metadata)
}
