package broker

import (
	"github.com/maxpert/amqp-go/protocol"
)

// registerConsumer is the compatibility hinge that used to live inside
// StorageBroker.RegisterConsumer as `if consumer.ID == "" { consumer.ID =
// consumerID }`.
//
// It lives here, in the tests, because this is the only place it was ever used.
// Unit tests in this package build a *protocol.Consumer by hand and register it
// under a bare tag, so the tag is also the identity; the server mints
// consumer.ID at basic.consume and always passes the two in agreement. Keeping
// the fallback in production meant RegisterConsumer wrote to a struct the
// server had already published to its delivery loop — a write that was safe
// only because it happened to be unreachable from the one caller that mattered.
//
// The write is safe here: nothing has published these fixtures anywhere.
func registerConsumer(b *StorageBroker, queueName, consumerID string, consumer *protocol.Consumer) error {
	consumer.ID = consumerID
	return b.RegisterConsumer(queueName, consumerID, consumer)
}
