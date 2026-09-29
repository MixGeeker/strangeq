package server

import (
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
	"github.com/stretchr/testify/require"
)

func TestBasicCancelKeepsDeliveredMessagesUntilSettlement(t *testing.T) {
	for _, action := range []string{"ack", "nack", "close"} {
		t.Run(action, func(t *testing.T) {
			srv := newTestServer(t)
			conn := newPipeConn(t)
			channel := protocol.NewChannel(1, conn)
			conn.Channels.Store(uint16(1), channel)
			conn.ChannelCount.Add(1)
			t.Cleanup(func() { srv.cleanupConnection(conn) })
			const queue = "cancel-drain"
			declareCtagQueue(t, srv, queue)
			channel.PrefetchCount = 1
			require.NoError(t, srv.handleBasicConsume(conn, 1, consumePayload(t, queue, "worker", false)))
			consumer := lookupConsumer(t, channel, "worker")
			require.NoError(t, srv.Broker.PublishMessage("", queue, &protocol.Message{Body: []byte("inflight")}))
			var delivery *protocol.Delivery
			select {
			case delivery = <-consumer.Messages:
			case <-time.After(5 * time.Second):
				t.Fatal("delivery timeout")
			}
			require.NoError(t, srv.sendBatchedDeliveries(conn, 1, "worker", []*protocol.Delivery{delivery}))
			payload, err := (&protocol.BasicCancelMethod{ConsumerTag: "worker"}).Serialize()
			require.NoError(t, err)
			require.NoError(t, srv.handleBasicCancel(conn, 1, payload))
			// cancel 后 queue 的 ready 部分应为空，消息仍可通过原 channel 确认。
			message, _, _, err := srv.Broker.GetMessageForGet(queue, true)
			require.NoError(t, err)
			require.Nil(t, message)
			switch action {
			case "ack":
				require.NoError(t, srv.handleBasicAck(conn, 1, makeAckPayload(1, false)))
			case "nack":
				require.NoError(t, srv.handleBasicNack(conn, 1, makeNackPayload(1, false, true)))
			case "close":
				srv.teardownChannel(conn, 1)
			}
			message, _, _, err = srv.Broker.GetMessageForGet(queue, true)
			require.NoError(t, err)
			if action == "ack" {
				require.Nil(t, message)
			} else {
				require.NotNil(t, message)
				require.Equal(t, "inflight", string(message.Body))
			}
		})
	}
}

func TestBasicCancelRequeuesBufferedMessagesAndAllowsTagReuse(t *testing.T) {
	srv := newTestServer(t)
	conn := newPipeConn(t)
	channel := protocol.NewChannel(1, conn)
	conn.Channels.Store(uint16(1), channel)
	conn.ChannelCount.Add(1)
	t.Cleanup(func() { srv.cleanupConnection(conn) })
	const queue = "cancel-buffered"
	declareCtagQueue(t, srv, queue)
	channel.PrefetchCount = 3
	require.NoError(t, srv.handleBasicConsume(conn, 1, consumePayload(t, queue, "worker", false)))
	consumer := lookupConsumer(t, channel, "worker")
	for _, body := range []string{"delivered", "buffered-a", "buffered-b"} {
		require.NoError(t, srv.Broker.PublishMessage("", queue, &protocol.Message{Body: []byte(body)}))
	}
	require.Eventually(t, func() bool { return len(consumer.Messages) == 3 }, 5*time.Second, time.Millisecond)
	delivery := <-consumer.Messages
	require.NoError(t, srv.sendBatchedDeliveries(conn, 1, "worker", []*protocol.Delivery{delivery}))
	payload, err := (&protocol.BasicCancelMethod{ConsumerTag: "worker"}).Serialize()
	require.NoError(t, err)
	require.NoError(t, srv.handleBasicCancel(conn, 1, payload))
	require.NoError(t, srv.handleBasicAck(conn, 1, makeAckPayload(1, false)))
	for i := 0; i < 2; i++ {
		message, _, _, err := srv.Broker.GetMessageForGet(queue, true)
		require.NoError(t, err)
		require.NotNil(t, message)
		require.Contains(t, []string{"buffered-a", "buffered-b"}, string(message.Body))
	}
	require.NoError(t, srv.handleBasicConsume(conn, 1, consumePayload(t, queue, "worker", false)))
	replacement := lookupConsumer(t, channel, "worker")
	require.NoError(t, srv.Broker.PublishMessage("", queue, &protocol.Message{Body: []byte("resumed")}))
	select {
	case delivery = <-replacement.Messages:
	case <-time.After(5 * time.Second):
		t.Fatal("resumed consumer did not receive a message")
	}
	require.NoError(t, srv.sendBatchedDeliveries(conn, 1, "worker", []*protocol.Delivery{delivery}))
	require.NoError(t, srv.handleBasicAck(conn, 1, makeAckPayload(2, false)))
	message, _, _, err := srv.Broker.GetMessageForGet(queue, true)
	require.NoError(t, err)
	require.Nil(t, message)
}
