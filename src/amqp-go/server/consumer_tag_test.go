package server

import (
	"encoding/binary"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/maxpert/amqp-go/protocol"
)

// ============================================================================
// Consumer-tag conformance at the frame level (AMQP 0-9-1 §1.8.3.3 basic.consume).
//
// Two spec rules are pinned here, both quoted from the basic.consume method
// definition:
//
//	consumer-tag: "Specifies the identifier for the consumer. The consumer tag
//	is local to a channel, so two clients can use the same consumer tags. If
//	this field is empty the server will generate a unique tag."
//
//	rule: "The client MUST NOT specify a tag that refers to an existing
//	consumer. Error code: not-allowed"
//
// 530 not-allowed is a CONNECTION exception in AMQP 0-9-1's reply-code table,
// so the duplicate-tag arm must produce connection.close (class 10, method 50)
// and NOT channel.close (class 20, method 40). This file asserts the frame
// identity directly, which is the only way to tell the two apart with
// certainty; consumer_tag_scope_test.go asserts the same thing end-to-end
// through amqp091-go.
//
// The generated-tag arm cannot be tested through amqp091-go at all: that
// client never sends an empty consumer-tag (channel.go substitutes
// uniqueConsumerTag() locally), so the server path would be untested from an
// integration test. Hence the handler-level harness here.
// ============================================================================

// ctagPipe is a protocol.Connection whose peer end stays readable, so a test
// can decode exactly which frames a handler emitted.
type ctagPipe struct {
	conn   *protocol.Connection
	client net.Conn
}

func newCtagPipe(t *testing.T) *ctagPipe {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() {
		_ = clientConn.Close()
		_ = serverConn.Close()
	})
	conn := protocol.NewConnection(serverConn)
	conn.ID = "ctag-test-conn"
	return &ctagPipe{conn: conn, client: clientConn}
}

// channel attaches a fresh channel with the given id to the connection.
func (p *ctagPipe) channel(id uint16) *protocol.Channel {
	ch := protocol.NewChannel(id, p.conn)
	p.conn.Channels.Store(id, ch)
	return ch
}

// readFrame decodes one frame from the peer end. net.Pipe is synchronous and
// unbuffered, so the handler under test must be running concurrently.
func (p *ctagPipe) readFrame(t *testing.T) *protocol.Frame {
	t.Helper()
	_ = p.client.SetReadDeadline(time.Now().Add(10 * time.Second))
	f, err := protocol.ReadFrame(p.client)
	if err != nil {
		t.Fatalf("reading response frame: %v", err)
	}
	return f
}

// methodID splits a method frame payload into (classID, methodID, arguments).
func methodID(t *testing.T, f *protocol.Frame) (uint16, uint16, []byte) {
	t.Helper()
	if f.Type != protocol.FrameMethod {
		t.Fatalf("frame type = %d; want a method frame", f.Type)
	}
	if len(f.Payload) < 4 {
		t.Fatalf("method frame payload too short: %d bytes", len(f.Payload))
	}
	return binary.BigEndian.Uint16(f.Payload[0:2]),
		binary.BigEndian.Uint16(f.Payload[2:4]),
		f.Payload[4:]
}

// readShortString decodes an AMQP shortstr at the head of b.
func readShortString(t *testing.T, b []byte) (string, []byte) {
	t.Helper()
	if len(b) < 1 {
		t.Fatal("truncated shortstr length octet")
	}
	n := int(b[0])
	if len(b) < 1+n {
		t.Fatalf("truncated shortstr: want %d bytes, have %d", n, len(b)-1)
	}
	return string(b[1 : 1+n]), b[1+n:]
}

// consumePayload builds a basic.consume method payload (arguments only — the
// class/method octets are added by the frame encoder, and the handler is
// called with the arguments slice).
func consumePayload(t *testing.T, queue, tag string, noAck bool) []byte {
	t.Helper()
	m := &protocol.BasicConsumeMethod{
		Queue:       queue,
		ConsumerTag: tag,
		NoAck:       noAck,
	}
	payload, err := m.Serialize()
	if err != nil {
		t.Fatalf("serialize basic.consume: %v", err)
	}
	return payload
}

// runConsume invokes handleBasicConsume concurrently (the pipe is synchronous)
// and returns a channel carrying its error.
func runConsume(srv *Server, p *ctagPipe, channelID uint16, payload []byte) <-chan error {
	errCh := make(chan error, 1)
	go func() { errCh <- srv.handleBasicConsume(p.conn, channelID, payload) }()
	return errCh
}

// declareCtagQueue creates a queue directly through the broker so the consume
// handler's queue-existence path is satisfied.
func declareCtagQueue(t *testing.T, srv *Server, name string) {
	t.Helper()
	if _, err := srv.Broker.DeclareQueue(name, false, false, false, nil); err != nil {
		t.Fatalf("declare queue %q: %v", name, err)
	}
}

// ---------------------------------------------------------------------------
// §1.8.3.3 — empty consumer-tag: the server generates one and returns it
// ---------------------------------------------------------------------------

func TestBasicConsume_EmptyTagIsServerGeneratedAndReturnedInConsumeOk(t *testing.T) {
	srv := newTestServer(t)
	p := newCtagPipe(t)
	ch := p.channel(1)
	declareCtagQueue(t, srv, "gen-tag-q")

	errCh := runConsume(srv, p, 1, consumePayload(t, "gen-tag-q", "", true))

	classID, mID, args := methodID(t, p.readFrame(t))
	if classID != 60 || mID != 21 {
		t.Fatalf("response was class %d method %d; want basic.consume-ok (60.21)", classID, mID)
	}
	gotTag, _ := readShortString(t, args)

	if err := <-errCh; err != nil {
		t.Fatalf("handleBasicConsume: %v", err)
	}

	if gotTag == "" {
		t.Fatal("basic.consume-ok carried an EMPTY consumer tag; " +
			"AMQP 0-9-1 requires the server to generate a unique tag and return it")
	}

	// The generated tag must be the identity the channel is keyed by, or a
	// later basic.cancel carrying it could not find the consumer.
	ch.Mutex.RLock()
	consumer, ok := ch.Consumers[gotTag]
	ch.Mutex.RUnlock()
	if !ok {
		ch.Mutex.RLock()
		keys := make([]string, 0, len(ch.Consumers))
		for k := range ch.Consumers {
			keys = append(keys, k)
		}
		ch.Mutex.RUnlock()
		t.Fatalf("generated tag %q is not registered on the channel; channel holds %v", gotTag, keys)
	}
	if consumer.Tag != gotTag {
		t.Fatalf("consumer.Tag = %q; want the generated tag %q returned in consume-ok", consumer.Tag, gotTag)
	}
}

// TestBasicConsume_GeneratedTagsAreUniquePerConsumer pins that repeated empty
// tags on ONE channel do not collide with each other. Under a naive
// implementation that returned a constant or a per-channel-reset name, the
// second registration would either overwrite the first or trip the
// duplicate-tag 530.
func TestBasicConsume_GeneratedTagsAreUniquePerConsumer(t *testing.T) {
	srv := newTestServer(t)
	p := newCtagPipe(t)
	ch := p.channel(1)
	declareCtagQueue(t, srv, "gen-uniq-q")

	const n = 8
	seen := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		errCh := runConsume(srv, p, 1, consumePayload(t, "gen-uniq-q", "", true))
		classID, mID, args := methodID(t, p.readFrame(t))
		if classID != 60 || mID != 21 {
			t.Fatalf("consume %d: response was class %d method %d; want basic.consume-ok", i, classID, mID)
		}
		tag, _ := readShortString(t, args)
		if err := <-errCh; err != nil {
			t.Fatalf("consume %d: %v", i, err)
		}
		if _, dup := seen[tag]; dup {
			t.Fatalf("consume %d re-issued generated tag %q; generated tags must be unique", i, tag)
		}
		seen[tag] = struct{}{}
	}

	ch.Mutex.RLock()
	got := len(ch.Consumers)
	ch.Mutex.RUnlock()
	if got != n {
		t.Fatalf("channel holds %d consumers; want %d — generated tags collided", got, n)
	}
}

// TestBasicConsume_GeneratedTagCannotCollideWithClientSuppliedTag is the
// property required by the mandate: a server-generated tag must never equal a
// tag the client already supplied on the same channel.
//
// The property is ASSERTED, not documented (canon rule 11). It cannot be
// asserted against the real generator: 128 bits of crypto/rand means the
// collision branch is unreachable, so a test that squatted a random tag and
// checked "the generated one differs" would pass by construction and gate
// nothing — it would stay green with the collision check deleted. Instead the
// generator is substituted for one whose FIRST draw is exactly the tag the
// client already holds, which forces the branch and makes the outcome depend
// on the check rather than on luck. A call counter is asserted at the end so a
// silently-unused stub cannot report success.
//
// The substitution is per-Server (Server.generateConsumerTag), not a package
// global: as a global its race-freedom rested on no test in this package ever
// calling t.Parallel(), which nothing enforced and nothing recorded.
func TestBasicConsume_GeneratedTagCannotCollideWithClientSuppliedTag(t *testing.T) {
	srv := newTestServer(t)
	p := newCtagPipe(t)
	ch := p.channel(1)
	declareCtagQueue(t, srv, "gen-collide-q")

	const squatted = "amq.ctag-client-squatted-this"
	const fallback = "amq.ctag-second-draw"

	// Register squatted as a CLIENT-supplied tag. It is now occupied, and it is
	// also what the generator is about to hand out first.
	errCh := runConsume(srv, p, 1, consumePayload(t, "gen-collide-q", squatted, true))
	if classID, mID, _ := methodID(t, p.readFrame(t)); classID != 60 || mID != 21 {
		t.Fatalf("client-supplied tag: response was class %d method %d; want basic.consume-ok", classID, mID)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("client-supplied tag consume: %v", err)
	}

	draws := 0
	orig := srv.generateConsumerTag
	srv.generateConsumerTag = func() string {
		draws++
		if draws == 1 {
			return squatted // collide with the client's live tag
		}
		return fallback
	}
	t.Cleanup(func() { srv.generateConsumerTag = orig })

	// Ask for a generated tag on the same channel.
	errCh = runConsume(srv, p, 1, consumePayload(t, "gen-collide-q", "", true))
	classID, mID, args := methodID(t, p.readFrame(t))
	if classID == 10 && mID == 50 {
		t.Fatal("a colliding first draw produced a connection.close; " +
			"generation must re-draw around an occupied tag, not fail the client")
	}
	if classID != 60 || mID != 21 {
		t.Fatalf("empty tag after squat: response was class %d method %d; want basic.consume-ok", classID, mID)
	}
	generated, _ := readShortString(t, args)
	if err := <-errCh; err != nil {
		t.Fatalf("generated tag consume: %v", err)
	}

	if generated == squatted {
		t.Fatalf("server generated %q, which collides with the client-supplied tag on the same channel; "+
			"the client's consumer has been silently overwritten", generated)
	}
	if generated != fallback {
		t.Fatalf("generated tag = %q; want the second draw %q", generated, fallback)
	}

	// Active-assertion check: if the stub was never called, the two assertions
	// above were vacuous and this test gates nothing.
	if draws < 2 {
		t.Fatalf("generator was called %d time(s); the collision branch was never exercised, "+
			"so this test proved nothing", draws)
	}

	ch.Mutex.RLock()
	got := len(ch.Consumers)
	_, squattedStillThere := ch.Consumers[squatted]
	ch.Mutex.RUnlock()
	if !squattedStillThere {
		t.Error("the client-supplied consumer was evicted by the generated tag")
	}
	if got != 2 {
		t.Fatalf("channel holds %d consumers; want 2 — the generated tag overwrote the client's", got)
	}
}

// TestGenerateConsumerTag_ProductionGeneratorIsReserved pins that the real
// generator (the one the substituted stub above stands in for) produces the
// reserved amq.ctag- namespace, so the substitution test is testing the same
// shape production emits.
//
// It also pins the DEFAULT of the seam. Server.generateConsumerTag is nil in
// production and consumerTagGenerator() falls back to the real generator; that
// fallback is the reason no construction site has to remember to wire the
// field, and it is exactly the kind of arrangement that fails silently if it
// ever regresses, so it is asserted by calling the accessor on an unsubstituted
// server rather than by inspecting the field.
func TestGenerateConsumerTag_ProductionGeneratorIsReserved(t *testing.T) {
	tag := protocol.GenerateConsumerTag()
	if !strings.HasPrefix(tag, "amq.ctag-") {
		t.Fatalf("generated tag %q does not carry the reserved amq.ctag- prefix", tag)
	}

	srv := newTestServer(t)
	if srv.generateConsumerTag != nil {
		t.Fatal("a freshly constructed Server already carries a substituted consumer-tag generator; " +
			"production must leave the seam nil")
	}
	gen := srv.consumerTagGenerator()
	if gen == nil {
		t.Fatal("consumerTagGenerator() returned nil on an unsubstituted server; " +
			"basic.consume with an empty consumer-tag would panic")
	}
	got := gen()
	if !strings.HasPrefix(got, "amq.ctag-") {
		t.Fatalf("an unsubstituted server generated %q; want the production amq.ctag- shape — "+
			"the fallback is not wired to protocol.GenerateConsumerTag", got)
	}
}

// ---------------------------------------------------------------------------
// Duplicate tag, same channel -> 530 not-allowed, CONNECTION exception
// ---------------------------------------------------------------------------

func TestBasicConsume_DuplicateTagOnSameChannelIsConnectionClose530(t *testing.T) {
	srv := newTestServer(t)
	p := newCtagPipe(t)
	p.channel(1)
	declareCtagQueue(t, srv, "dup-tag-q")

	const tag = "same-channel-tag"
	errCh := runConsume(srv, p, 1, consumePayload(t, "dup-tag-q", tag, true))
	if classID, mID, _ := methodID(t, p.readFrame(t)); classID != 60 || mID != 21 {
		t.Fatalf("first consume: response was class %d method %d; want basic.consume-ok", classID, mID)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("first consume: %v", err)
	}

	errCh = runConsume(srv, p, 1, consumePayload(t, "dup-tag-q", tag, true))
	f := p.readFrame(t)
	classID, mID, args := methodID(t, f)

	switch {
	case classID == 20 && mID == 40:
		t.Fatalf("duplicate tag raised a CHANNEL exception (channel.close); " +
			"AMQP 0-9-1 classes 530 not-allowed as a CONNECTION exception")
	case classID == 60 && mID == 21:
		t.Fatal("duplicate consumer tag on the same channel was ACCEPTED (basic.consume-ok); " +
			"the spec rule is: the client MUST NOT specify a tag that refers to an existing consumer")
	case classID != 10 || mID != 50:
		t.Fatalf("response was class %d method %d; want connection.close (10.50)", classID, mID)
	}

	if len(args) < 2 {
		t.Fatal("connection.close payload too short for a reply code")
	}
	replyCode := binary.BigEndian.Uint16(args[0:2])
	if replyCode != 530 {
		t.Fatalf("connection.close reply-code = %d; want 530 not-allowed", replyCode)
	}
	replyText, rest := readShortString(t, args[2:])
	if len(rest) >= 4 {
		if failClass, failMethod := binary.BigEndian.Uint16(rest[0:2]), binary.BigEndian.Uint16(rest[2:4]); failClass != 60 || failMethod != 20 {
			t.Errorf("connection.close blamed class %d method %d; want basic.consume (60.20)", failClass, failMethod)
		}
	}
	if !strings.Contains(strings.ToUpper(replyText), "NOT_ALLOWED") {
		t.Errorf("connection.close reply-text = %q; want it to name NOT_ALLOWED", replyText)
	}

	// The handler must report the failure upward so the connection is torn down.
	if err := <-errCh; err == nil {
		t.Error("handleBasicConsume returned nil on a connection exception; " +
			"the frame loop needs a non-nil error to tear the connection down")
	}
	if !p.conn.Closed.Load() {
		t.Error("connection was not marked closed after a 530 connection exception")
	}
}

// TestBasicConsume_SameTagOnDifferentChannelsGetsDistinctIdentity is the
// regression guard against an over-broad "reject every duplicate tag" fix, and
// it pins the mechanism: the two consumers must carry DIFFERENT broker-internal
// identities while presenting the SAME tag on the wire.
func TestBasicConsume_SameTagOnDifferentChannelsGetsDistinctIdentity(t *testing.T) {
	srv := newTestServer(t)
	p := newCtagPipe(t)
	ch1 := p.channel(1)
	ch2 := p.channel(2)
	declareCtagQueue(t, srv, "cross-chan-q1")
	declareCtagQueue(t, srv, "cross-chan-q2")

	const tag = "cross-channel-tag"
	for i, spec := range []struct {
		channelID uint16
		queue     string
	}{{1, "cross-chan-q1"}, {2, "cross-chan-q2"}} {
		errCh := runConsume(srv, p, spec.channelID, consumePayload(t, spec.queue, tag, true))
		classID, mID, args := methodID(t, p.readFrame(t))
		if classID == 10 && mID == 50 {
			t.Fatalf("consume %d on channel %d was rejected with a connection exception; "+
				"AMQP 0-9-1: the consumer tag is local to a channel", i, spec.channelID)
		}
		if classID != 60 || mID != 21 {
			t.Fatalf("consume %d on channel %d: response was class %d method %d; want basic.consume-ok",
				i, spec.channelID, classID, mID)
		}
		if got, _ := readShortString(t, args); got != tag {
			t.Fatalf("consume-ok echoed tag %q; want the client-supplied %q", got, tag)
		}
		if err := <-errCh; err != nil {
			t.Fatalf("consume %d on channel %d: %v", i, spec.channelID, err)
		}
	}

	c1 := lookupConsumer(t, ch1, tag)
	c2 := lookupConsumer(t, ch2, tag)

	if c1.Tag != tag || c2.Tag != tag {
		t.Fatalf("wire tags = %q / %q; both must remain the client-supplied %q", c1.Tag, c2.Tag, tag)
	}
	if c1.ID == "" || c2.ID == "" {
		t.Fatalf("consumer identities are empty (%q / %q); registration must mint an internal identity", c1.ID, c2.ID)
	}
	if c1.ID == c2.ID {
		t.Fatalf("both channels' consumers share the internal identity %q; "+
			"the broker keys consumers by it, so one silently overwrites the other", c1.ID)
	}
	if c1.Queue == c2.Queue {
		t.Fatalf("both consumers bound to queue %q; want distinct queues", c1.Queue)
	}

	// The broker must hold BOTH, under their distinct identities.
	consumers := srv.Broker.GetConsumers()
	if _, ok := consumers[c1.ID]; !ok {
		t.Errorf("broker does not hold channel-1 consumer under identity %q", c1.ID)
	}
	if _, ok := consumers[c2.ID]; !ok {
		t.Errorf("broker does not hold channel-2 consumer under identity %q", c2.ID)
	}
	if n := len(consumers); n != 2 {
		t.Fatalf("broker holds %d consumers; want 2 — same-tag registrations collapsed into one", n)
	}
}

// ---------------------------------------------------------------------------
// queue.delete's consumer-cancel notification is identity-scoped
// ---------------------------------------------------------------------------

// notifyScopeRounds / notifyScopeMinRounds size the repetition in
// TestQueueDelete_NotifyConsumersCancelledIsIdentityScoped below.
//
// That test's detector is PROBABILISTIC, and the probability is not a property
// of the test — it is Go map iteration order. notifyConsumersCancelled walks
// conn.Channels with sync.Map.Range, and the pre-fix (bare wire tag) scan
// cancelled whichever channel that walk reached first. With two channels
// holding the same tag it is therefore a coin flip whether a broken
// implementation picks the right one by luck. Measured on the pre-fix shape:
// 7 failures in 20 single-round runs, so p(detect) is about 0.35 per round.
//
// A ONE-ROUND version of this test would pass on the broken code roughly two
// times in three, which is worse than having no test at all — it would look
// like coverage. notifyScopeRounds drives a miss to (1-0.35)^32 ~ 8e-6; the
// floor below is the point past which the run stops being evidence, and it is
// CHECKED at runtime rather than described here, because a comment cannot stop
// someone shrinking the loop.
const (
	notifyScopeRounds    = 32
	notifyScopeMinRounds = 20
)

// TestQueueDelete_NotifyConsumersCancelledIsIdentityScoped pins the second
// instance of the per-channel-scoping defect: queue.delete's server-initiated
// basic.cancel notification.
//
// Pre-fix, notifyConsumersCancelled took a bare wire tag and searched every
// channel of every connection for it, cancelling the FIRST match. Two clients
// running the same binary mint the same default tag, so deleting queue Q1 could
// close a consumer on Q2 belonging to an unrelated connection: that consumer's
// Cancel channel was closed, it was deleted from its channel map, and it was
// sent a basic.cancel for a queue it had never consumed — after which it
// silently stopped draining. The fix scans by Consumer.ID and sends the matched
// consumer's OWN wire tag.
//
// The shape here is the minimum that can tell the two apart: one tag, two
// channels, two distinct identities, cancel by identity, assert the other one
// survives.
func TestQueueDelete_NotifyConsumersCancelledIsIdentityScoped(t *testing.T) {
	srv := newTestServer(t)

	const tag = "notify-shared-tag"
	rounds := 0
	for round := 0; round < notifyScopeRounds; round++ {
		q1 := fmt.Sprintf("notify-q1-%d", round)
		q2 := fmt.Sprintf("notify-q2-%d", round)
		declareCtagQueue(t, srv, q1)
		declareCtagQueue(t, srv, q2)

		p := newCtagPipe(t)
		p.conn.ID = fmt.Sprintf("notify-conn-%d", round)
		ch1 := p.channel(1)
		ch2 := p.channel(2)

		srv.Mutex.Lock()
		srv.Connections[p.conn.ID] = p.conn
		srv.Mutex.Unlock()

		for _, spec := range []struct {
			channelID uint16
			queue     string
		}{{1, q1}, {2, q2}} {
			errCh := runConsume(srv, p, spec.channelID, consumePayload(t, spec.queue, tag, true))
			p.readFrame(t)
			if err := <-errCh; err != nil {
				t.Fatalf("round %d: consume on channel %d: %v", round, spec.channelID, err)
			}
		}

		victim := lookupConsumer(t, ch1, tag)   // attached to q1, the deleted queue
		survivor := lookupConsumer(t, ch2, tag) // attached to q2, must be untouched

		// Active-assertion check: this round proves nothing unless the fixture
		// actually built the ambiguous shape — one wire tag, two identities. A
		// fixture that silently stopped colliding the tags would pass every
		// assertion below on broken code.
		if victim.Tag != tag || survivor.Tag != tag {
			t.Fatalf("round %d: fixture did not reproduce the shared-tag shape; wire tags are %q and %q, want both %q",
				round, victim.Tag, survivor.Tag, tag)
		}
		if victim.ID == survivor.ID {
			t.Fatalf("round %d: fixture did not reproduce the distinct-identity shape; "+
				"both consumers carry identity %q, so nothing here can distinguish identity from tag",
				round, victim.ID)
		}

		done := make(chan struct{})
		go func() {
			srv.notifyConsumersCancelled([]string{victim.ID})
			close(done)
		}()
		// The server-initiated basic.cancel. net.Pipe is unbuffered, so this read
		// is also the rendezvous that lets the notification complete.
		classID, mID, args := methodID(t, p.readFrame(t))
		<-done

		if classID != 60 || mID != 30 {
			t.Fatalf("round %d: frame was class %d method %d; want basic.cancel (60.30)", round, classID, mID)
		}
		if got, _ := readShortString(t, args); got != tag {
			t.Fatalf("round %d: basic.cancel carried tag %q; want the consumer's own wire tag %q", round, got, tag)
		}

		ch1.Mutex.RLock()
		_, ch1Has := ch1.Consumers[tag]
		ch1.Mutex.RUnlock()
		ch2.Mutex.RLock()
		c2, ch2Has := ch2.Consumers[tag]
		ch2.Mutex.RUnlock()

		if !ch2Has {
			t.Fatalf("round %d: notifyConsumersCancelled cancelled the WRONG consumer: channel 2's consumer "+
				"(identity %q, queue %s) was removed while the cancellation targeted identity %q on channel 1; "+
				"a queue.delete just silently stopped an unrelated consumer",
				round, survivor.ID, q2, victim.ID)
		}
		if ch1Has {
			t.Fatalf("round %d: the consumer on the deleted queue's channel (identity %q) was NOT cancelled",
				round, victim.ID)
		}
		if c2.ID != survivor.ID {
			t.Fatalf("round %d: channel 2 now holds identity %q; want %q", round, c2.ID, survivor.ID)
		}

		srv.Mutex.Lock()
		delete(srv.Connections, p.conn.ID)
		srv.Mutex.Unlock()
		p.conn.Closed.Store(true)
		_ = srv.Broker.UnregisterConsumer(victim.ID)
		_ = srv.Broker.UnregisterConsumer(survivor.ID)

		rounds++
	}

	// Premise check, not a comment: everything above is evidence only if enough
	// rounds actually ran. See notifyScopeRounds for the measured per-round
	// detection rate this floor is derived from.
	if rounds < notifyScopeMinRounds {
		t.Fatalf("fixture completed %d round(s); below %d the per-round detection rate (~0.35, "+
			"measured 7/20 on the pre-fix shape) makes a green here a coin flip rather than evidence",
			rounds, notifyScopeMinRounds)
	}
}

func lookupConsumer(t *testing.T, ch *protocol.Channel, tag string) *protocol.Consumer {
	t.Helper()
	ch.Mutex.RLock()
	defer ch.Mutex.RUnlock()
	c, ok := ch.Consumers[tag]
	if !ok {
		t.Fatalf("channel %d has no consumer tagged %q", ch.ID, tag)
	}
	return c
}

// TestBasicCancel_TargetsOnlyTheCancellingChannelsConsumer pins the teardown
// half of per-channel scoping: cancelling tag T on channel 1 must not touch
// the consumer that presents the same tag T on channel 2. Under broker-global
// tag keying, one connection's cancel killed another's consumer.
func TestBasicCancel_TargetsOnlyTheCancellingChannelsConsumer(t *testing.T) {
	srv := newTestServer(t)
	p := newCtagPipe(t)
	ch1 := p.channel(1)
	ch2 := p.channel(2)
	declareCtagQueue(t, srv, "cancel-scope-q1")
	declareCtagQueue(t, srv, "cancel-scope-q2")

	const tag = "cancel-scope-tag"
	for _, spec := range []struct {
		channelID uint16
		queue     string
	}{{1, "cancel-scope-q1"}, {2, "cancel-scope-q2"}} {
		errCh := runConsume(srv, p, spec.channelID, consumePayload(t, spec.queue, tag, true))
		p.readFrame(t)
		if err := <-errCh; err != nil {
			t.Fatalf("consume on channel %d: %v", spec.channelID, err)
		}
	}
	survivor := lookupConsumer(t, ch2, tag)

	cancelMethod := &protocol.BasicCancelMethod{ConsumerTag: tag}
	cancelPayload, err := cancelMethod.Serialize()
	if err != nil {
		t.Fatalf("serialize basic.cancel: %v", err)
	}
	cancelErr := make(chan error, 1)
	go func() { cancelErr <- srv.handleBasicCancel(p.conn, 1, cancelPayload) }()
	if classID, mID, _ := methodID(t, p.readFrame(t)); classID != 60 || mID != 31 {
		t.Fatalf("cancel response was class %d method %d; want basic.cancel-ok (60.31)", classID, mID)
	}
	if err := <-cancelErr; err != nil {
		t.Fatalf("handleBasicCancel: %v", err)
	}

	ch1.Mutex.RLock()
	_, stillOnCh1 := ch1.Consumers[tag]
	ch1.Mutex.RUnlock()
	if stillOnCh1 {
		t.Error("channel 1's consumer survived its own basic.cancel")
	}

	ch2.Mutex.RLock()
	_, stillOnCh2 := ch2.Consumers[tag]
	ch2.Mutex.RUnlock()
	if !stillOnCh2 {
		t.Fatal("cancelling tag on channel 1 removed channel 2's consumer from its channel map")
	}

	consumers := srv.Broker.GetConsumers()
	if _, ok := consumers[survivor.ID]; !ok {
		t.Fatalf("cancelling channel 1's consumer also unregistered channel 2's consumer %q from the broker; "+
			"broker now holds %d consumer(s)", survivor.ID, len(consumers))
	}
	if n := len(consumers); n != 1 {
		t.Errorf("broker holds %d consumers after one cancel; want exactly 1", n)
	}
}
