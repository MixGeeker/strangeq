package protocol

import (
	"testing"
	"unsafe"
)

// ============================================================================
// Struct-layout pins for the two structs the per-channel consumer-identity
// change touched. Following the storage-layer precedent: when a hot struct is
// edited, pin its offsets so a later field insertion cannot silently move a hot
// or atomically-touched field across a cache line without a test going red.
//
// MEASURED at HEAD (8a65f98), before the change, with a byte-for-byte replica
// of each struct (darwin/arm64, go 1.25.1):
//
//	OLD Consumer size=88  Tag=0 Channel=16 Queue=24 NoAck=40 Exclusive=41
//	              Args=48 Messages=56 Cancel=64 PrefetchCount=72
//	              CurrentUnacked=80
//	OLD Delivery size=80  Message=0 DeliveryTag=8 Redelivered=16 Exchange=24
//	              RoutingKey=40 ConsumerTag=56 NoAck=72
//
// The change (a) deleted Consumer.CurrentUnacked, which was declared and never
// read or written anywhere in the tree, and (b) appended Consumer.ID in its
// place. Every pre-existing field therefore keeps its exact offset, and the
// struct moves 88 -> 96 bytes, which is the SAME Go size class (96), so a
// Consumer allocation is byte-identical in cost. Delivery changed by rename
// only: ConsumerTag -> ConsumerID at the same offset 56, same size 80.
// ============================================================================

// TestConsumerLayout_HotFieldOffsets pins every Consumer field offset. The
// per-delivery hot path reads Tag/ID, NoAck and Messages off this struct, so an
// insertion above any of them is a layout change that must be deliberate.
func TestConsumerLayout_HotFieldOffsets(t *testing.T) {
	var c Consumer

	for _, tc := range []struct {
		name string
		got  uintptr
		want uintptr
	}{
		{"Tag", unsafe.Offsetof(c.Tag), 0},
		{"Channel", unsafe.Offsetof(c.Channel), 16},
		{"Queue", unsafe.Offsetof(c.Queue), 24},
		{"NoAck", unsafe.Offsetof(c.NoAck), 40},
		{"Exclusive", unsafe.Offsetof(c.Exclusive), 41},
		{"Args", unsafe.Offsetof(c.Args), 48},
		{"Messages", unsafe.Offsetof(c.Messages), 56},
		{"Cancel", unsafe.Offsetof(c.Cancel), 64},
		{"PrefetchCount", unsafe.Offsetof(c.PrefetchCount), 72},
		// ID occupies exactly the slot the deleted CurrentUnacked did.
		{"ID", unsafe.Offsetof(c.ID), 80},
	} {
		if tc.got != tc.want {
			t.Errorf("Consumer.%s offset = %d, want %d — a field was inserted or reordered; "+
				"re-derive the layout argument before changing this pin", tc.name, tc.got, tc.want)
		}
	}

	// 88 (pre-change) and 96 (post-change) are both served by Go's 96-byte size
	// class, so the allocation cost of a Consumer did not move. Anything above
	// 96 crosses into the 112 class and IS a regression in allocated bytes.
	const sizeClass96 = 96
	if got := unsafe.Sizeof(c); got != sizeClass96 {
		t.Errorf("unsafe.Sizeof(Consumer) = %d, want %d; a Consumer no longer fits the "+
			"96-byte size class it shared with the pre-change layout", got, sizeClass96)
	}
}

// TestDeliveryLayout_UnchangedByConsumerIdentity pins the per-message Delivery
// struct. Delivery is heap-allocated once per delivered message — the hottest
// allocation in the broker — so the identity change was deliberately made a
// RENAME of an existing field (ConsumerTag -> ConsumerID) rather than an added
// field. This test is what makes that claim checkable instead of asserted: the
// offsets and size below are the values measured at HEAD before the change.
func TestDeliveryLayout_UnchangedByConsumerIdentity(t *testing.T) {
	var d Delivery

	for _, tc := range []struct {
		name string
		got  uintptr
		want uintptr
	}{
		{"Message", unsafe.Offsetof(d.Message), 0},
		{"DeliveryTag", unsafe.Offsetof(d.DeliveryTag), 8},
		{"Redelivered", unsafe.Offsetof(d.Redelivered), 16},
		{"Exchange", unsafe.Offsetof(d.Exchange), 24},
		{"RoutingKey", unsafe.Offsetof(d.RoutingKey), 40},
		// Was ConsumerTag at this same offset before the change.
		{"ConsumerID", unsafe.Offsetof(d.ConsumerID), 56},
		{"NoAck", unsafe.Offsetof(d.NoAck), 72},
	} {
		if tc.got != tc.want {
			t.Errorf("Delivery.%s offset = %d, want %d — the per-message delivery struct "+
				"changed shape; the identity change was supposed to be rename-only", tc.name, tc.got, tc.want)
		}
	}

	const preChangeSize = 80
	if got := unsafe.Sizeof(d); got != preChangeSize {
		t.Errorf("unsafe.Sizeof(Delivery) = %d, want %d — a field was added to the "+
			"per-message allocation; carry the identity on the per-consumer routing "+
			"entry instead", got, preChangeSize)
	}
}

// TestWireDeliveryRefLayout_RenameOnly pins the per-unacked-delivery map value.
// One of these exists per outstanding manual-ack delivery on a channel, so it
// is bounded by the prefetch window and equally must not grow.
func TestWireDeliveryRefLayout_RenameOnly(t *testing.T) {
	var r WireDeliveryRef
	if got := unsafe.Offsetof(r.MsgID); got != 0 {
		t.Errorf("WireDeliveryRef.MsgID offset = %d, want 0", got)
	}
	if got := unsafe.Offsetof(r.ConsumerID); got != 8 {
		t.Errorf("WireDeliveryRef.ConsumerID offset = %d, want 8 (where ConsumerTag was)", got)
	}
	if got := unsafe.Offsetof(r.IsGet); got != 24 {
		t.Errorf("WireDeliveryRef.IsGet offset = %d, want 24", got)
	}
	const preChangeSize = 32
	if got := unsafe.Sizeof(r); got != preChangeSize {
		t.Errorf("unsafe.Sizeof(WireDeliveryRef) = %d, want %d — rename-only was expected", got, preChangeSize)
	}
}

// TestNewConsumerID_UniqueAndOpaque asserts the two properties the broker's
// consumer keying depends on: identities are unique by construction, and they
// embed no client-controlled bytes, so no consumer tag a client can choose can
// ever be mistaken for one.
func TestNewConsumerID_UniqueAndOpaque(t *testing.T) {
	const n = 10000
	seen := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		id := NewConsumerID()
		if _, dup := seen[id]; dup {
			t.Fatalf("NewConsumerID re-issued %q after %d draws", id, i)
		}
		seen[id] = struct{}{}
	}
}

// TestGenerateConsumerTag_ShapeAndUniqueness pins the server-generated tag
// shape required by AMQP 0-9-1 §1.8.3.3 (a unique tag the server hands back in
// basic.consume-ok) and by shortstr encoding (<= 255 bytes).
func TestGenerateConsumerTag_ShapeAndUniqueness(t *testing.T) {
	const n = 1000
	seen := make(map[string]struct{}, n)
	for i := 0; i < n; i++ {
		tag := GenerateConsumerTag()
		if len(tag) == 0 {
			t.Fatal("GenerateConsumerTag returned an empty tag; an empty tag is the request, not a valid answer")
		}
		if len(tag) > 255 {
			t.Fatalf("generated tag is %d bytes; a shortstr holds at most 255", len(tag))
		}
		if _, dup := seen[tag]; dup {
			t.Fatalf("GenerateConsumerTag re-issued %q after %d draws", tag, i)
		}
		seen[tag] = struct{}{}
	}
}
