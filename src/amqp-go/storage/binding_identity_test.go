package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Step 7 D — the binding filename carried the queue-name tier's non-injective
// join, unfixed, one function away from the encoder that fixes it.
//
// makeBindingFilename mapped each component's unsafe bytes onto "_" while using
// a bare "_" as the JOIN separator, and never escaped a "_" already inside a
// component. StoreBinding writes with no occupancy check, so two distinct
// triples landing on one filename meant the second declare silently overwrote
// the first. ListBindings — which LoadAllMetadata calls at boot — then saw only
// the survivor, so the loss became permanent at the next restart with no error
// anywhere.
//
// The assertions below are written at the store's own API (declare every
// triple, then list them back) rather than against makeBindingFilename's
// output, because that is the contract: whatever encoding a future build
// chooses, a binding that was declared must come back as itself.
// ---------------------------------------------------------------------------

// bindingTriple is one (queue, exchange, routing key) identity.
type bindingTriple struct{ queue, exchange, routingKey string }

// collidingBindingTriples is the adversarial corpus for the JOIN. Every pair
// that shares a row group collided into ONE filename under the old "_"-joined
// scheme.
//
// Every entry here must be a triple the OLD join could actually put on disk.
// That is not tidiness, it is what makes the collision arm attributable: a
// mutation restoring the old join has to reach the COLLISION assertion, and a
// triple the old join cannot write fails at DECLARE instead, aborting the test
// before any collision is measured — a red proving nothing about the predicate
// under proof. The entries the old join could not write live in
// unwritableUnderTheOldJoinTriples, and the property is asserted at runtime by
// TestCollidingBindingTriples_AreWritableUnderTheOldJoin rather than trusted.
func collidingBindingTriples() []bindingTriple {
	return []bindingTriple{
		// The join ambiguity, with NO adversarial characters at all — this pair
		// is two ordinary AMQP declarations a real deployment could make.
		{"orders_us", "east", "orders.created"},
		{"orders", "us_east", "orders.created"},

		// The separator migrating across each of the two join positions.
		{"a_b", "c", "d"},
		{"a", "b_c", "d"},
		{"a", "b", "c_d"},
		{"a_b_c", "d", "e"},

		// Characters the old safe() folded onto "_", which collapsed them onto
		// each other AND onto a literal "_".
		{"x/y", "e", "k"},
		{"x\\y", "e", "k"},
		{"x:y", "e", "k"},
		{"x_y", "e", "k"},
		{"x*y", "e", "k"},
		{"x?y", "e", "k"},
		{"x<y", "e", "k"},
		{"x>y", "e", "k"},
		{"x|y", "e", "k"},

		// Traversal, which must not escape the data directory.
		{"../../outside/pwned", "e", "k"},
		{"..", "..", ".."},

		// Empty components: the default exchange and an empty routing key are
		// both legal AMQP, and "" adjacent to a separator is where a join is
		// most likely to become ambiguous.
		{"q", "", ""},
		{"q", "", "k"},
		{"", "e", "k"},
	}
}

// unwritableUnderTheOldJoinTriples are the corpus entries the OLD join could
// not put on disk at all — it folded eight punctuation bytes but not NUL, and
// bounded nothing. They are held apart from collidingBindingTriples for the
// attribution reason stated there, and they are declared and round-tripped by
// TestBindingPath_NamesTheOldJoinCouldNotWrite, so nothing is dropped.
func unwritableUnderTheOldJoinTriples() []bindingTriple {
	return []bindingTriple{
		// A byte no filesystem accepts in a name.
		{"nul\x00byte", "e", "k"},

		// Length: three legal 255-byte AMQP shortstrs. The old join produced a
		// ~772-byte name and failed ENAMETOOLONG on APFS and ext4.
		{strings.Repeat("q", 255), strings.Repeat("e", 255), strings.Repeat("k", 255)},
		{strings.Repeat("q", 255), strings.Repeat("e", 255), strings.Repeat("z", 255)},
	}
}

// oldJoinCouldWrite reports whether a build predating the injective join could
// actually have put this filename on disk: the old join bounded nothing and did
// not fold NUL, so it spells names the filesystem rejects.
//
// This arithmetic deliberately lives in the test rather than in production.
// removeLegacySlot used to carry it as a guard so it could tell "nothing to
// clean" from "cleanup failed"; identity verification subsumed that, and a
// failed read of an impossible path is now simply "not ours". The property is
// still needed HERE, because it is what keeps the collision corpus attributable
// — but it is a fact about this test's corpus, not a rule production obeys.
func oldJoinCouldWrite(filename string) bool {
	return len(filename)+len(TempFileExtension) <= 255 &&
		!strings.ContainsRune(filename, 0)
}

// TestCollidingBindingTriples_AreWritableUnderTheOldJoin states the corpus
// split's invariant once, at runtime, instead of leaving it in the comment on
// the table for the next person to break by appending one row.
//
// Both halves are checked: a corpus entry that the old join could not write
// would silently retire the collision arm, and an entry in the other list that
// the old join COULD write belongs in the corpus rather than off to the side.
func TestCollidingBindingTriples_AreWritableUnderTheOldJoin(t *testing.T) {
	for _, tr := range collidingBindingTriples() {
		name := legacyBindingFilename(tr.queue, tr.exchange, tr.routingKey)
		require.True(t, oldJoinCouldWrite(name),
			"ATTRIBUTION: corpus triple (queue=%q exchange=%q key=%q) spells a %d-byte legacy "+
				"name the old join could not write, so a mutation restoring that join fails this "+
				"triple's DECLARE and the COLLISION assertion never runs — move it to "+
				"unwritableUnderTheOldJoinTriples", tr.queue, tr.exchange, tr.routingKey, len(name))
	}

	for _, tr := range unwritableUnderTheOldJoinTriples() {
		name := legacyBindingFilename(tr.queue, tr.exchange, tr.routingKey)
		require.False(t, oldJoinCouldWrite(name),
			"(queue=%q exchange=%q key=%q) IS writable under the old join, so it belongs in the "+
				"collision corpus where it can carry a collision assertion",
			tr.queue, tr.exchange, tr.routingKey)
	}
}

// TestBindingPath_NamesTheOldJoinCouldNotWrite covers the entries held out of
// the collision corpus. The property is the same — declare it, get it back as
// itself — but these are the names the old join FAILED on rather than collided
// on, so their gate is that they now persist at all.
func TestBindingPath_NamesTheOldJoinCouldNotWrite(t *testing.T) {
	dataDir, canaryDir := canaryTree(t)
	pm, err := NewPersistentMetadataStore(dataDir)
	require.NoError(t, err)

	triples := unwritableUnderTheOldJoinTriples()
	for i, tr := range triples {
		require.NoError(t,
			pm.StoreBinding(tr.queue, tr.exchange, tr.routingKey,
				map[string]interface{}{"ordinal": i}),
			"declare %d (queue=%d bytes exchange=%d bytes key=%d bytes)",
			i, len(tr.queue), len(tr.exchange), len(tr.routingKey))
		require.NoError(t,
			pm.StoreExchangeBinding(tr.queue, tr.exchange, tr.routingKey,
				map[string]interface{}{"ordinal": i}),
			"exchange-binding declare %d", i)
	}

	entries, err := os.ReadDir(canaryDir)
	require.NoError(t, err)
	require.Empty(t, entries,
		"ESCAPE: binding files landed outside the data directory: %v", entries)

	got, err := pm.ListBindings()
	require.NoError(t, err)

	seen := make(map[bindingTriple]int, len(got))
	for _, b := range got {
		seen[bindingTriple{b.QueueName, b.ExchangeName, b.RoutingKey}]++
	}
	for _, tr := range triples {
		require.Equal(t, 1, seen[tr],
			"binding (queue=%d bytes exchange=%d bytes key=%d bytes) came back %d times, want 1",
			len(tr.queue), len(tr.exchange), len(tr.routingKey), seen[tr])
	}
	require.Len(t, got, len(triples),
		"%d triples were declared but %d came back", len(triples), len(got))
}

// TestBindingPath_DistinctTriplesNeverShareAFile is the collision gate. Each
// triple is declared, then every one must come back from ListBindings as
// itself. A collision presents as a MISSING triple, because the colliding
// declare overwrote its file.
func TestBindingPath_DistinctTriplesNeverShareAFile(t *testing.T) {
	dataDir, canaryDir := canaryTree(t)
	pm, err := NewPersistentMetadataStore(dataDir)
	require.NoError(t, err)

	triples := collidingBindingTriples()

	// PREMISE: the corpus must actually contain colliding pairs, or this test
	// asserts nothing. Assert it at runtime rather than trusting the table.
	require.Greater(t, len(triples), 1, "PREMISE: corpus must hold more than one triple")

	for i, tr := range triples {
		require.NoError(t,
			pm.StoreBinding(tr.queue, tr.exchange, tr.routingKey,
				map[string]interface{}{"ordinal": i}),
			"declare %d %+v", i, tr)
	}

	entries, err := os.ReadDir(canaryDir)
	require.NoError(t, err)
	require.Empty(t, entries,
		"ESCAPE: binding files landed outside the data directory: %v", entries)

	got, err := pm.ListBindings()
	require.NoError(t, err)

	seen := make(map[bindingTriple]int, len(got))
	for _, b := range got {
		seen[bindingTriple{b.QueueName, b.ExchangeName, b.RoutingKey}]++
	}

	for _, tr := range triples {
		require.Equal(t, 1, seen[tr],
			"COLLISION: binding (queue=%q exchange=%q key=%q) came back %d times, want exactly 1 — "+
				"another triple's declare overwrote its file, and LoadAllMetadata would lose it at boot",
			tr.queue, tr.exchange, tr.routingKey, seen[tr])
	}
	require.Len(t, got, len(triples),
		"COLLISION: %d triples were declared but %d came back", len(triples), len(got))
}

// TestExchangeBindingPath_DistinctTriplesNeverShareAFile covers the
// exchange-to-exchange twin, which carried a byte-for-byte copy of the same
// join with the same absent occupancy check, on the same boot path.
func TestExchangeBindingPath_DistinctTriplesNeverShareAFile(t *testing.T) {
	dataDir, canaryDir := canaryTree(t)
	pm, err := NewPersistentMetadataStore(dataDir)
	require.NoError(t, err)

	triples := collidingBindingTriples()
	for i, tr := range triples {
		require.NoError(t,
			pm.StoreExchangeBinding(tr.queue, tr.exchange, tr.routingKey,
				map[string]interface{}{"ordinal": i}),
			"declare %d %+v", i, tr)
	}

	entries, err := os.ReadDir(canaryDir)
	require.NoError(t, err)
	require.Empty(t, entries,
		"ESCAPE: exchange-binding files landed outside the data directory: %v", entries)

	// GetExchangeBindingsFrom is the reader on the routing path. Group the
	// corpus by source so each source's full set can be checked.
	wantBySource := make(map[string]map[bindingTriple]bool)
	for _, tr := range triples {
		if wantBySource[tr.queue] == nil {
			wantBySource[tr.queue] = make(map[bindingTriple]bool)
		}
		wantBySource[tr.queue][tr] = true
	}

	for source, want := range wantBySource {
		got, err := pm.GetExchangeBindingsFrom(source)
		require.NoError(t, err)

		seen := make(map[bindingTriple]int, len(got))
		for _, b := range got {
			seen[bindingTriple{b.Source, b.Destination, b.RoutingKey}]++
		}
		for tr := range want {
			require.Equal(t, 1, seen[tr],
				"COLLISION: exchange binding (source=%q dest=%q key=%q) came back %d times, want exactly 1",
				tr.queue, tr.exchange, tr.routingKey, seen[tr])
		}
		require.Len(t, got, len(want),
			"COLLISION: source %q had %d bindings declared but %d came back", source, len(want), len(got))
	}
}

// TestBindingPath_OrdinaryNamesKeepTheirExistingFile is the N-1 compatibility
// boundary. Changing a filename encoding orphans whatever the old build wrote,
// so the new encoding must be a no-op for exactly the components the old join
// was already unambiguous for: literal, and containing no "_". A binding
// declared by the previous build under an ordinary name must still be found by
// this one.
func TestBindingPath_OrdinaryNamesKeepTheirExistingFile(t *testing.T) {
	ordinary := []bindingTriple{
		{"orders", "amq.direct", "orders.created"},
		{"q1", "ex1", "rk1"},
		{"a.b.c", "d.e.f", "g.h.i"},
	}

	for _, tr := range ordinary {
		got := makeBindingFilename(tr.queue, tr.exchange, tr.routingKey)
		want := tr.queue + "_" + tr.exchange + "_" + tr.routingKey + FileExtension
		require.Equal(t, want, got,
			"N-1 BOUNDARY: (queue=%q exchange=%q key=%q) changed spelling, so a binding the "+
				"previous build wrote is orphaned on upgrade", tr.queue, tr.exchange, tr.routingKey)
	}
}

// TestBindingPath_FilenamesFitNameMax asserts the length bound directly, since
// ENAMETOOLONG is an errno the round-trip tests above would surface only as a
// declare failure with a less specific message.
func TestBindingPath_FilenamesFitNameMax(t *testing.T) {
	maxName := strings.Repeat("n", 255)

	for _, name := range []string{
		makeBindingFilename(maxName, maxName, maxName),
		makeExchangeBindingFilename(maxName, maxName, maxName),
	} {
		// atomicWriteFile writes through a sibling carrying TempFileExtension,
		// so the temp spelling is the one that has to fit, not the final one.
		require.LessOrEqual(t, len(name)+len(TempFileExtension), 255,
			"ENAMETOOLONG: %d-byte filename (with %q) exceeds NAME_MAX for three legal "+
				"255-byte AMQP shortstrs", len(name)+len(TempFileExtension), TempFileExtension)
	}
}

// TestBindingCacheKey_DistinctTriplesNeverShareAKey covers the second copy of
// the same collision. The bare "%s:%s:%s" join meant ("q","a:b","c") and
// ("q:a","b","c") keyed the same entry, so one binding's cache entry displaced
// another's and DeleteBinding could evict an entry belonging to a different
// triple.
func TestBindingCacheKey_DistinctTriplesNeverShareAKey(t *testing.T) {
	triples := append(collidingBindingTriples(),
		bindingTriple{"q", "a:b", "c"},
		bindingTriple{"q:a", "b", "c"},
		bindingTriple{"q:a:b", "", "c"},
	)

	keys := make(map[string]bindingTriple, len(triples))
	for _, tr := range triples {
		k := makeBindingCacheKey(tr.queue, tr.exchange, tr.routingKey)
		if prev, dup := keys[k]; dup {
			t.Fatalf("COLLISION: (queue=%q exchange=%q key=%q) and (queue=%q exchange=%q key=%q) "+
				"share cache key %q", prev.queue, prev.exchange, prev.routingKey,
				tr.queue, tr.exchange, tr.routingKey, k)
		}
		keys[k] = tr
	}
}

// TestBindingPath_UnbindFindsARecordWrittenUnderTheOldSpelling is the upgrade
// gate for changing a filename encoding.
//
// DeleteBinding builds the filename from the triple and swallows IsNotExist. So
// on its own, a new encoding turns unbind into a SILENT NO-OP for every binding
// the previous build spelled differently: os.Remove misses, the error is
// swallowed, DeleteBinding reports success, and ListBindings keeps returning the
// binding across every subsequent restart. An unbind that reports success and
// does nothing is worse than one that fails loudly.
//
// The fallback resolves by identity — the record names its owner, the filename
// is only a slot — so this test parks a record in a slot DeleteBinding does not
// compute and requires unbind to find it.
func TestBindingPath_UnbindFindsARecordWrittenUnderTheOldSpelling(t *testing.T) {
	dataDir, _ := canaryTree(t)
	pm, err := NewPersistentMetadataStore(dataDir)
	require.NoError(t, err)

	// A triple whose components contain "_", so the new encoding and the old
	// one disagree — which is exactly the population at risk.
	const q, ex, rk = "a_b", "c", "d"
	require.NoError(t, pm.StoreBinding(q, ex, rk, map[string]interface{}{"n": 1}))

	dir := filepath.Join(dataDir, MetadataDir, BindingsDir)
	files := filesUnder(t, dir)
	require.Len(t, files, 1, "fixture must produce exactly one binding file, got %v", files)

	// A slot derived from the canonical name at runtime, NOT the literal old
	// spelling. Asserting "the old and new spellings differ" would be a premise
	// measuring the encoding this change alters — it reads one way on a fixed
	// tree and another on a broken one, which makes it an outcome assertion
	// wearing a premise's clothes, and it reddens on any mutation that touches
	// the encoder rather than on the fallback under proof.
	//
	// What the fallback actually promises is wider than the old spelling:
	// identity resolves from ANY slot, because the record names its owner. So
	// that is what gets exercised, and the premise below holds under every
	// encoding.
	foreign := "slot-an-older-build-chose-" + files[0]
	require.NoError(t, os.Rename(filepath.Join(dir, files[0]), filepath.Join(dir, foreign)))
	require.NoFileExists(t, filepath.Join(dir, makeBindingFilename(q, ex, rk)),
		"PREMISE: the record must not sit in the slot DeleteBinding computes, or this "+
			"fixture cannot exercise the identity fallback at all")

	require.NoError(t, pm.DeleteBinding(q, ex, rk))

	got, err := pm.ListBindings()
	require.NoError(t, err)
	require.Empty(t, got,
		"SILENT NO-OP UNBIND: a binding written under the previous build's filename survived "+
			"DeleteBinding, which returned success — it will come back at every restart")
}

// TestBindingPath_RedeclareAfterUpgradeRetiresTheOldSpellingsFile is the STORE
// side of the same upgrade event the unbind test covers, and it is reached by
// the ordinary sequence: an older build binds a triple, the broker is upgraded,
// and a client re-declares that same binding.
//
// StoreBinding writes under the current spelling without looking for the old
// one, and ListBindings does not dedupe by identity — it appends every file
// that decodes — so the directory ends up holding TWO files for one triple.
//
// That is not a double delivery. The routing path dedupes by queue name before
// enqueuing, so a duplicate changes no message's fate. The harm is on the
// delete side: DeleteBinding removes the CURRENT filename first and only falls
// back to the identity scan when that returns IsNotExist, so with both files
// present the legacy twin is never looked for. Unbind returns success, the
// binding is still returned by ListBindings on this run and after every
// restart, and messages keep routing to a queue the client unbound. The last
// assertion here is the one that measures that, and it is why this is a
// routing-correctness defect rather than a boot artifact.
func TestBindingPath_RedeclareAfterUpgradeRetiresTheOldSpellingsFile(t *testing.T) {
	dataDir, _ := canaryTree(t)
	pm, err := NewPersistentMetadataStore(dataDir)
	require.NoError(t, err)

	const q, ex, rk = "a_b", "c", "d"
	dir := filepath.Join(dataDir, MetadataDir, BindingsDir)

	// Seed the directory as an older build left it: write the record through
	// the store so its bytes are exactly what the store writes, then move it
	// into the slot that build would have used.
	require.NoError(t, pm.StoreBinding(q, ex, rk, map[string]interface{}{"n": 1}))
	seeded := filesUnder(t, dir)
	require.Len(t, seeded, 1, "fixture must seed exactly one binding file, got %v", seeded)
	require.NoError(t, os.Rename(
		filepath.Join(dir, seeded[0]),
		filepath.Join(dir, legacyBindingFilename(q, ex, rk))))
	require.Len(t, filesUnder(t, dir), 1,
		"PREMISE: the seeded directory must hold exactly one record for this triple")

	// The re-declare.
	require.NoError(t, pm.StoreBinding(q, ex, rk, map[string]interface{}{"n": 2}))

	require.Len(t, filesUnder(t, dir), 1,
		"DUPLICATE: re-declaring a binding an older build wrote left %v on disk — two files "+
			"decoding to one triple", filesUnder(t, dir))

	got, err := pm.ListBindings()
	require.NoError(t, err)
	require.Len(t, got, 1, "DUPLICATE: one triple came back %d times from ListBindings", len(got))

	// The harm the duplicate actually causes: DeleteBinding's fast path removes
	// the current filename and never scans for the twin.
	require.NoError(t, pm.DeleteBinding(q, ex, rk))
	got, err = pm.ListBindings()
	require.NoError(t, err)
	require.Empty(t, got,
		"SILENT NO-OP UNBIND: DeleteBinding reported success and the binding is still here — "+
			"it survives this run and every restart, and the routing path keeps delivering to a "+
			"queue the client unbound")
}

// TestExchangeBindingPath_RedeclareAfterUpgradeRetiresTheOldSpellingsFile is
// the exchange-to-exchange twin. The two tiers share one legacy spelling
// because the old encoder was copied byte for byte, and they share the
// delete-side fast path, so they share the defect.
func TestExchangeBindingPath_RedeclareAfterUpgradeRetiresTheOldSpellingsFile(t *testing.T) {
	dataDir, _ := canaryTree(t)
	pm, err := NewPersistentMetadataStore(dataDir)
	require.NoError(t, err)

	const src, dst, rk = "a_b", "c", "d"
	dir := filepath.Join(dataDir, MetadataDir, ExchangeBindingsDir)

	require.NoError(t, pm.StoreExchangeBinding(src, dst, rk, map[string]interface{}{"n": 1}))
	seeded := filesUnder(t, dir)
	require.Len(t, seeded, 1, "fixture must seed exactly one binding file, got %v", seeded)
	require.NoError(t, os.Rename(
		filepath.Join(dir, seeded[0]),
		filepath.Join(dir, legacyBindingFilename(src, dst, rk))))
	require.Len(t, filesUnder(t, dir), 1,
		"PREMISE: the seeded directory must hold exactly one record for this triple")

	require.NoError(t, pm.StoreExchangeBinding(src, dst, rk, map[string]interface{}{"n": 2}))

	require.Len(t, filesUnder(t, dir), 1,
		"DUPLICATE: re-declaring an exchange binding an older build wrote left %v on disk",
		filesUnder(t, dir))

	got, err := pm.GetExchangeBindingsFrom(src)
	require.NoError(t, err)
	require.Len(t, got, 1,
		"DUPLICATE: one triple came back %d times from GetExchangeBindingsFrom", len(got))

	require.NoError(t, pm.DeleteExchangeBinding(src, dst, rk))
	got, err = pm.GetExchangeBindingsFrom(src)
	require.NoError(t, err)
	require.Empty(t, got,
		"SILENT NO-OP UNBIND: DeleteExchangeBinding reported success and the binding is still here")
}

// TestBindingPath_EveryDeclaredTripleIsOnDiskExactlyOnce checks the filesystem
// directly rather than through the store's own reader, so a bug that lost a
// file and a bug that lost it only from ListBindings cannot present alike.
func TestBindingPath_EveryDeclaredTripleIsOnDiskExactlyOnce(t *testing.T) {
	dataDir, _ := canaryTree(t)
	pm, err := NewPersistentMetadataStore(dataDir)
	require.NoError(t, err)

	triples := collidingBindingTriples()
	for i, tr := range triples {
		require.NoError(t, pm.StoreBinding(tr.queue, tr.exchange, tr.routingKey,
			map[string]interface{}{"ordinal": i}))
	}

	files := filesUnder(t, filepath.Join(dataDir, MetadataDir, BindingsDir))
	require.Len(t, files, len(triples),
		"COLLISION: %d triples were declared but %d files exist on disk: %v",
		len(triples), len(files), files)
}

// ---------------------------------------------------------------------------
// The cross-triple eviction class. removeLegacySlot's first version unlinked
// the legacy slot on the strength of its NAME, and one triple's legacy name can
// equal a DIFFERENT triple's current name — so declaring one ordinary binding
// destroyed another one that was already live.
//
// The tests above could not see it: every one of them declares a set of
// triples and asks whether they all came back. That detects a write landing in
// the wrong place. It cannot detect a write that lands correctly and takes a
// BYSTANDER with it, because a corpus of well-formed singletons has no
// bystander in it. These fixtures supply the decoy.
// ---------------------------------------------------------------------------

// legacyFoldedBytes are the eight characters the pre-injective-join encoder
// mapped onto "_". Each one, alone in a component, makes that component's
// legacy spelling "_" — which is exactly what filenameComponent emits for an
// EMPTY component, and that is the collision.
var legacyFoldedBytes = []string{"/", "\\", ":", "*", "?", "<", ">", "|"}

// evictionPair is a live binding and the ordinary declare that used to delete
// it: `victim` is stored first, then `attacker`, whose legacy spelling names
// the victim's file.
type evictionPair struct {
	victim, attacker bindingTriple
	why              string
}

// crossTripleEvictionPairs enumerates the class rather than the one instance
// review found. Every folded byte, in every one of the three join positions,
// plus a hex-tail case that involves no empty component at all.
func crossTripleEvictionPairs() []evictionPair {
	var pairs []evictionPair
	for _, c := range legacyFoldedBytes {
		pairs = append(pairs,
			evictionPair{
				victim:   bindingTriple{"", "ex", "key"},
				attacker: bindingTriple{c, "ex", "key"},
				why:      "folded byte in the queue position",
			},
			evictionPair{
				victim:   bindingTriple{"q", "", "key"},
				attacker: bindingTriple{"q", c, "key"},
				why:      "folded byte in the exchange position",
			},
			evictionPair{
				victim:   bindingTriple{"q", "ex", ""},
				attacker: bindingTriple{"q", "ex", c},
				why:      "folded byte in the routing-key position",
			})
	}

	// No empty component anywhere: the fold turns ":" into "_", leaving
	// "_615f62" — byte-identical to the "_"+hex escape filenameComponent emits
	// for "a_b". Held separately because the empty-component cases would all
	// pass a fix that only special-cased "".
	pairs = append(pairs, evictionPair{
		victim:   bindingTriple{"a_b", "ex", "key"},
		attacker: bindingTriple{":615f62", "ex", "key"},
		why:      "legacy fold synthesises the current encoder's escape shape",
	})
	return pairs
}

// TestBindingPath_DeclaringOneBindingNeverEvictsAnother is the gate for the
// class. Two triples, both declared, both must survive.
func TestBindingPath_DeclaringOneBindingNeverEvictsAnother(t *testing.T) {
	for _, p := range crossTripleEvictionPairs() {
		t.Run(p.why+"/"+p.attacker.queue+p.attacker.exchange+p.attacker.routingKey, func(t *testing.T) {
			dataDir, _ := canaryTree(t)
			pm, err := NewPersistentMetadataStore(dataDir)
			require.NoError(t, err)

			// PREMISE: the pair must actually collide, or this case proves
			// nothing. Asserted from the two production encoders, which is
			// invariant under the fix — verification changes which file is
			// unlinked, never what either encoder spells.
			require.Equal(t,
				makeBindingFilename(p.victim.queue, p.victim.exchange, p.victim.routingKey),
				legacyBindingFilename(p.attacker.queue, p.attacker.exchange, p.attacker.routingKey),
				"PREMISE: %+v's current spelling must equal %+v's legacy spelling, "+
					"or this pair cannot exercise the eviction", p.victim, p.attacker)

			require.NoError(t, pm.StoreBinding(
				p.victim.queue, p.victim.exchange, p.victim.routingKey,
				map[string]interface{}{"who": "victim"}))
			require.NoError(t, pm.StoreBinding(
				p.attacker.queue, p.attacker.exchange, p.attacker.routingKey,
				map[string]interface{}{"who": "attacker"}))

			got, err := pm.ListBindings()
			require.NoError(t, err)

			seen := make(map[bindingTriple]bool, len(got))
			for _, b := range got {
				seen[bindingTriple{b.QueueName, b.ExchangeName, b.RoutingKey}] = true
			}
			require.True(t, seen[p.victim],
				"EVICTED: declaring (queue=%q exchange=%q key=%q) destroyed the live binding "+
					"(queue=%q exchange=%q key=%q) — %s. Two ordinary AMQP declares, no upgrade, "+
					"and the routing path stops delivering to a queue nobody unbound",
				p.attacker.queue, p.attacker.exchange, p.attacker.routingKey,
				p.victim.queue, p.victim.exchange, p.victim.routingKey, p.why)
			require.True(t, seen[p.attacker], "the declare under test did not persist")
			require.Len(t, got, 2, "expected exactly two bindings, got %d", len(got))
		})
	}
}

// TestExchangeBindingPath_DeclaringOneBindingNeverEvictsAnother is the
// exchange-to-exchange twin: same shared legacy spelling, same encoder, so the
// tier inherits the defect and needs its own gate.
func TestExchangeBindingPath_DeclaringOneBindingNeverEvictsAnother(t *testing.T) {
	for _, p := range crossTripleEvictionPairs() {
		t.Run(p.why+"/"+p.attacker.queue+p.attacker.exchange+p.attacker.routingKey, func(t *testing.T) {
			dataDir, _ := canaryTree(t)
			pm, err := NewPersistentMetadataStore(dataDir)
			require.NoError(t, err)

			// PREMISE, the exchange tier's own — NOT inherited from the queue
			// tier's copy above. The corpus is shared but the encoder is not:
			// crossTripleEvictionPairs collides under makeBindingFilename, and
			// this test's subject is makeExchangeBindingFilename. Without this
			// line the case is VACUOUS the moment those two diverge — the pair
			// stops colliding, no eviction is possible, the two-file assertion
			// below holds trivially, and it reports PASS. Measured: reverting
			// the exchange encoder alone left this test green 25/25 while its
			// precondition was false (review-9 F-6).
			//
			// That is not hypothetical. The structural repair this tier still
			// owes — a %escaped/ subdirectory making the namespaces disjoint,
			// as metadataEscapeDir already does one tier over — changes exactly
			// this encoder, so the next intentional change here is the one that
			// would have silently retired the gate.
			require.Equal(t,
				makeExchangeBindingFilename(p.victim.queue, p.victim.exchange, p.victim.routingKey),
				legacyBindingFilename(p.attacker.queue, p.attacker.exchange, p.attacker.routingKey),
				"PREMISE: %+v's current exchange-binding spelling must equal %+v's legacy "+
					"spelling, or this pair cannot exercise the eviction", p.victim, p.attacker)

			require.NoError(t, pm.StoreExchangeBinding(
				p.victim.queue, p.victim.exchange, p.victim.routingKey,
				map[string]interface{}{"who": "victim"}))
			require.NoError(t, pm.StoreExchangeBinding(
				p.attacker.queue, p.attacker.exchange, p.attacker.routingKey,
				map[string]interface{}{"who": "attacker"}))

			files := filesUnder(t, filepath.Join(dataDir, MetadataDir, ExchangeBindingsDir))
			require.Len(t, files, 2,
				"EVICTED: declaring (source=%q dest=%q key=%q) left %v on disk — it destroyed "+
					"the live exchange binding (source=%q dest=%q key=%q); %s",
				p.attacker.queue, p.attacker.exchange, p.attacker.routingKey, files,
				p.victim.queue, p.victim.exchange, p.victim.routingKey, p.why)
		})
	}
}

// TestBindingComponentBound_KeepsTheEscapePrefix asserts the premise that
// filenameComponent's truncation branch depends on, at the bound this package
// actually passes it.
//
// Below maxLen 66 the branch computes full[:maxLen-65] and silently drops the
// leading "_", so a generated component stops being distinguishable from a
// literal one and the join stops being injective; below 65 it panics. Its
// sibling metadataEscapedName carries a keep<1 guard for the same shape, but a
// guard on an unreachable branch is code no mutation can attribute — Step 6's
// redundant-guard finding. The reachable risk is not a hostile caller, it is
// someone LOWERING this constant later, and that is what this asserts.
func TestBindingComponentBound_KeepsTheEscapePrefix(t *testing.T) {
	const sha256HexLen = 64
	require.Greater(t, bindingComponentMaxLen, sha256HexLen+1,
		"bindingComponentMaxLen=%d leaves filenameComponent's truncation branch no room for the "+
			"leading %q: at %d it drops the escape prefix and breaks injectivity SILENTLY, and "+
			"below %d it panics",
		bindingComponentMaxLen, "_", sha256HexLen+1, sha256HexLen+1)

	// The property itself, not just the arithmetic: a component long enough to
	// take the truncation branch must still be marked as generated.
	long := strings.Repeat("z", bindingComponentMaxLen*2)
	require.True(t, strings.HasPrefix(filenameComponent(long, bindingComponentMaxLen), "_"),
		"a truncated component lost its %q prefix, so it can no longer be told apart from a "+
			"literal one and the three-part join is no longer injective", "_")
}
