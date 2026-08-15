package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	bet0DrainIdentityBytes = 16
	bet0DrainIdentityMagic = uint32(0x42304452) // "B0DR"
	bet0MissingSampleLimit = 8
)

// bet0DrainIdentity names one publication without a shared producer-side
// counter. A producer advances seq only after Publish returns nil, so its
// frozen successful target is the prefix [0, successCount).
type bet0DrainIdentity struct {
	queue    uint16
	producer uint16
	seq      uint64
}

func bet0EncodeDrainIdentity(body []byte, id bet0DrainIdentity) {
	if len(body) < bet0DrainIdentityBytes {
		return
	}
	binary.BigEndian.PutUint32(body[0:4], bet0DrainIdentityMagic)
	binary.BigEndian.PutUint16(body[4:6], id.queue)
	binary.BigEndian.PutUint16(body[6:8], id.producer)
	binary.BigEndian.PutUint64(body[8:16], id.seq)
}

func bet0DecodeDrainIdentity(body []byte) (bet0DrainIdentity, bool) {
	if len(body) < bet0DrainIdentityBytes || binary.BigEndian.Uint32(body[0:4]) != bet0DrainIdentityMagic {
		return bet0DrainIdentity{}, false
	}
	return bet0DrainIdentity{
		queue:    binary.BigEndian.Uint16(body[4:6]),
		producer: binary.BigEndian.Uint16(body[6:8]),
		seq:      binary.BigEndian.Uint64(body[8:16]),
	}, true
}

type bet0DrainTarget struct {
	queue           uint16
	successPrefixes []uint64
	total           int64
}

func newBet0DrainTarget(queue uint16, prefixes []uint64) bet0DrainTarget {
	target := bet0DrainTarget{queue: queue, successPrefixes: append([]uint64(nil), prefixes...)}
	for _, prefix := range target.successPrefixes {
		target.total += int64(prefix)
	}
	return target
}

func (t bet0DrainTarget) contains(id bet0DrainIdentity) bool {
	return id.queue == t.queue && int(id.producer) < len(t.successPrefixes) && id.seq < t.successPrefixes[id.producer]
}

// bet0ObservedIdentities retains observations that can precede publisher target
// updates. FreezeTarget installs the immutable successful-publish target under
// the same lock as Add, making ExpectedCount an exact, monotonic inclusion
// count rather than a raw-delivery cardinality.
type bet0ObservedIdentities struct {
	mu                     sync.Mutex
	seen                   map[bet0DrainIdentity]struct{}
	target                 *bet0DrainTarget
	expectedObserved       int64
	changed                chan<- struct{}
	beforeExpectedSnapshot func(*bet0ObservedIdentities) // test seam; invoked with mu held
}

func newBet0ObservedIdentities(changed chan<- struct{}) *bet0ObservedIdentities {
	return &bet0ObservedIdentities{
		seen:    make(map[bet0DrainIdentity]struct{}),
		changed: changed,
	}
}

func (s *bet0ObservedIdentities) Add(id bet0DrainIdentity) bool {
	s.mu.Lock()
	if _, duplicate := s.seen[id]; duplicate {
		s.mu.Unlock()
		return false
	}
	s.seen[id] = struct{}{}
	if s.target != nil && s.target.contains(id) {
		s.expectedObserved++
	}
	s.mu.Unlock()
	if s.changed != nil {
		select {
		case s.changed <- struct{}{}:
		default:
		}
	}
	return true
}

func (s *bet0ObservedIdentities) FreezeTarget(target bet0DrainTarget) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.target = &target
	s.expectedObserved = 0
	for id := range s.seen {
		if target.contains(id) {
			s.expectedObserved++
		}
	}
}

func (s *bet0ObservedIdentities) ExpectedCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.expectedObserved
}

func (s *bet0ObservedIdentities) expectedSnapshot(limit int) (int64, []bet0DrainIdentity) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.beforeExpectedSnapshot != nil {
		s.beforeExpectedSnapshot(s)
	}
	if s.target == nil || limit <= 0 {
		return s.expectedObserved, nil
	}
	missing := make([]bet0DrainIdentity, 0, limit)
	for producer, prefix := range s.target.successPrefixes {
		for seq := uint64(0); seq < prefix && len(missing) < limit; seq++ {
			id := bet0DrainIdentity{queue: s.target.queue, producer: uint16(producer), seq: seq}
			if _, ok := s.seen[id]; !ok {
				missing = append(missing, id)
			}
		}
		if len(missing) == limit {
			break
		}
	}
	return s.expectedObserved, missing
}

func (s *bet0ObservedIdentities) MissingSample(limit int) []bet0DrainIdentity {
	_, missing := s.expectedSnapshot(limit)
	return missing
}

// bet0ObserveDrainBody rejects identities outside the consumer's queue and
// producer namespace. Successful-prefix membership is deliberately evaluated
// only after publisher join and target freeze.
func bet0ObserveDrainBody(observed *bet0ObservedIdentities, queue uint16, producers int, body []byte) bool {
	id, ok := bet0DecodeDrainIdentity(body)
	if !ok || id.queue != queue || int(id.producer) >= producers {
		return false
	}
	return observed.Add(id)
}

// bet0AckAndObserve records an identity only after the delivery Ack succeeds.
func bet0AckAndObserve(ack func() error, observed *bet0ObservedIdentities, queue uint16, producers int, body []byte) error {
	if err := ack(); err != nil {
		return err
	}
	bet0ObserveDrainBody(observed, queue, producers, body)
	return nil
}

type bet0DrainVerdict int

const (
	bet0DrainContinue bet0DrainVerdict = iota
	bet0DrainDone
	bet0DrainStalled
)

type bet0DrainSnapshot struct {
	verdict             bet0DrainVerdict
	observed            int64
	target              int64
	missing             int64
	missingSample       []bet0DrainIdentity
	inactivity          time.Duration
	inactivityThreshold time.Duration
	maxExpectedGap      time.Duration
}

// bet0DrainState has no total deadline. Its injected elapsed time measures only
// inactivity since the last newly observed identity in the frozen target.
type bet0DrainState struct {
	target              bet0DrainTarget
	observed            *bet0ObservedIdentities
	window              time.Duration
	initialized         bool
	lastCount           int64
	lastProgress        time.Duration
	maxExpectedGap      time.Duration
	beforeStallSnapshot func() // deterministic test seam for the cheap-count/final-snapshot interleaving
}

func newBet0DrainState(target bet0DrainTarget, observed *bet0ObservedIdentities, window time.Duration) *bet0DrainState {
	observed.FreezeTarget(target)
	return &bet0DrainState{target: target, observed: observed, window: window}
}

// Observe takes a fresh exact-target snapshot on every call. In particular, a
// timer firing cannot report a stale stall: if the expected missing set shrank,
// this call records progress and resets the inactivity baseline first.
func (s *bet0DrainState) Observe(elapsed time.Duration) bet0DrainSnapshot {
	count := s.observed.ExpectedCount()
	if !s.initialized {
		s.initialized = true
		s.lastCount = count
		s.lastProgress = elapsed
		if count >= s.target.total {
			return s.snapshot(bet0DrainDone, elapsed, count)
		}
		return s.snapshot(bet0DrainContinue, elapsed, count)
	}
	if count > s.lastCount {
		gap := elapsed - s.lastProgress
		if gap > s.maxExpectedGap {
			s.maxExpectedGap = gap
		}
		s.lastCount = count
		s.lastProgress = elapsed
		if count >= s.target.total {
			return s.snapshot(bet0DrainDone, elapsed, count)
		}
		return s.snapshot(bet0DrainContinue, elapsed, count)
	}
	if count >= s.target.total {
		return s.snapshot(bet0DrainDone, elapsed, count)
	}
	if elapsed-s.lastProgress >= s.window {
		if s.beforeStallSnapshot != nil {
			s.beforeStallSnapshot()
		}
		// Count, completion, and missing evidence come from one lock acquisition.
		// This is the linearization point for a possible STALLED verdict.
		freshCount, missingSample := s.observed.expectedSnapshot(bet0MissingSampleLimit)
		if freshCount > s.lastCount {
			gap := elapsed - s.lastProgress
			if gap > s.maxExpectedGap {
				s.maxExpectedGap = gap
			}
			s.lastCount = freshCount
			s.lastProgress = elapsed
			if freshCount >= s.target.total {
				return s.snapshot(bet0DrainDone, elapsed, freshCount)
			}
			return s.snapshot(bet0DrainContinue, elapsed, freshCount)
		}
		if freshCount >= s.target.total {
			return s.snapshot(bet0DrainDone, elapsed, freshCount)
		}
		snapshot := s.snapshot(bet0DrainStalled, elapsed, freshCount)
		snapshot.missingSample = missingSample
		return snapshot
	}
	return s.snapshot(bet0DrainContinue, elapsed, count)
}

func (s *bet0DrainState) snapshot(verdict bet0DrainVerdict, elapsed time.Duration, count int64) bet0DrainSnapshot {
	return bet0DrainSnapshot{
		verdict:             verdict,
		observed:            count,
		target:              s.target.total,
		missing:             s.target.total - count,
		inactivity:          elapsed - s.lastProgress,
		inactivityThreshold: s.window,
		maxExpectedGap:      s.maxExpectedGap,
	}
}

func (s *bet0DrainState) RemainingInactivity(elapsed time.Duration) time.Duration {
	remaining := s.window - (elapsed - s.lastProgress)
	if remaining < 0 {
		return 0
	}
	return remaining
}

func bet0DrainDiagnostic(queue string, snapshot bet0DrainSnapshot) string {
	return fmt.Sprintf("queue %s STALLED with %d expected identities still missing: observed=%d target=%d "+
		"missing sample=%v inactivity=%s threshold=%s", queue, snapshot.missing, snapshot.observed,
		snapshot.target, snapshot.missingSample, snapshot.inactivity, snapshot.inactivityThreshold)
}

// bet0CheckBeforePublisherJoin preserves P0 attribution when a publisher is
// wedged: a failing frozen-window check returns without invoking the join.
func bet0CheckBeforePublisherJoin(check func() error, wait func()) error {
	if err := check(); err != nil {
		return err
	}
	wait()
	return nil
}

// bet0FreezePublisherTargets is deliberately seam-injected: waiting happens
// before the first prefix load, so the frozen target cannot race a publisher's
// post-Publish successful-count update.
func bet0FreezePublisherTargets(wait func(), queues, producers int, load func(queue, producer int) uint64) []bet0DrainTarget {
	wait()
	targets := make([]bet0DrainTarget, queues)
	for queue := 0; queue < queues; queue++ {
		prefixes := make([]uint64, producers)
		for producer := 0; producer < producers; producer++ {
			prefixes[producer] = load(queue, producer)
		}
		targets[queue] = newBet0DrainTarget(uint16(queue), prefixes)
	}
	return targets
}

func bet0ValidatePublisherTargets(targets []bet0DrainTarget, published []int64, expectedProducers int) error {
	if len(targets) != len(published) {
		return fmt.Errorf("publisher target count %d does not match result count %d", len(targets), len(published))
	}
	for queue, target := range targets {
		if target.queue != uint16(queue) {
			return fmt.Errorf("queue %d target namespace is %d", queue, target.queue)
		}
		if len(target.successPrefixes) != expectedProducers {
			return fmt.Errorf("queue %d producer prefix count %d does not match expected %d",
				queue, len(target.successPrefixes), expectedProducers)
		}
		if target.total != published[queue] {
			return fmt.Errorf("queue %d target total %d does not match published total %d",
				queue, target.total, published[queue])
		}
		if target.total == 0 {
			return fmt.Errorf("queue %d target total is zero", queue)
		}
		for producer, prefix := range target.successPrefixes {
			if prefix == 0 {
				return fmt.Errorf("queue %d producer %d successful prefix is zero", queue, producer)
			}
		}
	}
	return nil
}

func bet0IdentityBody(id bet0DrainIdentity) []byte {
	body := make([]byte, bet0DrainIdentityBytes)
	bet0EncodeDrainIdentity(body, id)
	return body
}

func TestBet0_DrainStateRequiresEveryExpectedIdentity(t *testing.T) {
	observed := newBet0ObservedIdentities(nil)
	target := newBet0DrainTarget(0, []uint64{2})
	expected0 := bet0DrainIdentity{queue: 0, producer: 0, seq: 0}
	expected1 := bet0DrainIdentity{queue: 0, producer: 0, seq: 1}
	observed.Add(expected0)
	observed.Add(bet0DrainIdentity{queue: 0, producer: 0, seq: 2}) // failed/out-of-prefix cannot substitute
	state := newBet0DrainState(target, observed, time.Second)
	observed.Add(expected0)                                        // duplicate after freeze cannot substitute
	observed.Add(bet0DrainIdentity{queue: 1, producer: 0, seq: 1}) // wrong queue cannot substitute

	first := state.Observe(0)
	require.Equal(t, bet0DrainContinue, first.verdict,
		"duplicate and out-of-prefix identities must not substitute for a missing target identity")
	require.Equal(t, []bet0DrainIdentity{expected1}, observed.MissingSample(8),
		"the exact expected identity, not merely a cardinality, must remain missing")
	observed.Add(expected1)
	require.Equal(t, bet0DrainDone, state.Observe(time.Hour).verdict,
		"observing the final expected identity must complete exact set inclusion")
}

func TestBet0_DrainTargetUsesExactProducerPrefix(t *testing.T) {
	target := newBet0DrainTarget(0, []uint64{1, 2})
	require.True(t, target.contains(bet0DrainIdentity{queue: 0, producer: 1, seq: 1}),
		"producer 1 sequence 1 must use producer 1's successful prefix")
	require.False(t, target.contains(bet0DrainIdentity{queue: 0, producer: 0, seq: 1}),
		"producer 0 sequence 1 must remain outside producer 0's shorter successful prefix")
}

func TestBet0_IdentityNamespacesQueueAndProducer(t *testing.T) {
	ids := []bet0DrainIdentity{
		{queue: 0, producer: 0, seq: 7},
		{queue: 0, producer: 1, seq: 7},
		{queue: 1, producer: 0, seq: 7},
	}
	decoded := make(map[bet0DrainIdentity]struct{})
	for _, id := range ids {
		body := bet0IdentityBody(id)
		got, ok := bet0DecodeDrainIdentity(body)
		require.True(t, ok)
		decoded[got] = struct{}{}
	}
	require.Len(t, decoded, len(ids),
		"queue and producer namespaces must remain distinct even at the same local sequence")
}

func TestBet0_DrainIdentityRejectsInvalidMagic(t *testing.T) {
	observed := newBet0ObservedIdentities(nil)
	target := newBet0DrainTarget(0, []uint64{1})
	state := newBet0DrainState(target, observed, time.Second)
	body := bet0IdentityBody(bet0DrainIdentity{queue: 0, producer: 0, seq: 0})
	binary.BigEndian.PutUint32(body[0:4], bet0DrainIdentityMagic+1)

	require.False(t, bet0ObserveDrainBody(observed, 0, 1, body),
		"a full-length identity with invalid magic must be rejected")
	require.Equal(t, int64(0), observed.ExpectedCount(),
		"invalid magic must not record expected progress")
	require.Equal(t, bet0DrainContinue, state.Observe(0).verdict,
		"invalid magic must not complete the frozen target")
}

func TestBet0_DrainStateFirstObservationEstablishesBaseline(t *testing.T) {
	observed := newBet0ObservedIdentities(nil)
	state := newBet0DrainState(newBet0DrainTarget(0, []uint64{1}), observed, 5*time.Second)
	require.Equal(t, bet0DrainContinue, state.Observe(100*time.Second).verdict,
		"the first observation establishes the baseline even beyond the inactivity window")
	require.Equal(t, bet0DrainContinue, state.Observe(101*time.Second).verdict,
		"inactivity must be measured from the explicit first observation, not duration zero")
}

func TestBet0_DrainStateFirstObservationReturnsDoneWhenAlreadyComplete(t *testing.T) {
	observed := newBet0ObservedIdentities(nil)
	observed.Add(bet0DrainIdentity{queue: 0, producer: 0, seq: 0})
	state := newBet0DrainState(newBet0DrainTarget(0, []uint64{1}), observed, 5*time.Second)

	snapshot := state.Observe(100 * time.Second)
	require.Equal(t, bet0DrainDone, snapshot.verdict,
		"the first observation must immediately report an already-complete target")
	require.Equal(t, int64(1), snapshot.observed)
	require.Zero(t, snapshot.missing)
}

func TestBet0_DrainStateContinuedExpectedProgressNeverFails(t *testing.T) {
	const targetCount = 500
	observed := newBet0ObservedIdentities(nil)
	state := newBet0DrainState(newBet0DrainTarget(0, []uint64{targetCount}), observed, 5*time.Second)
	require.Equal(t, bet0DrainContinue, state.Observe(100*time.Second).verdict)
	for seq := uint64(0); seq < targetCount-1; seq++ {
		observed.Add(bet0DrainIdentity{queue: 0, producer: 0, seq: seq})
		elapsed := 104*time.Second + time.Duration(seq)*4*time.Second
		require.Equalf(t, bet0DrainContinue, state.Observe(elapsed).verdict,
			"continued expected progress must return exactly CONTINUE at seq=%d elapsed=%s, even far past former total bounds", seq, elapsed)
		if seq == 0 {
			require.Equal(t, bet0DrainContinue, state.Observe(elapsed+4*time.Second).verdict,
				"expected progress must reset, not merely bypass, the inactivity baseline")
		}
	}
	observed.Add(bet0DrainIdentity{queue: 0, producer: 0, seq: targetCount - 1})
	require.Equal(t, bet0DrainDone, state.Observe(40*time.Hour).verdict)
}

func TestBet0_DrainStateIgnoresNonProgress(t *testing.T) {
	observed := newBet0ObservedIdentities(nil)
	state := newBet0DrainState(newBet0DrainTarget(0, []uint64{1, 1}), observed, 5*time.Second)
	require.Equal(t, bet0DrainContinue, state.Observe(10*time.Second).verdict)

	expected := bet0DrainIdentity{queue: 0, producer: 0, seq: 0}
	observed.Add(expected)
	require.Equal(t, bet0DrainContinue, state.Observe(11*time.Second).verdict)

	duplicate := bet0ObserveDrainBody(observed, 0, 2, bet0IdentityBody(expected))
	malformed := bet0ObserveDrainBody(observed, 0, 2, []byte{1, 2, 3})
	wrongQueue := bet0ObserveDrainBody(observed, 0, 2, bet0IdentityBody(bet0DrainIdentity{queue: 1, producer: 0, seq: 0}))
	wrongProducer := bet0ObserveDrainBody(observed, 0, 2, bet0IdentityBody(bet0DrainIdentity{queue: 0, producer: 2, seq: 0}))
	failedPrefix := bet0ObserveDrainBody(observed, 0, 2, bet0IdentityBody(bet0DrainIdentity{queue: 0, producer: 0, seq: 1}))
	if duplicate || malformed || wrongQueue || wrongProducer || !failedPrefix {
		t.Fatalf("non-progress classification wrong: duplicate=%v malformed=%v wrongQueue=%v wrongProducer=%v failedPrefixRecorded=%v",
			duplicate, malformed, wrongQueue, wrongProducer, failedPrefix)
	}
	snapshot := state.Observe(16 * time.Second)
	require.Equal(t, bet0DrainStalled, snapshot.verdict,
		"duplicate, malformed, wrong namespace, and failed-prefix observations must not reset expected progress")
}

func TestBet0_DrainStateStallsAtExactInactivityBoundary(t *testing.T) {
	observed := newBet0ObservedIdentities(nil)
	state := newBet0DrainState(newBet0DrainTarget(0, []uint64{2}), observed, 5*time.Second)
	state.Observe(20 * time.Second)
	observed.Add(bet0DrainIdentity{queue: 0, producer: 0, seq: 0})
	require.Equal(t, bet0DrainContinue, state.Observe(21*time.Second).verdict)
	require.Equal(t, bet0DrainContinue, state.Observe(21*time.Second+5*time.Second-time.Nanosecond).verdict)
	require.Equal(t, bet0DrainStalled, state.Observe(26*time.Second).verdict,
		"a non-empty expected remainder must stall at the exact inactivity boundary")
}

func TestBet0_DrainStateRemainingInactivity(t *testing.T) {
	observed := newBet0ObservedIdentities(nil)
	state := newBet0DrainState(newBet0DrainTarget(0, []uint64{2}), observed, 5*time.Second)
	state.Observe(10 * time.Second)
	require.Equal(t, 5*time.Second, state.RemainingInactivity(10*time.Second),
		"the initial observation must start a full inactivity window")

	observed.Add(bet0DrainIdentity{queue: 0, producer: 0, seq: 0})
	state.Observe(12 * time.Second)
	require.Equal(t, 5*time.Second, state.RemainingInactivity(12*time.Second),
		"expected progress must reset the remaining inactivity window")
	require.Equal(t, time.Nanosecond, state.RemainingInactivity(17*time.Second-time.Nanosecond))
	require.Zero(t, state.RemainingInactivity(17*time.Second),
		"remaining inactivity must be zero at the boundary")
	require.Zero(t, state.RemainingInactivity(18*time.Second),
		"remaining inactivity must remain clamped at zero after the boundary")
}

func TestBet0_DrainStateTimerSnapshotResetsForQueuedProgress(t *testing.T) {
	observed := newBet0ObservedIdentities(nil)
	state := newBet0DrainState(newBet0DrainTarget(0, []uint64{2}), observed, 5*time.Second)
	state.Observe(0)
	// This expected observation is already recorded when the old timer fires.
	// Observe must take a fresh snapshot and reset instead of using timer age.
	observed.Add(bet0DrainIdentity{queue: 0, producer: 0, seq: 0})
	require.Equal(t, bet0DrainContinue, state.Observe(5*time.Second).verdict,
		"fresh expected progress at a queued timer boundary must reset inactivity")
}

func TestBet0_ExpectedSnapshotCountAndSampleShareLinearizationPoint(t *testing.T) {
	observed := newBet0ObservedIdentities(nil)
	observed.FreezeTarget(newBet0DrainTarget(0, []uint64{2}))
	observed.beforeExpectedSnapshot = func(locked *bet0ObservedIdentities) {
		id := bet0DrainIdentity{queue: 0, producer: 0, seq: 0}
		locked.seen[id] = struct{}{}
		locked.expectedObserved++
		locked.beforeExpectedSnapshot = nil
	}

	count, missing := observed.expectedSnapshot(bet0MissingSampleLimit)
	require.Equal(t, int64(1), count,
		"snapshot count must include the identity update at its linearization point")
	require.Equal(t, []bet0DrainIdentity{{queue: 0, producer: 0, seq: 1}}, missing,
		"snapshot missing evidence must describe the same identity set as its count")
}

func TestBet0_DrainStateRechecksAtomicSnapshotBeforeStall(t *testing.T) {
	observed := newBet0ObservedIdentities(nil)
	state := newBet0DrainState(newBet0DrainTarget(0, []uint64{1}), observed, 5*time.Second)
	state.Observe(0)
	state.beforeStallSnapshot = func() {
		observed.Add(bet0DrainIdentity{queue: 0, producer: 0, seq: 0})
	}

	snapshot := state.Observe(5 * time.Second)
	require.Equal(t, bet0DrainDone, snapshot.verdict,
		"progress between the cheap count and final stall snapshot must complete instead of reporting stale failure")
	require.Equal(t, int64(1), snapshot.observed)
	require.Zero(t, snapshot.missing)
	require.Empty(t, snapshot.missingSample)
}

func TestBet0_DrainFailureDiagnostic(t *testing.T) {
	snapshot := bet0DrainSnapshot{
		verdict:             bet0DrainStalled,
		observed:            2,
		target:              4,
		missing:             2,
		missingSample:       []bet0DrainIdentity{{queue: 1, producer: 2, seq: 3}},
		inactivity:          61 * time.Second,
		inactivityThreshold: 60 * time.Second,
	}
	msg := bet0DrainDiagnostic("bet0-q1", snapshot)
	for _, want := range []string{"bet0-q1", "STALLED", "2 expected identities still missing", "observed=2 target=4", "missing sample=[{1 2 3}]", "inactivity=1m1s", "threshold=1m0s"} {
		require.Containsf(t, msg, want, "drain diagnostic missing %q: %s", want, msg)
	}
	lower := strings.ToLower(msg)
	if strings.Contains(lower, "lost") || strings.Contains(lower, "never delivered") {
		t.Fatalf("stall diagnostic must not claim permanent loss: %s", msg)
	}
}

func TestBet0_P0LivenessCheckPrecedesPublisherJoin(t *testing.T) {
	p0Err := errors.New("injected P0 liveness failure")
	joined := false
	err := bet0CheckBeforePublisherJoin(func() error {
		return p0Err
	}, func() {
		joined = true
	})
	require.ErrorIs(t, err, p0Err, "the frozen-window P0 error must be returned directly")
	require.False(t, joined,
		"publisher join must not run before a failing frozen-window P0 assertion")
}

func TestBet0_PublisherTargetFreezeWaitsForJoin(t *testing.T) {
	joined := false
	targets := bet0FreezePublisherTargets(func() {
		joined = true
	}, 1, 1, func(_, _ int) uint64 {
		require.True(t, joined, "publisher target was loaded before all publisher goroutines joined")
		return 7
	})
	require.Equal(t, int64(7), targets[0].total,
		"target freeze must include the successful prefix published immediately before join")
}

func TestBet0_ValidatePublisherTargets(t *testing.T) {
	tests := []struct {
		name      string
		targets   []bet0DrainTarget
		published []int64
		wantError string
	}{
		{
			name:      "valid nonzero target",
			targets:   []bet0DrainTarget{newBet0DrainTarget(0, []uint64{2, 3})},
			published: []int64{5},
		},
		{
			name:      "target slice length",
			targets:   []bet0DrainTarget{newBet0DrainTarget(0, []uint64{2, 3})},
			published: []int64{5, 4},
			wantError: "publisher target count 1 does not match result count 2",
		},
		{
			name:      "wrong queue namespace",
			targets:   []bet0DrainTarget{newBet0DrainTarget(1, []uint64{2, 3})},
			published: []int64{5},
			wantError: "queue 0 target namespace is 1",
		},
		{
			name:      "wrong producer prefix length",
			targets:   []bet0DrainTarget{newBet0DrainTarget(0, []uint64{5})},
			published: []int64{5},
			wantError: "queue 0 producer prefix count 1 does not match expected 2",
		},
		{
			name:      "aggregate mismatch",
			targets:   []bet0DrainTarget{newBet0DrainTarget(0, []uint64{2, 3})},
			published: []int64{6},
			wantError: "queue 0 target total 5 does not match published total 6",
		},
		{
			name:      "zero target",
			targets:   []bet0DrainTarget{newBet0DrainTarget(0, []uint64{0, 0})},
			published: []int64{0},
			wantError: "queue 0 target total is zero",
		},
		{
			name:      "zero producer prefix",
			targets:   []bet0DrainTarget{newBet0DrainTarget(0, []uint64{5, 0})},
			published: []int64{5},
			wantError: "queue 0 producer 1 successful prefix is zero",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := bet0ValidatePublisherTargets(tt.targets, tt.published, 2)
			if tt.wantError == "" {
				require.NoError(t, err)
				return
			}
			require.EqualError(t, err, tt.wantError)
		})
	}
}

func TestBet0_SuccessfulAckIsObservedExactlyOnce(t *testing.T) {
	observed := newBet0ObservedIdentities(nil)
	observed.FreezeTarget(newBet0DrainTarget(0, []uint64{1}))
	idBody := bet0IdentityBody(bet0DrainIdentity{queue: 0, producer: 0, seq: 0})
	ackCalls := 0
	ack := func() error {
		ackCalls++
		return nil
	}

	require.NoError(t, bet0AckAndObserve(ack, observed, 0, 1, idBody))
	require.Equal(t, 1, ackCalls, "a successful delivery must be Acked exactly once")
	require.Equal(t, int64(1), observed.ExpectedCount(),
		"a nil Ack must record the expected identity exactly once")
}

func TestBet0_ObservedIdentityChangeNotificationIsEdgeTriggered(t *testing.T) {
	changed := make(chan struct{}, 1)
	observed := newBet0ObservedIdentities(changed)
	id := bet0DrainIdentity{queue: 0, producer: 0, seq: 0}

	require.True(t, observed.Add(id))
	select {
	case <-changed:
	default:
		t.Fatal("a new valid identity must emit a wake token")
	}
	require.False(t, observed.Add(id), "the second observation must be classified as a duplicate")
	select {
	case <-changed:
		t.Fatal("a duplicate identity must not emit another wake token")
	default:
	}
}

func TestBet0_FailedAckIsNotObserved(t *testing.T) {
	observed := newBet0ObservedIdentities(nil)
	target := newBet0DrainTarget(0, []uint64{1})
	observed.FreezeTarget(target)
	ackErr := errors.New("injected ack failure")
	err := bet0AckAndObserve(func() error { return ackErr }, observed, 0, 1,
		bet0IdentityBody(bet0DrainIdentity{queue: 0, producer: 0, seq: 0}))
	require.ErrorIs(t, err, ackErr, "the consumer must surface Ack failure")
	require.Equal(t, int64(0), observed.ExpectedCount(),
		"a delivery whose Ack failed must not be recorded as observed progress")
}
