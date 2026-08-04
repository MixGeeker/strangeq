package interfaces

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// TestRecoveryOutcome_ZeroValueIsFatal pins the single most important property
// of this design: the DEFAULT for an error nobody has classified is the safest
// outcome, not the most dangerous one.
//
// Before Step 3 the boot decision was an allow-list of two fatal sentinels, so
// an unclassified recovery error meant "boot with an empty broker and start
// confirming durable publishes". Making the zero value fatal moves that
// guarantee from "somebody remembered to extend a list" to "the type cannot
// express the unsafe default".
func TestRecoveryOutcome_ZeroValueIsFatal(t *testing.T) {
	var zero RecoveryOutcome
	if zero != RecoveryFatal {
		t.Fatalf("the zero value of RecoveryOutcome is %v, not RecoveryFatal; every error path "+
			"nobody has written yet is now unsafe by construction", zero)
	}
	if !zero.GatesBoot() {
		t.Fatalf("the zero-value outcome does not gate the boot")
	}

	// A zero-value fault must also be fatal — it is what a struct literal that
	// forgets to set Outcome produces.
	forgot := &RecoveryFault{Stage: "somewhere", Artifact: "/some/file"}
	if forgot.Outcome != RecoveryFatal {
		t.Fatalf("a RecoveryFault literal with no Outcome set is %v, not fatal", forgot.Outcome)
	}
}

func TestOutcomeOf_UnclassifiedErrorIsFatal(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want RecoveryOutcome
	}{
		{"nil is benign", nil, RecoveryBenign},
		{"bare error is fatal", errors.New("something nobody classified"), RecoveryFatal},
		{"wrapped bare error is fatal", fmt.Errorf("outer: %w", errors.New("inner")), RecoveryFatal},
		{"benign fault", BenignFault("s", "a", "d"), RecoveryBenign},
		{"degraded fault", DegradedFault("s", "a", "d", "c", nil), RecoveryDegraded},
		{"fatal fault", FatalFault("s", "a", "d", "c", nil), RecoveryFatal},
		{"wrapped degraded fault keeps its class",
			fmt.Errorf("context: %w", DegradedFault("s", "a", "d", "c", nil)), RecoveryDegraded},
		{"join takes the WORST, not the first",
			errors.Join(BenignFault("s", "a", "d"), DegradedFault("s", "a", "d", "c", nil)), RecoveryDegraded},
		{"join takes the worst across three",
			errors.Join(BenignFault("s", "a", "d"), FatalFault("s", "a", "d", "c", nil), DegradedFault("s", "a", "d", "c", nil)), RecoveryFatal},
		{"a classified join containing an UNCLASSIFIED member is fatal",
			errors.Join(BenignFault("s", "a", "d"), errors.New("unclassified")), RecoveryFatal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := OutcomeOf(tc.err); got != tc.want {
				t.Fatalf("OutcomeOf(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}

// The last case above deserves its own note: errors.Join with one classified
// and one unclassified member reports Fatal because FaultsOf finds only the
// classified one and worseOf never sees the other. That is coincidentally the
// right answer here only when a fatal fault is present. Assert the real
// property directly instead of relying on the coincidence.
func TestOutcomeOf_JoinWithUnclassifiedMemberIsFatal(t *testing.T) {
	joined := errors.Join(BenignFault("s", "a", "d"), errors.New("unclassified"))
	if got := OutcomeOf(joined); got != RecoveryFatal {
		t.Fatalf("a join containing an unclassified error reported %v; an unclassified error "+
			"must never be able to hide behind a benign sibling", got)
	}
	// Premise, checked: the join really does contain exactly one classified
	// fault, so this is testing the mixed case and not a single-fault case.
	if n := len(FaultsOf(joined)); n != 1 {
		t.Fatalf("fixture is out of range of its own defect: FaultsOf found %d faults, want 1", n)
	}
}

func TestRecoveryRefusalMessage_IsActionable(t *testing.T) {
	msg := RecoveryRefusalMessage("recovery", []*RecoveryFault{
		FatalFault("wal-scan", "/var/lib/strangeq/wal/shared/00000000000000000001.wal",
			"framing version 1 is not one this build can parse", "every durable message in this file is abandoned", nil),
		DegradedFault("wal-open", "/var/lib/strangeq/wal/shared/00000000000000000009.wal",
			"file could not be read", "its durable messages are abandoned for this boot", nil),
	})

	// What happened, which artifact, what it costs, how to proceed.
	for _, want := range []string{
		"00000000000000000001.wal",
		"00000000000000000009.wal",
		"framing version 1",
		"abandoned",
		UnsafeRecoveryFlag,
		"discarding",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("refusal message is missing %q:\n%s", want, msg)
		}
	}

	// Fatal is listed before degraded so the worst thing is read first.
	if strings.Index(msg, "00000000000000000001.wal") > strings.Index(msg, "00000000000000000009.wal") {
		t.Errorf("the fatal fault is listed after the degraded one:\n%s", msg)
	}
}

func TestFaultsOf_WalksBothUnwrapShapes(t *testing.T) {
	inner := DegradedFault("s", "inner", "d", "c", nil)
	wrapped := fmt.Errorf("layer1: %w", fmt.Errorf("layer2: %w", inner))
	joined := errors.Join(wrapped, FatalFault("s", "other", "d", "c", nil))

	got := FaultsOf(joined)
	if len(got) != 2 {
		t.Fatalf("FaultsOf found %d faults through a join-of-wrapped chain, want 2", len(got))
	}
	if got[0].Artifact != "inner" || got[1].Artifact != "other" {
		t.Fatalf("traversal order is not the order faults were joined in: %q, %q",
			got[0].Artifact, got[1].Artifact)
	}
}
