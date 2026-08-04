package interfaces

import (
	"fmt"
	"sort"
	"strings"
)

// RecoveryOutcome classifies how safe it is to serve traffic from a data
// directory after recovery has looked at it.
//
// THE ZERO VALUE IS RecoveryFatal, AND THAT IS THE POINT. Before Step 3 the
// broker classified recovery errors with an ALLOW-LIST of two sentinels
// (ErrLegacyDataDirectory, ErrOrdinalMismatch); everything else — including
// every error nobody had thought of yet — logged one line and fell through to
// "boot with an empty broker and start confirming durable publishes". An
// allow-list of fatal conditions makes the DEFAULT for an unclassified error
// "proceed", and an unclassified error is precisely the one nobody has
// reasoned about. Inverting the zero value makes the next unwritten error path
// safe by construction rather than by review: a plain errors.New that reaches
// the boot decision is Fatal because it carries no classification, not because
// somebody remembered to add it to a list.
//
// The three classes differ in what recovery DOES, not only in how loud it is:
//
//   - RecoveryBenign: nothing a client was ever CONFIRMED was lost. Proceeds
//     silently (it is still counted). A torn WAL tail lives here: the partial
//     trailing record was never fsynced, therefore never confirmed, therefore
//     no durability promise is broken. No broker in the reference study asks
//     for operator action on one (ref-rabbitmq.md §1.6).
//   - RecoveryDegraded: something that WAS confirmed has been abandoned, but
//     the loss is bounded and nameable — a quarantined unreadable WAL file, a
//     queue that could not be redeclared, a record that could not be loaded.
//     Refuses the boot unless --unsafe-recovery is set.
//   - RecoveryFatal: an artifact cannot be interpreted at all. Its records are
//     abandoned wholesale. Refuses the boot unless --unsafe-recovery is set.
//
// The split is Ra's (RabbitMQ's own quorum-queue WAL, see
// .notes/loop-2/ref-rabbitmq.md §1.3): a checksum failure on the LAST record
// drops that record and resumes; a checksum failure on an INTERIOR record
// throws, because everything after it is unintelligible; an unknown framing
// version exits. "Last record" is Ra's is_last_record/3 — a test of the
// remaining BYTES, not of an offset against the file size — because a
// zero-filled tail is what a power loss leaves.
//
// WHAT A GATING OUTCOME MEANS FOR THE REST OF THE DIRECTORY: nothing. Neither
// class abandons anything beyond its own artifact. Recovery scans every file
// and enumerates every fault before the boot decision is taken, so the artifact
// list a refusal prints is the COMPLETE set of what --unsafe-recovery would
// discard. That is what makes the cost statement true, and it is the property
// review-3 found broken.
type RecoveryOutcome uint8

const (
	// RecoveryFatal is the zero value. See the type doc.
	RecoveryFatal RecoveryOutcome = iota
	RecoveryDegraded
	RecoveryBenign
)

func (o RecoveryOutcome) String() string {
	switch o {
	case RecoveryDegraded:
		return "degraded"
	case RecoveryBenign:
		return "benign"
	default:
		return "fatal"
	}
}

// GatesBoot reports whether this outcome must stop the broker from starting
// unless --unsafe-recovery is set.
func (o RecoveryOutcome) GatesBoot() bool { return o != RecoveryBenign }

// worseOf returns the more severe of two outcomes. Severity runs DOWN the
// numeric order (Fatal == 0), so "worse" is min — which also means a
// zero-value RecoveryOutcome poisons any aggregation toward Fatal, matching
// the type's whole design.
func worseOf(a, b RecoveryOutcome) RecoveryOutcome {
	if a < b {
		return a
	}
	return b
}

// RecoveryFault is one classified thing that went wrong during recovery,
// produced AT THE SITE THAT KNOWS THE SEMANTICS. A torn WAL tail is degraded
// because the code that found it knows a partial trailing record was never
// fsynced; an unknown framing version is fatal because the code that read the
// header knows it cannot interpret a single byte after it. Nothing downstream
// re-derives severity from an error string.
type RecoveryFault struct {
	// Outcome is this fault's class. Zero value is RecoveryFatal.
	Outcome RecoveryOutcome

	// Stage names the recovery phase, e.g. "wal-scan", "metadata",
	// "queue-recovery", "segment-scan". Step 4 adds segment stages here
	// without touching anything else in this file.
	Stage string

	// Artifact identifies WHAT is affected, by absolute path when a file is
	// involved and by queue name when one is not. This is what an operator
	// needs and what --unsafe-recovery logging prints, so it is a required
	// field in spirit even though Go cannot enforce it.
	Artifact string

	// Detail is the human-readable cause.
	Detail string

	// Cost states, in the operator's terms, what starting anyway will lose.
	Cost string

	// Err is the underlying error, if any.
	Err error
}

func (f *RecoveryFault) Error() string {
	var b strings.Builder
	b.WriteString("recovery ")
	b.WriteString(f.Outcome.String())
	if f.Stage != "" {
		b.WriteString(" [")
		b.WriteString(f.Stage)
		b.WriteString("]")
	}
	if f.Artifact != "" {
		b.WriteString(" ")
		b.WriteString(f.Artifact)
	}
	if f.Detail != "" {
		b.WriteString(": ")
		b.WriteString(f.Detail)
	}
	if f.Err != nil {
		b.WriteString(": ")
		b.WriteString(f.Err.Error())
	}
	return b.String()
}

func (f *RecoveryFault) Unwrap() error { return f.Err }

// NewRecoveryFault builds a fault. Callers should prefer FatalFault /
// DegradedFault / BenignFault, which name the class at the call site.
func NewRecoveryFault(outcome RecoveryOutcome, stage, artifact, detail, cost string, err error) *RecoveryFault {
	return &RecoveryFault{Outcome: outcome, Stage: stage, Artifact: artifact, Detail: detail, Cost: cost, Err: err}
}

func FatalFault(stage, artifact, detail, cost string, err error) *RecoveryFault {
	return NewRecoveryFault(RecoveryFatal, stage, artifact, detail, cost, err)
}

func DegradedFault(stage, artifact, detail, cost string, err error) *RecoveryFault {
	return NewRecoveryFault(RecoveryDegraded, stage, artifact, detail, cost, err)
}

func BenignFault(stage, artifact, detail string) *RecoveryFault {
	return NewRecoveryFault(RecoveryBenign, stage, artifact, detail, "", nil)
}

// OutcomeOf classifies an arbitrary error.
//
//	nil                                -> RecoveryBenign
//	a *RecoveryFault (however wrapped)  -> that fault's outcome
//	errors.Join(...)                    -> the WORST of its members' outcomes
//	anything else                       -> RecoveryFatal
//
// The last line is the safety property: an error that nobody classified is
// treated as the most severe thing it could be.
//
// The traversal is per-BRANCH rather than "collect every fault and take the
// worst", and that distinction is load-bearing. A collect-then-worst
// implementation reports `errors.Join(BenignFault, errors.New("..."))` as
// BENIGN, because the unclassified sibling contributes no fault to collect and
// therefore disappears behind its classified neighbour — reopening exactly the
// silent-empty-boot hole this type exists to close, in the one shape (a joined
// error, which is how multi-file WAL faults are returned) where it is most
// likely to occur. Walking branches makes an unclassified leaf a Fatal branch
// that no sibling can outvote.
func OutcomeOf(err error) RecoveryOutcome {
	if err == nil {
		return RecoveryBenign
	}
	return outcomeOfBranch(err)
}

func outcomeOfBranch(e error) RecoveryOutcome {
	for e != nil {
		// A fault classifies itself and everything beneath it: its own Err is
		// the cause it was constructed to describe.
		if f, ok := e.(*RecoveryFault); ok {
			return f.Outcome
		}
		switch u := e.(type) {
		case interface{ Unwrap() []error }:
			subs := u.Unwrap()
			if len(subs) == 0 {
				return RecoveryFatal
			}
			out := RecoveryBenign
			for _, sub := range subs {
				out = worseOf(out, outcomeOfBranch(sub))
			}
			return out
		case interface{ Unwrap() error }:
			next := u.Unwrap()
			if next == nil {
				return RecoveryFatal
			}
			e = next
		default:
			return RecoveryFatal
		}
	}
	return RecoveryFatal
}

// ExplainedFaults returns an artifact list that ACCOUNTS FOR OutcomeOf(err).
//
// FaultsOf alone is not enough to drive a refusal message: an error can gate
// the boot through an unclassified branch that contributes no enumerable
// fault, and a refusal whose artifact list is empty tells an operator nothing.
// Whenever the outcome gates but nothing enumerated explains it, a synthetic
// fatal fault is appended so the two can never disagree.
//
// Every site that turns an error into a boot decision must use this rather
// than FaultsOf, so the enumeration and the classification stay one fact
// (canon rule 12).
func ExplainedFaults(stage string, err error) []*RecoveryFault {
	if err == nil {
		return nil
	}
	faults := FaultsOf(err)
	outcome := OutcomeOf(err)
	if !outcome.GatesBoot() {
		return faults
	}
	for _, f := range faults {
		if f.Outcome == outcome {
			return faults
		}
	}
	return append(faults, FatalFault(stage, "(unclassified)",
		"recovery reported an error carrying no severity classification, so it is treated as the most severe thing it could be",
		"recovery is abandoned; nothing from this data directory is loaded",
		err))
}

// FaultsOf collects every *RecoveryFault reachable from err, walking both
// single-error Unwrap() and the multi-error Unwrap() []error that errors.Join
// produces. Order is the traversal order, which is the order the faults were
// joined in, so an operator reads them in the order recovery hit them.
func FaultsOf(err error) []*RecoveryFault {
	var out []*RecoveryFault
	var walk func(error)
	walk = func(e error) {
		for e != nil {
			if f, ok := e.(*RecoveryFault); ok {
				out = append(out, f)
			}
			switch u := e.(type) {
			case interface{ Unwrap() []error }:
				for _, sub := range u.Unwrap() {
					walk(sub)
				}
				return
			case interface{ Unwrap() error }:
				e = u.Unwrap()
			default:
				return
			}
		}
	}
	walk(err)
	return out
}

// UnsafeRecoveryFlag is the operator-facing name of the override, in one place
// so the flag definition, the refusal message and the degraded-boot log can
// never drift apart (canon rule 12).
const UnsafeRecoveryFlag = "--unsafe-recovery"

// RecoveryRefusalMessage renders the message an operator sees when the broker
// refuses to start. A refusal that does not say how to proceed is an outage
// with no exit, so this always answers four questions: what happened, which
// artifact, what it will cost to proceed, and the exact flag to proceed with.
//
// `context` names where the refusal was raised (storage construction vs the
// recovery pass) so two different code paths reading the same damaged
// directory produce distinguishable messages.
func RecoveryRefusalMessage(context string, faults []*RecoveryFault) string {
	var b strings.Builder
	fmt.Fprintf(&b, "refusing to start: %s could not be completed safely on this data directory.\n", context)

	// Fatal first, then degraded; stable within each class.
	ordered := make([]*RecoveryFault, 0, len(faults))
	for _, f := range faults {
		if f.Outcome.GatesBoot() {
			ordered = append(ordered, f)
		}
	}
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Outcome < ordered[j].Outcome })

	for _, f := range ordered {
		fmt.Fprintf(&b, "  [%s] %s", f.Outcome, f.Artifact)
		if f.Detail != "" {
			fmt.Fprintf(&b, "\n      cause: %s", f.Detail)
		}
		if f.Err != nil {
			fmt.Fprintf(&b, "\n      error: %v", f.Err)
		}
		if f.Cost != "" {
			fmt.Fprintf(&b, "\n      starting anyway costs: %s", f.Cost)
		}
		b.WriteString("\n")
	}

	fmt.Fprintf(&b,
		"The list above is COMPLETE: recovery scanned every file before refusing, so this is the "+
			"whole report and not the first problem of several. To start anyway, restart with %s. "+
			"That flag is a data-loss switch: the broker will boot while discarding the artifacts "+
			"listed above AND ONLY THOSE — their confirmed durable messages will not be recovered, "+
			"everything else in the data directory is recovered normally, and the boot stays "+
			"flagged for the lifetime of the process. The safe alternative is to restore the data "+
			"directory from a backup, or to move the listed files OUT OF THE DIRECTORY (not merely "+
			"rename them within it) so you still have them, before restarting.",
		UnsafeRecoveryFlag)
	return b.String()
}
