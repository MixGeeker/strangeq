package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// rowStatus classifies a benchResult by which side(s) of the comparison
// actually had data for that benchmark.
type rowStatus int

const (
	// statusCompared: the benchmark appears on both sides; significant/deltaPct
	// are meaningful.
	statusCompared rowStatus = iota
	// statusMissingFromNew: the benchmark is in the baseline but the new run
	// has no data for it — e.g. deleted, or dropped from a -bench regex.
	// Silently allowing this would let a gated benchmark evade the gate by
	// disappearing rather than regressing; evaluate() treats it as a hard
	// failure.
	statusMissingFromNew
	// statusMissingFromBaseline: the benchmark is in the new run but the
	// baseline has no data for it — e.g. just added in this PR. Informational
	// only: it never gates, so adding coverage doesn't self-block before
	// make bench-baseline-refresh is deliberately run and reviewed.
	statusMissingFromBaseline
)

// benchResult is one benchstat A/B comparison row: one metric table (sec/op,
// B/op, allocs/op, ...) crossed with one benchmark name.
type benchResult struct {
	metric      string
	name        string
	status      rowStatus
	significant bool    // benchstat's own two-sample test called this row non-noise (status == statusCompared only)
	deltaPct    float64 // meaningful only when significant; positive = new is higher than baseline
}

// parseBenchstatCSV parses `benchstat -format=csv <baseline> <new>` output.
// Each metric table looks like:
//
//	,<baseline-file>,,<new-file>,,,
//	,<metric>,CI,<metric>,CI,vs base,P
//	<name>,<value>,<ci>,<value>,<ci>,<delta-or-~>,<p-or-empty>
//	...
//	geomean,<value>,,<value>,,<delta>,
//	<blank line>
//
// repeated once per metric. The two header rows (both have an empty first
// field) and the geomean summary row are skipped: the gate acts on "any
// benchmark" (design doc, W0 section), not the geomean aggregate.
//
// benchstat itself decides significance (its "~" marker) at the -alpha
// passed to it, so this parser does not re-derive it from the P field: a
// "~" row is always non-significant and never gates, matching "gate
// allocs/op via the two-sample test (p<0.05)... not a raw 0-delta".
//
// A benchmark present on only one side prints a SHORT row (benchstat has
// nothing to compare it against), not the full 7-field comparison row.
// Confirmed empirically against the pinned benchstat: baseline-only shows as
// exactly 3 fields (name, value, CI — right-truncated, no trailing commas);
// new-only shows as 5 fields with the first value/CI slot as empty strings
// (name,"","",value,CI). Naively parsing either shape's fields[len-2] as
// "vs base" silently misreads a raw value or CI token as a percent delta —
// e.g. a baseline-only row's raw value "5e-08" parses as a fine, tiny,
// bogus "significant" +5% delta, which used to slip under the sec/op
// threshold but would wrongly flag allocs/op (any significant increase
// fails there) purely by accident of the value's magnitude. Both shapes are
// now classified explicitly instead.
func parseBenchstatCSV(csvOutput []byte) ([]benchResult, error) {
	var results []benchResult
	var metric string
	var fullWidth int // field count of a normal (both-sides-present) row for the current table

	scanner := bufio.NewScanner(bytes.NewReader(csvOutput))
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		fields := strings.Split(line, ",")
		if len(fields) < 3 {
			// Metadata (goos:/goarch:/pkg:/cpu:) or a stray benchstat
			// diagnostic line ("Bn: need >= 6 samples..."); not a table row.
			continue
		}

		name := fields[0]
		if name == "" {
			// The filenames header row's second field is a file path; the
			// units header row's second field is the metric name itself
			// (e.g. "sec/op"). Only the latter matters here, and its field
			// count is what a normal comparison row for this table must
			// match.
			if isUnitHeader(fields[1]) {
				if !hasVsBaseColumn(fields) {
					return nil, fmt.Errorf(
						"benchstat emitted a single-configuration table for %s (unit header %q has no %q column), meaning the two inputs share no benchstat configuration and NOTHING was compared; refusing to derive a delta from raw values",
						fields[1], strings.Join(fields[1:], ","), vsBaseColumn)
				}
				metric = fields[1]
				fullWidth = len(fields)
			}
			continue
		}
		if name == "geomean" {
			continue
		}

		if fullWidth > 0 && len(fields) < fullWidth {
			if fields[1] == "" {
				results = append(results, benchResult{metric: metric, name: name, status: statusMissingFromBaseline})
			} else {
				results = append(results, benchResult{metric: metric, name: name, status: statusMissingFromNew})
			}
			continue
		}

		vsBase := fields[len(fields)-2]
		r := benchResult{metric: metric, name: name, status: statusCompared}
		switch vsBase {
		case "~", "":
			// benchstat's own two-sample test called this non-significant noise.
			results = append(results, r)
			continue
		case "?":
			// "?" means benchstat could not compute significance — usually too
			// few samples on one side (a truncated / under-sampled run). It never
			// gates, but swallowing it silently could turn a broken run green, so
			// surface it as a warning rather than treating it as clean "~".
			fmt.Fprintf(os.Stderr, "benchgate: WARNING: %s %s has no significance (\"?\" — likely too few samples); not gated, but the run may be unreliable\n", name, metric)
			results = append(results, r)
			continue
		}
		delta, err := parseDeltaPercent(vsBase)
		if err != nil {
			return nil, fmt.Errorf("benchgate: parsing delta %q for benchmark %s (%s): %w", vsBase, name, metric, err)
		}
		r.significant = true
		r.deltaPct = delta
		results = append(results, r)
	}
	return results, nil
}

// isUnitHeader reports whether s is a per-operation unit column header
// (sec/op, B/op, allocs/op, and any future "/op"-suffixed metric benchstat
// adds), as opposed to a file path in the filenames header row.
func isUnitHeader(s string) bool {
	return strings.HasSuffix(s, "/op")
}

// vsBaseColumn is benchstat's delta column header. Its PRESENCE in a unit
// header row is what makes a table a comparison at all.
const vsBaseColumn = "vs base"

// hasVsBaseColumn reports whether a unit header row describes a real A/B
// comparison table (",<metric>,CI,<metric>,CI,vs base,P") as opposed to
// benchstat's degenerate single-configuration table (",<metric>,CI"), which
// has no delta column because it holds one file's results and nothing to
// compare them against.
//
// This is the guard the old parser lacked, and it is why 41 fabricated
// regressions were reported in a single run. fullWidth was taken from
// whatever unit header appeared — a 3-field degenerate header set
// fullWidth=3, so the one-sided-row guard "len(fields) < fullWidth" could
// never fire, every row was classified statusCompared, and
// fields[len(fields)-2] read the RAW METRIC VALUE. B/op 248, identical on
// both sides, was printed as "regressed +248.00%".
//
// Note the layering: main.go already refuses to invoke benchstat when the two
// sections' goos/goarch/cpu disagree, which is the only way this shape has
// been observed to arise. This check stays as the parser's own contract
// assertion, because benchstat keys configurations on ANY "key: value" line
// in its input, not just the three that go test emits — so a shape main.go
// does not model must still not become a percentage.
func hasVsBaseColumn(headerFields []string) bool {
	for _, f := range headerFields {
		if f == vsBaseColumn {
			return true
		}
	}
	return false
}

func parseDeltaPercent(s string) (float64, error) {
	s = strings.TrimSuffix(strings.TrimSpace(s), "%")
	return strconv.ParseFloat(s, 64)
}
