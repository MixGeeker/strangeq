package main

import (
	"strings"
	"testing"
)

// Fixture captured from a real `benchstat -format=csv old.txt new.txt` run
// (golang.org/x/perf/cmd/benchstat, pinned via tools/go.mod) comparing 10
// samples per side: sec/op regresses +6% (significant), B/op and allocs/op
// jitter ±1 with no significant change. See cmd/benchgate's design notes
// for why -format=csv (not the classic text table) is used: this pinned
// version's CSV already emits "vs base"/"P" comparison columns, contrary to
// the older behavior the design doc's own risk note warned about — verified
// live before committing to this format.
const sampleBenchstatCSV = `goos: darwin
goarch: arm64
pkg: example.com/foo
,old.txt,,new.txt,,,
,sec/op,CI,sec/op,CI,vs base,P
Foo-8,1.0005e-07,1%,1.0605e-07,1%,+6.00%,p=0.000 n=10
geomean,1.0004999999999995e-07,,1.0605000000000003e-07,,+6.00%,

,old.txt,,new.txt,,,
,B/op,CI,B/op,CI,vs base,P
Foo-8,10,10%,10,0%,~,p=0.474 n=10
geomean,10.000000000000002,,10.000000000000002,,+0.00%,

,old.txt,,new.txt,,,
,allocs/op,CI,allocs/op,CI,vs base,P
Foo-8,1,0%,1,0%,~,p=1.000 n=10
geomean,1,,1,,+0.00%,
`

func TestParseBenchstatCSV_RealFixture(t *testing.T) {
	results, err := parseBenchstatCSV([]byte(sampleBenchstatCSV))
	if err != nil {
		t.Fatalf("parseBenchstatCSV: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3 (one per metric table): %+v", len(results), results)
	}

	want := map[string]benchResult{
		"sec/op":    {metric: "sec/op", name: "Foo-8", significant: true, deltaPct: 6.0},
		"B/op":      {metric: "B/op", name: "Foo-8", significant: false},
		"allocs/op": {metric: "allocs/op", name: "Foo-8", significant: false},
	}
	for _, r := range results {
		w, ok := want[r.metric]
		if !ok {
			t.Fatalf("unexpected metric %q in results", r.metric)
		}
		if r.name != w.name || r.significant != w.significant {
			t.Errorf("metric %s: got %+v, want name=%s significant=%v", r.metric, r, w.name, w.significant)
		}
		if w.significant && r.deltaPct != w.deltaPct {
			t.Errorf("metric %s: got deltaPct=%v, want %v", r.metric, r.deltaPct, w.deltaPct)
		}
	}
}

func TestParseBenchstatCSV_SkipsGeomeanAndHeaders(t *testing.T) {
	results, err := parseBenchstatCSV([]byte(sampleBenchstatCSV))
	if err != nil {
		t.Fatalf("parseBenchstatCSV: %v", err)
	}
	for _, r := range results {
		if r.name == "" || r.name == "geomean" {
			t.Errorf("geomean or header row leaked into results: %+v", r)
		}
	}
}

func TestParseBenchstatCSV_MultipleBenchmarksInOneTable(t *testing.T) {
	input := `goos: darwin
goarch: arm64
pkg: example.com/foo
,old.txt,,new.txt,,,
,sec/op,CI,sec/op,CI,vs base,P
BenchmarkA-8,100n,1%,101n,1%,~,p=0.900 n=10
BenchmarkB-8,100n,1%,105n,1%,+5.00%,p=0.001 n=10
geomean,100n,,103n,,+3.00%,
`
	results, err := parseBenchstatCSV([]byte(input))
	if err != nil {
		t.Fatalf("parseBenchstatCSV: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2: %+v", len(results), results)
	}
	if results[0].name != "BenchmarkA-8" || results[0].significant {
		t.Errorf("BenchmarkA-8: got %+v, want non-significant", results[0])
	}
	if results[1].name != "BenchmarkB-8" || !results[1].significant || results[1].deltaPct != 5.0 {
		t.Errorf("BenchmarkB-8: got %+v, want significant +5.00%%", results[1])
	}
}

func TestParseBenchstatCSV_MalformedDeltaIsError(t *testing.T) {
	input := `,old.txt,,new.txt,,,
,sec/op,CI,sec/op,CI,vs base,P
Foo-8,100n,1%,110n,1%,+not-a-number%,p=0.001 n=10
`
	if _, err := parseBenchstatCSV([]byte(input)); err == nil {
		t.Fatal("expected an error for an unparseable delta, got nil")
	}
}

// TestParseBenchstatCSV_MissingFromNew uses the exact truncated-row shape
// captured from a real benchstat run comparing a two-benchmark baseline
// against a new run missing one of them: the dropped benchmark's row is
// right-truncated to 3 fields (name, value, CI) — no vs-base/P columns,
// because there is nothing on the "new" side to compare against.
func TestParseBenchstatCSV_MissingFromNew(t *testing.T) {
	input := `,old.txt,,new.txt,,,
,sec/op,CI,sec/op,CI,vs base,P
A-8,1.0000000000000001e-07,∞,1.0000000000000001e-07,∞,~,p=1.000 n=3
B-8,5.0000000000000004e-08,∞
geomean,7.071067811865489e-08,,9.999999999999994e-08,,+0.00%,
`
	results, err := parseBenchstatCSV([]byte(input))
	if err != nil {
		t.Fatalf("parseBenchstatCSV: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2: %+v", len(results), results)
	}
	if results[0].name != "A-8" || results[0].status != statusCompared {
		t.Errorf("A-8: got %+v, want statusCompared", results[0])
	}
	if results[1].name != "B-8" || results[1].status != statusMissingFromNew {
		t.Errorf("B-8: got %+v, want statusMissingFromNew", results[1])
	}
	// The dropped row's raw value ("5e-08") must never be misread as a
	// delta percentage — this used to silently parse as a bogus tiny
	// "significant" delta.
	if results[1].significant {
		t.Errorf("B-8: significant=true derived from a raw value, not a real delta: %+v", results[1])
	}
}

// TestParseBenchstatCSV_MissingFromBaseline mirrors the above for a
// benchmark added in the new run: the row is 5 fields with the first
// value/CI slot empty (name,"","",value,CI), since there is nothing on the
// "old" side.
func TestParseBenchstatCSV_MissingFromBaseline(t *testing.T) {
	input := `,old.txt,,new.txt,,,
,sec/op,CI,sec/op,CI,vs base,P
A-8,1.0000000000000001e-07,∞,1.0000000000000001e-07,∞,~,p=1.000 n=3
C-8,,,9.99e-07,∞
geomean,9.999999999999994e-08,,3.1606961258558185e-07,,+0.00%,
`
	results, err := parseBenchstatCSV([]byte(input))
	if err != nil {
		t.Fatalf("parseBenchstatCSV: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("got %d results, want 2: %+v", len(results), results)
	}
	if results[1].name != "C-8" || results[1].status != statusMissingFromBaseline {
		t.Errorf("C-8: got %+v, want statusMissingFromBaseline", results[1])
	}
}

// singleConfigCSV is captured verbatim (shortened to two benchmarks per
// table) from a real pinned-benchstat run of the committed M4 Max baseline
// against an M3 Pro run of the broker package — the exact input that produced
// 41 fabricated regressions.
//
// When the two inputs share no benchstat *configuration*, benchstat does not
// emit a comparison at all: it emits one degenerate single-file table per
// configuration. Note the shape — the filenames header names ONE file, and
// the unit header is `,B/op,CI` with no "vs base"/"P" columns. There is no
// delta anywhere in this table because nothing was compared.
const singleConfigCSV = `cpu: Apple M3 Pro
goos: darwin
goarch: arm64
pkg: github.com/maxpert/amqp-go/broker
,/tmp/split/broker-new.txt,
,B/op,CI
QueueDispatch_Claim-12,248,0%
QueueDispatch_Publish-12,0,0%
geomean,,

cpu: Apple M4 Max
,/tmp/split/broker-baseline.txt,
,B/op,CI
QueueDispatch_Claim-16,248,0%
QueueDispatch_Publish-16,0,0%
geomean,,
`

// TestParseBenchstatCSV_SingleConfigTableIsHardError is the regression test
// for the fabricated-regression bug.
//
// The old parser took fullWidth from whatever unit header it saw, so a
// degenerate 3-field header set fullWidth=3; the one-sided-row guard
// (len(fields) < fullWidth) could then never fire, every row was classified
// statusCompared, and fields[len-2] — the RAW METRIC VALUE — was parsed as a
// percentage. B/op 248 on both sides was reported as "regressed +248.00%".
//
// A gate that cannot compare must say so and stop, never emit a number. The
// contract asserted here is: no results, and an error naming the reason.
func TestParseBenchstatCSV_SingleConfigTableIsHardError(t *testing.T) {
	results, err := parseBenchstatCSV([]byte(singleConfigCSV))
	if err == nil {
		t.Fatalf("parseBenchstatCSV accepted a table with nothing to compare and returned %d results: %+v", len(results), results)
	}
	if len(results) != 0 {
		t.Errorf("got %d results alongside the error, want 0 — a non-comparison must yield no numbers: %+v", len(results), results)
	}
	msg := err.Error()
	for _, want := range []string{"B/op", "vs base"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message does not mention %q, so it does not name the reason: %s", want, msg)
		}
	}
	// The fabricated value must not appear anywhere: 248 is the raw B/op,
	// identical on both sides, that the old parser printed as +248.00%.
	if strings.Contains(msg, "248") {
		t.Errorf("error message contains the raw metric value 248 as if it were a delta: %s", msg)
	}
}

func TestIsUnitHeader(t *testing.T) {
	cases := map[string]bool{
		"sec/op":             true,
		"B/op":               true,
		"allocs/op":          true,
		"old.txt":            false,
		"/some/path/new.txt": false,
	}
	for in, want := range cases {
		if got := isUnitHeader(in); got != want {
			t.Errorf("isUnitHeader(%q) = %v, want %v", in, got, want)
		}
	}
}
