package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// realTools points at the actual pinned benchstat module so these
// integration tests exercise the real subprocess, not a stub. Tests run
// with cwd = this package directory (cmd/benchgate).
const realToolsDir = "../../tools"

// benchLines renders n synthetic go-test-benchmark result lines for name,
// centered on baseNs nanoseconds with the given per-sample deltas (so
// callers control exactly how much jitter/shift each sample has, instead of
// relying on randomness that would make a "does this trip the gate" test
// flaky).
func benchLines(name string, baseNs float64, deltasNs []float64) string {
	var sb strings.Builder
	for _, d := range deltasNs {
		fmt.Fprintf(&sb, "%s-8    	   10000	       %.4f ns/op	      64 B/op	       2 allocs/op\n", name, baseNs+d)
	}
	return sb.String()
}

func pkgBlock(pkg, benchBody string) string {
	return pkgBlockCPU(pkg, "Test CPU", benchBody)
}

// pkgBlockCPU is pkgBlock with an explicit cpu: line, for the tests that turn
// on which machine a section was measured on. `go test` prints the header in
// the order goos:, goarch:, pkg:, cpu: — cpu AFTER pkg — and that ordering is
// load-bearing (see TestSplitByPackage_CPULineBelongsToItsOwnPackage).
func pkgBlockCPU(pkg, cpu, benchBody string) string {
	return "goos: darwin\ngoarch: arm64\npkg: " + pkg + "\ncpu: " + cpu + "\n" + benchBody + "PASS\nok  \t" + pkg + "\t0.01s\n"
}

// tenJitterSamples is a fixed, non-random ±1% jitter pattern with no true
// mean shift, used for both baseline and new to prove noise never trips the
// gate (TestRun_PassesWithNoiseOnly / TestRun_SelfComparisonAlwaysPasses).
func tenJitterSamples(baseNs float64) []float64 {
	pct := []float64{0, 1, -1, 0.5, -0.5, 0.8, -0.8, 0.2, -0.2, 0}
	out := make([]float64, len(pct))
	for i, p := range pct {
		out[i] = baseNs * p / 100
	}
	return out
}

func TestRun_CatchesInjectedRegressionOverThreshold(t *testing.T) {
	dir := t.TempDir()

	baseline := pkgBlock("pkg/regressed", benchLines("BenchmarkFoo", 100, tenJitterSamples(100))) +
		pkgBlock("pkg/clean", benchLines("BenchmarkBar", 50, tenJitterSamples(50)))

	// pkg/regressed shifts its mean by +10% (well over the 2% gate);
	// pkg/clean keeps the same jitter pattern with no true shift.
	shifted := make([]float64, 10)
	for i, d := range tenJitterSamples(100) {
		shifted[i] = d + 10 // +10ns on a ~100ns base ≈ +10%
	}
	newRun := pkgBlock("pkg/regressed", benchLines("BenchmarkFoo", 100, shifted)) +
		pkgBlock("pkg/clean", benchLines("BenchmarkBar", 50, tenJitterSamples(50)))

	baselinePath := filepath.Join(dir, "baseline.txt")
	newPath := filepath.Join(dir, "new.txt")
	writeFileT(t, baselinePath, baseline)
	writeFileT(t, newPath, newRun)

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-baseline", baselinePath,
		"-new", newPath,
		"-tools-dir", realToolsDir,
		"-scratch-dir", filepath.Join(dir, "scratch"),
	}, &stdout, &stderr)

	if code != 1 {
		t.Fatalf("got exit code %d, want 1 (regression must fail the gate)\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "pkg/regressed") || !strings.Contains(stderr.String(), "Foo") {
		t.Errorf("stderr does not identify the regressed package/benchmark:\n%s", stderr.String())
	}
	if strings.Contains(stderr.String(), "pkg/clean") {
		t.Errorf("clean package was reported as a regression:\n%s", stderr.String())
	}
}

func TestRun_PassesWithNoiseOnly(t *testing.T) {
	dir := t.TempDir()

	// Same jitter pattern on both sides, no true mean shift, for two
	// packages sharing a benchmark NAME — also proves per-package
	// separation doesn't accidentally cross-contaminate a clean result.
	baseline := pkgBlock("pkg/a", benchLines("BenchmarkSame", 100, tenJitterSamples(100))) +
		pkgBlock("pkg/b", benchLines("BenchmarkSame", 100, tenJitterSamples(100)))
	newRun := pkgBlock("pkg/a", benchLines("BenchmarkSame", 100, tenJitterSamples(100))) +
		pkgBlock("pkg/b", benchLines("BenchmarkSame", 100, tenJitterSamples(100)))

	baselinePath := filepath.Join(dir, "baseline.txt")
	newPath := filepath.Join(dir, "new.txt")
	writeFileT(t, baselinePath, baseline)
	writeFileT(t, newPath, newRun)

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-baseline", baselinePath,
		"-new", newPath,
		"-tools-dir", realToolsDir,
		"-scratch-dir", filepath.Join(dir, "scratch"),
	}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("got exit code %d, want 0 (identical noise must pass)\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
}

func TestRun_MissingBaselineSectionIsNotFatal(t *testing.T) {
	dir := t.TempDir()

	baseline := pkgBlock("pkg/old", benchLines("BenchmarkOld", 100, tenJitterSamples(100)))
	// "new" introduces a brand-new package the baseline has never seen (e.g.
	// a benchmark just added in this PR) — must be skipped with a note, not
	// treated as a gate failure, so adding coverage doesn't self-block.
	newRun := pkgBlock("pkg/old", benchLines("BenchmarkOld", 100, tenJitterSamples(100))) +
		pkgBlock("pkg/new", benchLines("BenchmarkNew", 100, tenJitterSamples(100)))

	baselinePath := filepath.Join(dir, "baseline.txt")
	newPath := filepath.Join(dir, "new.txt")
	writeFileT(t, baselinePath, baseline)
	writeFileT(t, newPath, newRun)

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-baseline", baselinePath,
		"-new", newPath,
		"-tools-dir", realToolsDir,
		"-scratch-dir", filepath.Join(dir, "scratch"),
	}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("got exit code %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stdout.String(), "pkg/new") {
		t.Errorf("expected a note about the uncovered new package, got stdout:\n%s", stdout.String())
	}
}

// TestRun_WholePackageDroppedIsHardFailure proves that a package the
// baseline covers but this run has no section for at all (e.g. its `go test
// <pkg> -bench=...` line was removed from the Makefile) fails the gate,
// rather than only printing an info line — every benchmark in it evaded the
// gate by disappearing, not by regressing.
func TestRun_WholePackageDroppedIsHardFailure(t *testing.T) {
	dir := t.TempDir()

	baseline := pkgBlock("pkg/dropped", benchLines("BenchmarkWholePkgGone", 100, tenJitterSamples(100)))
	newRun := pkgBlock("pkg/other", benchLines("BenchmarkStillHere", 50, tenJitterSamples(50)))

	baselinePath := filepath.Join(dir, "baseline.txt")
	newPath := filepath.Join(dir, "new.txt")
	writeFileT(t, baselinePath, baseline)
	writeFileT(t, newPath, newRun)

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-baseline", baselinePath,
		"-new", newPath,
		"-tools-dir", realToolsDir,
		"-scratch-dir", filepath.Join(dir, "scratch"),
	}, &stdout, &stderr)

	if code != 1 {
		t.Fatalf("got exit code %d, want 1 (a whole package disappearing must fail the gate)\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "pkg/dropped") {
		t.Errorf("stderr does not identify the dropped package:\n%s", stderr.String())
	}
}

// TestRun_BenchmarkDroppedFromExistingPackageIsHardFailure proves the
// finer-grained case: pkg/mixed is present in both baseline and new (so it
// IS benchstat-compared), but one of its two baselined benchmarks no longer
// appears in the new run — e.g. deleted, or no longer matched by the
// -bench regex. That single benchmark must fail the gate even though its
// sibling in the same package passes cleanly.
func TestRun_BenchmarkDroppedFromExistingPackageIsHardFailure(t *testing.T) {
	dir := t.TempDir()

	baseline := pkgBlock("pkg/mixed",
		benchLines("BenchmarkKept", 100, tenJitterSamples(100))+
			benchLines("BenchmarkDropped", 50, tenJitterSamples(50)))
	newRun := pkgBlock("pkg/mixed", benchLines("BenchmarkKept", 100, tenJitterSamples(100)))

	baselinePath := filepath.Join(dir, "baseline.txt")
	newPath := filepath.Join(dir, "new.txt")
	writeFileT(t, baselinePath, baseline)
	writeFileT(t, newPath, newRun)

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-baseline", baselinePath,
		"-new", newPath,
		"-tools-dir", realToolsDir,
		"-scratch-dir", filepath.Join(dir, "scratch"),
	}, &stdout, &stderr)

	if code != 1 {
		t.Fatalf("got exit code %d, want 1 (a benchmark disappearing from a still-gated package must fail)\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	// benchstat's display names drop the "Benchmark" prefix (e.g. "Foo-8",
	// not "BenchmarkFoo-8" — see TestRun_CatchesInjectedRegressionOverThreshold's
	// equivalent "Foo" substring check above).
	if !strings.Contains(stderr.String(), "Dropped-8") {
		t.Errorf("stderr does not identify the dropped benchmark:\n%s", stderr.String())
	}
	if strings.Contains(stderr.String(), "Kept-8") {
		t.Errorf("the still-present benchmark was wrongly flagged:\n%s", stderr.String())
	}
}

// TestRun_HardwareMismatchedBaselineIsHardError is the primary
// fabricated-regression regression test, in its most dangerous form.
//
// Here the two runs have IDENTICAL benchmark names and identical numbers;
// only the machine differs. Before the fix, splitByPackage dropped the first
// (and only) package's cpu: line entirely, so both sections looked
// same-hardware to benchstat and the gate reported a clean PASS — a
// cross-hardware comparison presented as a valid result. Nothing about the
// output would have told anyone.
//
// A gate that cannot compare must fail loudly and name the reason. It must
// not pass, and it must not print a percentage.
func TestRun_HardwareMismatchedBaselineIsHardError(t *testing.T) {
	dir := t.TempDir()

	body := benchLines("BenchmarkFoo", 100, tenJitterSamples(100))
	baseline := pkgBlockCPU("pkg/x", "Apple M4 Max", body)
	newRun := pkgBlockCPU("pkg/x", "Apple M3 Pro", body)

	baselinePath := filepath.Join(dir, "baseline.txt")
	newPath := filepath.Join(dir, "new.txt")
	writeFileT(t, baselinePath, baseline)
	writeFileT(t, newPath, newRun)

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-baseline", baselinePath,
		"-new", newPath,
		"-tools-dir", realToolsDir,
		"-scratch-dir", filepath.Join(dir, "scratch"),
	}, &stdout, &stderr)

	if code != 2 {
		t.Fatalf("got exit code %d, want 2 (a baseline from different hardware is uncomparable, not a verdict)\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	all := stdout.String() + stderr.String()
	for _, want := range []string{"cpu", "Apple M4 Max", "Apple M3 Pro", "pkg/x"} {
		if !strings.Contains(all, want) {
			t.Errorf("output does not name %q, so it does not name the reason:\n%s", want, all)
		}
	}
	if strings.Contains(all, "regressed") || strings.Contains(all, "PASS") {
		t.Errorf("output reports a verdict on a comparison that could not be made:\n%s", all)
	}
}

// TestRun_ZeroNameOverlapAcrossMachinesEmitsNoPercentages reproduces the
// observed incident shape end to end: an M4 Max baseline with -16 name
// suffixes against an M3 Pro run with -12 suffixes, across TWO packages (the
// arrangement in which the old cpu-line misattribution actually surfaced).
//
// The observed failure was 41 lines of the form "QueueDispatch_Claim-12 B/op
// regressed +248.00%", where 248 is the raw B/op value — identical on both
// sides. This test's real assertion is the negative one: no percentage may
// appear anywhere in the output.
func TestRun_ZeroNameOverlapAcrossMachinesEmitsNoPercentages(t *testing.T) {
	dir := t.TempDir()

	baseline := pkgBlockCPU("pkg/root", "Apple M4 Max", benchLines("BenchmarkVersus_AutoAck", 4000, tenJitterSamples(4000))) +
		pkgBlockCPU("pkg/broker", "Apple M4 Max", benchLines("BenchmarkQueueDispatch_Claim", 102, tenJitterSamples(102)))
	newRun := pkgBlockCPU("pkg/root", "Apple M3 Pro", benchLines("BenchmarkVersus_AutoAck", 3000, tenJitterSamples(3000))) +
		pkgBlockCPU("pkg/broker", "Apple M3 Pro", benchLines("BenchmarkQueueDispatch_Claim", 89, tenJitterSamples(89)))

	baselinePath := filepath.Join(dir, "baseline.txt")
	newPath := filepath.Join(dir, "new.txt")
	writeFileT(t, baselinePath, baseline)
	writeFileT(t, newPath, newRun)

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-baseline", baselinePath,
		"-new", newPath,
		"-tools-dir", realToolsDir,
		"-scratch-dir", filepath.Join(dir, "scratch"),
	}, &stdout, &stderr)

	if code != 2 {
		t.Fatalf("got exit code %d, want 2\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	all := stdout.String() + stderr.String()
	if strings.Contains(all, "regressed") {
		t.Errorf("the gate fabricated a regression from a comparison it could not make:\n%s", all)
	}
	if i := strings.Index(all, "%"); i >= 0 {
		t.Errorf("the gate emitted a percentage (%q) for a comparison it could not make:\n%s", all[max(0, i-40):i+1], all)
	}
}

// TestRun_SameHardwareZeroOverlapStillReportsMissing guards against
// over-broadening the new hard error. Same machine on both sides, but every
// benchmark was renamed: that is a real, comparable run in which the gated
// benchmarks disappeared, and it must keep failing as
// "missing from this run" (exit 1) rather than being reclassified as
// "cannot compare" (exit 2).
func TestRun_SameHardwareZeroOverlapStillReportsMissing(t *testing.T) {
	dir := t.TempDir()

	baseline := pkgBlockCPU("pkg/x", "Test CPU", benchLines("BenchmarkOldName", 100, tenJitterSamples(100)))
	newRun := pkgBlockCPU("pkg/x", "Test CPU", benchLines("BenchmarkNewName", 100, tenJitterSamples(100)))

	baselinePath := filepath.Join(dir, "baseline.txt")
	newPath := filepath.Join(dir, "new.txt")
	writeFileT(t, baselinePath, baseline)
	writeFileT(t, newPath, newRun)

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-baseline", baselinePath,
		"-new", newPath,
		"-tools-dir", realToolsDir,
		"-scratch-dir", filepath.Join(dir, "scratch"),
	}, &stdout, &stderr)

	if code != 1 {
		t.Fatalf("got exit code %d, want 1 (comparable run, gated benchmark vanished)\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	if !strings.Contains(stderr.String(), "missing from this run") {
		t.Errorf("stderr does not report the renamed benchmark as missing:\n%s", stderr.String())
	}
}

// TestRun_MatchingHardwarePasses is the explicit no-break check for the
// working path: identical cpu on both sides must still compare and pass.
func TestRun_MatchingHardwarePasses(t *testing.T) {
	dir := t.TempDir()

	body := benchLines("BenchmarkFoo", 100, tenJitterSamples(100))
	baseline := pkgBlockCPU("pkg/x", "Apple M3 Pro", body) + pkgBlockCPU("pkg/y", "Apple M3 Pro", body)
	newRun := pkgBlockCPU("pkg/x", "Apple M3 Pro", body) + pkgBlockCPU("pkg/y", "Apple M3 Pro", body)

	baselinePath := filepath.Join(dir, "baseline.txt")
	newPath := filepath.Join(dir, "new.txt")
	writeFileT(t, baselinePath, baseline)
	writeFileT(t, newPath, newRun)

	var stdout, stderr bytes.Buffer
	code := run([]string{
		"-baseline", baselinePath,
		"-new", newPath,
		"-tools-dir", realToolsDir,
		"-scratch-dir", filepath.Join(dir, "scratch"),
	}, &stdout, &stderr)

	if code != 0 {
		t.Fatalf("got exit code %d, want 0 (same-machine comparison must still pass)\nstdout:\n%s\nstderr:\n%s", code, stdout.String(), stderr.String())
	}
	for _, want := range []string{"pkg/x", "pkg/y"} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("stdout does not report %s as compared — a silently skipped package is not a pass:\n%s", want, stdout.String())
		}
	}
}

func writeFileT(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}
