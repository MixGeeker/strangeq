package main

import (
	"strings"
	"testing"
)

func TestSplitByPackage_SinglePackage(t *testing.T) {
	input := "goos: darwin\n" +
		"goarch: arm64\n" +
		"pkg: github.com/maxpert/amqp-go/broker\n" +
		"cpu: Apple M4 Max\n" +
		"BenchmarkFoo-16    	     200	        21.04 ns/op	       0 B/op	       0 allocs/op\n" +
		"PASS\n" +
		"ok  	github.com/maxpert/amqp-go/broker	0.239s\n"

	sections := splitByPackage([]byte(input))
	if len(sections) != 1 {
		t.Fatalf("got %d sections, want 1", len(sections))
	}
	if sections[0].pkg != "github.com/maxpert/amqp-go/broker" {
		t.Fatalf("got pkg %q", sections[0].pkg)
	}
	if !strings.Contains(string(sections[0].body), "BenchmarkFoo-16") {
		t.Fatalf("section body missing benchmark line:\n%s", sections[0].body)
	}
	if !strings.Contains(string(sections[0].body), "goos: darwin") {
		t.Fatalf("section body missing preamble:\n%s", sections[0].body)
	}
}

func TestSplitByPackage_MultiplePackagesAppended(t *testing.T) {
	// Mirrors what `make bench-gate` produces: several `go test <pkg>
	// -bench=... >> file` invocations appended in sequence, each with its
	// own complete goos/goarch/pkg/cpu header.
	input := "goos: darwin\n" +
		"goarch: arm64\n" +
		"pkg: github.com/maxpert/amqp-go\n" +
		"cpu: Apple M4 Max\n" +
		"BenchmarkVersus_AutoAck-16    	     200	      4704 ns/op	    1472 B/op	      32 allocs/op\n" +
		"PASS\n" +
		"ok  	github.com/maxpert/amqp-go	0.239s\n" +
		"goos: darwin\n" +
		"goarch: arm64\n" +
		"pkg: github.com/maxpert/amqp-go/broker\n" +
		"cpu: Apple M4 Max\n" +
		"BenchmarkQueueDispatch_Publish-16    	     200	        21.04 ns/op	       0 B/op	       0 allocs/op\n" +
		"PASS\n" +
		"ok  	github.com/maxpert/amqp-go/broker	0.204s\n"

	sections := splitByPackage([]byte(input))
	if len(sections) != 2 {
		t.Fatalf("got %d sections, want 2", len(sections))
	}
	if sections[0].pkg != "github.com/maxpert/amqp-go" {
		t.Fatalf("section 0 pkg = %q", sections[0].pkg)
	}
	if sections[1].pkg != "github.com/maxpert/amqp-go/broker" {
		t.Fatalf("section 1 pkg = %q", sections[1].pkg)
	}
	// Cross-package isolation: a benchmark name repeated in each package
	// must not leak from one section's body into the other.
	if strings.Contains(string(sections[0].body), "QueueDispatch_Publish") {
		t.Fatalf("root section leaked broker package content:\n%s", sections[0].body)
	}
	if strings.Contains(string(sections[1].body), "Versus_AutoAck") {
		t.Fatalf("broker section leaked root package content:\n%s", sections[1].body)
	}
}

// TestSplitByPackage_CPULineBelongsToItsOwnPackage pins the metadata
// attribution contract that the two tests above cannot see, because both use
// the SAME cpu string for every package.
//
// `go test` emits its header in the order goos:, goarch:, pkg:, cpu: — the
// cpu line comes AFTER the pkg line, unlike the other two. Classifying all
// three as "preamble buffered until the next pkg: line" therefore attaches
// each package's cpu line to the FOLLOWING package's section, leaves the
// first package with no cpu line at all, and drops the last package's
// entirely.
//
// This is not cosmetic. The cpu line is the only record of which machine a
// section was measured on, and benchstat keys its configurations on it: a
// section that has lost its cpu line silently compares as if it were
// same-hardware. See TestRun_HardwareMismatchedBaselineIsHardError.
func TestSplitByPackage_CPULineBelongsToItsOwnPackage(t *testing.T) {
	input := "goos: darwin\ngoarch: arm64\npkg: pkg/a\ncpu: CPU Alpha\n" +
		"BenchmarkX-8    	     200	        21.04 ns/op\n" +
		"PASS\nok  	pkg/a	0.10s\n" +
		"goos: darwin\ngoarch: arm64\npkg: pkg/b\ncpu: CPU Beta\n" +
		"BenchmarkY-8    	     200	        22.04 ns/op\n" +
		"PASS\nok  	pkg/b	0.10s\n"

	sections := splitByPackage([]byte(input))
	if len(sections) != 2 {
		t.Fatalf("got %d sections, want 2", len(sections))
	}

	for i, want := range []struct{ pkg, cpu, notCPU string }{
		{"pkg/a", "cpu: CPU Alpha", "cpu: CPU Beta"},
		{"pkg/b", "cpu: CPU Beta", "cpu: CPU Alpha"},
	} {
		body := string(sections[i].body)
		if sections[i].pkg != want.pkg {
			t.Fatalf("section %d pkg = %q, want %q", i, sections[i].pkg, want.pkg)
		}
		if !strings.Contains(body, want.cpu) {
			t.Errorf("section %d (%s) is missing its own %q — the machine it was measured on is unrecorded:\n%s",
				i, want.pkg, want.cpu, body)
		}
		if strings.Contains(body, want.notCPU) {
			t.Errorf("section %d (%s) carries %q, which belongs to the other package:\n%s",
				i, want.pkg, want.notCPU, body)
		}
	}
}

func TestMergeSections_DuplicatePackageConcatenates(t *testing.T) {
	sections := []pkgSection{
		{pkg: "a", body: []byte("BenchmarkX-1  1  1 ns/op\n")},
		{pkg: "b", body: []byte("BenchmarkY-1  1  1 ns/op\n")},
		{pkg: "a", body: []byte("BenchmarkX-1  1  2 ns/op\n")},
	}
	byPkg, order := mergeSections(sections)

	if got := []string{"a", "b"}; order[0] != got[0] || order[1] != got[1] {
		t.Fatalf("order = %v, want %v", order, got)
	}
	if count := strings.Count(string(byPkg["a"]), "BenchmarkX-1"); count != 2 {
		t.Fatalf("merged package %q has %d benchmark lines, want 2:\n%s", "a", count, byPkg["a"])
	}
}

func TestSplitByPackage_Empty(t *testing.T) {
	if sections := splitByPackage([]byte("")); len(sections) != 0 {
		t.Fatalf("got %d sections for empty input, want 0", len(sections))
	}
}
