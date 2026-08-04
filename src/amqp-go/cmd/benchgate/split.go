package main

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// pkgSection is one package's slice of a combined `go test -bench` output
// stream: the "goos:"/"goarch:"/"cpu:"/"pkg:" header for that package, plus
// every following line up to (but not including) the next "pkg:" line or
// EOF.
type pkgSection struct {
	pkg  string
	body []byte
}

// splitByPackage splits a benchmark output stream that may contain results
// from several packages — e.g. `make bench-gate` appends one `go test <pkg>
// -bench=...` invocation's output per in-scope package into a single file —
// into one section per package, keyed by the import path on each
// "pkg: <import-path>" line.
//
// This is what makes per-package benchstat comparisons possible from a
// single committed baseline file: two benchmarks that happen to share a
// name in different packages (root/broker/storage/protocol) never end up in
// the same benchstat comparison, because they never share a benchstat input
// file — each package's body is diffed against its own baseline body only
// (see main.go).
//
// Metadata-line attribution is not uniform, and getting it wrong silently
// destroys provenance. `go test` prints its header in the order
//
//	goos: / goarch: / pkg: / cpu:
//
// — goos and goarch BEFORE the "pkg:" line, cpu AFTER it. Treating all three
// as "buffer until the next pkg: line" (which this function used to do)
// therefore attached every package's cpu line to the FOLLOWING package's
// section, left the first package with no cpu line at all, and dropped the
// last package's entirely.
//
// That is not cosmetic: the cpu line is the only record of which machine a
// section was measured on, benchstat keys its configurations on it, and
// main.go refuses to compare sections whose configurations differ. A section
// that has lost its cpu line compares as if it were same-hardware.
//
// The rule used instead: a metadata line belongs to the OPEN section if that
// section has not started emitting results yet (i.e. it is still inside its
// own header), and otherwise begins the preamble of the next section.
func splitByPackage(data []byte) []pkgSection {
	var sections []pkgSection
	var cur *pkgSection
	var preamble []string
	inHeader := false // the open section is still inside its own go-test header

	finish := func() {
		if cur != nil {
			sections = append(sections, *cur)
			cur = nil
		}
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case isMetadataLine(line):
			if cur != nil && inHeader {
				appendLine(cur, line)
			} else {
				preamble = append(preamble, line)
			}
			continue
		case strings.HasPrefix(line, "pkg: "):
			finish()
			cur = &pkgSection{pkg: strings.TrimSpace(strings.TrimPrefix(line, "pkg: "))}
			for _, l := range preamble {
				appendLine(cur, l)
			}
			preamble = preamble[:0]
			appendLine(cur, line)
			inHeader = true
			continue
		}
		if cur == nil {
			// Content before any "pkg:" line has no section to attribute
			// it to (shouldn't happen for well-formed `go test` output).
			continue
		}
		if line != "" {
			inHeader = false
		}
		appendLine(cur, line)
	}
	finish()
	return sections
}

// configKeys are the go-test header keys that identify the machine and
// toolchain a benchmark section was measured on. benchstat treats these as
// part of a result's *configuration*: two files whose configurations differ
// are not merged into one comparison table at all, they are reported as two
// separate single-file tables with no delta column (see parse.go).
//
// They are listed here in one place because both splitByPackage (which must
// route them to the right section) and sectionConfig (which must compare
// them across sections) encode the same fact.
var configKeys = []string{"goos", "goarch", "cpu"}

func isMetadataLine(line string) bool {
	for _, k := range configKeys {
		if strings.HasPrefix(line, k+":") {
			return true
		}
	}
	return false
}

// sectionConfig extracts a section's machine/toolchain identity. A key absent
// from the section is absent from the map — callers must treat
// present-on-one-side-only as a mismatch, not as a match, since a section
// that never recorded its cpu is a section whose hardware is unknown, not one
// whose hardware agrees.
func sectionConfig(body []byte) map[string]string {
	cfg := make(map[string]string, len(configKeys))
	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for scanner.Scan() {
		line := scanner.Text()
		for _, k := range configKeys {
			if strings.HasPrefix(line, k+":") {
				cfg[k] = strings.TrimSpace(strings.TrimPrefix(line, k+":"))
			}
		}
	}
	return cfg
}

// configMismatch describes every configuration key on which two sections
// disagree, in configKeys order, as "key: baseline X vs new Y". An empty
// result means the two sections are comparable.
func configMismatch(baseline, current map[string]string) []string {
	var diffs []string
	for _, k := range configKeys {
		b, bOK := baseline[k]
		c, cOK := current[k]
		if bOK && cOK && b == c {
			continue
		}
		if !bOK && !cOK {
			continue
		}
		diffs = append(diffs, fmt.Sprintf("%s: baseline %s vs new run %s", k, quoteOrAbsent(b, bOK), quoteOrAbsent(c, cOK)))
	}
	return diffs
}

func quoteOrAbsent(v string, ok bool) string {
	if !ok {
		return "(not recorded)"
	}
	return strconv.Quote(v)
}

func appendLine(s *pkgSection, line string) {
	s.body = append(s.body, line...)
	s.body = append(s.body, '\n')
}

// mergeSections merges sections sharing the same package (concatenating
// their bodies, in encounter order) and returns them keyed by package,
// alongside the first-seen package order for deterministic reporting.
func mergeSections(sections []pkgSection) (byPkg map[string][]byte, order []string) {
	byPkg = make(map[string][]byte)
	for _, s := range sections {
		if _, ok := byPkg[s.pkg]; !ok {
			order = append(order, s.pkg)
		}
		byPkg[s.pkg] = append(byPkg[s.pkg], s.body...)
	}
	return byPkg, order
}
