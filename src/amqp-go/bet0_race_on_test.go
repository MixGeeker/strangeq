//go:build race

package main

// bet0RaceEnabled reports whether this test binary was built with -race.
//
// It exists because the race detector changes what the multi-queue liveness
// numbers MEAN, not merely how large they are — see the header of
// bet0_liveness_predicate_test.go. CI runs -race and only -race, so this
// constant selects the predicate CI actually evaluates.
const bet0RaceEnabled = true
