//go:build !race

package main

// bet0RaceEnabled reports whether this test binary was built with -race.
// See bet0_race_on_test.go for why the distinction is load-bearing.
const bet0RaceEnabled = false
