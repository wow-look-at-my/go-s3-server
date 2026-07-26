//go:build race

package main

// raceDetectorEnabled is true when the test binary is built with -race. The
// load test uses it to skip the heap-size assertion, whose threshold is only
// meaningful without the race detector's allocation overhead.
const raceDetectorEnabled = true
