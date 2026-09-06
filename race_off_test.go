//go:build !race

package main

// raceDetectorEnabled is false in normal (non-race) builds, where the load
// test's heap-size assertion is meaningful.
const raceDetectorEnabled = false
