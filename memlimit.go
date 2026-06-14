package main

import (
	"log"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

// memLimitRatio is the fraction of the container's memory limit used as the Go
// soft memory limit (GOMEMLIMIT). The headroom below the hard cgroup limit lets
// the garbage collector reclaim before the kernel OOM-killer fires.
const memLimitRatio = 0.9

// configureMemoryLimit sets a soft Go memory limit (GOMEMLIMIT) derived from the
// container's cgroup memory limit, so the runtime works to stay under the limit
// instead of letting RSS climb into an OOM-kill — the failure a fronting proxy
// reports as a 502. It is defense in depth: with body streaming the server's
// heap is already tiny, but a soft limit guarantees that any unexpected
// allocation pressure triggers GC rather than a kill.
//
// Precedence: an explicit GOMEMLIMIT env var wins (the Go runtime already
// applied it at startup), so this only acts when the operator has not set one
// and the process is running under a finite cgroup memory limit.
func configureMemoryLimit() {
	if os.Getenv("GOMEMLIMIT") != "" {
		return // operator set it explicitly; the runtime already honors it
	}
	limit, ok := detectCgroupMemoryLimit()
	if !ok {
		return
	}
	soft := computeMemLimit(limit, memLimitRatio)
	if soft <= 0 {
		return
	}
	debug.SetMemoryLimit(soft)
	log.Printf("memory: GOMEMLIMIT auto-set to %d bytes (%.0f%% of cgroup limit %d bytes)",
		soft, memLimitRatio*100, limit)
}

// computeMemLimit returns ratio*limit as an int64, or 0 if the inputs are unusable.
func computeMemLimit(limit int64, ratio float64) int64 {
	if limit <= 0 || ratio <= 0 {
		return 0
	}
	return int64(float64(limit) * ratio)
}

// detectCgroupMemoryLimit reads the container memory limit from cgroup v2, then
// v1. Returns ok=false when there is no finite limit (unlimited, or not in a
// constrained cgroup), in which case no soft limit is set.
func detectCgroupMemoryLimit() (int64, bool) {
	if v, ok := parseCgroupMemMax(readFileTrim("/sys/fs/cgroup/memory.max")); ok {
		return v, true // cgroup v2
	}
	if v, ok := parseCgroupMemMax(readFileTrim("/sys/fs/cgroup/memory/memory.limit_in_bytes")); ok {
		return v, true // cgroup v1
	}
	return 0, false
}

func readFileTrim(path string) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}

// parseCgroupMemMax parses a cgroup memory-limit value. It rejects the v2 "max"
// sentinel, empty/garbage values, and the very large v1 "unlimited" sentinel
// (near 2^63), returning ok=false for an effectively unbounded limit.
func parseCgroupMemMax(s string) (int64, bool) {
	if s == "" || s == "max" {
		return 0, false
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || v <= 0 {
		return 0, false
	}
	// cgroup v1 uses a near-max sentinel (e.g. 0x7ffffffffffff000) for
	// "unlimited"; treat any absurdly large value as no real limit.
	if v >= (int64(1) << 62) {
		return 0, false
	}
	return v, true
}
