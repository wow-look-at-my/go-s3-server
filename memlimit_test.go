package main

import (
	"os"
	"path/filepath"
	"runtime/debug"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseCgroupMemMax(t *testing.T) {
	cases := []struct {
		in   string
		want int64
		ok   bool
	}{
		{"max", 0, false},
		{"", 0, false},
		{"garbage", 0, false},
		{"0", 0, false},
		{"-5", 0, false},
		{"1073741824", 1073741824, true},  // 1 GiB
		{"536870912", 536870912, true},    // 512 MiB
		{"9223372036854771712", 0, false}, // cgroup v1 "unlimited" sentinel
	}
	for _, c := range cases {
		got, ok := parseCgroupMemMax(c.in)
		assert.Equalf(t, c.ok, ok, "ok for %q", c.in)
		assert.Equalf(t, c.want, got, "value for %q", c.in)
	}
}

func TestComputeMemLimit(t *testing.T) {
	cases := []struct {
		limit int64
		ratio float64
		want  int64
	}{
		{1073741824, 0.9, 966367641}, // 90% of 1 GiB
		{1000, 0.5, 500},
		{0, 0.9, 0},
		{1000, 0, 0},
		{-1, 0.9, 0},
	}
	for _, c := range cases {
		assert.Equal(t, c.want, computeMemLimit(c.limit, c.ratio))
	}
}

func TestReadFileTrim(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "v")
	require.NoError(t, os.WriteFile(p, []byte("  1073741824\n"), 0o644))
	assert.Equal(t, "1073741824", readFileTrim(p))
	assert.Equal(t, "", readFileTrim(filepath.Join(dir, "missing")))
}

func TestDetectCgroupMemoryLimit(t *testing.T) {
	dir := t.TempDir()
	v2 := filepath.Join(dir, "memory.max")
	v1 := filepath.Join(dir, "memory.limit_in_bytes")

	orig2, orig1 := cgroupV2MemMaxPath, cgroupV1MemLimitPath
	t.Cleanup(func() { cgroupV2MemMaxPath, cgroupV1MemLimitPath = orig2, orig1 })
	cgroupV2MemMaxPath, cgroupV1MemLimitPath = v2, v1

	// v2 present and finite -> use it.
	require.NoError(t, os.WriteFile(v2, []byte("2147483648\n"), 0o644))
	got, ok := detectCgroupMemoryLimit()
	assert.True(t, ok)
	assert.Equal(t, int64(2147483648), got)

	// v2 "max" -> fall through to v1.
	require.NoError(t, os.WriteFile(v2, []byte("max\n"), 0o644))
	require.NoError(t, os.WriteFile(v1, []byte("1073741824\n"), 0o644))
	got, ok = detectCgroupMemoryLimit()
	assert.True(t, ok)
	assert.Equal(t, int64(1073741824), got)

	// Neither finite -> no limit.
	require.NoError(t, os.Remove(v1))
	got, ok = detectCgroupMemoryLimit()
	assert.False(t, ok)
	assert.Equal(t, int64(0), got)
}

func TestConfigureMemoryLimit(t *testing.T) {
	orig := debug.SetMemoryLimit(-1) // snapshot without changing
	t.Cleanup(func() { debug.SetMemoryLimit(orig) })

	// Explicit GOMEMLIMIT -> no-op (the runtime already applied it).
	t.Setenv("GOMEMLIMIT", "512MiB")
	before := debug.SetMemoryLimit(-1)
	configureMemoryLimit()
	assert.Equal(t, before, debug.SetMemoryLimit(-1), "must not override an explicit GOMEMLIMIT")

	// No env, finite cgroup limit -> soft limit set to 90%.
	t.Setenv("GOMEMLIMIT", "")
	dir := t.TempDir()
	v2 := filepath.Join(dir, "memory.max")
	require.NoError(t, os.WriteFile(v2, []byte("1073741824\n"), 0o644)) // 1 GiB
	orig2, orig1 := cgroupV2MemMaxPath, cgroupV1MemLimitPath
	t.Cleanup(func() { cgroupV2MemMaxPath, cgroupV1MemLimitPath = orig2, orig1 })
	cgroupV2MemMaxPath = v2
	cgroupV1MemLimitPath = filepath.Join(dir, "absent")

	configureMemoryLimit()
	assert.Equal(t, int64(966367641), debug.SetMemoryLimit(-1), "should set 90% of the 1 GiB cgroup limit")
}
