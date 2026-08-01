package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// fakeProbes builds a probe set describing a hypothetical data dir. No test
// can arrange a real ZFS dataset, which is exactly why the probes are seams.
func fakeProbes(onZFS bool, dataset, compression string) compressionProbes {
	return compressionProbes{
		onZFS: func(string) bool { return onZFS },
		datasetFor: func(string) (string, bool) {
			return dataset, dataset != ""
		},
		property: func(string, string) (string, bool) {
			return compression, compression != ""
		},
	}
}

func TestCompressionAdvisory(t *testing.T) {
	cases := []struct {
		name        string
		probes      compressionProbes
		wantWarning bool
	}{
		{"not on zfs", fakeProbes(false, "tank/cache", "lz4"), false},
		{"zfs with compression off", fakeProbes(true, "tank/cache", "off"), false},
		{"zfs with an unset property", fakeProbes(true, "tank/cache", "-"), false},
		{"zfs with lz4", fakeProbes(true, "tank/cache", "lz4"), true},
		{"zfs with zstd", fakeProbes(true, "tank/cache", "zstd-3"), true},
		{"zfs with gzip", fakeProbes(true, "tank/cache", "gzip-9"), true},
		// Every uncertainty stays silent: an advisory nobody can act on is
		// noise on every boot.
		{"dataset unknown", fakeProbes(true, "", "lz4"), false},
		{"property unreadable", fakeProbes(true, "tank/cache", ""), false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			msg := compressionAdvisory("/data", tc.probes)
			if !tc.wantWarning {
				assert.Empty(t, msg)
				return
			}
			assert.Contains(t, msg, "WARNING")
			assert.Contains(t, msg, "tank/cache")
			// The point is not "you have compression on" but "it is the
			// SECOND pass over the same bytes", plus the exact fix.
			assert.Contains(t, msg, "SECOND time")
			assert.Contains(t, msg, "zfs set compression=off tank/cache")
		})
	}
}

func TestCompressionAdvisoryNamesTheCompressor(t *testing.T) {
	// The operator needs to know WHICH compressor is burning the CPU, since
	// gzip-9 over already-lz4 data costs far more than lz4 does.
	msg := compressionAdvisory("/data", fakeProbes(true, "tank/cache", "gzip-9"))
	assert.Contains(t, msg, "compression=gzip-9")
}

func TestLogCompressionAdvisorySilentForMissingDir(t *testing.T) {
	var lines []string
	logCompressionAdvisory("/definitely/not/a/real/dir", func(f string, a ...any) {
		lines = append(lines, f)
	})
	assert.Empty(t, lines)
}

func TestLogCompressionAdvisoryOnRealDataDir(t *testing.T) {
	// Whatever this machine's filesystem is, the startup path must not blow
	// up -- and on a non-ZFS dir it must stay quiet.
	var lines []string
	logCompressionAdvisory(t.TempDir(), func(f string, a ...any) {
		lines = append(lines, strings.TrimSpace(f))
	})
	if dirIsZFS(t.TempDir()) {
		t.Skip("this machine's temp dir is on ZFS; the advisory legitimately depends on its properties")
	}
	assert.Empty(t, lines)
}
