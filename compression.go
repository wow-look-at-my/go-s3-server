package main

// Nothing in this server compresses. Bodies arrive already lz4-compressed by
// the go-toolchain client, are stored byte-for-byte by PutStream, and are
// served back untouched -- no gzip middleware, no Content-Encoding, and the
// batch tar is uncompressed. The only decompression is the read-path guards
// peeking one lz4 block, memoized per key (see cleanmemo.go).
//
// That leaves exactly one place a second compression pass can hide: the
// filesystem. A ZFS dataset with compression enabled will run every stored
// body through lz4 (or worse, gzip/zstd) a SECOND time, for data the client
// already squeezed -- burning CPU on every write in exchange for approximately
// nothing. ZFS's early-abort heuristic limits the damage on incompressible
// data but does not remove the attempt, and a cache under CI load is close to
// write-saturated.
//
// The server cannot fix the operator's dataset, so it says so once, at
// startup, next to the other "this configuration will hurt you" warnings.

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// compressionProbes are the three questions the advisory needs answered, as
// seams: whether the data dir lives on ZFS, which dataset it belongs to, and
// what that dataset's compression property is. Swapped out in tests -- no test
// can arrange for a real ZFS dataset.
type compressionProbes struct {
	onZFS      func(dir string) bool
	datasetFor func(dir string) (string, bool)
	property   func(dataset, name string) (string, bool)
}

// defaultCompressionProbes reads the real system.
var defaultCompressionProbes = compressionProbes{
	onZFS:      dirIsZFS,
	datasetFor: zfsDatasetFor,
	property:   zfsProperty,
}

// compressionAdvisory returns the startup warning for a data dir that sits on
// a compressing filesystem, or "" when there is nothing to say.
//
// Silence is the default on every uncertainty: a non-ZFS filesystem, a dataset
// that cannot be identified, an unreadable property, or compression already
// off. An advisory nobody can act on is just noise on every boot.
func compressionAdvisory(dataDir string, p compressionProbes) string {
	if !p.onZFS(dataDir) {
		return ""
	}
	dataset, ok := p.datasetFor(dataDir)
	if !ok {
		return ""
	}
	value, ok := p.property(dataset, "compression")
	if !ok {
		return ""
	}
	value = strings.TrimSpace(value)
	if value == "" || value == "off" || value == "-" {
		return ""
	}
	return fmt.Sprintf(
		"WARNING: data_dir is on ZFS dataset %s with compression=%s, which compresses every stored body a SECOND time. "+
			"Cache bodies arrive already lz4-compressed from the client and this server never compresses or recompresses them, "+
			"so the dataset's pass costs CPU on every write for approximately no space saved -- a visible cost under CI write bursts. "+
			"Consider: zfs set compression=off %s (existing data keeps its current compression until rewritten).",
		dataset, value, dataset)
}

// zfsDatasetFor returns the ZFS dataset backing dir, by asking the zfs tool
// which dataset owns the path.
func zfsDatasetFor(dir string) (string, bool) {
	out, err := exec.Command("zfs", "list", "-H", "-o", "name", dir).Output()
	if err != nil {
		return "", false
	}
	name := strings.TrimSpace(string(out))
	if name == "" {
		return "", false
	}
	return name, true
}

// zfsProperty reads one property of a dataset.
func zfsProperty(dataset, name string) (string, bool) {
	out, err := exec.Command("zfs", "get", "-H", "-o", "value", name, dataset).Output()
	if err != nil {
		return "", false
	}
	value := strings.TrimSpace(string(out))
	if value == "" {
		return "", false
	}
	return value, true
}

// logCompressionAdvisory prints the advisory, if any. Called once at startup.
func logCompressionAdvisory(dataDir string, logf func(string, ...any)) {
	if _, err := os.Stat(dataDir); err != nil {
		return
	}
	if msg := compressionAdvisory(dataDir, defaultCompressionProbes); msg != "" {
		logf("%s", msg)
	}
}
