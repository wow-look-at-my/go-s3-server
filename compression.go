package main

// Nothing in this server compresses.

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// compressionProbes are each questions the advisory needs answered, as seams.
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
// Silence is the default on every uncertainty: a non-ZFS filesystem, a
// dataset that cannot be identified, an unreadable property, or compression
// already off. An advisory nobody can act on is noise on every boot.
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

// zfsProperty reads a single property of a dataset.
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

// logCompressionAdvisory prints the advisory, if any. Called a single time at startup.
func logCompressionAdvisory(dataDir string, logf func(string, ...any)) {
	if _, err := os.Stat(dataDir); err != nil {
		return
	}
	if msg := compressionAdvisory(dataDir, defaultCompressionProbes); msg != "" {
		logf("%s", msg)
	}
}
