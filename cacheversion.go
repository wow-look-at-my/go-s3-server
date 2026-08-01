package main

// The data_dir's format version and the startup hygiene that goes with it:
// purging a cache written under a version we no longer trust, and sweeping the
// temp files an interrupted upload left behind. Both run once, before the
// server serves anything, under the data_dir's exclusive lock.

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// ensureCacheVersion reads the data_dir's version marker. If missing, it is
// treated as version 1. If the stored version does not match
// currentCacheVersion, every entry in the data_dir (except the lock file) is
// removed and a new version marker is written. This forces the operator to
// rebuild the cache from trusted inputs whenever we bump the version, e.g.
// after fixing a vulnerability that could have let an attacker populate the
// cache.
func ensureCacheVersion(dataDir string) error {
	stored, err := readCacheVersion(dataDir)
	if err != nil {
		return err
	}
	if stored == currentCacheVersion {
		return nil
	}
	log.Printf("cache: stored version %d != current %d; purging data_dir=%s", stored, currentCacheVersion, dataDir)
	if err := purgeDataDir(dataDir); err != nil {
		return fmt.Errorf("purge data dir: %w", err)
	}
	if err := writeCacheVersion(dataDir, currentCacheVersion); err != nil {
		return fmt.Errorf("write cache version: %w", err)
	}
	log.Printf("cache: purged and marked as version %d", currentCacheVersion)
	return nil
}

func readCacheVersion(dataDir string) (int, error) {
	data, err := os.ReadFile(filepath.Join(dataDir, cacheVersionFile))
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			// No marker: treat as version 1 (any cache predating this feature).
			return 1, nil
		}
		return 0, fmt.Errorf("read cache version: %w", err)
	}
	s := strings.TrimSpace(string(data))
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("cache version file %s is corrupt (%q): %w", cacheVersionFile, s, err)
	}
	return v, nil
}

func writeCacheVersion(dataDir string, v int) error {
	return os.WriteFile(filepath.Join(dataDir, cacheVersionFile), []byte(strconv.Itoa(v)+"\n"), 0644)
}

// purgeDataDir removes every entry in dataDir except the lock file. Used when
// the cache version is incompatible.
func purgeDataDir(dataDir string) error {
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Name() == lockFileName {
			continue
		}
		p := filepath.Join(dataDir, e.Name())
		if err := os.RemoveAll(p); err != nil {
			return fmt.Errorf("remove %s: %w", p, err)
		}
	}
	return nil
}

// sweepTempFiles removes leftover PutStream temp files (".tmp-*"). Only call
// while holding the data_dir's exclusive lock and before serving begins, when
// no temp file can be legitimately in flight.
func sweepTempFiles(dataDir string) {
	var removed int
	filepath.WalkDir(dataDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if strings.HasPrefix(d.Name(), ".tmp-") {
			if rmErr := os.Remove(path); rmErr == nil {
				removed++
			} else {
				log.Printf("startup: cannot remove orphaned temp file %s: %v", path, rmErr)
			}
		}
		return nil
	})
	if removed > 0 {
		log.Printf("startup: removed %d orphaned .tmp-* file(s) left by interrupted uploads", removed)
	}
}
