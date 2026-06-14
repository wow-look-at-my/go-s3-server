package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

var (
	ErrNotFound           = errors.New("not found")
	ErrWriteOnceConflict  = errors.New("object already exists with different content")
	ErrWriteOnceDuplicate = errors.New("object already exists")
)

// currentCacheVersion is the on-disk data-dir format version. When this
// server starts and finds a data_dir with a different version (or no version
// marker, which is treated as version 1), it wipes the data_dir to avoid
// serving content written under an older, possibly-compromised regime.
//
// Bump this whenever prior cache contents should not be trusted. For
// example, version 2 forces a purge of any cache that was populated while
// the auth-bypass bug in auth.go (pre-fix) could have been exploited to
// upload attacker-controlled artifacts.
//
// Version 3 purges caches that may hold poisoned Go module-index objects. The
// module index is stored opaquely here (the server cannot tell a good index
// from a mis-keyed one), and a wrong one served for a std package's key breaks
// every consumer's build at package load ("package runtime is not in std" /
// "corrupt index"). The go-toolchain client now refuses to upload or serve
// module-index blobs, but that only protects clients that have updated; this
// purge removes the already-stored poison so EVERY client -- updated or not --
// is repaired at once (a missing index key is simply recomputed locally).
const currentCacheVersion = 3

const cacheVersionFile = ".cache_version"
const lockFileName = ".lock"

type Storage struct {
	dataDir   string
	writeOnce WriteOnceConfig
	lockFile  *os.File
	Index     *Index // sqlite index for time-range queries; nil if unavailable

	// accessShards tracks the last-access time (unix seconds) of each key so the
	// eviction sweeper can prune entries by least-recent *use*, not merely by
	// write time. It is allocated only when eviction is enabled
	// (EnableAccessTracking); while nil, recordAccess is a no-op and the read
	// hot path pays nothing. Sharded so the per-GET update never serializes on a
	// single global lock — the same lock-convoy concern that shaped the index's
	// hot path. mtime stays the authoritative write time (the prefetch system
	// keys on it); access time is kept here, separately, so the two never
	// interfere. The map holds only keys read since startup (the working set),
	// not the whole cache. The type and methods live in eviction.go.
	accessShards []*accessShard
}

type ObjectMeta struct {
	Metadata map[string]string
	ModTime  time.Time
	Size     int64
}

type ListResult struct {
	Objects               []ListObject
	IsTruncated           bool
	NextContinuationToken string
}

type ListObject struct {
	Key          string
	Size         int64
	LastModified time.Time
}

func NewStorage(dataDir string, writeOnce WriteOnceConfig) (*Storage, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	lockPath := filepath.Join(dataDir, lockFileName)
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := lockExclusive(lockFile); err != nil {
		lockFile.Close()
		return nil, fmt.Errorf("data directory is locked by another process: %w", err)
	}

	if err := ensureCacheVersion(dataDir); err != nil {
		unlockFile(lockFile)
		lockFile.Close()
		return nil, err
	}

	s := &Storage{
		dataDir:   dataDir,
		writeOnce: writeOnce,
		lockFile:  lockFile,
	}

	s.Index = NewIndex(s)
	return s, nil
}

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

func (s *Storage) Close() error {
	if s.lockFile != nil {
		unlockFile(s.lockFile)
		return s.lockFile.Close()
	}
	return nil
}

// isKeySafe returns true if the key contains only safe filesystem characters:
// alphanumeric, '/', '-', '_'. Dots are excluded to prevent ".." traversal.
func isKeySafe(key string) bool {
	if len(key) == 0 {
		return false
	}
	for i := 0; i < len(key); i++ {
		c := key[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '/' || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

const hashedPrefix = "__hashed__"

// keyToPath converts a cache key to a sharded filesystem path.
// Safe keys use direct sharding:
//
//	go-buildcache/v1aabbccdd11223344 → {dataDir}/go-buildcache/v1/aa/bbccdd11223344
//
// Unsafe keys are SHA256-hashed into a separate tree:
//
//	../../etc/passwd → {dataDir}/__hashed__/{hash[:2]}/{hash[2:4]}/{hash[4:]}
func (s *Storage) keyToPath(key string) string {
	if isKeySafe(key) {
		return s.shardPath(s.dataDir, key)
	}
	h := sha256.Sum256([]byte(key))
	name := hex.EncodeToString(h[:])
	return filepath.Join(s.dataDir, hashedPrefix, name[:2], name[2:4], name[4:])
}

func (s *Storage) shardPath(base, key string) string {
	dir := filepath.Dir(key)
	name := filepath.Base(key)

	switch {
	case len(name) > 4:
		return filepath.Join(base, dir, name[:2], name[2:4], name[4:])
	case len(name) > 2:
		return filepath.Join(base, dir, name[:2], name[2:])
	default:
		return filepath.Join(base, dir, name)
	}
}

// pathToKey reverses the sharding to reconstruct the original cache key.
// For hashed keys (under __hashed__/), the original key is read from xattr.
func (s *Storage) pathToKey(path string) string {
	rel, _ := filepath.Rel(s.dataDir, path)
	rel = filepath.ToSlash(rel)

	// Hashed key — original key stored in xattr
	if strings.HasPrefix(rel, hashedPrefix+"/") {
		if key, err := getOriginalKey(path); err == nil {
			return key
		}
		return ""
	}

	parts := strings.Split(rel, "/")
	if len(parts) >= 3 {
		n := len(parts)
		shard2 := parts[n-3]
		shard1 := parts[n-2]
		remainder := parts[n-1]
		if len(shard2) == 2 && len(shard1) == 2 {
			prefix := strings.Join(parts[:n-3], "/")
			name := shard2 + shard1 + remainder
			if prefix == "" {
				return name
			}
			return prefix + "/" + name
		}
	}
	if len(parts) >= 2 {
		n := len(parts)
		shard := parts[n-2]
		remainder := parts[n-1]
		if len(shard) == 2 {
			prefix := strings.Join(parts[:n-2], "/")
			name := shard + remainder
			if prefix == "" {
				return name
			}
			return prefix + "/" + name
		}
	}
	return rel
}

// Put stores data under key. It is a convenience wrapper around PutStream for
// callers (and tests) that already hold the full body in memory.
func (s *Storage) Put(key string, data []byte, meta map[string]string, audit map[string]string) error {
	return s.PutStream(key, bytes.NewReader(data), meta, audit)
}

// PutStream stores the body read from r under key, streaming it straight to disk
// so the server never buffers a whole upload in memory. This is the OOM-safety
// property that lets many large concurrent PUTs coexist under a tight memory
// budget: io.Copy uses a fixed 32 KiB buffer, so resident memory per upload is
// flat regardless of object size. The actual byte count is recorded as the
// audit content_length.
func (s *Storage) PutStream(key string, r io.Reader, meta map[string]string, audit map[string]string) (err error) {
	start := time.Now()
	defer func() {
		status := "ok"
		if err != nil {
			status = "error"
		}
		storageOpsTotal.WithLabelValues("put", status).Inc()
		storageOpDuration.WithLabelValues("put").Observe(time.Since(start).Seconds())
	}()
	path := s.keyToPath(key)
	hashed := !isKeySafe(key)

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create dirs: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmp.Name()

	n, copyErr := io.Copy(tmp, r)
	if copyErr != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("write temp: %w", copyErr)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close temp: %w", err)
	}

	// write_once is applied after the body is on disk so the content comparison
	// streams from two files instead of buffering either in memory.
	if s.writeOnce.Action == "deny" {
		if _, statErr := os.Stat(path); statErr == nil { // object already exists
			switch s.writeOnce.Notification {
			case "always":
				os.Remove(tmpPath)
				return ErrWriteOnceDuplicate
			case "content_differs":
				eq, eqErr := filesEqual(tmpPath, path)
				os.Remove(tmpPath)
				if eqErr != nil {
					return fmt.Errorf("write_once compare: %w", eqErr)
				}
				if eq {
					return nil
				}
				return ErrWriteOnceConflict
			default:
				os.Remove(tmpPath)
				return nil
			}
		}
	}

	if err := setMetadata(tmpPath, meta); err != nil {
		os.Remove(tmpPath)
		return err
	}

	// Audit is best-effort: if the filesystem doesn't support extended
	// attributes, the request log still captures the same fields, and it's
	// better to accept the upload than to refuse it over missing forensics.
	// Log loudly so operators notice if they're losing the on-disk trail.
	if audit != nil {
		audit["content_length"] = strconv.FormatInt(n, 10)
	}
	if err := setAudit(tmpPath, audit); err != nil {
		log.Printf("audit: failed to persist xattrs for key %q (request log still has the data): %v", key, err)
	}

	if hashed {
		if err := setOriginalKey(tmpPath, key); err != nil {
			os.Remove(tmpPath)
			return err
		}
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename: %w", err)
	}
	if s.Index != nil {
		s.Index.Put(key, n)
	}
	return nil
}

// filesEqual reports whether two files have identical contents, comparing in
// fixed-size chunks so neither file is ever loaded into memory whole.
func filesEqual(a, b string) (bool, error) {
	fa, err := os.Open(a)
	if err != nil {
		return false, err
	}
	defer fa.Close()
	fb, err := os.Open(b)
	if err != nil {
		return false, err
	}
	defer fb.Close()

	const chunk = 64 * 1024
	ba := make([]byte, chunk)
	bb := make([]byte, chunk)
	for {
		na, ea := io.ReadFull(fa, ba)
		nb, eb := io.ReadFull(fb, bb)
		if na != nb || !bytes.Equal(ba[:na], bb[:nb]) {
			return false, nil
		}
		aDone := ea == io.EOF || ea == io.ErrUnexpectedEOF
		bDone := eb == io.EOF || eb == io.ErrUnexpectedEOF
		if ea != nil && !aDone {
			return false, ea
		}
		if eb != nil && !bDone {
			return false, eb
		}
		if aDone || bDone {
			return aDone && bDone, nil
		}
	}
}

func (s *Storage) Get(key string) (_ []byte, _ *ObjectMeta, err error) {
	start := time.Now()
	defer func() {
		status := "ok"
		if err != nil {
			status = "error"
		}
		storageOpsTotal.WithLabelValues("get", status).Inc()
		storageOpDuration.WithLabelValues("get").Observe(time.Since(start).Seconds())
	}()
	path := s.keyToPath(key)

	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}

	info, err := os.Stat(path)
	if err != nil {
		return nil, nil, err
	}

	meta := &ObjectMeta{
		Metadata: make(map[string]string),
		ModTime:  info.ModTime(),
		Size:     info.Size(),
	}

	getMetadata(path, meta)
	s.recordAccess(key)

	return data, meta, nil
}

// Stat returns an object's metadata (size, mtime, user metadata) WITHOUT reading
// its body. The batch endpoint uses it to build the response manifest, and the
// GET path uses it where only size/metadata are needed — so a large object's
// bytes are never pulled into memory just to learn how big it is.
func (s *Storage) Stat(key string) (*ObjectMeta, error) {
	path := s.keyToPath(key)
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	meta := &ObjectMeta{
		Metadata: make(map[string]string),
		ModTime:  info.ModTime(),
		Size:     info.Size(),
	}
	getMetadata(path, meta)
	return meta, nil
}

// Open opens an object's body for streaming and returns it with its metadata.
// The caller MUST Close the returned file. Streaming straight from the open file
// (instead of ReadFile + Write) is what keeps GET and batch-GET memory flat
// under load: only an io.Copy-sized buffer is resident, never the whole object.
// meta.Size comes from the open fd, so it matches the bytes that will be read.
func (s *Storage) Open(key string) (_ *os.File, _ *ObjectMeta, err error) {
	start := time.Now()
	defer func() {
		status := "ok"
		if err != nil {
			status = "error"
		}
		storageOpsTotal.WithLabelValues("get", status).Inc()
		storageOpDuration.WithLabelValues("get").Observe(time.Since(start).Seconds())
	}()
	path := s.keyToPath(key)
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil, ErrNotFound
		}
		return nil, nil, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, nil, err
	}
	meta := &ObjectMeta{
		Metadata: make(map[string]string),
		ModTime:  info.ModTime(),
		Size:     info.Size(),
	}
	getMetadata(path, meta)
	s.recordAccess(key)
	return f, meta, nil
}

// Delete removes the object stored under key, returning ErrNotFound if no such
// object exists. It is the surgical counterpart to the whole-dir cache-version
// purge: an operator can evict a single poisoned entry -- e.g. a cross-
// contaminated build-cache object that hashes to its own outputID yet belongs
// under a different action key -- without rebuilding the entire cache. The
// index entry is dropped too, so the server stops advertising a key it no
// longer stores.
func (s *Storage) Delete(key string) (err error) {
	start := time.Now()
	defer func() {
		status := "ok"
		if err != nil {
			status = "error"
		}
		storageOpsTotal.WithLabelValues("delete", status).Inc()
		storageOpDuration.WithLabelValues("delete").Observe(time.Since(start).Seconds())
	}()
	path := s.keyToPath(key)
	if rmErr := os.Remove(path); rmErr != nil {
		if errors.Is(rmErr, fs.ErrNotExist) {
			return ErrNotFound
		}
		return rmErr
	}
	if s.Index != nil {
		s.Index.Remove(key)
	}
	s.forgetAccess(key)
	return nil
}

func (s *Storage) List(prefix string, maxKeys int, continuationToken string) (_ *ListResult, err error) {
	metricsStart := time.Now()
	defer func() {
		status := "ok"
		if err != nil {
			status = "error"
		}
		storageOpsTotal.WithLabelValues("list", status).Inc()
		storageOpDuration.WithLabelValues("list").Observe(time.Since(metricsStart).Seconds())
	}()
	var allKeys []ListObject

	err = filepath.WalkDir(s.dataDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // skip errors
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if name == lockFileName || name == cacheVersionFile || strings.HasPrefix(name, ".tmp-") {
			return nil
		}

		key := s.pathToKey(path)
		if key == "" || !strings.HasPrefix(key, prefix) {
			return nil
		}

		info, err := d.Info()
		if err != nil {
			return nil
		}

		allKeys = append(allKeys, ListObject{
			Key:          key,
			Size:         info.Size(),
			LastModified: info.ModTime(),
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(allKeys, func(i, j int) bool {
		return allKeys[i].Key < allKeys[j].Key
	})

	start := 0
	if continuationToken != "" {
		for i, obj := range allKeys {
			if obj.Key > continuationToken {
				start = i
				break
			}
			if i == len(allKeys)-1 {
				start = len(allKeys)
			}
		}
	}

	remaining := allKeys[start:]
	result := &ListResult{}

	if len(remaining) > maxKeys {
		result.Objects = remaining[:maxKeys]
		result.IsTruncated = true
		result.NextContinuationToken = remaining[maxKeys-1].Key
	} else {
		result.Objects = remaining
	}

	return result, nil
}
