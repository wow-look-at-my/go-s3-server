package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var (
	ErrNotFound           = errors.New("not found")
	ErrWriteOnceConflict  = errors.New("object already exists with different content")
	ErrWriteOnceDuplicate = errors.New("object already exists")
)

type Storage struct {
	dataDir   string
	writeOnce WriteOnceConfig
	lockFile  *os.File
	Index     *Index // sqlite index for time-range queries; nil if unavailable
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

	lockPath := filepath.Join(dataDir, ".lock")
	lockFile, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("open lock file: %w", err)
	}
	if err := lockExclusive(lockFile); err != nil {
		lockFile.Close()
		return nil, fmt.Errorf("data directory is locked by another process: %w", err)
	}

	s := &Storage{
		dataDir:   dataDir,
		writeOnce: writeOnce,
		lockFile:  lockFile,
	}

	s.Index = NewIndex(s)
	return s, nil
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

// keyToPath converts an S3 key to a sharded filesystem path.
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

// pathToKey reverses the sharding to reconstruct the original S3 key.
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

func (s *Storage) Put(key string, data []byte, meta map[string]string) (err error) {
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

	if s.writeOnce.Action == "deny" {
		if existing, err := os.ReadFile(path); err == nil {
			switch s.writeOnce.Notification {
			case "always":
				return ErrWriteOnceDuplicate
			case "content_differs":
				if bytes.Equal(existing, data) {
					return nil
				}
				return ErrWriteOnceConflict
			default:
				return nil
			}
		}
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create dirs: %w", err)
	}

	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("create temp: %w", err)
	}
	tmpPath := tmp.Name()

	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("write temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close temp: %w", err)
	}

	if err := setMetadata(tmpPath, meta); err != nil {
		os.Remove(tmpPath)
		return err
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
		s.Index.Put(key, int64(len(data)))
	}
	return nil
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

	return data, meta, nil
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
		if name == ".lock" || strings.HasPrefix(name, ".tmp-") {
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
