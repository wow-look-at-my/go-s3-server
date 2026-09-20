// Copyright 2017 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package cachedisk is the build cache's directory on disk: the entry files,
// the output files, and the trim that drops what nothing has read.
//
// It is the half of the cache that speaks to no network, so a program that
// must not depend on net can hold a cache anyway. The go command's bootstrap
// build is that program.
package cachedisk

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// An ActionID hashes a repeatable computation's whole description.
type ActionID [HashSize]byte

// An OutputID is a cache output key, the hash of an output of a computation.
type OutputID [HashSize]byte

// Cache stores an action's output under that action's key.
//
// After a Get or a Put answers, OutputFile names a file that exists until
// Close. That holds ACROSS processes: a trim here must not delete an output
// another process just read, which is why nothing used in the last day goes.
type Cache interface {
	// Get answers the entry for an action, or a *MissError.
	Get(ActionID) (Entry, error)

	// Put stores an output, seeking the reader to the start and leaving the position anywhere.
	Put(ActionID, io.ReadSeeker) (_ OutputID, size int64, _ error)

	// Close ends this process's use: the trim and any waiting run here.
	Close() error

	// OutputFile names a stored output, which a get or a put just answered for.
	OutputFile(OutputID) string

	// FuzzDir names where fuzz files are stored.
	FuzzDir() string
}

// A DiskCache is a cache backed by a directory tree.
type DiskCache struct {
	dir string
	now func() time.Time
}

// Open answers the cache in a directory.
//
// Processes on ONE machine may share a directory: they coordinate with file
// locks, and duplicate work rather than corrupt it. Processes on different
// machines may not, because a network filesystem's locking cannot be relied
// on.
func Open(dir string) (*DiskCache, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, &fs.PathError{Op: "open", Path: dir, Err: fmt.Errorf("not a directory")}
	}
	for i := 0; i < 256; i++ {
		name := filepath.Join(dir, fmt.Sprintf("%02x", i))
		if err := os.MkdirAll(name, 0o777); err != nil {
			return nil, err
		}
	}
	c := &DiskCache{
		dir: dir,
		now: time.Now,
	}
	return c, nil
}

// Dir is where a tier above this one keeps its own state too.
func (c *DiskCache) Dir() string { return c.dir }

// KeepVerified writes a body whose output ID a tier above checked already.
func (c *DiskCache) KeepVerified(id ActionID, out OutputID, data []byte) error {
	if err := c.copyFile(bytes.NewReader(data), out, int64(len(data))); err != nil {
		return err
	}
	return c.putIndexEntry(id, out, int64(len(data)), false)
}

// fileName returns the name of the file corresponding to the given id.
func (c *DiskCache) fileName(id [HashSize]byte, key string) string {
	return filepath.Join(c.dir, fmt.Sprintf("%02x", id[0]), fmt.Sprintf("%x", id)+"-"+key)
}

// A MissError says no entry is held, and why.
type MissError struct {
	Err error
}

// Miss answers the error a tier reports for an action it does not hold.
func Miss(reason error) error { return &MissError{Err: reason} }

func (e *MissError) Error() string {
	if e.Err == nil {
		return "cache entry not found"
	}
	return fmt.Sprintf("cache entry not found: %v", e.Err)
}

func (e *MissError) Unwrap() error {
	return e.Err
}

const (
	// action entry file is "v1 <hex id> <hex out> <decimal size space-padded to 20 bytes> <unixnano space-padded to 20 bytes>\n"
	hexSize   = HashSize * 2
	entrySize = 2 + 1 + hexSize + 1 + hexSize + 1 + 20 + 1 + 20 + 1
)

var (
	// Under GODEBUG=gocacheverify=1 every Get misses, and Put then checks what
	// it is handed against what is stored. An action that is not reproducible
	// shows up as a mismatch rather than as a hit nobody re-derived.
	verify        = false
	errVerifyMode = errors.New("gocacheverify=1")
)

// DebugTest is set when GODEBUG=gocachetest=1 is in the environment.
var DebugTest = false

func init() { initEnv() }

// initEnv reads the switches out of GODEBUG. This module is outside the go
// command's tree, so the counters internal/godebug keeps are not reachable.
func initEnv() {
	settings := os.Getenv("GODEBUG")
	verify = godebugOn(settings, "gocacheverify")
	debugHash = godebugOn(settings, "gocachehash")
	DebugTest = godebugOn(settings, "gocachetest")
}

// godebugOn reports whether a GODEBUG setting is on in the given value.
func godebugOn(settings, name string) bool {
	for field := range strings.SplitSeq(settings, ",") {
		key, value, found := strings.Cut(strings.TrimSpace(field), "=")
		if found && key == name && value == "1" {
			return true
		}
	}
	return false
}

// Get answers an action's output ID and size. An output ID it answers is not
// a promise that the file is still there.
func (c *DiskCache) Get(id ActionID) (Entry, error) {
	if verify {
		return Entry{}, &MissError{Err: errVerifyMode}
	}
	return c.get(id)
}

type Entry struct {
	OutputID OutputID
	Size     int64
	Time     time.Time // when added to cache
}

// get is Get but does not respect verify mode, so that Put can use it.
func (c *DiskCache) get(id ActionID) (Entry, error) {
	missing := func(reason error) (Entry, error) {
		return Entry{}, &MissError{Err: reason}
	}
	f, err := os.Open(c.fileName(id, "a"))
	if err != nil {
		return missing(err)
	}
	defer f.Close()
	entry := make([]byte, entrySize+1) // +1 to detect whether f is too long
	if n, err := io.ReadFull(f, entry); n > entrySize {
		return missing(errors.New("too long"))
	} else if err != io.ErrUnexpectedEOF {
		if err == io.EOF {
			return missing(errors.New("file is empty"))
		}
		return missing(err)
	} else if n < entrySize {
		return missing(errors.New("entry file incomplete"))
	}
	if entry[0] != 'v' || entry[1] != '1' || entry[2] != ' ' || entry[3+hexSize] != ' ' || entry[3+hexSize+1+hexSize] != ' ' || entry[3+hexSize+1+hexSize+1+20] != ' ' || entry[entrySize-1] != '\n' {
		return missing(errors.New("invalid header"))
	}
	eid, entry := entry[3:3+hexSize], entry[3+hexSize:]
	eout, entry := entry[1:1+hexSize], entry[1+hexSize:]
	esize, entry := entry[1:1+20], entry[1+20:]
	etime, entry := entry[1:1+20], entry[1+20:]
	var buf [HashSize]byte
	if _, err := hex.Decode(buf[:], eid); err != nil {
		return missing(fmt.Errorf("decoding ID: %v", err))
	} else if buf != id {
		return missing(errors.New("mismatched ID"))
	}
	if _, err := hex.Decode(buf[:], eout); err != nil {
		return missing(fmt.Errorf("decoding output ID: %v", err))
	}
	i := 0
	for i < len(esize) && esize[i] == ' ' {
		i++
	}
	size, err := strconv.ParseInt(string(esize[i:]), 10, 64)
	if err != nil {
		return missing(fmt.Errorf("parsing size: %v", err))
	} else if size < 0 {
		return missing(errors.New("negative size"))
	}
	i = 0
	for i < len(etime) && etime[i] == ' ' {
		i++
	}
	tm, err := strconv.ParseInt(string(etime[i:]), 10, 64)
	if err != nil {
		return missing(fmt.Errorf("parsing timestamp: %v", err))
	} else if tm < 0 {
		return missing(errors.New("negative timestamp"))
	}

	c.markUsed(c.fileName(id, "a"))

	return Entry{buf, size, time.Unix(0, tm)}, nil
}

// GetFile looks up the action ID in the cache and returns
// the name of the corresponding data file.
func GetFile(c Cache, id ActionID) (file string, entry Entry, err error) {
	entry, err = c.Get(id)
	if err != nil {
		return "", Entry{}, err
	}
	file = c.OutputFile(entry.OutputID)
	info, err := os.Stat(file)
	if err != nil {
		return "", Entry{}, &MissError{Err: err}
	}
	if info.Size() != entry.Size {
		return "", Entry{}, &MissError{Err: errors.New("file incomplete")}
	}
	return file, entry, nil
}

// GetBytes looks up the action ID in the cache and returns
// the corresponding output bytes.
// GetBytes should only be used for data that can be expected to fit in memory.
func GetBytes(c Cache, id ActionID) ([]byte, Entry, error) {
	entry, err := c.Get(id)
	if err != nil {
		return nil, entry, err
	}
	data, _ := os.ReadFile(c.OutputFile(entry.OutputID))
	if OutputID(sha256.Sum256(data)) != entry.OutputID {
		return nil, entry, &MissError{Err: errors.New("bad checksum")}
	}
	return data, entry, nil
}

// OutputFile names the file holding an output. A consumer that wants it
// MAPPED maps this path itself: how to map a file is a platform's business.
func (c *DiskCache) OutputFile(out OutputID) string {
	file := c.fileName(out, "d")
	c.markUsed(file)
	return file
}

const (
	// A file's mtime is its time of last use, stamped no more often than this,
	// so that a build does not rewrite every inode it reads. A scan runs no
	// more often than trimInterval, and drops what nothing has read for
	// trimLimit, where a month of measured reuse ran out
	// (golang.org/issue/22990).
	mtimeInterval = 1 * time.Hour
	trimInterval  = 24 * time.Hour
	trimLimit     = 5 * 24 * time.Hour
)

// markUsed stamps a file, best effort, and reports whether it is a directory.
func (c *DiskCache) markUsed(file string) (isDir bool) {
	info, err := os.Stat(file)
	if err != nil {
		return false
	}
	if now := c.now(); now.Sub(info.ModTime()) >= mtimeInterval {
		os.Chtimes(file, now, now)
	}
	return info.IsDir()
}

func (c *DiskCache) Close() error { return c.Trim() }

// Trim removes old cache entries that are likely not to be reused.
func (c *DiskCache) Trim() error {
	now := c.now()

	// dir/trim.txt holds when the last trim finished. A stamp that cannot be
	// parsed, or one from the future, trims anyway: a trim over an empty cache
	// costs nothing, and a full one may be what corrupted the stamp.
	skipTrim := func(data []byte) bool {
		if t, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64); err == nil {
			lastTrim := time.Unix(t, 0)
			if d := now.Sub(lastTrim); d < trimInterval && d > -mtimeInterval {
				return true
			}
		}
		return false
	}
	// Read it first, apart from the exclusive pass, so a skip takes no lock.
	stamp := filepath.Join(c.dir, "trim.txt")
	if data, err := os.ReadFile(stamp); err == nil {
		if skipTrim(data) {
			return nil
		}
	}

	errFileChanged := errors.New("file changed")

	// Stamp before the trim starts, so two commands here together do not both trim.
	err := transformFile(stamp, func(data []byte) ([]byte, error) {
		// A stamp that moved since the read above belongs to another command.
		if skipTrim(data) {
			return nil, errFileChanged
		}
		return fmt.Appendf(nil, "%d", now.Unix()), nil
	})
	if errors.Is(err, errors.ErrUnsupported) {
		return err
	}
	if errors.Is(err, errFileChanged) {
		return nil
	}

	// The extra mtimeInterval is the hour a stamp is allowed to be stale by.
	cutoff := now.Add(-trimLimit - mtimeInterval)
	for i := 0; i < 256; i++ {
		subdir := filepath.Join(c.dir, fmt.Sprintf("%02x", i))
		c.trimSubdir(subdir, cutoff)
	}

	return nil
}

// trimSubdir trims a single cache subdirectory.
func (c *DiskCache) trimSubdir(subdir string, cutoff time.Time) {
	// Read every name first: a remove can move the scan's own offset.
	f, err := os.Open(subdir)
	if err != nil {
		return
	}
	names, _ := f.Readdirnames(-1)
	f.Close()

	for _, name := range names {
		// Remove only cache entries (xxxx-a and xxxx-d).
		if !strings.HasSuffix(name, "-a") && !strings.HasSuffix(name, "-d") {
			continue
		}
		entry := filepath.Join(subdir, name)
		info, err := os.Stat(entry)
		if err == nil && info.ModTime().Before(cutoff) {
			if info.IsDir() { // executable cache entry
				os.RemoveAll(entry)
				continue
			}
			os.Remove(entry)
		}
	}
}

// putIndexEntry records that an action produced an output of a size.
//
// An action file stays writable: a repeat that embeds a timestamp or a
// temporary directory name produces a different output, which is ordinary
// rather than wrong. Verify mode is where it becomes a finding.
func (c *DiskCache) putIndexEntry(id ActionID, out OutputID, size int64, allowVerify bool) error {
	entry := fmt.Sprintf("v1 %x %x %20d %20d\n", id, out, size, time.Now().UnixNano())
	if verify && allowVerify {
		old, err := c.get(id)
		if err == nil && (old.OutputID != out || old.Size != size) {
			// panic to show stack trace, so we can see what code is generating this cache entry.
			msg := fmt.Sprintf("go: internal cache error: cache verify failed: id=%x changed:<<<\n%s\n>>>\nold: %x %d\nnew: %x %d", id, reverseHash(id), out, size, old.OutputID, old.Size)
			panic(msg)
		}
	}
	file := c.fileName(id, "a")

	// Copy file to cache directory.
	mode := os.O_WRONLY | os.O_CREATE
	f, err := os.OpenFile(file, mode, 0o666)
	if err != nil {
		return err
	}
	_, err = f.WriteString(entry)
	if err == nil {
		// AFTER the write: an O_TRUNC undoes an equal write while this one runs.
		err = f.Truncate(int64(len(entry)))
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		// TODO(bcmills): this races another command writing the same file.
		os.Remove(file)
		return err
	}
	os.Chtimes(file, c.now(), c.now()) // mainly for tests

	return nil
}

// noVerifyReadSeeker marks a body Put must not hold to the verify check.
type noVerifyReadSeeker struct {
	io.ReadSeeker
}

// Put stores an output under an action's key. It may read the file twice, and the content must not change between passes.
func (c *DiskCache) Put(id ActionID, file io.ReadSeeker) (OutputID, int64, error) {
	wrapper, isNoVerify := file.(noVerifyReadSeeker)
	if isNoVerify {
		file = wrapper.ReadSeeker
	}
	return c.put(id, file, !isNoVerify)
}

// PutNoVerify is Put for an output that is worth caching and is not
// reproducible, such as test output carrying a time.
func PutNoVerify(c Cache, id ActionID, file io.ReadSeeker) (OutputID, int64, error) {
	return c.Put(id, noVerifyReadSeeker{file})
}

func (c *DiskCache) put(id ActionID, file io.ReadSeeker, allowVerify bool) (OutputID, int64, error) {
	// Compute output ID.
	h := sha256.New()
	if _, err := file.Seek(0, 0); err != nil {
		return OutputID{}, 0, err
	}
	size, err := io.Copy(h, file)
	if err != nil {
		return OutputID{}, 0, err
	}
	var out OutputID
	h.Sum(out[:0])

	// Copy to cached output file (if not already present).
	if err := c.copyFile(file, out, size); err != nil {
		return out, size, err
	}

	// Add to cache index.
	return out, size, c.putIndexEntry(id, out, size, allowVerify)
}

// PutBytes stores the given bytes in the cache as the output for the action ID.
func PutBytes(c Cache, id ActionID, data []byte) error {
	_, _, err := c.Put(id, bytes.NewReader(data))
	return err
}

// copyFile writes a body into the cache under the output ID and size it is
// told, unless that file is there already.
func (c *DiskCache) copyFile(file io.ReadSeeker, out OutputID, size int64) error {
	name := c.fileName(out, "d")
	info, err := os.Stat(name)
	if err == nil && info.Size() == size {
		// Check hash.
		if f, err := os.Open(name); err == nil {
			h := sha256.New()
			io.Copy(h, f)
			f.Close()
			var out2 OutputID
			h.Sum(out2[:0])
			if out == out2 {
				return nil
			}
		}
		// Hash did not match. Fall through and rewrite file.
	}

	// Copy file to cache directory.
	mode := os.O_RDWR | os.O_CREATE
	if err == nil && info.Size() > size { // shouldn't happen but fix in case
		mode |= os.O_TRUNC
	}
	f, err := os.OpenFile(name, mode, 0o666)
	if err != nil {
		// A running program holds it, so another go process wrote it and ran
		// it. The bytes are there.
		if isETXTBSY(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	// One zero-length body exists, so the file is already right. This also
	// gives the copy below a last byte to hold back.
	if size == 0 {
		return nil
	}

	// Past here a failed write truncates the file, so it holds no bad bytes.
	// The copy runs through a hash as well, to check what it wrote.
	if _, err := file.Seek(0, 0); err != nil {
		f.Truncate(0)
		return err
	}
	h := sha256.New()
	w := io.MultiWriter(f, h)
	if _, err := io.CopyN(w, file, size-1); err != nil {
		f.Truncate(0)
		return err
	}
	// Check the last byte BEFORE writing it: the write makes the size match, and another process reading that size uses the file.
	buf := make([]byte, 1)
	if _, err := file.Read(buf); err != nil {
		f.Truncate(0)
		return err
	}
	h.Write(buf)
	sum := h.Sum(nil)
	if !bytes.Equal(sum, out[:]) {
		f.Truncate(0)
		return fmt.Errorf("file content changed underfoot")
	}

	// Commit cache file entry.
	if _, err := f.Write(buf); err != nil {
		f.Truncate(0)
		return err
	}
	if err := f.Close(); err != nil {
		// The file can carry the right size and not the data, so drop it.
		os.Remove(name)
		return err
	}
	os.Chtimes(name, c.now(), c.now()) // mainly for tests

	return nil
}

// FuzzDir names internal/fuzz's directory, which the trim leaves alone.
func (c *DiskCache) FuzzDir() string {
	return filepath.Join(c.dir, "fuzz")
}
