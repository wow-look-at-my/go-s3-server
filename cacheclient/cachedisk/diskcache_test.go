// All rights reserved. Use of this source code is
// governed by a BSD-style license that can be found
// in the LICENSE file.

package cachedisk

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// dummyID is a cache key with a number in it, so a test can name the entry it
// is talking about.
func dummyID(num int) [HashSize]byte {
	var out [HashSize]byte
	binary.LittleEndian.PutUint64(out[:], uint64(num))
	return out
}

// An entry survives being overwritten, and another cache over the same
// directory reads what the earliest a single wrote.
func TestDiskCacheSharesADirectory(t *testing.T) {
	dir := t.TempDir()
	_, err := Open(filepath.Join(dir, "notexist"))
	require.Error(t, err, "opening a directory that does not exist must fail")

	cdir := filepath.Join(dir, "c1")
	require.NoError(t, os.Mkdir(cdir, 0o777))

	first, err := Open(cdir)
	require.NoError(t, err)
	require.NoError(t, first.putIndexEntry(dummyID(1), dummyID(12), 13, true))
	require.NoError(t, first.putIndexEntry(dummyID(1), dummyID(2), 3, true))

	entry, err := first.Get(dummyID(1))
	require.NoError(t, err)
	assert.Equal(t, OutputID(dummyID(2)), entry.OutputID, "the second write is what a read answers")
	assert.EqualValues(t, 3, entry.Size)

	second, err := Open(cdir)
	require.NoError(t, err)
	entry, err = second.Get(dummyID(1))
	require.NoError(t, err)
	assert.Equal(t, OutputID(dummyID(2)), entry.OutputID)

	require.NoError(t, second.putIndexEntry(dummyID(2), dummyID(3), 4, true))
	entry, err = first.Get(dummyID(2))
	require.NoError(t, err)
	assert.Equal(t, OutputID(dummyID(3)), entry.OutputID, "what one cache writes, the other reads")
	assert.EqualValues(t, 4, entry.Size)
}

// Every entry written is readable, both as it is written and after the whole
// set is in. the next pass is what catches a write that a later a single moved.
func TestDiskCacheKeepsEveryEntry(t *testing.T) {
	cache, err := Open(t.TempDir())
	require.NoError(t, err)

	count := 10000
	if testing.Short() {
		count = 10
	}

	for num := range count {
		require.NoError(t, cache.putIndexEntry(dummyID(num), dummyID(num*99), int64(num)*101, true))
		entry, err := cache.Get(dummyID(num))
		require.NoError(t, err)
		require.Equal(t, OutputID(dummyID(num*99)), entry.OutputID)
		require.EqualValues(t, int64(num)*101, entry.Size)
	}
	for num := range count {
		entry, err := cache.Get(dummyID(num))
		require.NoError(t, err)
		require.Equal(t, OutputID(dummyID(num*99)), entry.OutputID)
		require.EqualValues(t, int64(num)*101, entry.Size)
	}
}

// A host with a GODEBUG registry of its own decides, and its answer overrides
// what this module read out of the environment.
func TestSetSwitchesOverridesTheEnvironment(t *testing.T) {
	t.Cleanup(initEnv)
	initEnv()
	require.False(t, verify)

	SetSwitches(true, true, true)
	assert.True(t, verify)
	assert.True(t, debugHash)
	assert.True(t, DebugTest)

	SetSwitches(false, false, false)
	assert.False(t, verify)
	assert.False(t, debugHash)
	assert.False(t, DebugTest)
}

// Verify mode exists to catch an action that is not reproducible. another Put
// of different bytes under a single key is exactly that, and it panics rather
// than storing quietly.
func TestDiskCacheVerifyModePanicsOnAMismatch(t *testing.T) {
	t.Setenv("GODEBUG", "gocacheverify=1")
	initEnv()
	t.Cleanup(func() {
		os.Unsetenv("GODEBUG")
		initEnv()
	})
	require.True(t, verify, "initEnv must read gocacheverify out of GODEBUG")

	cache, err := Open(t.TempDir())
	require.NoError(t, err)

	id := ActionID(dummyID(1))
	require.NoError(t, PutBytes(cache, id, []byte("abc")))

	assert.Panics(t, func() { PutBytes(cache, id, []byte("def")) },
		"verify mode must report a key whose action is not reproducible")
}

// The trim keeps what a build has used and drops what it has not. Its clock is
// the cache's own, so this walks days forward rather than sleeping.
func TestDiskCacheTrimKeepsWhatWasUsed(t *testing.T) {
	dir := t.TempDir()
	cache, err := Open(dir)
	require.NoError(t, err)
	const start = 1000000000
	now := int64(start)
	cache.now = func() time.Time { return time.Unix(now, 0) }

	checkTime := func(name string, mtime int64) {
		t.Helper()
		info, err := os.Stat(filepath.Join(cache.dir, name[:2], name))
		require.NoError(t, err)
		require.Equal(t, mtime, info.ModTime().Unix(), "%s", name)
	}

	id := ActionID(dummyID(1))
	require.NoError(t, PutBytes(cache, id, []byte("abc")))
	entry, err := cache.Get(id)
	require.NoError(t, err)
	require.NoError(t, PutBytes(cache, ActionID(dummyID(2)), []byte("def")))
	actionFile := fmt.Sprintf("%x-a", id)
	outputFile := fmt.Sprintf("%x-d", entry.OutputID)
	checkTime(actionFile, start)
	checkTime(outputFile, start)

	// A read inside the mtime interval leaves the stamp alone.
	now = start + 10
	_, err = cache.Get(id)
	require.NoError(t, err)
	checkTime(actionFile, start)
	checkTime(outputFile, start)

	// A read past that interval moves it.
	now = start + 5000
	used := now
	_, err = cache.Get(id)
	require.NoError(t, err)
	cache.OutputFile(entry.OutputID)
	checkTime(actionFile, used)
	checkTime(outputFile, used)

	// Everything is far too new to drop.
	if err := cache.Trim(); errors.Is(err, errors.ErrUnsupported) {
		t.Skipf("the trim needs a file lock this filesystem does not have: %v", err)
	} else {
		require.NoError(t, err)
	}
	_, err = cache.Get(id)
	require.NoError(t, err)
	stamp, err := os.ReadFile(filepath.Join(dir, "trim.txt"))
	require.NoError(t, err)
	checkTime(fmt.Sprintf("%x-a", dummyID(2)), start)

	// A trim inside the trim interval does nothing, and the stamp says so.
	now = start + 80000
	require.NoError(t, cache.Trim())
	// This read is what makes the earliest key a single a build has used since.
	_, err = cache.Get(id)
	require.NoError(t, err)
	cache.OutputFile(entry.OutputID)
	again, err := os.ReadFile(filepath.Join(dir, "trim.txt"))
	require.NoError(t, err)
	require.Equal(t, stamp, again, "a trim inside the interval must do nothing")

	// Days on. the earliest key was read and stays. the next was not.
	now += 5 * 86400
	require.NoError(t, cache.Trim())
	_, err = cache.Get(id)
	require.NoError(t, err)
	cache.OutputFile(entry.OutputID)
	kept := now
	_, err = cache.Get(dummyID(2))
	require.Error(t, err, "the trim must drop a key nothing has read")

	// Another days. checkTime reads a stamp without moving it.
	now += 5 * 86400
	require.NoError(t, cache.Trim())
	checkTime(actionFile, kept)
	checkTime(outputFile, kept)

	// Half a day later no trim runs, so the key is old enough and stays.
	now += 86400 / 2
	require.NoError(t, cache.Trim())
	checkTime(actionFile, kept)
	checkTime(outputFile, kept)

	// A full day since the last trim. this runs, and the key goes.
	now += 86400/2 + 1
	require.NoError(t, cache.Trim())
	_, err = cache.Get(dummyID(1))
	require.Error(t, err, "the trim must drop a key nothing has read for five days")
}
