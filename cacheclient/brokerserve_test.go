// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package cacheclient

import (
	"bytes"
	"encoding/binary"
	"os"
	"sync"
	"testing"

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

// serveOwner opens a cache over dir and serves it, then answers the cache a
// process below would hold.
func serveOwner(t *testing.T, dir string) (owner Cache, child Cache) {
	t.Helper()
	t.Setenv(brokerEnv, "")
	t.Setenv(brokerOffEnv, "")
	disk, err := Open(dir)
	require.NoError(t, err)
	serveBroker(disk, dir)
	t.Cleanup(StopBroker)
	require.NotEmpty(t, os.Getenv(brokerEnv), "the owner must name its endpoint in the environment")

	child = dialBroker()
	require.NotNil(t, child, "a process that finds a live owner must become a child")
	t.Cleanup(func() { child.Close() })
	return disk, child
}

// A child stores through the owner and reads back what the owner wrote. The
// body never crosses the ring: the answer names a file both can open.
func TestBrokerChildStoresAndReadsThroughTheOwner(t *testing.T) {
	dir := t.TempDir()
	owner, child := serveOwner(t, dir)

	id := ActionID(dummyID(7))
	body := []byte("what one process compiled and another one reads")
	out, size, err := child.Put(id, bytes.NewReader(body))
	require.NoError(t, err)
	require.EqualValues(t, len(body), size)

	entry, err := child.Get(id)
	require.NoError(t, err)
	assert.Equal(t, out, entry.OutputID)
	assert.EqualValues(t, len(body), entry.Size)

	stored, err := os.ReadFile(child.OutputFile(entry.OutputID))
	require.NoError(t, err)
	assert.Equal(t, body, stored, "the child must name the file the owner wrote")
	assert.Equal(t, owner.OutputFile(entry.OutputID), child.OutputFile(entry.OutputID),
		"the layout is the owner's, and the child computes the same name")
}

// A key nothing stored is an ordinary miss, exactly as a local one is.
func TestBrokerChildReportsAMissAsAMiss(t *testing.T) {
	_, child := serveOwner(t, t.TempDir())

	_, err := child.Get(ActionID(dummyID(11)))
	require.Error(t, err)
	var miss *MissError
	assert.ErrorAs(t, err, &miss, "a broker miss must be the miss every caller already branches on")
}

// The owner names which tier answered, and a child passes that on. Without it
// a trace cannot tell a hit off local disk from one fetched over the network.
func TestBrokerChildReportsTheOwnersTier(t *testing.T) {
	_, child := serveOwner(t, t.TempDir())

	id := ActionID(dummyID(13))
	_, _, err := child.Put(id, bytes.NewReader([]byte("body")))
	require.NoError(t, err)

	reporter, ok := child.(Tiered)
	require.True(t, ok, "a child must report the tier the owner answered from")
	_, tier, err := reporter.GetTiered(id)
	require.NoError(t, err)
	assert.Equal(t, TierDisk, tier)
}

// Many children asking for one action share the first one's lookup, and many
// offering one body store it once. Nothing here polls: an asker parks until
// the first one's channel closes.
func TestBrokerSharesOneLookupAndOneStore(t *testing.T) {
	_, child := serveOwner(t, t.TempDir())

	id := ActionID(dummyID(17))
	body := []byte("two children built the same object")

	const askers = 16
	var group sync.WaitGroup
	group.Add(askers)
	for range askers {
		go func() {
			defer group.Done()
			child.Put(id, bytes.NewReader(body))
		}()
	}
	group.Wait()

	group.Add(askers)
	for range askers {
		go func() {
			defer group.Done()
			entry, err := child.Get(id)
			assert.NoError(t, err)
			assert.EqualValues(t, len(body), entry.Size)
		}()
	}
	group.Wait()
}

// The name outlives the process that made it, so a stale one in the
// environment must not turn the next process into a child of nothing.
func TestBrokerDeadNameMakesTheNextProcessTheOwner(t *testing.T) {
	t.Setenv(brokerOffEnv, "")
	t.Setenv(brokerEnv, "gobuildcache-nobody-holds-this")
	assert.Nil(t, dialBroker(), "a name nobody answers must not be dialed")
}

// The off switch is what a bisect of a broker-shaped problem wants: every
// process then opens the directory for itself.
func TestBrokerOffSwitchServesNothing(t *testing.T) {
	t.Setenv(brokerEnv, "")
	t.Setenv(brokerOffEnv, "1")
	dir := t.TempDir()
	disk, err := Open(dir)
	require.NoError(t, err)
	serveBroker(disk, dir)
	t.Cleanup(StopBroker)
	assert.Empty(t, os.Getenv(brokerEnv), "the off switch must leave no endpoint to find")
	assert.Empty(t, BrokerEnviron())
}
