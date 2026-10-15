package main

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/wow-look-at-my/go-containers/set"
	"github.com/wow-look-at-my/go-s3-server/cacheclient"
)

// A build of a module stores the list of what it used. The next build of the
// same module, with nothing local, gets every one of those objects into its
// local tier before it asks. That build is with the server's window off.
func TestManifestBringsTheLastBuildAhead(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "0")
	ts, _ := testSetupWithStorage(t)
	base := cacheclient.WebConfig{Bucket: "testbucket", Endpoint: ts.URL, AccessKey: "anyone", SecretKey: "unchecked"}
	objs := makeWireObjects(20)

	firstCfg := base
	firstCfg.IndexDir = t.TempDir()
	first, err := cacheclient.NewWebBackend(firstCfg)
	require.NoError(t, err)
	first.OnBatchEntries = func([]cacheclient.BatchEntry) {}
	first.SetModule("example.com/m")
	for _, o := range objs {
		_, _, _, miss := first.Get(o.actionID)
		require.True(t, miss)
		require.NoError(t, first.Put(o.actionID, o.outputID, o.body))
	}
	require.NoError(t, first.Close())

	secondCfg := base
	secondCfg.IndexDir = t.TempDir()
	second, err := cacheclient.NewWebBackend(secondCfg)
	require.NoError(t, err)
	t.Cleanup(func() { second.Close() })
	var mu sync.Mutex
	got := set.New[string]()
	second.OnBatchEntries = func(entries []cacheclient.BatchEntry) {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range entries {
			got.Add(e.Key)
		}
	}
	second.SetModule("example.com/m")
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return got.Len() == len(objs)
	}, 20*time.Second, 20*time.Millisecond, "the manifest must bring every object the last build used")
}

// A different module reads a different manifest, so it fetches nothing ahead.
func TestManifestIsPerModule(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "0")
	ts, _ := testSetupWithStorage(t)
	base := cacheclient.WebConfig{Bucket: "testbucket", Endpoint: ts.URL, AccessKey: "anyone", SecretKey: "unchecked"}
	objs := makeWireObjects(5)

	firstCfg := base
	firstCfg.IndexDir = t.TempDir()
	first, err := cacheclient.NewWebBackend(firstCfg)
	require.NoError(t, err)
	first.OnBatchEntries = func([]cacheclient.BatchEntry) {}
	first.SetModule("example.com/m")
	for _, o := range objs {
		_, _, _, miss := first.Get(o.actionID)
		require.True(t, miss)
		require.NoError(t, first.Put(o.actionID, o.outputID, o.body))
	}
	require.NoError(t, first.Close())

	otherCfg := base
	otherCfg.IndexDir = t.TempDir()
	other, err := cacheclient.NewWebBackend(otherCfg)
	require.NoError(t, err)
	var mu sync.Mutex
	got := 0
	other.OnBatchEntries = func(entries []cacheclient.BatchEntry) {
		mu.Lock()
		got += len(entries)
		mu.Unlock()
	}
	other.SetModule("example.com/other")
	require.NoError(t, other.Close())
	mu.Lock()
	defer mu.Unlock()
	require.Zero(t, got)
}
