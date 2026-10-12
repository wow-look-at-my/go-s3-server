package main

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/wow-look-at-my/go-s3-server/cacheclient"
)

// One build stores its objects together in time. A later client that hits the
// first of them gets the rest from its look-ahead pool, through this server's
// window, before it asks.
func TestLookAheadBringsTheRestOfABuild(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}
	t.Setenv("GO_TOOLCHAIN_CACHE_LOOKAHEAD", "1")
	ts, _ := testSetupWithStorage(t)
	base := cacheclient.WebConfig{Bucket: "testbucket", Endpoint: ts.URL, AccessKey: "anyone", SecretKey: "unchecked"}

	objs := makeWireObjects(20)
	writerCfg := base
	writerCfg.IndexDir = t.TempDir()
	writer, err := cacheclient.NewWebBackend(writerCfg)
	require.NoError(t, err)
	for _, o := range objs {
		require.NoError(t, writer.Put(o.actionID, o.outputID, o.body))
	}
	require.NoError(t, writer.Close())

	readerCfg := base
	readerCfg.IndexDir = t.TempDir()
	reader, err := cacheclient.NewWebBackend(readerCfg)
	require.NoError(t, err)
	t.Cleanup(func() { reader.Close() })
	var mu sync.Mutex
	got := map[string]bool{}
	reader.OnBatchEntries = func(entries []cacheclient.BatchEntry) {
		mu.Lock()
		defer mu.Unlock()
		for _, e := range entries {
			got[e.Key] = true
		}
	}

	_, _, _, miss := reader.Get(objs[0].actionID)
	require.False(t, miss)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == len(objs)-1
	}, 10*time.Second, 20*time.Millisecond, "the window around the first hit must carry the rest of the build")
}
