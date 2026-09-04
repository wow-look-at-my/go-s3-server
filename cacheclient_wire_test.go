package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-s3-server/cacheclient"
)

// wireObject is one cache object as the client stores it: the action ID it is
// keyed by, the body, and the output ID, which the GOCACHEPROG contract defines
// as the sha256 of that body. A client discards a download whose body does not
// hash to the output ID, so the two cannot be made up independently.
type wireObject struct {
	actionID string
	outputID string
	body     []byte
}

func makeWireObjects(n int) []wireObject {
	objs := make([]wireObject, n)
	for i := range objs {
		body := []byte("!<arch>\ncached output " + strings.Repeat(fmt.Sprintf("%d ", i), 64))
		action := sha256.Sum256([]byte(fmt.Sprintf("action-%d", i)))
		output := sha256.Sum256(body)
		objs[i] = wireObject{
			actionID: hex.EncodeToString(action[:]),
			outputID: hex.EncodeToString(output[:]),
			body:     body,
		}
	}
	return objs
}

// TestCacheClientColdGetIsServedOverTheWire is the round-trip check on the
// protocol this repository defines on both sides: cacheclient is the real
// client, and the server under test is the real server.
//
// It replaces a dats suite that drove a real `go build` through
// GOCACHEPROG='go-toolchain cacheprog'. That subcommand no longer exists —
// gosmopolitan's cmd/go calls cacheclient in process — so the subprocess it
// tested is not a path any build takes now.
//
// The second backend is the point. It is a separate client with its own state,
// so it holds nothing locally and every byte it returns came from the server.
func TestCacheClientColdGetIsServedOverTheWire(t *testing.T) {
	if !forkMetrics(t) {
		return
	}

	// The client caches the key index under TMPDIR. A private one keeps this
	// test off any index another run left behind.
	t.Setenv("TMPDIR", t.TempDir())
	// One batch, shipped by Close, rather than a window that expires mid-test.
	t.Setenv("GO_TOOLCHAIN_CACHE_PUT_WINDOW_MS", "30000")

	ts, storage := testSetupWithStorage(t)
	// The client requires credentials whatever the server does with them; this
	// server runs with disable_auth, so any pair gets through the gate.
	cfg := cacheclient.WebConfig{
		Bucket:    "testbucket",
		Endpoint:  ts.URL,
		AccessKey: "anyone",
		SecretKey: "unchecked",
	}
	objs := makeWireObjects(8)

	writer, err := cacheclient.NewWebBackend(cfg)
	require.NoError(t, err)
	for _, o := range objs {
		require.NoError(t, writer.Put(o.actionID, o.outputID, strings.NewReader(string(o.body)), int64(len(o.body))))
	}
	// Close flushes the PUT coalescer: until it returns, an upload is claimed
	// in the client's index but not yet stored on the server.
	require.NoError(t, writer.Close())

	for _, o := range objs {
		hash, ok := extractActionHash("go-buildcache/v1" + o.actionID)
		require.True(t, ok)
		require.True(t, storage.Index.Contains(hash), "the server must advertise what the client uploaded")
	}

	streamedBefore := testutil.ToFloat64(batchKeysTotal.WithLabelValues("streamed"))

	reader, err := cacheclient.NewWebBackend(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { reader.Close() })

	for _, o := range objs {
		outputID, body, size, _, miss, _, err := reader.Get(o.actionID)
		require.NoError(t, err)
		require.False(t, miss, "a cold client must be served the key the server advertises")

		got, err := io.ReadAll(body)
		require.NoError(t, err)
		body.Close()

		require.Equal(t, o.body, got)
		require.Equal(t, o.outputID, outputID, "the served output ID must be the body's own hash")
		require.Equal(t, int64(len(o.body)), size)
	}

	require.Greater(t, testutil.ToFloat64(batchKeysTotal.WithLabelValues("streamed")), streamedBefore,
		"the bodies must have come off this server, not out of the client")

	// A key nobody stored is a clean miss, not an error and not a stray body.
	absent := sha256.Sum256([]byte("never-stored"))
	_, _, _, _, miss, _, err := reader.Get(hex.EncodeToString(absent[:]))
	require.NoError(t, err)
	require.True(t, miss)
}
