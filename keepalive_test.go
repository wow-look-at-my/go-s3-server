package main

import (
	"archive/tar"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestKeepAliveSendsTheStatusAndSpacesWhileTheWorkRuns(t *testing.T) {
	t.Serial() // batchKeepAlive is package state
	old := batchKeepAlive
	batchKeepAlive = 5 * time.Millisecond
	defer func() { batchKeepAlive = old }()

	w := httptest.NewRecorder()
	ka := startKeepAlive(w)
	time.Sleep(60 * time.Millisecond)
	require.True(t, ka.stop(), "a run past the period must have committed the status")
	require.Equal(t, 200, w.Code)
	require.Equal(t, "application/json", w.Header().Get("Content-Type"))
	body := w.Body.String()
	require.NotEmpty(t, body)
	require.Empty(t, strings.TrimSpace(body), "a keepalive writes only whitespace, which JSON skips")

	sent := w.Body.Len()
	time.Sleep(20 * time.Millisecond)
	require.Equal(t, sent, w.Body.Len(), "nothing is written after stop")
}

func TestKeepAliveThatStopsInTimeCommitsNothing(t *testing.T) {
	w := httptest.NewRecorder()
	ka := startKeepAlive(w)
	require.False(t, ka.stop())
	require.Zero(t, w.Body.Len())
}

// The client reads the reply with json.Unmarshal, so a reply that opens with keepalive spaces must still parse.
func TestBatchPut_KeepAliveReplyStillParses(t *testing.T) {
	t.Serial() // batchKeepAlive is package state
	old := batchKeepAlive
	batchKeepAlive = time.Nanosecond
	defer func() { batchKeepAlive = old }()

	ts := testSetup(t)
	key := "go-buildcache/v1" + strings.Repeat("d", 64)
	body := batchPutBody(t, []batchPutManifestEntry{{
		Key:      key,
		Metadata: map[string]string{"outputid": "outputidd", "compression": "lz4"},
	}}, [][]byte{lz4Compress(t, []byte("compiled object body"))})
	resp := doRequest(t, ts, "PUT", "/testbucket/_batch/put", body,
		map[string]string{"Content-Type": "application/x-tar"})
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)
	out := parseBatchPutResponse(t, resp)
	require.Len(t, out.Results, 1)
	require.Equal(t, storeStatusStored, out.Results[0].Status)
	require.Empty(t, out.Error)
}

// A failure after the status went out lands in the body, with no results, so the client rolls every key back.
func TestBatchPut_FailureAfterTheStatusLandsInTheBody(t *testing.T) {
	t.Serial() // batchKeepAlive is package state
	old := batchKeepAlive
	batchKeepAlive = time.Nanosecond
	defer func() { batchKeepAlive = old }()

	ts := testSetup(t)
	pr, pw := io.Pipe()
	go func() {
		tw := tar.NewWriter(pw)
		manifest, _ := json.Marshal(batchPutManifest{Entries: []batchPutManifestEntry{{Key: "go-buildcache/v1" + strings.Repeat("e", 64)}}})
		_ = tw.WriteHeader(&tar.Header{Name: "manifest.json", Mode: 0o644, Size: int64(len(manifest))})
		_, _ = tw.Write(manifest)
		_ = tw.Flush()
		// The handler sits in tr.Next while this sleeps, so the keepalive commits the status first.
		time.Sleep(50 * time.Millisecond)
		_ = tw.WriteHeader(&tar.Header{Name: "data/stranger", Mode: 0o644, Size: 1})
		_, _ = tw.Write([]byte("x"))
		_ = tw.Close()
		_ = pw.Close()
	}()
	req, err := http.NewRequest("PUT", ts.URL+"/testbucket/_batch/put", pr)
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-tar")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode, "the keepalive already sent the status")
	out := parseBatchPutResponse(t, resp)
	require.Empty(t, out.Results)
	require.Contains(t, out.Error, "invalid_request")
	require.Contains(t, out.Error, "data/stranger")
}
