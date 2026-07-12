package main

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchPut_Basic(t *testing.T) {
	ts := testSetup(t)

	manifest := batchPutManifest{
		Entries: []batchPutManifestEntry{
			{Key: "go-buildcache/v1abc123", Metadata: map[string]string{"outputid": "out1", "compression": "lz4"}},
			{Key: "go-buildcache/v1def456", Metadata: map[string]string{"outputid": "out2", "compression": "lz4"}},
		},
	}

	tarBody := buildTestTar(t, manifest, map[string][]byte{
		"go-buildcache/v1abc123": []byte("data-1"),
		"go-buildcache/v1def456": []byte("data-2"),
	})

	resp := doRequest(t, ts, "PUT", "/testbucket/_batch/put", tarBody, nil)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	var bResp batchPutResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&bResp))
	require.Len(t, bResp.Results, 2)
	for _, r := range bResp.Results {
		assert.Equal(t, "stored", r.Status, "key %s", r.Key)
	}

	// Verify objects are retrievable via individual GET.
	resp2 := doRequest(t, ts, "GET", "/testbucket/go-buildcache/v1abc123", nil, nil)
	defer resp2.Body.Close()
	require.Equal(t, 200, resp2.StatusCode)
	body, _ := io.ReadAll(resp2.Body)
	assert.Equal(t, []byte("data-1"), body)
}

func TestBatchPut_EmptyManifest(t *testing.T) {
	ts := testSetup(t)

	manifest := batchPutManifest{Entries: []batchPutManifestEntry{}}
	tarBody := buildTestTar(t, manifest, nil)

	resp := doRequest(t, ts, "PUT", "/testbucket/_batch/put", tarBody, nil)
	defer resp.Body.Close()
	require.Equal(t, 200, resp.StatusCode)

	var bResp batchPutResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&bResp))
	assert.Empty(t, bResp.Results)
}

func TestBatchPut_InvalidTar(t *testing.T) {
	ts := testSetup(t)
	resp := doRequest(t, ts, "PUT", "/testbucket/_batch/put", []byte("not a tar"), nil)
	defer resp.Body.Close()
	assert.Equal(t, 400, resp.StatusCode)
}

func buildTestTar(t *testing.T, manifest batchPutManifest, data map[string][]byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	mdata, err := json.Marshal(manifest)
	require.NoError(t, err)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "manifest.json", Size: int64(len(mdata)), Mode: 0644}))
	_, err = tw.Write(mdata)
	require.NoError(t, err)

	for _, e := range manifest.Entries {
		d := data[e.Key]
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: "data/" + e.Key, Size: int64(len(d)), Mode: 0644}))
		_, err = tw.Write(d)
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	return buf.Bytes()
}
