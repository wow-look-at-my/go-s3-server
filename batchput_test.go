package main

import (
	"archive/tar"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// batchPutBody builds an application/x-tar request body for /_batch/put from a
// manifest and a parallel slice of (already-compressed) member bodies. The
// bodies are written in manifest order under data/<key>, mirroring the wire
// contract the go-toolchain client implements.
func batchPutBody(t testing.TB, entries []batchPutManifestEntry, bodies [][]byte) []byte {
	t.Helper()
	require.Equal(t, len(entries), len(bodies), "one body per manifest entry")

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)

	manifest := batchPutManifest{Entries: entries}
	mdata, err := json.Marshal(manifest)
	require.NoError(t, err)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "manifest.json", Size: int64(len(mdata)), Mode: 0644}))
	_, err = tw.Write(mdata)
	require.NoError(t, err)

	for i, e := range entries {
		name := "data/" + e.Key
		require.NoError(t, tw.WriteHeader(&tar.Header{Name: name, Size: int64(len(bodies[i])), Mode: 0644}))
		_, err = tw.Write(bodies[i])
		require.NoError(t, err)
	}
	require.NoError(t, tw.Close())
	return buf.Bytes()
}

func parseBatchPutResponse(t testing.TB, resp *http.Response) batchPutResponse {
	t.Helper()
	var out batchPutResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	return out
}

// TestBatchPut_StoresMultipleObjects covers the happy path: a tar of three
// normal objects is accepted in one request, each is reported "stored", and each
// round-trips via a single GET with the correct body + outputid metadata and is
// advertised in /_index.
func TestBatchPut_StoresMultipleObjects(t *testing.T) {
	ts := testSetup(t)

	type obj struct {
		key      string
		hash     string
		body     []byte
		outputID string
	}
	objs := []obj{
		{key: "go-buildcache/v1" + strings.Repeat("a", 64)},
		{key: "go-buildcache/v1" + strings.Repeat("b", 64)},
		{key: "go-buildcache/v1" + strings.Repeat("c", 64)},
	}

	var entries []batchPutManifestEntry
	var bodies [][]byte
	for i := range objs {
		raw := []byte("compiled object body number " + string(rune('0'+i)))
		objs[i].body = lz4Compress(t, raw)
		objs[i].outputID = "outputid" + string(rune('0'+i))
		objs[i].hash = strings.Repeat(string("abc"[i]), 64)
		entries = append(entries, batchPutManifestEntry{
			Key: objs[i].key,
			Metadata: map[string]string{
				"outputid":    objs[i].outputID,
				"compression": "lz4",
			},
		})
		bodies = append(bodies, objs[i].body)
	}

	body := batchPutBody(t, entries, bodies)
	resp := doRequest(t, ts, "PUT", "/testbucket/_batch/put", body,
		map[string]string{"Content-Type": "application/x-tar"})
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
	out := parseBatchPutResponse(t, resp)
	resp.Body.Close()

	require.Len(t, out.Results, 3)
	for i, r := range out.Results {
		require.Equal(t, objs[i].key, r.Key)
		require.Equal(t, storeStatusStored, r.Status, "result %d: %s", i, r.Message)
	}

	// Each object round-trips via a single GET, with metadata intact.
	for _, o := range objs {
		resp = doRequest(t, ts, "GET", "/testbucket/"+o.key, nil, nil)
		require.Equal(t, 200, resp.StatusCode)
		got, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		require.Equal(t, o.body, got, "body must round-trip byte-for-byte")
		require.Equal(t, o.outputID, resp.Header.Get("X-Cache-Meta-Outputid"))
	}

	// Each appears in the binary /_index blob (by action hash).
	resp = doRequest(t, ts, "GET", "/testbucket/_index", nil, nil)
	idx, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	for _, o := range objs {
		hashHex := strings.TrimPrefix(o.key, "go-buildcache/v1")
		hb, err := hex.DecodeString(hashHex)
		require.NoError(t, err)
		require.True(t, bytes.Contains(idx, hb), "key %s must be advertised in /_index", o.key)
	}
}

// TestBatchPut_DropsModuleIndexMember covers the module-index refusal per
// member: a member that decodes to a Go module index is reported "dropped" and
// is NOT stored (a later GET 404s), while a normal sibling member still stores.
func TestBatchPut_DropsModuleIndexMember(t *testing.T) {
	ts := testSetup(t)

	idxKey := "go-buildcache/v1" + strings.Repeat("d", 64)
	objKey := "go-buildcache/v1" + strings.Repeat("e", 64)

	indexBody := lz4Compress(t, incompressibleIndexBody(t, 8192))
	normalRaw := []byte("!<arch>\nnormal compiled object body")
	normalBody := lz4Compress(t, normalRaw)

	entries := []batchPutManifestEntry{
		{Key: idxKey, Metadata: map[string]string{"compression": "lz4", "outputid": "idxout"}},
		{Key: objKey, Metadata: map[string]string{"compression": "lz4", "outputid": "objout"}},
	}
	body := batchPutBody(t, entries, [][]byte{indexBody, normalBody})

	resp := doRequest(t, ts, "PUT", "/testbucket/_batch/put", body,
		map[string]string{"Content-Type": "application/x-tar"})
	require.Equal(t, 200, resp.StatusCode)
	out := parseBatchPutResponse(t, resp)
	resp.Body.Close()

	require.Len(t, out.Results, 2)
	require.Equal(t, idxKey, out.Results[0].Key)
	require.Equal(t, storeStatusDropped, out.Results[0].Status, "a module index member must be dropped, not stored")
	require.Equal(t, objKey, out.Results[1].Key)
	require.Equal(t, storeStatusStored, out.Results[1].Status)

	// The index member was not stored: a GET 404s it.
	resp = doRequest(t, ts, "GET", "/testbucket/"+idxKey, nil, nil)
	require.Equal(t, 404, resp.StatusCode)
	resp.Body.Close()

	// The normal member was stored and round-trips.
	resp = doRequest(t, ts, "GET", "/testbucket/"+objKey, nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, normalBody, got)
}

// TestBatchPut_WriteOnceConflict covers a write_once conflict member: under a
// deny/content_differs write_once policy, a member re-uploading a key with
// different content is reported "conflict" (accepted, not overwritten), while a
// fresh sibling member still stores.
func TestBatchPut_WriteOnceConflict(t *testing.T) {
	ts := testSetupWriteOnce(t)

	conflictKey := "go-buildcache/v1" + strings.Repeat("1", 64)
	freshKey := "go-buildcache/v1" + strings.Repeat("2", 64)

	original := lz4Compress(t, []byte("original content"))

	// Seed the conflict key with the original content via a single PUT.
	resp := doRequest(t, ts, "PUT", "/testbucket/"+conflictKey, original,
		map[string]string{"X-Cache-Meta-Compression": "lz4", "X-Cache-Meta-Outputid": "orig"})
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// Batch: re-upload conflictKey with DIFFERENT content (conflict), plus a fresh key.
	different := lz4Compress(t, []byte("different content"))
	fresh := lz4Compress(t, []byte("brand new content"))
	entries := []batchPutManifestEntry{
		{Key: conflictKey, Metadata: map[string]string{"compression": "lz4", "outputid": "diff"}},
		{Key: freshKey, Metadata: map[string]string{"compression": "lz4", "outputid": "fresh"}},
	}
	body := batchPutBody(t, entries, [][]byte{different, fresh})

	resp = doRequest(t, ts, "PUT", "/testbucket/_batch/put", body,
		map[string]string{"Content-Type": "application/x-tar"})
	require.Equal(t, 200, resp.StatusCode)
	out := parseBatchPutResponse(t, resp)
	resp.Body.Close()

	require.Len(t, out.Results, 2)
	require.Equal(t, storeStatusConflict, out.Results[0].Status, "a write_once conflict must be reported as conflict")
	require.Equal(t, storeStatusStored, out.Results[1].Status, "a fresh key alongside a conflict must still store")

	// The conflict key still serves the ORIGINAL content (not overwritten).
	resp = doRequest(t, ts, "GET", "/testbucket/"+conflictKey, nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, original, got, "a write_once conflict must not overwrite the stored body")
}

// TestBatchPut_MalformedTar covers the whole-request 400 for an unparseable body.
func TestBatchPut_MalformedTar(t *testing.T) {
	ts := testSetup(t)

	resp := doRequest(t, ts, "PUT", "/testbucket/_batch/put", []byte("this is not a tar archive at all"),
		map[string]string{"Content-Type": "application/x-tar"})
	require.Equal(t, 400, resp.StatusCode)
	require.Equal(t, "invalid_request", resp.Header.Get("X-Cache-Error-Code"))
	resp.Body.Close()
}

// TestBatchPut_MissingManifest covers the whole-request 400 when the first tar
// member is not manifest.json.
func TestBatchPut_MissingManifest(t *testing.T) {
	ts := testSetup(t)

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	// First member is a data member, not manifest.json.
	data := []byte("body")
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "data/go-buildcache/v1" + strings.Repeat("a", 64), Size: int64(len(data)), Mode: 0644}))
	_, err := tw.Write(data)
	require.NoError(t, err)
	require.NoError(t, tw.Close())

	resp := doRequest(t, ts, "PUT", "/testbucket/_batch/put", buf.Bytes(),
		map[string]string{"Content-Type": "application/x-tar"})
	require.Equal(t, 400, resp.StatusCode)
	require.Equal(t, "invalid_request", resp.Header.Get("X-Cache-Error-Code"))
	resp.Body.Close()
}

// TestBatchPut_DataMemberWithoutManifestEntry covers the whole-request 400 when
// a data member has no corresponding manifest entry.
func TestBatchPut_DataMemberWithoutManifestEntry(t *testing.T) {
	ts := testSetup(t)

	key := "go-buildcache/v1" + strings.Repeat("a", 64)
	orphan := "go-buildcache/v1" + strings.Repeat("f", 64)

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	manifest := batchPutManifest{Entries: []batchPutManifestEntry{{Key: key}}}
	mdata, _ := json.Marshal(manifest)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "manifest.json", Size: int64(len(mdata)), Mode: 0644}))
	_, _ = tw.Write(mdata)
	// A data member with NO manifest entry.
	orphanBody := lz4Compress(t, []byte("orphan"))
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "data/" + orphan, Size: int64(len(orphanBody)), Mode: 0644}))
	_, _ = tw.Write(orphanBody)
	require.NoError(t, tw.Close())

	resp := doRequest(t, ts, "PUT", "/testbucket/_batch/put", buf.Bytes(),
		map[string]string{"Content-Type": "application/x-tar"})
	require.Equal(t, 400, resp.StatusCode)
	require.Equal(t, "invalid_request", resp.Header.Get("X-Cache-Error-Code"))
	resp.Body.Close()
}

// TestBatchPut_ManifestEntryWithoutDataMember covers the whole-request 400 when
// a manifest entry has no matching data member.
func TestBatchPut_ManifestEntryWithoutDataMember(t *testing.T) {
	ts := testSetup(t)

	present := "go-buildcache/v1" + strings.Repeat("a", 64)
	missing := "go-buildcache/v1" + strings.Repeat("b", 64)

	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	manifest := batchPutManifest{Entries: []batchPutManifestEntry{{Key: present}, {Key: missing}}}
	mdata, _ := json.Marshal(manifest)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "manifest.json", Size: int64(len(mdata)), Mode: 0644}))
	_, _ = tw.Write(mdata)
	// Only the first key has a data member.
	b := lz4Compress(t, []byte("body"))
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "data/" + present, Size: int64(len(b)), Mode: 0644}))
	_, _ = tw.Write(b)
	require.NoError(t, tw.Close())

	resp := doRequest(t, ts, "PUT", "/testbucket/_batch/put", buf.Bytes(),
		map[string]string{"Content-Type": "application/x-tar"})
	require.Equal(t, 400, resp.StatusCode)
	require.Equal(t, "invalid_request", resp.Header.Get("X-Cache-Error-Code"))
	resp.Body.Close()
}

// TestBatchPut_TooManyEntries covers the over-cap rejection (413) when the
// manifest declares more than maxBatchKeys entries.
func TestBatchPut_TooManyEntries(t *testing.T) {
	ts := testSetup(t)

	// Only the manifest needs to be oversized; no data members are needed since
	// the entry-count check runs right after decoding the manifest.
	entries := make([]batchPutManifestEntry, maxBatchKeys+1)
	for i := range entries {
		entries[i] = batchPutManifestEntry{Key: "go-buildcache/v1" + strings.Repeat("a", 64)}
	}
	var buf bytes.Buffer
	tw := tar.NewWriter(&buf)
	manifest := batchPutManifest{Entries: entries}
	mdata, _ := json.Marshal(manifest)
	require.NoError(t, tw.WriteHeader(&tar.Header{Name: "manifest.json", Size: int64(len(mdata)), Mode: 0644}))
	_, _ = tw.Write(mdata)
	require.NoError(t, tw.Close())

	resp := doRequest(t, ts, "PUT", "/testbucket/_batch/put", buf.Bytes(),
		map[string]string{"Content-Type": "application/x-tar"})
	require.Equal(t, 413, resp.StatusCode)
	require.Equal(t, "too_large", resp.Header.Get("X-Cache-Error-Code"))
	resp.Body.Close()
}

// TestBatchPut_RequiresAuth confirms /_batch/put goes through the same auth gate
// as every other route: an unauthenticated request is rejected when auth is on.
func TestBatchPut_RequiresAuth(t *testing.T) {
	ts, _ := testSetupWithAuth(t)

	entries := []batchPutManifestEntry{
		{Key: "go-buildcache/v1" + strings.Repeat("a", 64), Metadata: map[string]string{"compression": "lz4"}},
	}
	bodies := [][]byte{lz4Compress(t, []byte("body"))}
	body := batchPutBody(t, entries, bodies)

	// No credentials: rejected.
	resp := doRequest(t, ts, "PUT", "/testbucket/_batch/put", body,
		map[string]string{"Content-Type": "application/x-tar"})
	require.Equal(t, 403, resp.StatusCode)
	require.Equal(t, "access_denied", resp.Header.Get("X-Cache-Error-Code"))
	resp.Body.Close()

	// With valid credentials: accepted.
	req, err := http.NewRequest("PUT", ts.URL+"/testbucket/_batch/put", bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-tar")
	req.SetBasicAuth("alice", "password1")
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()
}
