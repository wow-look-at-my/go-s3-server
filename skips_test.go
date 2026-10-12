package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-s3-server/cacheclient"
)

// The real client misses a key its fresh index lacks without a request. The
// server must still count that miss, or its hit rate is wrong.
func TestCacheClientReportsIndexSkips(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}
	t.Setenv("TMPDIR", t.TempDir())
	t.Setenv("GO_TOOLCHAIN_CACHE_PUT_WINDOW_MS", "30000")

	ts, _ := testSetupWithStorage(t)
	const module = "example.com/skiptest"
	cfg := cacheclient.WebConfig{
		Bucket: "testbucket", Endpoint: ts.URL,
		AccessKey: "anyone", SecretKey: "unchecked",
		Module: module, IndexMaxAge: -1,
	}
	objs := makeWireObjects(2)
	writer, err := cacheclient.NewWebBackend(cfg)
	require.NoError(t, err)
	for _, o := range objs {
		require.NoError(t, writer.Put(o.actionID, o.outputID, o.body))
	}
	require.NoError(t, writer.Close())

	notInIndex := clientSkipsTotal.WithLabelValues("get", "not_in_index")
	putKnown := clientSkipsTotal.WithLabelValues("put", "known")
	projectMiss := projectObjectsTotal.WithLabelValues(module, objKindMiss)
	requested := batchKeysTotal.WithLabelValues("requested")
	notInIndexBefore, putKnownBefore := testutil.ToFloat64(notInIndex), testutil.ToFloat64(putKnown)
	projectMissBefore, requestedBefore := testutil.ToFloat64(projectMiss), testutil.ToFloat64(requested)

	// An empty index directory makes the earliest Get wait for a fresh, authoritative index.
	cfg.IndexDir = t.TempDir()
	reader, err := cacheclient.NewWebBackend(cfg)
	require.NoError(t, err)
	for i := range 3 {
		absent := sha256.Sum256([]byte{byte(i)})
		_, _, _, miss := reader.Get(hex.EncodeToString(absent[:]))
		require.True(t, miss)
	}
	require.NoError(t, reader.Put(objs[0].actionID, objs[0].outputID, objs[0].body))
	require.Equal(t, uint32(3), reader.SkippedNotInIndex.Load(), "the fresh index must answer every absent key locally")
	require.NoError(t, reader.Close())

	assert.Equal(t, requestedBefore, testutil.ToFloat64(requested), "no absent key may reach the server")
	assert.Equal(t, 3.0, testutil.ToFloat64(notInIndex)-notInIndexBefore, "Close must report each skipped get")
	assert.Equal(t, 1.0, testutil.ToFloat64(putKnown)-putKnownBefore, "Close must report the skipped put")
	assert.Equal(t, 3.0, testutil.ToFloat64(projectMiss)-projectMissBefore, "a skipped get is a miss for the project that asked")
}

func postSkips(t *testing.T, url, body string) *http.Response {
	t.Helper()
	req, err := http.NewRequest("POST", url+"/testbucket/_skips", strings.NewReader(body))
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	return resp
}

func TestSkipReportRejectsBadBodies(t *testing.T) {
	ts, _ := testSetupWithStorage(t)
	// Valid JSON with only known fields, so the size bound is the only reason to refuse it.
	large, err := json.MarshalIndent(skipReport{GetNotInIndex: 1}, "", strings.Repeat(" ", maxSkipReportBytes))
	require.NoError(t, err)
	assert.Equal(t, http.StatusRequestEntityTooLarge, postSkips(t, ts.URL, string(large)).StatusCode)

	for name, body := range map[string]string{
		"unknown field": `{"get_not_in_index":1,"bogus":2}`,
		"not json":      `nope`,
		"negative":      `{"get_not_in_index":-1}`,
		"out of range":  `{"put_known":4294967296}`,
	} {
		t.Run(name, func(t *testing.T) {
			resp := postSkips(t, ts.URL, body)
			assert.GreaterOrEqual(t, resp.StatusCode, 400, "a bad report must be refused, not counted")
		})
	}
	assert.Equal(t, http.StatusNoContent, postSkips(t, ts.URL, `{"get_known_miss":2}`).StatusCode)
}

// The server decodes the client's struct.
func TestSkipReportWireMatchesClient(t *testing.T) {
	data, err := json.Marshal(cacheclient.SkipReport{GetNotInIndex: 1, GetEmptyIndex: 2, GetBatchBackoff: 3, GetKnownMiss: 4, PutKnown: 5})
	require.NoError(t, err)
	rec := httptest.NewRecorder()
	handleSkipReport(rec, httptest.NewRequest("POST", "/testbucket/_skips", strings.NewReader(string(data))))
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())

	var rep skipReport
	require.NoError(t, json.Unmarshal(data, &rep))
	assert.Equal(t, skipReport{GetNotInIndex: 1, GetEmptyIndex: 2, GetBatchBackoff: 3, GetKnownMiss: 4, PutKnown: 5}, rep)
}
