package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

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

	// The index loads in the background. Waiting for it gives every Get below a fresh, authoritative index.
	cfg.IndexDir = t.TempDir()
	reader, err := cacheclient.NewWebBackend(cfg)
	require.NoError(t, err)
	reader.WaitIndex()
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
	data, err := json.Marshal(cacheclient.SkipReport{GetNotInIndex: 1, GetEmptyIndex: 2, GetBatchBackoff: 3, GetKnownMiss: 4, PutKnown: 5, PutFailed: 6, PutRefusedBuildID: 7, PutRefusedModIndex: 9, PutAbandoned: 8, SkippedKeys: []string{strings.Repeat("ab", 32)}, SkippedAgo: []int64{12}})
	require.NoError(t, err)
	failed := clientUploadsLostTotal.WithLabelValues("failed")
	abandoned := clientUploadsLostTotal.WithLabelValues("abandoned")
	buildID := clientUploadsLostTotal.WithLabelValues("refused_buildid")
	failedBefore, abandonedBefore, buildIDBefore := testutil.ToFloat64(failed), testutil.ToFloat64(abandoned), testutil.ToFloat64(buildID)
	rec := httptest.NewRecorder()
	handleSkipReport(rec, httptest.NewRequest("POST", "/testbucket/_skips", strings.NewReader(string(data))), nil)
	require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())

	var rep skipReport
	require.NoError(t, json.Unmarshal(data, &rep))
	assert.Equal(t, skipReport{GetNotInIndex: 1, GetEmptyIndex: 2, GetBatchBackoff: 3, GetKnownMiss: 4, PutKnown: 5, PutFailed: 6, PutRefusedBuildID: 7, PutRefusedModIndex: 9, PutAbandoned: 8, SkippedKeys: []string{strings.Repeat("ab", 32)}, SkippedAgo: []int64{12}}, rep)
	assert.Equal(t, 6.0, testutil.ToFloat64(failed)-failedBefore, "an upload the client could not complete is counted")
	assert.Equal(t, 8.0, testutil.ToFloat64(abandoned)-abandonedBefore)
	assert.Equal(t, 7.0, testutil.ToFloat64(buildID)-buildIDBefore)
}

// An older client sends no loss fields, or one refusal count for both
// kinds, and its report still counts.
func TestSkipReportFromAnOlderClient(t *testing.T) {
	refused := clientUploadsLostTotal.WithLabelValues("refused")
	before := testutil.ToFloat64(refused)
	for _, body := range []string{
		`{"get_not_in_index":1,"get_empty_index":0,"get_batch_backoff":0,"get_known_miss":0,"put_known":2}`,
		`{"get_not_in_index":1,"put_known":2,"put_failed":0,"put_refused":3,"put_abandoned":0}`,
	} {
		rec := httptest.NewRecorder()
		handleSkipReport(rec, httptest.NewRequest("POST", "/testbucket/_skips", strings.NewReader(body)), nil)
		require.Equal(t, http.StatusNoContent, rec.Code, rec.Body.String())
	}
	assert.Equal(t, 3.0, testutil.ToFloat64(refused)-before)
}

// The server says what it knows of each sampled key: evicted by the
// sweeper, advertised now, or never held. A malformed key fails the report.
func TestSkipReportSampleIsClassified(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}
	ts, storage := testSetupWithStorage(t)
	evicted := cacheprogKey('a')
	require.NoError(t, storage.Put(evicted, []byte("body"), map[string]string{"outputid": "x"}, nil))
	_, err := storage.Evict(0, 1, time.Now().Add(time.Hour))
	require.NoError(t, err)
	held := cacheprogKey('b')
	require.NoError(t, storage.Put(held, []byte("body"), map[string]string{"outputid": "y"}, nil))
	hexOf := func(key string) string { return strings.TrimPrefix(key, gbciKeyPrefix) }

	counts := func(verdict string) float64 {
		return testutil.ToFloat64(clientSkipSamplesTotal.WithLabelValues(verdict))
	}
	evictedBefore, advertisedBefore, unknownBefore := counts("evicted"), counts("advertised"), counts("unknown")
	body, err := json.Marshal(skipReport{GetNotInIndex: 3, SkippedKeys: []string{hexOf(evicted), hexOf(held), hexOf(cacheprogKey('c'))}})
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, postSkips(t, ts.URL, string(body)).StatusCode)
	assert.Equal(t, 1.0, counts("evicted")-evictedBefore)
	assert.Equal(t, 1.0, counts("advertised")-advertisedBefore)
	assert.Equal(t, 1.0, counts("unknown")-unknownBefore)

	assert.Equal(t, http.StatusBadRequest, postSkips(t, ts.URL, `{"get_not_in_index":1,"skipped_keys":["nope"]}`).StatusCode)
	many, err := json.Marshal(skipReport{SkippedKeys: make([]string, maxSkipSample+1)})
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, postSkips(t, ts.URL, string(many)).StatusCode)
	negative, err := json.Marshal(skipReport{SkippedKeys: []string{hexOf(held)}, SkippedAgo: []int64{-1}})
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, postSkips(t, ts.URL, string(negative)).StatusCode, "an age is never negative")
	extra, err := json.Marshal(skipReport{SkippedAgo: []int64{1}})
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, postSkips(t, ts.URL, string(extra)).StatusCode, "an age belongs to a key")
}

// A report arrives when the client exits, after it uploaded what it built.
// A held key the server stored after the client skipped it is that upload,
// not a stale index copy, and the sample says so.
func TestSkipReportSeparatesKeysStoredAfterTheSkip(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}
	ts, storage := testSetupWithStorage(t)
	stale := cacheprogKey('d')
	require.NoError(t, storage.Put(stale, []byte("body"), map[string]string{"outputid": "x"}, nil))
	old := time.Now().Add(-10 * time.Minute)
	require.NoError(t, os.Chtimes(storage.keyToPath(stale), old, old))
	later := cacheprogKey('e')
	require.NoError(t, storage.Put(later, []byte("body"), map[string]string{"outputid": "y"}, nil))
	hexOf := func(key string) string { return strings.TrimPrefix(key, gbciKeyPrefix) }

	counts := func(verdict string) float64 {
		return testutil.ToFloat64(clientSkipSamplesTotal.WithLabelValues(verdict))
	}
	advertisedBefore, laterBefore := counts("advertised"), counts("stored_later")
	// Both keys were skipped a minute before the report.
	body, err := json.Marshal(skipReport{GetNotInIndex: 2, SkippedKeys: []string{hexOf(stale), hexOf(later)}, SkippedAgo: []int64{60, 60}})
	require.NoError(t, err)
	assert.Equal(t, http.StatusNoContent, postSkips(t, ts.URL, string(body)).StatusCode)
	assert.Equal(t, 1.0, counts("advertised")-advertisedBefore, "stored ten minutes before the skip: the client's index copy was stale")
	assert.Equal(t, 1.0, counts("stored_later")-laterBefore, "stored after the skip: an upload during the run")
}
