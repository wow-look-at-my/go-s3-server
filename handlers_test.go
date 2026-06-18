package main

import (
	"bytes"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testSetup(t *testing.T) *httptest.Server {
	t.Helper()
	dir := t.TempDir()

	cfg := &Config{
		Listen:      ":0",
		Bucket:      "testbucket",
		DataDir:     dir,
		WriteOnce:   WriteOnceConfig{Action: "allow"},
		DisableAuth: true,
	}

	storage, err := NewStorage(cfg.DataDir, cfg.WriteOnce)
	require.Nil(t, err)

	t.Cleanup(func() { storage.Close() })

	srv := NewServer(cfg, storage)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	return ts
}

func testSetupWriteOnce(t *testing.T) *httptest.Server {
	t.Helper()
	dir := t.TempDir()

	cfg := &Config{
		Listen:      ":0",
		Bucket:      "testbucket",
		DataDir:     dir,
		WriteOnce:   WriteOnceConfig{Action: "deny", Notification: "content_differs"},
		DisableAuth: true,
	}

	storage, err := NewStorage(cfg.DataDir, cfg.WriteOnce)
	require.Nil(t, err)

	t.Cleanup(func() { storage.Close() })

	srv := NewServer(cfg, storage)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	return ts
}

func doRequest(t *testing.T, ts *httptest.Server, method, path string, body []byte, extraHeaders map[string]string) *http.Response {
	t.Helper()
	var bodyReader io.Reader
	if body != nil {
		bodyReader = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, ts.URL+path, bodyReader)
	require.Nil(t, err)

	if body != nil {
		req.ContentLength = int64(len(body))
	}
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}

	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)

	return resp
}

func TestPutAndGetObject(t *testing.T) {
	ts := testSetup(t)

	content := []byte("hello world cache data")
	headers := map[string]string{"X-Cache-Meta-Outputid": "abc123def456"}
	resp := doRequest(t, ts, "PUT", "/testbucket/go-buildcache/v1aabbccdd11223344", content, headers)
	require.Equal(t, 200, resp.StatusCode)

	resp.Body.Close()

	resp = doRequest(t, ts, "GET", "/testbucket/go-buildcache/v1aabbccdd11223344", nil, nil)
	require.Equal(t, 200, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	require.Equal(t, string(content), string(body))

	// Native header carries the metadata.
	require.Equal(t, "abc123def456", resp.Header.Get("X-Cache-Meta-Outputid"))
	// Deprecated S3 header is still emitted for not-yet-upgraded clients.
	require.Equal(t, "abc123def456", resp.Header.Get("X-Amz-Meta-Outputid"))

	lm := resp.Header.Get("Last-Modified")
	require.NotEqual(t, "", lm)

	_, err := time.Parse(http.TimeFormat, lm)
	require.Nil(t, err)
}

func TestGetObjectNotFound(t *testing.T) {
	ts := testSetup(t)

	resp := doRequest(t, ts, "GET", "/testbucket/nonexistent/key", nil, nil)
	defer resp.Body.Close()

	require.Equal(t, 404, resp.StatusCode)

	// Native plain-text error: the machine-readable code is in a header and the
	// body is "<code>: <message>" (no S3 XML envelope).
	require.Equal(t, "not_found", resp.Header.Get("X-Cache-Error-Code"))
	body, _ := io.ReadAll(resp.Body)
	require.Contains(t, string(body), "not_found")
}

func TestWriteOnce(t *testing.T) {
	ts := testSetupWriteOnce(t)

	key := "/testbucket/cache/v1aabb000000000001"
	content1 := []byte("original content")
	content2 := []byte("overwrite attempt")
	oid := map[string]string{"X-Cache-Meta-Outputid": "o1"}

	resp := doRequest(t, ts, "PUT", key, content1, oid)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "PUT", key, content2, oid)
	require.Equal(t, 409, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "PUT", key, content1, oid)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "GET", key, nil, nil)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, string(content1), string(body))
}

func testSetupWriteOnceAlways(t *testing.T) *httptest.Server {
	t.Helper()
	dir := t.TempDir()

	cfg := &Config{
		Listen:      ":0",
		Bucket:      "testbucket",
		DataDir:     dir,
		WriteOnce:   WriteOnceConfig{Action: "deny", Notification: "always"},
		DisableAuth: true,
	}

	storage, err := NewStorage(cfg.DataDir, cfg.WriteOnce)
	require.Nil(t, err)
	t.Cleanup(func() { storage.Close() })

	srv := NewServer(cfg, storage)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	return ts
}

func testSetupWriteOnceNever(t *testing.T) *httptest.Server {
	t.Helper()
	dir := t.TempDir()

	cfg := &Config{
		Listen:      ":0",
		Bucket:      "testbucket",
		DataDir:     dir,
		WriteOnce:   WriteOnceConfig{Action: "deny", Notification: "never"},
		DisableAuth: true,
	}

	storage, err := NewStorage(cfg.DataDir, cfg.WriteOnce)
	require.Nil(t, err)
	t.Cleanup(func() { storage.Close() })

	srv := NewServer(cfg, storage)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	return ts
}

func TestWriteOnceNotificationAlways(t *testing.T) {
	ts := testSetupWriteOnceAlways(t)

	key := "/testbucket/cache/v1aabb000000000002"
	content := []byte("some content")
	oid := map[string]string{"X-Cache-Meta-Outputid": "o2"}

	resp := doRequest(t, ts, "PUT", key, content, oid)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "PUT", key, content, oid)
	require.Equal(t, 409, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "PUT", key, []byte("different"), oid)
	require.Equal(t, 409, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "GET", key, nil, nil)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, string(content), string(body))
}

func TestWriteOnceNotificationNever(t *testing.T) {
	ts := testSetupWriteOnceNever(t)

	key := "/testbucket/cache/v1aabb000000000003"
	content1 := []byte("original")
	content2 := []byte("different")
	oid := map[string]string{"X-Cache-Meta-Outputid": "o3"}

	resp := doRequest(t, ts, "PUT", key, content1, oid)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "PUT", key, content2, oid)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "GET", key, nil, nil)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, string(content1), string(body))
}

func TestNoSuchBucket(t *testing.T) {
	ts := testSetup(t)

	resp := doRequest(t, ts, "GET", "/wrongbucket/key", nil, nil)
	defer resp.Body.Close()

	require.Equal(t, 404, resp.StatusCode)
}

func TestShardingRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStorage(dir, WriteOnceConfig{Action: "allow"})
	require.Nil(t, err)

	defer s.Close()

	keys := []string{
		"go-buildcache/v1aabbccdd11223344",
		"go-buildcache/v1eeff001122334455",
		"ab",
		"abc",
		"abcd",
		"abcde",
		"dir/abcdef",
	}

	for _, key := range keys {
		path := s.keyToPath(key)
		reconstructed := s.pathToKey(path)
		assert.Equal(t, key, reconstructed)
	}
}

func TestLockExclusion(t *testing.T) {
	dir := t.TempDir()
	s1, err := NewStorage(dir, WriteOnceConfig{Action: "allow"})
	require.Nil(t, err)

	defer s1.Close()

	_, err = NewStorage(dir, WriteOnceConfig{Action: "allow"})
	require.NotNil(t, err)
}

func TestLoadConfig(t *testing.T) {
	dir := t.TempDir()

	// Valid config
	validJSON := `{
		"listen": ":8080",
		"bucket": "mybucket",
		"data_dir": "/tmp/test",
		"write_once": {"action": "deny", "notification": "content_differs"},
		"credentials": [{"username": "admin", "password": "secret"}]
	}`
	validPath := dir + "/valid.json"
	os.WriteFile(validPath, []byte(validJSON), 0644)
	cfg, err := LoadConfig(validPath)
	require.Nil(t, err)
	require.Equal(t, ":8080", cfg.Listen)
	require.Equal(t, "mybucket", cfg.Bucket)
	require.Equal(t, "deny", cfg.WriteOnce.Action)
	require.Equal(t, "content_differs", cfg.WriteOnce.Notification)

	// Defaults
	defaultJSON := `{"bucket": "b", "data_dir": "/tmp/d", "disable_auth": true}`
	defaultPath := dir + "/defaults.json"
	os.WriteFile(defaultPath, []byte(defaultJSON), 0644)
	cfg, err = LoadConfig(defaultPath)
	require.Nil(t, err)
	require.Equal(t, ":9000", cfg.Listen)
	require.True(t, cfg.DisableAuth)

	// Missing file
	_, err = LoadConfig(dir + "/nonexistent.json")
	require.NotNil(t, err)

	// Invalid JSON
	badPath := dir + "/bad.json"
	os.WriteFile(badPath, []byte("{invalid"), 0644)
	_, err = LoadConfig(badPath)
	require.NotNil(t, err)

	cred := `"credentials": [{"username": "admin", "password": "secret"}]`

	// Missing bucket
	noBucketPath := dir + "/nobucket.json"
	os.WriteFile(noBucketPath, []byte(`{"data_dir": "/tmp", `+cred+`}`), 0644)
	_, err = LoadConfig(noBucketPath)
	require.NotNil(t, err)

	// Missing data_dir
	noDirPath := dir + "/nodir.json"
	os.WriteFile(noDirPath, []byte(`{"bucket": "b", `+cred+`}`), 0644)
	_, err = LoadConfig(noDirPath)
	require.NotNil(t, err)

	// No credentials
	noCredPath := dir + "/nocred.json"
	os.WriteFile(noCredPath, []byte(`{"bucket": "b", "data_dir": "/tmp"}`), 0644)
	_, err = LoadConfig(noCredPath)
	require.NotNil(t, err)

	// Mismatched credential (username set, password empty)
	badCredPath := dir + "/badcred.json"
	os.WriteFile(badCredPath, []byte(`{"bucket": "b", "data_dir": "/tmp", "credentials": [{"username": "admin", "password": ""}]}`), 0644)
	_, err = LoadConfig(badCredPath)
	require.NotNil(t, err)

	// write_once defaults
	woDefaultPath := dir + "/wo_default.json"
	os.WriteFile(woDefaultPath, []byte(`{"bucket": "b", "data_dir": "/tmp", `+cred+`}`), 0644)
	cfg, err = LoadConfig(woDefaultPath)
	require.Nil(t, err)
	require.Equal(t, "allow", cfg.WriteOnce.Action)
	require.Equal(t, "never", cfg.WriteOnce.Notification)

	// Invalid write_once.action
	woInvalidAction := dir + "/wo_bad_action.json"
	os.WriteFile(woInvalidAction, []byte(`{"bucket": "b", "data_dir": "/tmp", "write_once": {"action": "invalid"}, `+cred+`}`), 0644)
	_, err = LoadConfig(woInvalidAction)
	require.NotNil(t, err)

	// Invalid write_once.notification
	woInvalidNotif := dir + "/wo_bad_notif.json"
	os.WriteFile(woInvalidNotif, []byte(`{"bucket": "b", "data_dir": "/tmp", "write_once": {"action": "deny", "notification": "invalid"}, `+cred+`}`), 0644)
	_, err = LoadConfig(woInvalidNotif)
	require.NotNil(t, err)

	// Envvar credential
	t.Setenv("TEST_CFG_USER", "myuser")
	t.Setenv("TEST_CFG_PASS", "mypass")
	envCredPath := dir + "/envcred.json"
	os.WriteFile(envCredPath, []byte(`{"bucket": "b", "data_dir": "/tmp", "credentials": [{"username": {"type": "envvar", "name": "TEST_CFG_USER"}, "password": {"type": "envvar", "name": "TEST_CFG_PASS"}}]}`), 0644)
	cfg, err = LoadConfig(envCredPath)
	require.Nil(t, err)
	require.Equal(t, "myuser", cfg.Credentials[0].Username.Value)
	require.Equal(t, "mypass", cfg.Credentials[0].Password.Value)

	// Invalid envvar type
	badEnvPath := dir + "/badenv.json"
	os.WriteFile(badEnvPath, []byte(`{"bucket": "b", "data_dir": "/tmp", "credentials": [{"username": {"type": "notenvvar", "name": "X"}, "password": "p"}]}`), 0644)
	_, err = LoadConfig(badEnvPath)
	require.NotNil(t, err)

	// Empty envvar name
	emptyEnvPath := dir + "/emptyenv.json"
	os.WriteFile(emptyEnvPath, []byte(`{"bucket": "b", "data_dir": "/tmp", "credentials": [{"username": {"type": "envvar", "name": ""}, "password": "p"}]}`), 0644)
	_, err = LoadConfig(emptyEnvPath)
	require.NotNil(t, err)
}

func TestMetadataRoundTrip(t *testing.T) {
	ts := testSetup(t)

	content := []byte("test data with meta")
	headers := map[string]string{
		"X-Cache-Meta-Outputid": "out1",
		"X-Cache-Meta-Custom":   "val2",
	}
	resp := doRequest(t, ts, "PUT", "/testbucket/meta/v1test000000000001", content, headers)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "GET", "/testbucket/meta/v1test000000000001", nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "out1", resp.Header.Get("X-Cache-Meta-Outputid"))
	require.Equal(t, "val2", resp.Header.Get("X-Cache-Meta-Custom"))
	resp.Body.Close()
}

// TestLegacyAmzMetaCompat verifies the deprecated S3 metadata path still works:
// a client uploading with X-Amz-Meta-* headers stores metadata, and a GET serves
// it back under both the native and legacy header names. The deprecation counter
// is bumped so the lingering S3 traffic stays observable.
func TestLegacyAmzMetaCompat(t *testing.T) {
	ts := testSetup(t)

	before := testutil.ToFloat64(deprecatedRequestsTotal.WithLabelValues(featureAmzMeta))

	content := []byte("legacy client data")
	headers := map[string]string{"X-Amz-Meta-Outputid": "legacy123"}
	resp := doRequest(t, ts, "PUT", "/testbucket/meta/v1legacy00000000001", content, headers)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "GET", "/testbucket/meta/v1legacy00000000001", nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "legacy123", resp.Header.Get("X-Cache-Meta-Outputid"))
	require.Equal(t, "legacy123", resp.Header.Get("X-Amz-Meta-Outputid"))
	resp.Body.Close()

	after := testutil.ToFloat64(deprecatedRequestsTotal.WithLabelValues(featureAmzMeta))
	require.Greater(t, after, before, "deprecated-request counter should increase on X-Amz-Meta use")
}

func TestMethodNotAllowed(t *testing.T) {
	ts := testSetup(t)

	resp := doRequest(t, ts, "PATCH", "/testbucket/some/key", nil, nil)
	defer resp.Body.Close()
	require.Equal(t, 405, resp.StatusCode)
}

// TestDeleteObject covers the surgical eviction lever: a stored object can be
// removed, a subsequent GET 404s, the key is dropped from the /_index blob, and
// DELETE is idempotent (deleting a missing key still succeeds). This is the
// mechanism for evicting a poisoned build-cache entry without a full purge.
func TestDeleteObject(t *testing.T) {
	ts := testSetup(t)

	const actionHex = "10f94fc02dcc245820dd861f4c6c25dee23ceb750f6be498fe84f67dfd2f1f9b"
	hashBytes, err := hex.DecodeString(actionHex)
	require.Nil(t, err)
	key := "/testbucket/go-buildcache/v1" + actionHex
	content := []byte("cache data to evict")

	resp := doRequest(t, ts, "PUT", key, content, map[string]string{"X-Cache-Meta-Outputid": "deadbeef"})
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// The key's action hash is advertised in the index before deletion.
	resp = doRequest(t, ts, "GET", "/testbucket/_index", nil, nil)
	idxBefore, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.True(t, bytes.Contains(idxBefore, hashBytes), "index should list the key before delete")

	// Delete it.
	resp = doRequest(t, ts, "DELETE", key, nil, nil)
	require.Equal(t, 204, resp.StatusCode)
	resp.Body.Close()

	// GET now 404s.
	resp = doRequest(t, ts, "GET", key, nil, nil)
	require.Equal(t, 404, resp.StatusCode)
	resp.Body.Close()

	// And the key is gone from the index blob.
	resp = doRequest(t, ts, "GET", "/testbucket/_index", nil, nil)
	idxAfter, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.False(t, bytes.Contains(idxAfter, hashBytes), "index must not list the key after delete")

	// DELETE is idempotent: removing it again still succeeds.
	resp = doRequest(t, ts, "DELETE", key, nil, nil)
	require.Equal(t, 204, resp.StatusCode)
	resp.Body.Close()
}

// TestSelfHealEvictsOutputIDLessObject covers the self-healing path: an object
// stored without outputid metadata -- a relic of an earlier cache-data
// iteration, or one whose xattrs were stripped by a data-dir move -- is unusable
// by every client yet pins its key in /_index, so clients skip re-uploading it
// and every build that needs the action takes a forced miss. The first GET must
// evict it (dropping it from the index, returning a clean 404) so a subsequent
// PUT repopulates a correct, usable object. This is what keeps a "missing
// outputid metadata" relic from permanently wedging a hot cache key.
func TestSelfHealEvictsOutputIDLessObject(t *testing.T) {
	ts := testSetup(t)

	const actionHex = "a1b2c3d4e5f6071829304a5b6c7d8e9f0011223344556677889900aabbccddee"
	hashBytes, err := hex.DecodeString(actionHex)
	require.Nil(t, err)
	key := "/testbucket/go-buildcache/v1" + actionHex

	// Store a body with NO outputid metadata -- the poisoned shape we must heal.
	resp := doRequest(t, ts, "PUT", key, []byte("stale body"), nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// It starts out advertised in the index, so clients would skip re-uploading.
	resp = doRequest(t, ts, "GET", "/testbucket/_index", nil, nil)
	idxBefore, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.True(t, bytes.Contains(idxBefore, hashBytes), "outputid-less key should start in the index")

	healedBefore := testutil.ToFloat64(selfHealEvictionsTotal)

	// First GET self-heals: a clean miss (404), not a 200 the client cannot use.
	resp = doRequest(t, ts, "GET", key, nil, nil)
	require.Equal(t, 404, resp.StatusCode)
	require.Equal(t, "not_found", resp.Header.Get("X-Cache-Error-Code"))
	resp.Body.Close()

	require.Greater(t, testutil.ToFloat64(selfHealEvictionsTotal), healedBefore,
		"self-heal eviction counter should increase")

	// The key is gone from the index, so the next client re-uploads instead of skipping.
	resp = doRequest(t, ts, "GET", "/testbucket/_index", nil, nil)
	idxAfter, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.False(t, bytes.Contains(idxAfter, hashBytes), "evicted key must leave the index")

	// A subsequent correct PUT (with outputid) repopulates a usable object.
	resp = doRequest(t, ts, "PUT", key, []byte("rebuilt body"),
		map[string]string{"X-Cache-Meta-Outputid": "abc123"})
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "GET", key, nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	gotOutputID := resp.Header.Get("X-Cache-Meta-Outputid")
	resp.Body.Close()
	require.Equal(t, "rebuilt body", string(body))
	require.Equal(t, "abc123", gotOutputID, "repopulated object must serve its outputid")
}

func TestUnsafeKeyHashedStorage(t *testing.T) {
	ts := testSetup(t)

	oid := map[string]string{"X-Cache-Meta-Outputid": "u1"}
	traversalKey := "prefix/../../etc/passwd"
	content := []byte("safe content")
	resp := doRequest(t, ts, "PUT", "/testbucket/"+traversalKey, content, oid)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "GET", "/testbucket/"+traversalKey, nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, string(content), string(body))

	dotKey := "a..b/file.txt"
	resp = doRequest(t, ts, "PUT", "/testbucket/"+dotKey, []byte("dot data"), oid)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "GET", "/testbucket/"+dotKey, nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, "dot data", string(body))
}

func TestSafeKeyUnchangedBehavior(t *testing.T) {
	ts := testSetup(t)

	key := "go-buildcache/v1aabb000000000099"
	content := []byte("cache data")
	resp := doRequest(t, ts, "PUT", "/testbucket/"+key, content, map[string]string{"X-Cache-Meta-Outputid": "s1"})
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "GET", "/testbucket/"+key, nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, string(content), string(body))
}

func TestIsKeySafe(t *testing.T) {
	assert.True(t, isKeySafe("go-buildcache/v1aabbccdd11223344"))
	assert.True(t, isKeySafe("prefix/abc_DEF-123"))
	assert.True(t, isKeySafe("simple"))
	assert.False(t, isKeySafe(""))
	assert.False(t, isKeySafe("../etc/passwd"))
	assert.False(t, isKeySafe("a..b"))
	assert.False(t, isKeySafe("path/with spaces"))
	assert.False(t, isKeySafe("file.txt"))
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
