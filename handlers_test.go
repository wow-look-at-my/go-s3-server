package main

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/wow-look-at-my/testify/assert"
	"github.com/wow-look-at-my/testify/require"
)

func testSetup(t *testing.T) *httptest.Server {
	t.Helper()
	dir := t.TempDir()

	cfg := &Config{
		Listen:    ":0",
		Bucket:    "testbucket",
		DataDir:   dir,
		WriteOnce:   WriteOnceConfig{Action: "allow"},
		Credentials: []Credential{{Username: ConfigString{Value: ""}, Password: ConfigString{Value: ""}}},
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
		Listen:    ":0",
		Bucket:    "testbucket",
		DataDir:   dir,
		WriteOnce:   WriteOnceConfig{Action: "deny", Notification: "content_differs"},
		Credentials: []Credential{{Username: ConfigString{Value: ""}, Password: ConfigString{Value: ""}}},
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
	headers := map[string]string{"X-Amz-Meta-Outputid": "abc123def456"}
	resp := doRequest(t, ts, "PUT", "/testbucket/go-buildcache/v1aabbccdd11223344", content, headers)
	require.Equal(t, 200, resp.StatusCode)

	resp.Body.Close()

	resp = doRequest(t, ts, "GET", "/testbucket/go-buildcache/v1aabbccdd11223344", nil, nil)
	require.Equal(t, 200, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	require.Equal(t, string(content), string(body))

	outputID := resp.Header.Get("X-Amz-Meta-Outputid")
	require.Equal(t, "abc123def456", outputID)

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

	var s3err S3Error
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&s3err))

	require.Equal(t, "NoSuchKey", s3err.Code)
}

func TestListObjectsV2(t *testing.T) {
	ts := testSetup(t)

	keys := []string{
		"go-buildcache/v1aaaa000000000001",
		"go-buildcache/v1bbbb000000000002",
		"go-buildcache/v1cccc000000000003",
	}
	for _, key := range keys {
		resp := doRequest(t, ts, "PUT", "/testbucket/"+key, []byte("data-"+key), nil)
		require.Equal(t, 200, resp.StatusCode)
		resp.Body.Close()
	}

	resp := doRequest(t, ts, "GET", "/testbucket?list-type=2&prefix=go-buildcache/", nil, nil)
	require.Equal(t, 200, resp.StatusCode)

	var result ListBucketResult
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))
	resp.Body.Close()

	require.Equal(t, 3, len(result.Contents))

	for i := 1; i < len(result.Contents); i++ {
		require.GreaterOrEqual(t, result.Contents[i].Key, result.Contents[i-1].Key)
	}
}

func TestListObjectsV2Pagination(t *testing.T) {
	ts := testSetup(t)

	keys := []string{
		"prefix/v1aaaa000000000001",
		"prefix/v1bbbb000000000002",
		"prefix/v1cccc000000000003",
		"prefix/v1dddd000000000004",
		"prefix/v1eeee000000000005",
	}
	for _, key := range keys {
		resp := doRequest(t, ts, "PUT", "/testbucket/"+key, []byte("d"), nil)
		resp.Body.Close()
	}

	// Page 1: max-keys=2
	resp := doRequest(t, ts, "GET", "/testbucket?list-type=2&prefix=prefix/&max-keys=2", nil, nil)
	var page1 ListBucketResult
	xml.NewDecoder(resp.Body).Decode(&page1)
	resp.Body.Close()

	require.Equal(t, 2, len(page1.Contents))
	require.True(t, page1.IsTruncated)
	require.NotEqual(t, "", page1.NextContinuationToken)

	// Page 2
	resp = doRequest(t, ts, "GET",
		"/testbucket?list-type=2&prefix=prefix/&max-keys=2&continuation-token="+page1.NextContinuationToken,
		nil, nil)
	var page2 ListBucketResult
	xml.NewDecoder(resp.Body).Decode(&page2)
	resp.Body.Close()

	require.Equal(t, 2, len(page2.Contents))
	require.True(t, page2.IsTruncated)

	// Page 3 (last)
	resp = doRequest(t, ts, "GET",
		"/testbucket?list-type=2&prefix=prefix/&max-keys=2&continuation-token="+page2.NextContinuationToken,
		nil, nil)
	var page3 ListBucketResult
	xml.NewDecoder(resp.Body).Decode(&page3)
	resp.Body.Close()

	require.Equal(t, 1, len(page3.Contents))
	require.False(t, page3.IsTruncated)
}

func TestWriteOnce(t *testing.T) {
	ts := testSetupWriteOnce(t)

	key := "/testbucket/cache/v1aabb000000000001"
	content1 := []byte("original content")
	content2 := []byte("overwrite attempt")

	resp := doRequest(t, ts, "PUT", key, content1, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "PUT", key, content2, nil)
	require.Equal(t, 409, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "PUT", key, content1, nil)
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
		Listen:    ":0",
		Bucket:    "testbucket",
		DataDir:   dir,
		WriteOnce:   WriteOnceConfig{Action: "deny", Notification: "always"},
		Credentials: []Credential{{Username: ConfigString{Value: ""}, Password: ConfigString{Value: ""}}},
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
		Listen:    ":0",
		Bucket:    "testbucket",
		DataDir:   dir,
		WriteOnce:   WriteOnceConfig{Action: "deny", Notification: "never"},
		Credentials: []Credential{{Username: ConfigString{Value: ""}, Password: ConfigString{Value: ""}}},
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

	resp := doRequest(t, ts, "PUT", key, content, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "PUT", key, content, nil)
	require.Equal(t, 409, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "PUT", key, []byte("different"), nil)
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

	resp := doRequest(t, ts, "PUT", key, content1, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "PUT", key, content2, nil)
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
	defaultJSON := `{"bucket": "b", "data_dir": "/tmp/d", "credentials": [{"username": "", "password": ""}]}`
	defaultPath := dir + "/defaults.json"
	os.WriteFile(defaultPath, []byte(defaultJSON), 0644)
	cfg, err = LoadConfig(defaultPath)
	require.Nil(t, err)
	require.Equal(t, ":9000", cfg.Listen)

	// Missing file
	_, err = LoadConfig(dir + "/nonexistent.json")
	require.NotNil(t, err)

	// Invalid JSON
	badPath := dir + "/bad.json"
	os.WriteFile(badPath, []byte("{invalid"), 0644)
	_, err = LoadConfig(badPath)
	require.NotNil(t, err)

	cred := `"credentials": [{"username": "", "password": ""}]`

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

func TestPutObjectReadBodyError(t *testing.T) {
	ts := testSetup(t)

	content := []byte("test data with meta")
	headers := map[string]string{
		"X-Amz-Meta-Outputid": "out1",
		"X-Amz-Meta-Custom":   "val2",
	}
	resp := doRequest(t, ts, "PUT", "/testbucket/meta/v1test000000000001", content, headers)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "GET", "/testbucket/meta/v1test000000000001", nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "out1", resp.Header.Get("X-Amz-Meta-Outputid"))
	require.Equal(t, "val2", resp.Header.Get("X-Amz-Meta-Custom"))
	resp.Body.Close()
}

func TestMethodNotAllowed(t *testing.T) {
	ts := testSetup(t)

	resp := doRequest(t, ts, "DELETE", "/testbucket/some/key", nil, nil)
	defer resp.Body.Close()
	require.Equal(t, 405, resp.StatusCode)
}

func TestListEmptyBucket(t *testing.T) {
	ts := testSetup(t)

	resp := doRequest(t, ts, "GET", "/testbucket?list-type=2&prefix=nonexistent/", nil, nil)
	require.Equal(t, 200, resp.StatusCode)

	var result ListBucketResult
	xml.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()

	require.Equal(t, 0, len(result.Contents))
	require.False(t, result.IsTruncated)
}

func TestUnsafeKeyHashedStorage(t *testing.T) {
	ts := testSetup(t)

	traversalKey := "prefix/../../etc/passwd"
	content := []byte("safe content")
	resp := doRequest(t, ts, "PUT", "/testbucket/"+traversalKey, content, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "GET", "/testbucket/"+traversalKey, nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, string(content), string(body))

	dotKey := "a..b/file.txt"
	resp = doRequest(t, ts, "PUT", "/testbucket/"+dotKey, []byte("dot data"), nil)
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
	resp := doRequest(t, ts, "PUT", "/testbucket/"+key, content, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "GET", "/testbucket/"+key, nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, string(content), string(body))
}

func TestHashedKeyListing(t *testing.T) {
	ts := testSetup(t)

	unsafeKey := "special.key/with.dots"
	resp := doRequest(t, ts, "PUT", "/testbucket/"+unsafeKey, []byte("data"), nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doRequest(t, ts, "GET", "/testbucket?list-type=2&prefix=special.", nil, nil)
	require.Equal(t, 200, resp.StatusCode)

	var result ListBucketResult
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))
	resp.Body.Close()

	require.Equal(t, 1, len(result.Contents))
	require.Equal(t, unsafeKey, result.Contents[0].Key)
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
