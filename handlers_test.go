package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
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

func testSetup(t *testing.T) (*httptest.Server, *Config) {
	t.Helper()
	dir := t.TempDir()

	cfg := &Config{
		Listen:    ":0",
		Bucket:    "testbucket",
		Region:    "us-east-1",
		DataDir:   dir,
		WriteOnce: WriteOnceConfig{Action: "allow"},
		Credentials: []Credential{
			{AccessKey: "AKIAIOSFODNN7EXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"},
		},
	}

	storage, err := NewStorage(cfg.DataDir, cfg.WriteOnce)
	require.Nil(t, err)

	t.Cleanup(func() { storage.Close() })

	srv := NewServer(cfg, storage)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	return ts, cfg
}

func testSetupWriteOnce(t *testing.T) (*httptest.Server, *Config) {
	t.Helper()
	dir := t.TempDir()

	cfg := &Config{
		Listen:    ":0",
		Bucket:    "testbucket",
		Region:    "us-east-1",
		DataDir:   dir,
		WriteOnce: WriteOnceConfig{Action: "deny", Notification: "content_differs"},
		Credentials: []Credential{
			{AccessKey: "AKIAIOSFODNN7EXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"},
		},
	}

	storage, err := NewStorage(cfg.DataDir, cfg.WriteOnce)
	require.Nil(t, err)

	t.Cleanup(func() { storage.Close() })

	srv := NewServer(cfg, storage)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	return ts, cfg
}

// signTestRequest signs a request for testing using the same SigV4 logic.
func signTestRequest(r *http.Request, accessKey, secretKey, region string, body []byte) {
	now := time.Now().UTC()
	datestamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")

	r.Header.Set("X-Amz-Date", amzDate)
	r.Header.Set("Host", r.URL.Host)

	var payloadHash string
	if body != nil {
		h := sha256.Sum256(body)
		payloadHash = hex.EncodeToString(h[:])
	} else {
		payloadHash = "UNSIGNED-PAYLOAD"
	}
	r.Header.Set("X-Amz-Content-Sha256", payloadHash)

	signedHeaders, canonicalHeaders := testBuildCanonicalHeaders(r)
	canonicalRequest := r.Method + "\n" +
		uriEncodePath(r.URL.Path) + "\n" +
		canonicalQueryString(r.URL.RawQuery) + "\n" +
		canonicalHeaders + "\n" +
		signedHeaders + "\n" +
		payloadHash

	scope := datestamp + "/" + region + "/s3/aws4_request"
	canonHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := "AWS4-HMAC-SHA256\n" + amzDate + "\n" + scope + "\n" + hex.EncodeToString(canonHash[:])

	signingKey := deriveSigningKey(secretKey, datestamp, region, "s3")
	sig := hmacSHA256(signingKey, []byte(stringToSign))
	signature := hex.EncodeToString(sig)

	auth := "AWS4-HMAC-SHA256 Credential=" + accessKey + "/" + scope +
		", SignedHeaders=" + signedHeaders + ", Signature=" + signature
	r.Header.Set("Authorization", auth)
}

func testBuildCanonicalHeaders(r *http.Request) (signedHeaders, canonicalHeaders string) {
	type hdr struct{ k, v string }
	var hdrs []hdr
	hdrs = append(hdrs, hdr{"host", r.URL.Host})
	for k, vals := range r.Header {
		lk := toLower(k)
		if len(lk) > 6 && lk[:6] == "x-amz-" {
			hdrs = append(hdrs, hdr{lk, trimSpace(vals[0])})
		}
	}
	// Sort
	for i := 0; i < len(hdrs); i++ {
		for j := i + 1; j < len(hdrs); j++ {
			if hdrs[j].k < hdrs[i].k {
				hdrs[i], hdrs[j] = hdrs[j], hdrs[i]
			}
		}
	}
	var names, canonical []string
	for _, h := range hdrs {
		names = append(names, h.k)
		canonical = append(canonical, h.k+":"+h.v)
	}
	signedHeaders = join(names, ";")
	canonicalHeaders = join(canonical, "\n") + "\n"
	return
}

func toLower(s string) string {
	b := make([]byte, len(s))
	for i := range s {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return string(b)
}

func trimSpace(s string) string {
	i := 0
	for i < len(s) && (s[i] == ' ' || s[i] == '\t') {
		i++
	}
	j := len(s)
	for j > i && (s[j-1] == ' ' || s[j-1] == '\t') {
		j--
	}
	return s[i:j]
}

func join(parts []string, sep string) string {
	if len(parts) == 0 {
		return ""
	}
	result := parts[0]
	for _, p := range parts[1:] {
		result += sep + p
	}
	return result
}

func doSignedRequest(t *testing.T, ts *httptest.Server, cfg *Config, method, path string, body []byte, extraHeaders map[string]string) *http.Response {
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
	signTestRequest(req, cfg.Credentials[0].AccessKey, cfg.Credentials[0].SecretKey, cfg.Region, body)

	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)

	return resp
}

func TestPutAndGetObject(t *testing.T) {
	ts, cfg := testSetup(t)

	content := []byte("hello world cache data")
	headers := map[string]string{"X-Amz-Meta-Outputid": "abc123def456"}
	resp := doSignedRequest(t, ts, cfg, "PUT", "/testbucket/go-buildcache/v1aabbccdd11223344", content, headers)
	require.Equal(t, 200, resp.StatusCode)

	resp.Body.Close()

	resp = doSignedRequest(t, ts, cfg, "GET", "/testbucket/go-buildcache/v1aabbccdd11223344", nil, nil)
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
	ts, cfg := testSetup(t)

	resp := doSignedRequest(t, ts, cfg, "GET", "/testbucket/nonexistent/key", nil, nil)
	defer resp.Body.Close()

	require.Equal(t, 404, resp.StatusCode)

	var s3err S3Error
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&s3err))

	require.Equal(t, "NoSuchKey", s3err.Code)

}

func TestListObjectsV2(t *testing.T) {
	ts, cfg := testSetup(t)

	// Put several objects
	keys := []string{
		"go-buildcache/v1aaaa000000000001",
		"go-buildcache/v1bbbb000000000002",
		"go-buildcache/v1cccc000000000003",
	}
	for _, key := range keys {
		resp := doSignedRequest(t, ts, cfg, "PUT", "/testbucket/"+key, []byte("data-"+key), nil)
		require.Equal(t, 200, resp.StatusCode)

		resp.Body.Close()
	}

	// List all with prefix
	resp := doSignedRequest(t, ts, cfg, "GET", "/testbucket?list-type=2&prefix=go-buildcache/", nil, nil)
	require.Equal(t, 200, resp.StatusCode)

	var result ListBucketResult
	require.NoError(t, xml.NewDecoder(resp.Body).Decode(&result))

	resp.Body.Close()

	require.Equal(t, 3, len(result.Contents))

	// Verify sorted order
	for i := 1; i < len(result.Contents); i++ {
		require.GreaterOrEqual(t, result.Contents[i].Key, result.Contents[i-1].Key)

	}
}

func TestListObjectsV2Pagination(t *testing.T) {
	ts, cfg := testSetup(t)

	keys := []string{
		"prefix/v1aaaa000000000001",
		"prefix/v1bbbb000000000002",
		"prefix/v1cccc000000000003",
		"prefix/v1dddd000000000004",
		"prefix/v1eeee000000000005",
	}
	for _, key := range keys {
		resp := doSignedRequest(t, ts, cfg, "PUT", "/testbucket/"+key, []byte("d"), nil)
		resp.Body.Close()
	}

	// Page 1: max-keys=2
	resp := doSignedRequest(t, ts, cfg, "GET", "/testbucket?list-type=2&prefix=prefix/&max-keys=2", nil, nil)
	var page1 ListBucketResult
	xml.NewDecoder(resp.Body).Decode(&page1)
	resp.Body.Close()

	require.Equal(t, 2, len(page1.Contents))

	require.True(t, page1.IsTruncated)

	require.NotEqual(t, "", page1.NextContinuationToken)

	// Page 2
	resp = doSignedRequest(t, ts, cfg, "GET",
		"/testbucket?list-type=2&prefix=prefix/&max-keys=2&continuation-token="+page1.NextContinuationToken,
		nil, nil)
	var page2 ListBucketResult
	xml.NewDecoder(resp.Body).Decode(&page2)
	resp.Body.Close()

	require.Equal(t, 2, len(page2.Contents))

	require.True(t, page2.IsTruncated)

	// Page 3 (last)
	resp = doSignedRequest(t, ts, cfg, "GET",
		"/testbucket?list-type=2&prefix=prefix/&max-keys=2&continuation-token="+page2.NextContinuationToken,
		nil, nil)
	var page3 ListBucketResult
	xml.NewDecoder(resp.Body).Decode(&page3)
	resp.Body.Close()

	require.Equal(t, 1, len(page3.Contents))

	require.False(t, page3.IsTruncated)

}

func TestAuthFailure(t *testing.T) {
	ts, cfg := testSetup(t)

	req, _ := http.NewRequest("GET", ts.URL+"/testbucket/some/key", nil)
	// Sign with wrong secret
	signTestRequest(req, cfg.Credentials[0].AccessKey, "wrongsecretkey123456", cfg.Region, nil)

	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)

	defer resp.Body.Close()

	require.Equal(t, 403, resp.StatusCode)

}

func TestWriteOnce(t *testing.T) {
	ts, cfg := testSetupWriteOnce(t)

	key := "/testbucket/cache/v1aabb000000000001"
	content1 := []byte("original content")
	content2 := []byte("overwrite attempt")

	// First PUT
	resp := doSignedRequest(t, ts, cfg, "PUT", key, content1, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// Second PUT with different data should return 409
	resp = doSignedRequest(t, ts, cfg, "PUT", key, content2, nil)
	require.Equal(t, 409, resp.StatusCode)
	resp.Body.Close()

	// Third PUT with same data should return 200 (idempotent)
	resp = doSignedRequest(t, ts, cfg, "PUT", key, content1, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// GET should return original content
	resp = doSignedRequest(t, ts, cfg, "GET", key, nil, nil)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, string(content1), string(body))
}

func testSetupWriteOnceAlways(t *testing.T) (*httptest.Server, *Config) {
	t.Helper()
	dir := t.TempDir()

	cfg := &Config{
		Listen:    ":0",
		Bucket:    "testbucket",
		Region:    "us-east-1",
		DataDir:   dir,
		WriteOnce: WriteOnceConfig{Action: "deny", Notification: "always"},
		Credentials: []Credential{
			{AccessKey: "AKIAIOSFODNN7EXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"},
		},
	}

	storage, err := NewStorage(cfg.DataDir, cfg.WriteOnce)
	require.Nil(t, err)
	t.Cleanup(func() { storage.Close() })

	srv := NewServer(cfg, storage)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	return ts, cfg
}

func testSetupWriteOnceNever(t *testing.T) (*httptest.Server, *Config) {
	t.Helper()
	dir := t.TempDir()

	cfg := &Config{
		Listen:    ":0",
		Bucket:    "testbucket",
		Region:    "us-east-1",
		DataDir:   dir,
		WriteOnce: WriteOnceConfig{Action: "deny", Notification: "never"},
		Credentials: []Credential{
			{AccessKey: "AKIAIOSFODNN7EXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"},
		},
	}

	storage, err := NewStorage(cfg.DataDir, cfg.WriteOnce)
	require.Nil(t, err)
	t.Cleanup(func() { storage.Close() })

	srv := NewServer(cfg, storage)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	return ts, cfg
}

func TestWriteOnceNotificationAlways(t *testing.T) {
	ts, cfg := testSetupWriteOnceAlways(t)

	key := "/testbucket/cache/v1aabb000000000002"
	content := []byte("some content")

	// First PUT succeeds
	resp := doSignedRequest(t, ts, cfg, "PUT", key, content, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// Second PUT with same content returns 409 (always notifies)
	resp = doSignedRequest(t, ts, cfg, "PUT", key, content, nil)
	require.Equal(t, 409, resp.StatusCode)
	resp.Body.Close()

	// Third PUT with different content also returns 409
	resp = doSignedRequest(t, ts, cfg, "PUT", key, []byte("different"), nil)
	require.Equal(t, 409, resp.StatusCode)
	resp.Body.Close()

	// GET returns original
	resp = doSignedRequest(t, ts, cfg, "GET", key, nil, nil)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, string(content), string(body))
}

func TestWriteOnceNotificationNever(t *testing.T) {
	ts, cfg := testSetupWriteOnceNever(t)

	key := "/testbucket/cache/v1aabb000000000003"
	content1 := []byte("original")
	content2 := []byte("different")

	// First PUT succeeds
	resp := doSignedRequest(t, ts, cfg, "PUT", key, content1, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// Second PUT with different data returns 200 (silent skip)
	resp = doSignedRequest(t, ts, cfg, "PUT", key, content2, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// GET returns original content (overwrite was denied silently)
	resp = doSignedRequest(t, ts, cfg, "GET", key, nil, nil)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, string(content1), string(body))
}

func TestNoSuchBucket(t *testing.T) {
	ts, cfg := testSetup(t)

	resp := doSignedRequest(t, ts, cfg, "GET", "/wrongbucket/key", nil, nil)
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
		"region": "eu-west-1",
		"data_dir": "/tmp/test",
		"write_once": {"action": "deny", "notification": "content_differs"},
		"credentials": [{"access_key": "AK", "secret_key": "SK"}]
	}`
	validPath := dir + "/valid.json"
	os.WriteFile(validPath, []byte(validJSON), 0644)
	cfg, err := LoadConfig(validPath)
	require.Nil(t, err)
	require.Equal(t, ":8080", cfg.Listen)
	require.Equal(t, "mybucket", cfg.Bucket)
	require.Equal(t, "eu-west-1", cfg.Region)
	require.Equal(t, "deny", cfg.WriteOnce.Action)
	require.Equal(t, "content_differs", cfg.WriteOnce.Notification)

	// Defaults
	defaultJSON := `{"bucket": "b", "data_dir": "/tmp/d", "credentials": [{"access_key": "AK", "secret_key": "SK"}]}`
	defaultPath := dir + "/defaults.json"
	os.WriteFile(defaultPath, []byte(defaultJSON), 0644)
	cfg, err = LoadConfig(defaultPath)
	require.Nil(t, err)
	require.Equal(t, ":9000", cfg.Listen)
	require.Equal(t, "us-east-1", cfg.Region)

	// Missing file
	_, err = LoadConfig(dir + "/nonexistent.json")
	require.NotNil(t, err)

	// Invalid JSON
	badPath := dir + "/bad.json"
	os.WriteFile(badPath, []byte("{invalid"), 0644)
	_, err = LoadConfig(badPath)
	require.NotNil(t, err)

	// Missing bucket
	noBucketPath := dir + "/nobucket.json"
	os.WriteFile(noBucketPath, []byte(`{"data_dir": "/tmp", "credentials": [{"access_key": "AK", "secret_key": "SK"}]}`), 0644)
	_, err = LoadConfig(noBucketPath)
	require.NotNil(t, err)

	// Missing data_dir
	noDirPath := dir + "/nodir.json"
	os.WriteFile(noDirPath, []byte(`{"bucket": "b", "credentials": [{"access_key": "AK", "secret_key": "SK"}]}`), 0644)
	_, err = LoadConfig(noDirPath)
	require.NotNil(t, err)

	// No credentials
	noCredPath := dir + "/nocred.json"
	os.WriteFile(noCredPath, []byte(`{"bucket": "b", "data_dir": "/tmp"}`), 0644)
	_, err = LoadConfig(noCredPath)
	require.NotNil(t, err)

	// Empty credential fields
	emptyCredPath := dir + "/emptycred.json"
	os.WriteFile(emptyCredPath, []byte(`{"bucket": "b", "data_dir": "/tmp", "credentials": [{"access_key": "", "secret_key": "SK"}]}`), 0644)
	_, err = LoadConfig(emptyCredPath)
	require.NotNil(t, err)

	// write_once defaults
	woDefaultPath := dir + "/wo_default.json"
	os.WriteFile(woDefaultPath, []byte(`{"bucket": "b", "data_dir": "/tmp", "credentials": [{"access_key": "AK", "secret_key": "SK"}]}`), 0644)
	cfg, err = LoadConfig(woDefaultPath)
	require.Nil(t, err)
	require.Equal(t, "allow", cfg.WriteOnce.Action)
	require.Equal(t, "never", cfg.WriteOnce.Notification)

	// Invalid write_once.action
	woInvalidAction := dir + "/wo_bad_action.json"
	os.WriteFile(woInvalidAction, []byte(`{"bucket": "b", "data_dir": "/tmp", "write_once": {"action": "invalid"}, "credentials": [{"access_key": "AK", "secret_key": "SK"}]}`), 0644)
	_, err = LoadConfig(woInvalidAction)
	require.NotNil(t, err)

	// Invalid write_once.notification
	woInvalidNotif := dir + "/wo_bad_notif.json"
	os.WriteFile(woInvalidNotif, []byte(`{"bucket": "b", "data_dir": "/tmp", "write_once": {"action": "deny", "notification": "invalid"}, "credentials": [{"access_key": "AK", "secret_key": "SK"}]}`), 0644)
	_, err = LoadConfig(woInvalidNotif)
	require.NotNil(t, err)
}

func TestFindCredential(t *testing.T) {
	cfg := &Config{
		Credentials: []Credential{
			{AccessKey: "AK1", SecretKey: "SK1"},
			{AccessKey: "AK2", SecretKey: "SK2"},
		},
	}
	require.NotNil(t, cfg.FindCredential("AK1"))
	require.NotNil(t, cfg.FindCredential("AK2"))
	require.Nil(t, cfg.FindCredential("AK3"))
}

func TestPutObjectReadBodyError(t *testing.T) {
	ts, cfg := testSetup(t)

	// Test PUT with metadata
	content := []byte("test data with meta")
	headers := map[string]string{
		"X-Amz-Meta-Outputid": "out1",
		"X-Amz-Meta-Custom":   "val2",
	}
	resp := doSignedRequest(t, ts, cfg, "PUT", "/testbucket/meta/v1test000000000001", content, headers)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// GET and verify both metadata keys
	resp = doSignedRequest(t, ts, cfg, "GET", "/testbucket/meta/v1test000000000001", nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "out1", resp.Header.Get("X-Amz-Meta-Outputid"))
	require.Equal(t, "val2", resp.Header.Get("X-Amz-Meta-Custom"))
	resp.Body.Close()
}

func TestMethodNotAllowed(t *testing.T) {
	ts, cfg := testSetup(t)

	resp := doSignedRequest(t, ts, cfg, "DELETE", "/testbucket/some/key", nil, nil)
	defer resp.Body.Close()
	require.Equal(t, 405, resp.StatusCode)
}

func TestListEmptyBucket(t *testing.T) {
	ts, cfg := testSetup(t)

	resp := doSignedRequest(t, ts, cfg, "GET", "/testbucket?list-type=2&prefix=nonexistent/", nil, nil)
	require.Equal(t, 200, resp.StatusCode)

	var result ListBucketResult
	xml.NewDecoder(resp.Body).Decode(&result)
	resp.Body.Close()

	require.Equal(t, 0, len(result.Contents))
	require.False(t, result.IsTruncated)
}

func TestUnsafeKeyHashedStorage(t *testing.T) {
	ts, cfg := testSetup(t)

	// Path traversal key gets hashed — stored safely, not at /etc/passwd
	traversalKey := "prefix/../../etc/passwd"
	content := []byte("safe content")
	resp := doSignedRequest(t, ts, cfg, "PUT", "/testbucket/"+traversalKey, content, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// GET with the same key retrieves the data
	resp = doSignedRequest(t, ts, cfg, "GET", "/testbucket/"+traversalKey, nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, string(content), string(body))

	// Key with dots (not alphanumeric) gets hashed too
	dotKey := "a..b/file.txt"
	resp = doSignedRequest(t, ts, cfg, "PUT", "/testbucket/"+dotKey, []byte("dot data"), nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doSignedRequest(t, ts, cfg, "GET", "/testbucket/"+dotKey, nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, "dot data", string(body))
}

func TestSafeKeyUnchangedBehavior(t *testing.T) {
	ts, cfg := testSetup(t)

	// Normal alphanumeric key works as before
	key := "go-buildcache/v1aabb000000000099"
	content := []byte("cache data")
	resp := doSignedRequest(t, ts, cfg, "PUT", "/testbucket/"+key, content, nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doSignedRequest(t, ts, cfg, "GET", "/testbucket/"+key, nil, nil)
	require.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, string(content), string(body))
}

func TestHashedKeyListing(t *testing.T) {
	ts, cfg := testSetup(t)

	// Put a hashed key
	unsafeKey := "special.key/with.dots"
	resp := doSignedRequest(t, ts, cfg, "PUT", "/testbucket/"+unsafeKey, []byte("data"), nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// List should return the original key
	resp = doSignedRequest(t, ts, cfg, "GET", "/testbucket?list-type=2&prefix=special.", nil, nil)
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
