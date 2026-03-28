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
)

func testSetup(t *testing.T) (*httptest.Server, *Config) {
	t.Helper()
	dir := t.TempDir()

	cfg := &Config{
		Listen: ":0",
		Bucket: "testbucket",
		Region: "us-east-1",
		DataDir: dir,
		WriteOnce: false,
		Credentials: []Credential{
			{AccessKey: "AKIAIOSFODNN7EXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"},
		},
	}

	storage, err := NewStorage(cfg.DataDir, cfg.WriteOnce)
	if err != nil {
		t.Fatal(err)
	}
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
		WriteOnce: true,
		Credentials: []Credential{
			{AccessKey: "AKIAIOSFODNN7EXAMPLE", SecretKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"},
		},
	}

	storage, err := NewStorage(cfg.DataDir, cfg.WriteOnce)
	if err != nil {
		t.Fatal(err)
	}
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
	if err != nil {
		t.Fatal(err)
	}
	if body != nil {
		req.ContentLength = int64(len(body))
	}
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}
	signTestRequest(req, cfg.Credentials[0].AccessKey, cfg.Credentials[0].SecretKey, cfg.Region, body)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestPutAndGetObject(t *testing.T) {
	ts, cfg := testSetup(t)

	content := []byte("hello world cache data")
	headers := map[string]string{"X-Amz-Meta-Outputid": "abc123def456"}
	resp := doSignedRequest(t, ts, cfg, "PUT", "/testbucket/go-buildcache/v1aabbccdd11223344", content, headers)
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("PUT: expected 200, got %d: %s", resp.StatusCode, body)
	}
	resp.Body.Close()

	resp = doSignedRequest(t, ts, cfg, "GET", "/testbucket/go-buildcache/v1aabbccdd11223344", nil, nil)
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET: expected 200, got %d: %s", resp.StatusCode, body)
	}

	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if string(body) != string(content) {
		t.Fatalf("GET body mismatch: got %q, want %q", body, content)
	}

	outputID := resp.Header.Get("X-Amz-Meta-Outputid")
	if outputID != "abc123def456" {
		t.Fatalf("X-Amz-Meta-Outputid: got %q, want %q", outputID, "abc123def456")
	}

	lm := resp.Header.Get("Last-Modified")
	if lm == "" {
		t.Fatal("missing Last-Modified header")
	}
	if _, err := time.Parse(http.TimeFormat, lm); err != nil {
		t.Fatalf("Last-Modified parse error: %v", err)
	}
}

func TestGetObjectNotFound(t *testing.T) {
	ts, cfg := testSetup(t)

	resp := doSignedRequest(t, ts, cfg, "GET", "/testbucket/nonexistent/key", nil, nil)
	defer resp.Body.Close()

	if resp.StatusCode != 404 {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}

	var s3err S3Error
	if err := xml.NewDecoder(resp.Body).Decode(&s3err); err != nil {
		t.Fatalf("decode error response: %v", err)
	}
	if s3err.Code != "NoSuchKey" {
		t.Fatalf("expected NoSuchKey, got %s", s3err.Code)
	}
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
		if resp.StatusCode != 200 {
			t.Fatalf("PUT %s: expected 200, got %d", key, resp.StatusCode)
		}
		resp.Body.Close()
	}

	// List all with prefix
	resp := doSignedRequest(t, ts, cfg, "GET", "/testbucket?list-type=2&prefix=go-buildcache/", nil, nil)
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("LIST: expected 200, got %d: %s", resp.StatusCode, body)
	}

	var result ListBucketResult
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	resp.Body.Close()

	if len(result.Contents) != 3 {
		t.Fatalf("expected 3 objects, got %d", len(result.Contents))
	}

	// Verify sorted order
	for i := 1; i < len(result.Contents); i++ {
		if result.Contents[i].Key < result.Contents[i-1].Key {
			t.Fatalf("keys not sorted: %s < %s", result.Contents[i].Key, result.Contents[i-1].Key)
		}
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

	if len(page1.Contents) != 2 {
		t.Fatalf("page1: expected 2 objects, got %d", len(page1.Contents))
	}
	if !page1.IsTruncated {
		t.Fatal("page1: expected IsTruncated=true")
	}
	if page1.NextContinuationToken == "" {
		t.Fatal("page1: missing NextContinuationToken")
	}

	// Page 2
	resp = doSignedRequest(t, ts, cfg, "GET",
		"/testbucket?list-type=2&prefix=prefix/&max-keys=2&continuation-token="+page1.NextContinuationToken,
		nil, nil)
	var page2 ListBucketResult
	xml.NewDecoder(resp.Body).Decode(&page2)
	resp.Body.Close()

	if len(page2.Contents) != 2 {
		t.Fatalf("page2: expected 2 objects, got %d", len(page2.Contents))
	}
	if !page2.IsTruncated {
		t.Fatal("page2: expected IsTruncated=true")
	}

	// Page 3 (last)
	resp = doSignedRequest(t, ts, cfg, "GET",
		"/testbucket?list-type=2&prefix=prefix/&max-keys=2&continuation-token="+page2.NextContinuationToken,
		nil, nil)
	var page3 ListBucketResult
	xml.NewDecoder(resp.Body).Decode(&page3)
	resp.Body.Close()

	if len(page3.Contents) != 1 {
		t.Fatalf("page3: expected 1 object, got %d", len(page3.Contents))
	}
	if page3.IsTruncated {
		t.Fatal("page3: expected IsTruncated=false")
	}
}

func TestAuthFailure(t *testing.T) {
	ts, cfg := testSetup(t)

	req, _ := http.NewRequest("GET", ts.URL+"/testbucket/some/key", nil)
	// Sign with wrong secret
	signTestRequest(req, cfg.Credentials[0].AccessKey, "wrongsecretkey123456", cfg.Region, nil)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 403 {
		t.Fatalf("expected 403, got %d", resp.StatusCode)
	}
}

func TestWriteOnce(t *testing.T) {
	ts, cfg := testSetupWriteOnce(t)

	key := "/testbucket/cache/v1aabb000000000001"
	content1 := []byte("original content")
	content2 := []byte("overwrite attempt")

	// First PUT
	resp := doSignedRequest(t, ts, cfg, "PUT", key, content1, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("first PUT: expected 200, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// Second PUT with different data
	resp = doSignedRequest(t, ts, cfg, "PUT", key, content2, nil)
	if resp.StatusCode != 200 {
		t.Fatalf("second PUT: expected 200, got %d", resp.StatusCode)
	}
	resp.Body.Close()

	// GET should return original content
	resp = doSignedRequest(t, ts, cfg, "GET", key, nil, nil)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()

	if string(body) != string(content1) {
		t.Fatalf("write-once violated: got %q, want %q", body, content1)
	}
}

func TestNoSuchBucket(t *testing.T) {
	ts, cfg := testSetup(t)

	resp := doSignedRequest(t, ts, cfg, "GET", "/wrongbucket/key", nil, nil)
	defer resp.Body.Close()

	if resp.StatusCode != 404 {
		t.Fatalf("expected 404, got %d", resp.StatusCode)
	}
}

func TestShardingRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, err := NewStorage(dir, false)
	if err != nil {
		t.Fatal(err)
	}
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
		if reconstructed != key {
			t.Errorf("sharding round-trip failed: %q -> %q -> %q", key, path, reconstructed)
		}
	}
}

func TestLockExclusion(t *testing.T) {
	dir := t.TempDir()
	s1, err := NewStorage(dir, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()

	_, err = NewStorage(dir, false)
	if err == nil {
		t.Fatal("expected error from second NewStorage on same dir")
	}
}

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}
