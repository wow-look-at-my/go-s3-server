package main

import (
	"bytes"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// skipIfNoXattr skips the test when dir's filesystem cannot persist the
// server's audit xattrs. This is a local-environment escape hatch: CI runs
// on ext4 where xattrs work, but developer VMs may use a filesystem
// (v9fs, some tmpfs configurations) that doesn't. The production code
// treats audit as best-effort, so losing xattrs is a degradation rather
// than a failure; we simply skip the assertions that can't be verified.
func skipIfNoXattr(t *testing.T, dir string) {
	t.Helper()
	canary, err := os.CreateTemp(dir, "xattr-probe-*")
	require.Nil(t, err)

	canary.Close()
	defer os.Remove(canary.Name())
	if err := setAudit(canary.Name(), map[string]string{"probe": "1"}); err != nil {
		t.Skipf("skipping: filesystem does not support user xattr: %v", err)
	}
}

func testSetupWithAuth(t *testing.T) (*httptest.Server, *Config) {
	t.Helper()
	dir := t.TempDir()

	cfg := &Config{
		Listen:    ":0",
		Bucket:    "testbucket",
		DataDir:   dir,
		WriteOnce: WriteOnceConfig{Action: "allow"},
		Credentials: []Credential{
			{Username: ConfigString{Value: "alice"}, Password: ConfigString{Value: "password1"}},
			{Username: ConfigString{Value: "bob"}, Password: ConfigString{Value: "password2"}},
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

func doAuthRequest(t *testing.T, ts *httptest.Server, user, pass, method, path string, body []byte) *http.Response {
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
	req.SetBasicAuth(user, pass)
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	return resp
}

func TestBasicAuth(t *testing.T) {
	ts, _ := testSetupWithAuth(t)

	content := []byte("auth test data")

	// alice can PUT + GET
	resp := doAuthRequest(t, ts, "alice", "password1", "PUT", "/testbucket/auth/v1aabb000000000001", content)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	resp = doAuthRequest(t, ts, "alice", "password1", "GET", "/testbucket/auth/v1aabb000000000001", nil)
	require.Equal(t, 200, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, string(content), string(body))

	// bob can also GET
	resp = doAuthRequest(t, ts, "bob", "password2", "GET", "/testbucket/auth/v1aabb000000000001", nil)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()
}

func TestBasicAuthWrongPassword(t *testing.T) {
	ts, _ := testSetupWithAuth(t)

	resp := doAuthRequest(t, ts, "alice", "wrongpassword", "GET", "/testbucket/some/key", nil)
	defer resp.Body.Close()
	require.Equal(t, 403, resp.StatusCode)
}

func TestBasicAuthUnknownUser(t *testing.T) {
	ts, _ := testSetupWithAuth(t)

	resp := doAuthRequest(t, ts, "unknown", "password1", "GET", "/testbucket/some/key", nil)
	defer resp.Body.Close()
	require.Equal(t, 403, resp.StatusCode)
}

func TestBasicAuthNoHeader(t *testing.T) {
	ts, _ := testSetupWithAuth(t)

	req, _ := http.NewRequest("GET", ts.URL+"/testbucket/some/key", nil)
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	defer resp.Body.Close()
	require.Equal(t, 403, resp.StatusCode)
}

func TestBasicAuthMalformed(t *testing.T) {
	ts, _ := testSetupWithAuth(t)

	// Invalid base64
	req, _ := http.NewRequest("GET", ts.URL+"/testbucket/some/key", nil)
	req.Header.Set("Authorization", "Basic not-valid-base64!!!")
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 403, resp.StatusCode)
	resp.Body.Close()

	// Valid base64 but no colon
	req, _ = http.NewRequest("GET", ts.URL+"/testbucket/some/key", nil)
	req.Header.Set("Authorization", "Basic "+base64.StdEncoding.EncodeToString([]byte("nocolon")))
	resp, err = http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 403, resp.StatusCode)
	resp.Body.Close()
}

// TestAuditXattrsOnUpload verifies that when a user PUTs an object, the
// server persists enough metadata on disk to later reconstruct who uploaded
// it, when, from where, and with what client. This is the forensic trail
// that was missing while the auth-bypass bug was exploitable.
func TestAuditXattrsOnUpload(t *testing.T) {
	dir := t.TempDir()
	skipIfNoXattr(t, dir)

	cfg := &Config{
		Listen:    ":0",
		Bucket:    "testbucket",
		DataDir:   dir,
		WriteOnce: WriteOnceConfig{Action: "allow"},
		Credentials: []Credential{
			{Username: ConfigString{Value: "alice"}, Password: ConfigString{Value: "password1"}},
		},
	}

	storage, err := NewStorage(cfg.DataDir, cfg.WriteOnce)
	require.Nil(t, err)
	t.Cleanup(func() { storage.Close() })

	srv := NewServer(cfg, storage)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	key := "go-buildcache/v1aabb000000000042"
	content := []byte("audit me")

	req, _ := http.NewRequest("PUT", ts.URL+"/testbucket/"+key, bytes.NewReader(content))
	req.ContentLength = int64(len(content))
	req.SetBasicAuth("alice", "password1")
	req.Header.Set("User-Agent", "go-toolchain-test/1.2.3")
	req.Header.Set("CF-Connecting-IP", "203.0.113.42")
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	path := storage.keyToPath(key)
	audit := getAudit(path)
	require.NotNil(t, audit, "audit map must be populated for uploads")
	require.Equal(t, "alice", audit["uploader"])
	require.Equal(t, "203.0.113.42", audit["client_ip"])
	require.Equal(t, "go-toolchain-test/1.2.3", audit["user_agent"])
	require.Equal(t, strconv.Itoa(len(content)), audit["content_length"])
	// uploaded_at is an RFC3339 timestamp; just require it parses.
	_, tsErr := time.Parse(time.RFC3339Nano, audit["uploaded_at"])
	require.NoError(t, tsErr)
}

func TestEnvVarCredentials(t *testing.T) {
	t.Setenv("TEST_S3_USER", "envuser")
	t.Setenv("TEST_S3_PASS", "envpass")

	dir := t.TempDir()
	cfgJSON := `{
		"bucket": "testbucket",
		"data_dir": "` + dir + `",
		"credentials": [{"username": {"type": "envvar", "name": "TEST_S3_USER"}, "password": {"type": "envvar", "name": "TEST_S3_PASS"}}]
	}`
	cfgPath := dir + "/config.json"
	os.WriteFile(cfgPath, []byte(cfgJSON), 0644)

	cfg, err := LoadConfig(cfgPath)
	require.Nil(t, err)
	require.Equal(t, "envuser", cfg.Credentials[0].Username.Value)
	require.Equal(t, "envpass", cfg.Credentials[0].Password.Value)

	storage, err := NewStorage(cfg.DataDir, cfg.WriteOnce)
	require.Nil(t, err)
	t.Cleanup(func() { storage.Close() })

	srv := NewServer(cfg, storage)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	// Valid creds work
	resp := doAuthRequest(t, ts, "envuser", "envpass", "PUT", "/testbucket/env/v1test000000000001", []byte("data"))
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()

	// Wrong creds fail
	resp = doAuthRequest(t, ts, "envuser", "wrong", "GET", "/testbucket/env/v1test000000000001", nil)
	require.Equal(t, 403, resp.StatusCode)
	resp.Body.Close()
}

func TestDisableAuth(t *testing.T) {
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

	// No auth header required
	req, _ := http.NewRequest("PUT", ts.URL+"/testbucket/noauth/v1test000000000001", bytes.NewReader([]byte("data")))
	req.ContentLength = 4
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	require.Equal(t, 200, resp.StatusCode)
	resp.Body.Close()
}

// TestAuthNotBypassedByEmptyCredential is a regression for the auth-bypass
// bug where authenticate() short-circuited to success on the first credential
// entry with an empty username and password. Even if someone bypasses
// LoadConfig and constructs a Config directly with an empty credential,
// authenticate() MUST still require valid Basic Auth when DisableAuth is
// false.
func TestAuthNotBypassedByEmptyCredential(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		Listen:    ":0",
		Bucket:    "testbucket",
		DataDir:   dir,
		WriteOnce: WriteOnceConfig{Action: "allow"},
		Credentials: []Credential{
			{Username: ConfigString{Value: "alice"}, Password: ConfigString{Value: "password1"}},
			{Username: ConfigString{Value: ""}, Password: ConfigString{Value: ""}},
		},
	}

	storage, err := NewStorage(cfg.DataDir, cfg.WriteOnce)
	require.Nil(t, err)
	t.Cleanup(func() { storage.Close() })

	srv := NewServer(cfg, storage)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)

	// No Authorization header — must be rejected.
	req, _ := http.NewRequest("GET", ts.URL+"/testbucket/some/key", nil)
	resp, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	defer resp.Body.Close()
	require.Equal(t, 403, resp.StatusCode)

	// Empty username/password — must be rejected.
	req, _ = http.NewRequest("GET", ts.URL+"/testbucket/some/key", nil)
	req.SetBasicAuth("", "")
	resp2, err := http.DefaultClient.Do(req)
	require.Nil(t, err)
	defer resp2.Body.Close()
	require.Equal(t, 403, resp2.StatusCode)
}

func TestLoadConfigRejectsEmptyCredential(t *testing.T) {
	dir := t.TempDir()
	cfgJSON := `{
		"bucket": "b",
		"data_dir": "` + dir + `",
		"credentials": [{"username": "", "password": ""}]
	}`
	path := dir + "/cfg.json"
	os.WriteFile(path, []byte(cfgJSON), 0644)
	_, err := LoadConfig(path)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "must both be non-empty")
}

func TestLoadConfigRejectsEmptyCredentialAmongRealOnes(t *testing.T) {
	dir := t.TempDir()
	cfgJSON := `{
		"bucket": "b",
		"data_dir": "` + dir + `",
		"credentials": [
			{"username": "alice", "password": "password1"},
			{"username": "", "password": ""}
		]
	}`
	path := dir + "/cfg.json"
	os.WriteFile(path, []byte(cfgJSON), 0644)
	_, err := LoadConfig(path)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "must both be non-empty")
}

func TestLoadConfigRejectsDisableAuthWithCredentials(t *testing.T) {
	dir := t.TempDir()
	cfgJSON := `{
		"bucket": "b",
		"data_dir": "` + dir + `",
		"disable_auth": true,
		"credentials": [{"username": "alice", "password": "password1"}]
	}`
	path := dir + "/cfg.json"
	os.WriteFile(path, []byte(cfgJSON), 0644)
	_, err := LoadConfig(path)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "disable_auth is true")
}

func TestLoadConfigRejectsUnsetEnvVarCredential(t *testing.T) {
	// Regression for a silent-insecure footgun: an env var that resolves to
	// "" used to satisfy the "empty = disable auth" shortcut. With the new
	// validation, an empty resolved credential is an error.
	os.Unsetenv("GOSTEST_UNSET_CRED")
	dir := t.TempDir()
	cfgJSON := `{
		"bucket": "b",
		"data_dir": "` + dir + `",
		"credentials": [{
			"username": {"type": "envvar", "name": "GOSTEST_UNSET_CRED"},
			"password": {"type": "envvar", "name": "GOSTEST_UNSET_CRED"}
		}]
	}`
	path := dir + "/cfg.json"
	os.WriteFile(path, []byte(cfgJSON), 0644)
	_, err := LoadConfig(path)
	require.NotNil(t, err)
	require.Contains(t, err.Error(), "must both be non-empty")
}
