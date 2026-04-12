package main

import (
	"bytes"
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/wow-look-at-my/testify/require"
)

func testSetupWithAuth(t *testing.T) (*httptest.Server, *Config) {
	t.Helper()
	dir := t.TempDir()

	cfg := &Config{
		Listen:    ":0",
		Bucket:    "testbucket",
		DataDir:   dir,
		WriteOnce: WriteOnceConfig{Action: "allow"},
		Credentials: []Credential{
			{Username: "alice", Password: "password1"},
			{Username: "bob", Password: "password2"},
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

func TestNoAuthWithEmptyCredentials(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		Listen:      ":0",
		Bucket:      "testbucket",
		DataDir:     dir,
		WriteOnce:   WriteOnceConfig{Action: "allow"},
		Credentials: []Credential{{Username: "", Password: ""}},
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
