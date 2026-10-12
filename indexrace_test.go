package main

import (
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-s3-server/cacheclient"
)

// A key absent from a young copy misses as soon as the index load proves it
// absent. It must not wait for the probe the copy's doubt started. The probe
// delay is far above the bound, so a loaded machine does not blur both.
func TestAbsentKeyMissesWhenTheLoadLandsNotWhenTheProbeDoes(t *testing.T) {
	if !inOwnProcess(t) {
		return
	}
	ts, _ := testSetupWithStorage(t)
	const probeDelay, bound = 20 * time.Second, 10 * time.Second
	target, err := url.Parse(ts.URL)
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(target)
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/_batch/get") {
			time.Sleep(probeDelay)
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(slow.Close)

	cfg := cacheclient.WebConfig{Bucket: "testbucket", Endpoint: slow.URL, AccessKey: "anyone", SecretKey: "unchecked", IndexDir: t.TempDir()}
	objs := makeWireObjects(2)
	seeder, err := cacheclient.NewWebBackend(cfg)
	require.NoError(t, err)
	_, _, _, miss := seeder.Get(objs[0].actionID)
	require.True(t, miss, "nothing is stored yet; this writes the young copy")
	seeder.Close()

	reader, err := cacheclient.NewWebBackend(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { reader.Close() })
	start := time.Now()
	_, _, _, miss = reader.Get(objs[1].actionID)
	require.True(t, miss)
	require.Less(t, time.Since(start), bound, "the miss waited for the probe, not for the index")
}
