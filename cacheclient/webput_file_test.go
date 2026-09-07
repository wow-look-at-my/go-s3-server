package cacheclient

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collectStoredBodies serves /_batch/put and keeps every member it receives,
// so a test can read back what a Put actually published.
func collectStoredBodies(t *testing.T) (*WebBackend, map[string][]byte, *sync.Mutex) {
	t.Helper()
	hermeticOTel(t)
	t.Setenv("GO_TOOLCHAIN_CACHE_PUT_WINDOW_MS", "5000") // one batch, flushed on Close

	var mu sync.Mutex
	got := map[string][]byte{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/testbucket/_batch/put" {
			manifest, data := parsePutTarSafe(r.Body)
			mu.Lock()
			for k, v := range data {
				got[k] = v
			}
			mu.Unlock()
			writePutResults(w, manifest, func(string) string { return "stored" })
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	return newTestBackend(t, srv.URL), got, &mu
}

// PutFile must publish exactly what Put publishes. The only difference is where
// the body is read: PutFile hands the prep pool a path, so the caller neither
// waits for the read nor allocates the body, and the queue holds a path rather
// than megabytes while the job waits its turn.
func TestPutFileStoresTheSameObjectAsPut(t *testing.T) {
	const action = "aabbccdd11223300"
	payload := largePayload(512)
	path := filepath.Join(t.TempDir(), "out")
	require.NoError(t, os.WriteFile(path, []byte(payload), 0o600))

	byPath, gotPath, muPath := collectStoredBodies(t)
	require.NoError(t, byPath.PutFile(action, testOutputID(payload), path))
	require.NoError(t, byPath.Close())

	byBytes, gotBytes, muBytes := collectStoredBodies(t)
	require.NoError(t, byBytes.Put(action, testOutputID(payload), []byte(payload)))
	require.NoError(t, byBytes.Close())

	muPath.Lock()
	defer muPath.Unlock()
	muBytes.Lock()
	defer muBytes.Unlock()
	require.Len(t, gotBytes, 1, "the bytes path must store one member to compare against")
	assert.Equal(t, gotBytes, gotPath, "both paths must publish the same member under the same key")
}

// A body that is gone by the time a worker reads it costs the upload, never the
// build. What it must not cost is the claim: a key left claimed is a key
// nothing ever stores, for the rest of the process's life.
func TestPutFileDropsTheClaimWhenTheBodyIsGone(t *testing.T) {
	const action = "aabbccdd11223301"
	b, _, _ := collectStoredBodies(t)

	require.NoError(t, b.PutFile(action, testOutputID("x"), filepath.Join(t.TempDir(), "absent")))
	b.prep.await()

	assert.False(t, claimed(b, action),
		"a body that could not be read must release its key so a later run re-uploads")
	require.NoError(t, b.Close())
}

// An empty path is a caller bug. It is refused where the caller can see it,
// rather than on a worker whose warning nobody is reading.
func TestPutFileRefusesAnEmptyPath(t *testing.T) {
	b, _, _ := collectStoredBodies(t)
	assert.Error(t, b.PutFile("aabbccdd11223302", testOutputID("x"), ""))
	require.NoError(t, b.Close())
}
