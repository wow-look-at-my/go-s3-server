package cacheclient

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// PutFile must store exactly what Put stores. The only difference is where the
// body is read: PutFile hands the prep pool a path, so the caller neither waits
// for the read nor allocates the body, and the queue holds a path rather than
// megabytes while the job waits.
func TestPutFileStoresTheSameObjectAsPut(t *testing.T) {
	body := archiveBody(t)

	byPath := putThrough(t, func(b *WebBackend, actionID, outputID string) error {
		path := filepath.Join(t.TempDir(), "out")
		require.NoError(t, os.WriteFile(path, body, 0o600))
		return b.PutFile(actionID, outputID, path)
	})
	byBytes := putThrough(t, func(b *WebBackend, actionID, outputID string) error {
		return b.Put(actionID, outputID, body)
	})

	require.NotEmpty(t, byBytes, "the bytes path must store something to compare against")
	assert.Equal(t, byBytes, byPath, "the two paths must publish the same members")
}

// A body that is gone by the time a worker reads it costs the upload, never the
// build. What it must not cost is the claim: a key left claimed is a key
// nothing ever stores, for the life of the process.
func TestPutFileDropsTheClaimWhenTheBodyIsGone(t *testing.T) {
	b := testBackend(t)
	actionID := testActionID(1)

	require.NoError(t, b.PutFile(actionID, testOutputID(1), filepath.Join(t.TempDir(), "absent")))
	b.prep.await()

	h, ok := parseActionHash(actionID)
	require.True(t, ok)
	b.keysMu.Lock()
	claimed := b.keys.Contains(h)
	b.keysMu.Unlock()
	assert.False(t, claimed, "a body that could not be read must release its key for a later run")
}

// An empty path is a caller bug, and it is refused where the caller can see it
// rather than on a worker whose warning nobody is reading.
func TestPutFileRefusesAnEmptyPath(t *testing.T) {
	b := testBackend(t)
	assert.Error(t, b.PutFile(testActionID(1), testOutputID(1), ""))
}
