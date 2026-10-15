package cacheclient

import (
	"bytes"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Retrievable names what Get can supply: what disk holds, and what a
// confirmed index names. An unconfirmed index names nothing, because the
// store can have evicted any key it lists.
func TestRetrievable(t *testing.T) {
	keys := sevenKeys()
	blob := marshalIndex(keySetToHashes(keys))
	serveIndex := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bk/_index" || !serveIndex {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", indexETag(blob))
		w.Write(blob)
	}))
	defer srv.Close()

	open := func() *storeTier {
		store, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
		require.NoError(t, err)
		disk, err := Open(t.TempDir())
		require.NoError(t, err)
		return layerStore(disk, store)
	}
	var listed ActionID
	for key := range keys.All() {
		raw, err := hex.DecodeString(key[len(gbciKeyPrefix):])
		require.NoError(t, err)
		copy(listed[:], raw)
		break
	}
	var unlisted ActionID
	unlisted[0] = 0xee

	tier := open()
	tier.store.awaitIndex()
	require.True(t, tier.Retrievable(listed), "a confirmed index names it")
	require.False(t, tier.Retrievable(unlisted), "nothing holds it")

	var local ActionID
	local[0] = 0xdd
	_, _, err := tier.DiskCache.Put(local, bytes.NewReader([]byte("body")))
	require.NoError(t, err)
	require.True(t, tier.Retrievable(local), "disk holds it")

	// An action entry whose output file is gone, as a trim or a lazy hit leaves it, holds nothing.
	var orphan ActionID
	orphan[0] = 0xcc
	out, _, err := tier.DiskCache.Put(orphan, bytes.NewReader([]byte("gone")))
	require.NoError(t, err)
	require.NoError(t, os.Remove(tier.DiskCache.OutputFile(out)))
	require.False(t, tier.Retrievable(orphan), "an entry without its output is not on disk")

	serveIndex = false
	unconfirmed := open()
	unconfirmed.store.awaitIndex()
	require.False(t, unconfirmed.store.SummarySnapshot().IndexAuthoritative)
	require.False(t, unconfirmed.Retrievable(listed), "an unconfirmed index proves nothing")
}
