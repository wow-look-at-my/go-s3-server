package cacheclient

import (
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
)

// TestDeltaIsSavedForTheNextProcess pins that a delta merges into the disk
// copy. Without it every go command asked for the same growing delta.
func TestDeltaIsSavedForTheNextProcess(t *testing.T) {
	dir := t.TempDir()
	keys := sevenKeys()
	full := marshalIndex(keySetToHashes(keys))
	added := set.New[string]()
	added.Add(extraKey())
	delta := marshalIndex(keySetToHashes(added))

	var mu sync.Mutex
	var seqs []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bk/_index" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		seq := r.URL.Query().Get("seq")
		mu.Lock()
		seqs = append(seqs, seq)
		mu.Unlock()
		w.Header().Set(HeaderIndexEpoch, "9")
		switch seq {
		case "":
			w.Header().Set(HeaderIndexSeq, "1")
			w.Header().Set("ETag", indexETag(full))
			w.Write(full)
		case "1":
			w.Header().Set(HeaderIndexSeq, "2")
			w.Header().Set(HeaderIndexDelta, "1")
			w.Write(delta)
		default:
			w.Header().Set(HeaderIndexSeq, seq)
			w.Header().Set(HeaderIndexDelta, "1")
			w.Write(marshalIndex(newHashSet(0)))
		}
	}))
	defer srv.Close()

	cfg := WebConfig{Bucket: "bk", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: dir}
	for run := range 3 {
		b, err := NewWebBackend(cfg)
		require.NoError(t, err)
		b.awaitIndex()
		// The first process takes the full index, which predates the delta.
		require.Equal(t, run > 0, b.Present(idOf(extraKey())), "the delta's key is in the live set")
		require.True(t, b.Present(idOf(anyKey(keys))), "the copy's keys are in the live set")
		b.Close()
	}

	reader := &WebBackend{indexDir: dir, endpoint: srv.URL, bucket: "bk"}
	disk := reader.readDiskIndex(reader.indexCachePath())
	require.Equal(t, keys.Len()+1, disk.count, "the disk copy carries the delta")
	require.Equal(t, indexGen{epoch: 9, seq: 2}, disk.gen)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"", "1", "2"}, seqs, "each process asks from where the last one stopped")
}
