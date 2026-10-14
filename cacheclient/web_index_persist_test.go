package cacheclient

import (
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
)

// TestDeltaIsSavedForTheNextProcess pins that a delta merges into the disk
// copy. Without it every go command asked for the same growing delta.
func TestDeltaIsSavedForTheNextProcess(t *testing.T) {
	t.Serial() // persistDeltaMin is package state
	saved := persistDeltaMin
	persistDeltaMin = 1
	defer func() { persistDeltaMin = saved }()
	seqs, disk := runDeltaProcesses(t)
	require.Equal(t, sevenKeys().Len()+1, disk.count, "the disk copy carries the delta")
	require.Equal(t, indexGen{epoch: 9, seq: 2}, disk.gen)
	require.Equal(t, []string{"", "1", "2"}, seqs, "each process asks from where the last one stopped")
}

// TestSmallDeltaLeavesTheCopy pins that a delta too small to pay for a
// rewrite leaves the copy and its position as they were.
func TestSmallDeltaLeavesTheCopy(t *testing.T) {
	seqs, disk := runDeltaProcesses(t)
	require.Equal(t, sevenKeys().Len(), disk.count, "the copy is not rewritten")
	require.Equal(t, indexGen{epoch: 9, seq: 1}, disk.gen)
	require.Equal(t, []string{"", "1", "1"}, seqs, "the next process asks for the same delta again")
}

// TestFailedDeltaTrustsAYoungCopy pins that a failed delta keeps a young copy
// authoritative. A key the copy lacks then misses with no request.
func TestFailedDeltaTrustsAYoungCopy(t *testing.T) {
	dir := t.TempDir()
	full := marshalIndex(keySetToHashes(sevenKeys()))
	var others atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bk/_index" {
			others.Add(1)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if r.URL.Query().Get("seq") != "" {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set(HeaderIndexEpoch, "9")
		w.Header().Set(HeaderIndexSeq, "1")
		w.Header().Set("ETag", indexETag(full))
		w.Write(full)
	}))
	defer srv.Close()

	cfg := WebConfig{Bucket: "bk", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: dir, IndexMaxAge: time.Hour}
	seed, err := NewWebBackend(cfg)
	require.NoError(t, err)
	seed.awaitIndex()
	seed.Close()

	b, err := NewWebBackend(cfg)
	require.NoError(t, err)
	defer b.Close()
	b.awaitIndex()
	require.True(t, b.SummarySnapshot().IndexAuthoritative, "a young copy stays authoritative when its delta fails")
	_, _, _, miss := b.Get(idOf(extraKey()))
	require.True(t, miss)
	require.Zero(t, others.Load(), "a key the young copy lacks costs no request")
}

// runDeltaProcesses runs backends in turn over a single index directory. The
// server adds a key after its first full answer. It returns the positions the
// backends asked from and the disk copy they leave.
func runDeltaProcesses(t *testing.T) ([]string, diskIndex) {
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
	var path string
	for run := range 3 {
		b, err := NewWebBackend(cfg)
		require.NoError(t, err)
		path = b.indexCachePath()
		b.awaitIndex()
		// The first process takes the full index, which predates the delta.
		require.Equal(t, run > 0, b.Present(idOf(extraKey())), "the delta's key is in the live set")
		require.True(t, b.Present(idOf(anyKey(keys))), "the copy's keys are in the live set")
		b.Close()
	}

	mu.Lock()
	defer mu.Unlock()
	return slices.Clone(seqs), (&WebBackend{}).readDiskIndex(path)
}
