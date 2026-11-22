// The server ends an index epoch each time it removes a key.

package cacheclient

import (
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
)

// epochServer serves an index whose epoch and keys a test moves.
type epochServer struct {
	mu    sync.Mutex
	epoch uint64
	keys  set.Set[string]
	whole int
	gone  int
	// failWhole makes the next whole fetch fail.
	failWhole bool
}

func (s *epochServer) move(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.epoch++
	s.keys.Add(key)
}

func (s *epochServer) counts() (whole, gone int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.whole, s.gone
}

func (s *epochServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/bk/_index" {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	epoch := strconv.FormatUint(s.epoch, 10)
	query := r.URL.Query()
	if query.Get("seq") != "" {
		if query.Get("epoch") != epoch {
			s.gone++
			w.WriteHeader(http.StatusGone)
			return
		}
		w.Header().Set(HeaderIndexEpoch, epoch)
		w.Header().Set(HeaderIndexSeq, query.Get("seq"))
		w.Header().Set(HeaderIndexDelta, "1")
		w.Write(marshalIndex(newKeySet(0)))
		return
	}
	s.whole++
	if s.failWhole {
		s.failWhole = false
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	full := marshalIndex(keySetToHashes(s.keys))
	w.Header().Set(HeaderIndexEpoch, epoch)
	w.Header().Set(HeaderIndexSeq, "1")
	w.Header().Set("ETag", indexETag(full))
	w.Write(full)
}

// laterKey is a second key outside sevenKeys.
func laterKey() string {
	var h [gbciHashSize]byte
	h[0] = 0xef
	return gbciKeyPrefix + hex.EncodeToString(h[:])
}

// allowDelta clears the rate limit of the live delta, as a couple of seconds do.
func allowDelta(b *WebBackend) {
	b.deltaMu.Lock()
	b.deltaAt = time.Time{}
	b.deltaMu.Unlock()
}

// The client then fetches the whole index, so a key another build stored
// later is found. Without that the set stays as it was for the rest of the
// build, and every such key is a miss. A sweep ends many epochs in a row.
// The whole fetch has its own rate limit, and a miss inside it keeps the
// position and asks again later.
func TestEndedEpochFetchesTheIndexWhole(t *testing.T) {
	server := &epochServer{epoch: 9, keys: sevenKeys()}
	srv := httptest.NewServer(server)
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	defer b.Close()
	b.awaitIndex()
	require.False(t, b.Present(idOf(extraKey())))

	server.move(extraKey())
	allowDelta(b)
	require.True(t, b.refreshLiveDelta(), "an ended epoch is answered by a whole fetch")
	require.True(t, b.Present(idOf(extraKey())), "the key stored after the epoch ended is in the live set")
	require.True(t, b.Present(idOf(anyKey(sevenKeys()))), "the keys the set held stay")
	b.keysMu.RLock()
	gen := b.liveGen
	b.keysMu.RUnlock()
	require.Equal(t, indexGen{epoch: 10, seq: 1}, gen, "the set's position is in the new epoch")
	whole, gone := server.counts()
	require.Equal(t, 2, whole, "one fetch at the start and one for the ended epoch")
	require.Equal(t, 1, gone)

	// Another epoch ends at once, as in a sweep. The whole fetch waits.
	server.move(laterKey())
	allowDelta(b)
	require.False(t, b.refreshLiveDelta())
	require.False(t, b.Present(idOf(laterKey())))
	whole, gone = server.counts()
	require.Equal(t, 2, whole, "a second whole fetch inside the interval is not sent")
	require.Equal(t, 2, gone)

	// Past the interval the same miss fetches whole again.
	b.deltaMu.Lock()
	b.deltaAt, b.refetchAt = time.Time{}, time.Now().Add(-EpochRefetchInterval)
	b.deltaMu.Unlock()
	require.True(t, b.refreshLiveDelta())
	require.True(t, b.Present(idOf(laterKey())))
}

// A whole fetch that fails leaves the live set and its position alone. The
// next miss past the interval asks again, and the build is not left without
// updates for the rest of its run.
func TestEndedEpochRetriesAFailedWholeFetch(t *testing.T) {
	server := &epochServer{epoch: 9, keys: sevenKeys()}
	srv := httptest.NewServer(server)
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	defer b.Close()
	b.awaitIndex()

	server.move(extraKey())
	server.mu.Lock()
	server.failWhole = true
	server.mu.Unlock()
	allowDelta(b)
	require.False(t, b.refreshLiveDelta())
	b.keysMu.RLock()
	gen := b.liveGen
	b.keysMu.RUnlock()
	require.Equal(t, indexGen{epoch: 9, seq: 1}, gen, "a failed fetch keeps the position, so a later miss asks again")
	require.True(t, b.Present(idOf(anyKey(sevenKeys()))))

	b.deltaMu.Lock()
	b.deltaAt, b.refetchAt = time.Time{}, time.Now().Add(-EpochRefetchInterval)
	b.deltaMu.Unlock()
	require.True(t, b.refreshLiveDelta())
	require.True(t, b.Present(idOf(extraKey())))
}
