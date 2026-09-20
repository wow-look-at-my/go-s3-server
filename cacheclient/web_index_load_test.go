package cacheclient

import (
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-containers/set"
)

// idOf is the action ID a test key string names.
func idOf(key string) string {
	return strings.TrimPrefix(key, gbciKeyPrefix)
}

// extraKey is a key outside sevenKeys, for a blob the server moves on to.
func extraKey() string {
	var h [gbciHashSize]byte
	h[0] = 0xee
	return gbciKeyPrefix + hex.EncodeToString(h[:])
}

// anyKey is a single member of a key set.
func anyKey(keys set.Set[string]) string {
	for k := range keys.All() {
		return k
	}
	panic("empty key set")
}

// TestStaleCopyIsServedWhileItRefreshes pins stale-while-revalidate. A disk
// copy past the max age is installed at the same time, as non-authoritative,
// and the revalidation runs behind it. the earliest use must not wait for a
// download it does not need: the old load held every Get and Put of the
// process until the whole blob had arrived.
func TestStaleCopyIsServedWhileItRefreshes(t *testing.T) {
	dir := t.TempDir()
	keys := sevenKeys()
	first := marshalIndex(keySetToHashes(keys))
	moved := set.New[string]()
	for k := range keys.All() {
		moved.Add(k)
	}
	moved.Add(extraKey())
	second := marshalIndex(keySetToHashes(moved))

	release := make(chan struct{})
	var served atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bk/_index" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if served.Add(1) == 1 {
			w.Header().Set("ETag", indexETag(first))
			w.Write(first)
			return
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("ETag", indexETag(second))
		w.Write(second)
	}))
	defer srv.Close()
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })

	cfg := WebConfig{Bucket: "bk", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: dir, IndexMaxAge: time.Hour}
	seed, err := NewWebBackend(cfg)
	require.NoError(t, err)
	seed.awaitIndex()
	seed.Close()
	old := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(seed.indexCachePath(), old, old))

	b, err := NewWebBackend(cfg)
	require.NoError(t, err)
	defer b.Close()

	loaded := make(chan struct{})
	go func() {
		b.ensureIndex()
		close(loaded)
	}()
	select {
	case <-loaded:
	case <-time.After(2 * time.Second):
		releaseOnce.Do(func() { close(release) })
		<-loaded
		t.Fatal("the first use waited on the refresh although a disk copy existed")
	}

	require.True(t, b.Present(idOf(anyKey(keys))), "the stale copy's keys are served at once")
	require.False(t, b.Present(idOf(extraKey())), "the refresh has not landed yet")
	require.False(t, b.SummarySnapshot().IndexAuthoritative, "a copy past the max age is not authoritative until revalidated")

	// A claim made while the refresh runs must survive the set it installs.
	var claimed [gbciHashSize]byte
	claimed[0] = 0x55
	b.MarkPresent(hex.EncodeToString(claimed[:]))

	releaseOnce.Do(func() { close(release) })
	b.awaitIndex()
	require.True(t, b.Present(idOf(extraKey())), "the refreshed index replaces the stale copy")
	require.True(t, b.Present(hex.EncodeToString(claimed[:])), "a claim made during the refresh is kept")
	require.True(t, b.SummarySnapshot().IndexAuthoritative)
	require.Equal(t, 8, b.SummarySnapshot().IndexKeys)
	require.Equal(t, int32(2), served.Load())
}

// TestNoCopyGetIsBoundedWhileIndexLoads pins the bound on the thing wait
// left. With no disk copy the earliest use waits for the index, but only so
// long: the index is an optimization, never a gate. Here it streams too
// slowly to finish, as a large index does over a slow link, and a Get must
// still complete within the bound and fall through to the server.
func TestNoCopyGetIsBoundedWhileIndexLoads(t *testing.T) {
	t.Serial() // the logger is package state
	blob := testIndexBlob(64)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bk/_index" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		f := w.(http.Flusher)
		// A byte at a time: always progressing, never done inside the test.
		for i := 0; i < len(blob); i++ {
			w.Write(blob[i : i+1])
			f.Flush()
			select {
			case <-release:
				return
			case <-r.Context().Done():
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}))
	defer srv.Close()
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })

	logs := &levelLogger{}
	SetLogger(logs)
	t.Cleanup(func() { SetLogger(nil) })

	b, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	b.shortenIndexWaits(300*time.Millisecond, 10*time.Second)

	var absent [gbciHashSize]byte
	absent[0] = 0x99
	start := time.Now()
	got := make(chan bool)
	go func() {
		_, _, _, miss := b.Get(hex.EncodeToString(absent[:]))
		got <- miss
	}()
	select {
	case miss := <-got:
		require.True(t, miss)
	case <-time.After(3 * time.Second):
		releaseOnce.Do(func() { close(release) })
		<-got
		t.Fatal("a Get waited on an index download past the bound")
	}
	require.Less(t, time.Since(start), 3*time.Second)
	require.False(t, b.SummarySnapshot().IndexAuthoritative, "an index still downloading proves no absence")
	require.Equal(t, uint32(1), b.MissHTTP404.Load(), "the Get fell through to the server instead of missing blind")

	require.NoError(t, b.Close())
	require.Contains(t, logs.Warn(), "not loaded after 300ms", "the wait is visible without debug output")
	_, err = os.Stat(b.indexCachePath() + ".lock")
	require.True(t, os.IsNotExist(err), "Close releases the download lock")
}

// TestConcurrentClientsDownloadIndexOnce pins single-flight across processes
// that share an IndexDir. backends stand in for go commands: they share no
// memory, only the directory. the thing that takes the lock downloads; the
// other waits for it and reads what it wrote.
func TestConcurrentClientsDownloadIndexOnce(t *testing.T) {
	dir := t.TempDir()
	keys := sevenKeys()
	blob := marshalIndex(keySetToHashes(keys))
	var fetches atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bk/_index" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		fetches.Add(1)
		// Slow enough that both clients want the index while it is in flight.
		time.Sleep(300 * time.Millisecond)
		w.Header().Set("ETag", indexETag(blob))
		w.Write(blob)
	}))
	defer srv.Close()

	cfg := WebConfig{Bucket: "bk", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: dir}
	var backends []*WebBackend
	for i := 0; i < 2; i++ {
		b, err := NewWebBackend(cfg)
		require.NoError(t, err)
		b.shortenIndexWaits(10*time.Second, 10*time.Second)
		defer b.Close()
		backends = append(backends, b)
	}

	var wg sync.WaitGroup
	for _, b := range backends {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.awaitIndex()
		}()
	}
	wg.Wait()

	require.Equal(t, int32(1), fetches.Load(), "processes sharing an IndexDir download the index once")
	for _, b := range backends {
		require.True(t, b.SummarySnapshot().IndexAuthoritative, "the waiter's copy was confirmed by the server a moment ago")
		require.True(t, b.Present(idOf(anyKey(keys))))
	}
}

// TestDeadLockHolderIsTakenOver pins what a crashed downloader leaves: a lock
// nobody touches. Past the stale age the next process removes it and fetches.
func TestDeadLockHolderIsTakenOver(t *testing.T) {
	dir := t.TempDir()
	f := newIndexFixture(t, "bk", sevenKeys())
	b, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: f.srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: dir})
	require.NoError(t, err)
	defer b.Close()
	b.shortenIndexWaits(5*time.Second, 200*time.Millisecond)

	lock := b.indexCachePath() + ".lock"
	require.NoError(t, os.WriteFile(lock, []byte("1\n"), 0644))
	old := time.Now().Add(-time.Hour)
	require.NoError(t, os.Chtimes(lock, old, old))

	b.awaitIndex()
	require.Equal(t, int32(1), f.hits200.Load(), "the dead holder's download is taken over")
	require.True(t, b.SummarySnapshot().IndexAuthoritative)
	_, err = os.Stat(lock)
	require.True(t, os.IsNotExist(err), "the taken-over lock is released")
}

// TestFailedHolderLeavesCopyNonAuthoritative pins the waiter's side of a
// download that failed: it tries a single time itself, and when that
// fails too it keeps the copy it had, non-authoritative, rather than retrying forever.
func TestFailedHolderLeavesCopyNonAuthoritative(t *testing.T) {
	dir := t.TempDir()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: dir})
	require.NoError(t, err)
	defer b.Close()
	b.shortenIndexWaits(5*time.Second, 10*time.Second)

	// Another process holds the lock and gives up without writing a copy.
	lock := b.indexCachePath() + ".lock"
	require.NoError(t, os.WriteFile(lock, []byte("1\n"), 0644))
	go func() {
		time.Sleep(100 * time.Millisecond)
		os.Remove(lock)
	}()

	b.awaitIndex()
	require.False(t, b.SummarySnapshot().IndexAuthoritative)
	require.Equal(t, 0, b.SummarySnapshot().IndexKeys)
}

// TestUnlockableIndexDirStillLoads pins that the lock is coordination, not a
// precondition: a directory the lock cannot be made in still gets an index.
func TestUnlockableIndexDirStillLoads(t *testing.T) {
	f := newIndexFixture(t, "bk", sevenKeys())
	missing := filepath.Join(t.TempDir(), "absent")
	b, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: f.srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: missing})
	require.NoError(t, err)
	defer b.Close()

	b.awaitIndex()
	require.True(t, b.SummarySnapshot().IndexAuthoritative)
	require.Equal(t, 7, b.SummarySnapshot().IndexKeys)
}

// TestCloseAbandonsBackgroundRefresh pins that a refresh never holds up exit:
// Close cancels it and releases the lock for the next process.
func TestCloseAbandonsBackgroundRefresh(t *testing.T) {
	dir := t.TempDir()
	keys := sevenKeys()
	blob := marshalIndex(keySetToHashes(keys))
	var served atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if served.Add(1) == 1 {
			w.Header().Set("ETag", indexETag(blob))
			w.Write(blob)
			return
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	cfg := WebConfig{Bucket: "bk", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: dir, IndexMaxAge: -1}
	seed, err := NewWebBackend(cfg)
	require.NoError(t, err)
	seed.awaitIndex()
	seed.Close()

	b, err := NewWebBackend(cfg)
	require.NoError(t, err)
	b.ensureIndex()
	require.True(t, b.Present(idOf(anyKey(keys))))

	start := time.Now()
	require.NoError(t, b.Close())
	require.Less(t, time.Since(start), 2*time.Second, "Close must not wait out the refresh")
	_, err = os.Stat(b.indexCachePath() + ".lock")
	require.True(t, os.IsNotExist(err), "Close releases the download lock")
}
