//go:build !js && !wasip1

package cacheclient

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/wow-look-at-my/go-s3-server/cacheclient/cachedisk"
)

// handOffTest turns the hand-off on and replaces the uploader start with run.
func handOffTest(t *testing.T, run func(spool string) error) {
	t.Helper()
	hermeticOTel(t)
	wasOn := handOffOn.Load()
	handOffOn.Store(true)
	before := startUploader
	startUploader = run
	t.Cleanup(func() {
		handOffOn.Store(wasOn)
		startUploader = before
	})
}

// storeServer accepts /_batch/put and records each stored key with its body.
type storeServer struct {
	srv    *httptest.Server
	mu     sync.Mutex
	stored map[string][]byte
}

func newStoreServer(t *testing.T) *storeServer {
	t.Helper()
	ss := &storeServer{stored: map[string][]byte{}}
	ss.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/testbucket/_batch/put" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		manifest, data := parsePutTarSafe(r.Body)
		ss.mu.Lock()
		for key, body := range data {
			ss.stored[key] = body
		}
		ss.mu.Unlock()
		writePutResults(w, manifest, func(string) string { return "stored" })
	}))
	t.Cleanup(ss.srv.Close)
	return ss
}

func (ss *storeServer) keys() []string {
	ss.mu.Lock()
	defer ss.mu.Unlock()
	var keys []string
	for key := range ss.stored {
		keys = append(keys, key)
	}
	return keys
}

// useStoreConfig points ConfigEnv at url, as the uploader process reads it.
func useStoreConfig(t *testing.T, url string) {
	t.Helper()
	raw, err := json.Marshal(envConfig{Endpoint: url, Bucket: "testbucket", Username: "k", Password: "s"})
	require.NoError(t, err)
	t.Setenv(ConfigEnv, encodeConfig(t, string(raw)))
}

// testAction is a well-formed action ID made from name.
func testAction(name string) string {
	sum := sha256.Sum256([]byte("action " + name))
	return hex.EncodeToString(sum[:])
}

func readSpool(t *testing.T, path string) spoolFile {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var spool spoolFile
	require.NoError(t, json.Unmarshal(data, &spool))
	return spool
}

// Close must not wait for a server that does not answer. It cancels the
// uploads, writes each of them to a spool, and starts the uploader on it.
func TestCloseHandsOffWithoutWaitingOnTheServer(t *testing.T) {
	var spools []string
	handOffTest(t, func(spool string) error {
		spools = append(spools, spool)
		return nil
	})
	asked := make(chan struct{}, 1)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/testbucket/_batch/put" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// The server sees a client hang up only after it reads the whole body.
		_, _ = io.Copy(io.Discard, r.Body)
		select {
		case asked <- struct{}{}:
		default:
		}
		// The upload never gets an answer while the client waits.
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	store := newTestBackend(t, srv.URL)
	disk, err := Open(t.TempDir())
	require.NoError(t, err)
	tier := layerStore(disk, store)

	want := map[string]string{}
	for idx := range 3 {
		action := testAction(fmt.Sprint(idx))
		var id ActionID
		raw, _ := hex.DecodeString(action)
		copy(id[:], raw)
		out, _, err := tier.Put(id, bytes.NewReader([]byte(largePayload(512+idx))))
		require.NoError(t, err)
		want[action] = disk.OutputFile(out)
	}
	select {
	case <-asked:
	case <-time.After(5 * time.Second):
		t.Fatal("no upload reached the server")
	}

	start := time.Now()
	require.NoError(t, tier.Close())
	require.Less(t, time.Since(start), 2*time.Second, "Close must not wait for the uploads")

	require.Len(t, spools, 1, "Close starts one uploader")
	require.Equal(t, filepath.Join(disk.Dir(), spoolDirName), filepath.Dir(spools[0]))
	spool := readSpool(t, spools[0])
	got := map[string]string{}
	for _, entry := range spool.Entries {
		got[entry.ActionID] = entry.Path
	}
	require.Equal(t, want, got, "the spool names every upload the server did not confirm")
	require.Equal(t, int64(len(want)), store.HandedOff())
}

// The uploader stores every entry whose body is still there, then deletes the spool.
func TestRunUploadSpoolStoresEveryEntry(t *testing.T) {
	ss := newStoreServer(t)
	useStoreConfig(t, ss.srv.URL)
	dir := t.TempDir()

	body := []byte(largePayload(700))
	bodyPath := filepath.Join(dir, "body")
	require.NoError(t, os.WriteFile(bodyPath, body, 0o666))
	spool := spoolFile{Prefix: "go-buildcache/", Entries: []spoolEntry{
		{ActionID: testAction("file"), OutputID: testOutputID(string(body)), Path: bodyPath},
		{ActionID: testAction("bytes"), OutputID: testOutputID("manifest body"), Data: []byte("manifest body"), Replace: true},
		{ActionID: testAction("gone"), OutputID: testOutputID("x"), Path: filepath.Join(dir, "missing")},
	}}
	path, err := writeSpool(dir, spool)
	require.NoError(t, err)

	require.NoError(t, RunUploadSpool(path))

	require.ElementsMatch(t, []string{
		"go-buildcache/v1" + testAction("file"),
		"go-buildcache/v1" + testAction("bytes"),
	}, ss.keys(), "every entry with a body is stored, and no other")
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), "the spool is deleted")
	_, err = os.Stat(spoolLockPath(path))
	require.True(t, os.IsNotExist(err), "the spool's lock file goes with it")
}

// A spool another uploader holds is left to it.
func TestRunUploadSpoolLeavesALockedSpool(t *testing.T) {
	ss := newStoreServer(t)
	useStoreConfig(t, ss.srv.URL)
	dir := t.TempDir()
	path, err := writeSpool(dir, spoolFile{Prefix: "go-buildcache/", Entries: []spoolEntry{
		{ActionID: testAction("held"), OutputID: testOutputID("held"), Data: []byte("held")},
	}})
	require.NoError(t, err)
	unlock, err := cachedisk.TryLockFile(spoolLockPath(path))
	require.NoError(t, err)
	defer unlock()

	require.NoError(t, RunUploadSpool(path))
	require.Empty(t, ss.keys())
	_, err = os.Stat(path)
	require.NoError(t, err, "a locked spool stays")
}

// DrainSpools runs a spool no uploader holds, and waits out one that an
// uploader runs until that uploader deletes it.
func TestDrainSpoolsRunsAndWaits(t *testing.T) {
	ss := newStoreServer(t)
	useStoreConfig(t, ss.srv.URL)
	cacheDir := t.TempDir()
	dir := filepath.Join(cacheDir, spoolDirName)
	free, err := writeSpool(dir, spoolFile{Prefix: "go-buildcache/", Entries: []spoolEntry{
		{ActionID: testAction("free"), OutputID: testOutputID("free"), Data: []byte("free")},
	}})
	require.NoError(t, err)
	held, err := writeSpool(dir, spoolFile{Prefix: "go-buildcache/", Entries: []spoolEntry{
		{ActionID: testAction("held"), OutputID: testOutputID("held"), Data: []byte("held")},
	}})
	require.NoError(t, err)
	unlock, err := cachedisk.TryLockFile(spoolLockPath(held))
	require.NoError(t, err)

	drained := make(chan error, 1)
	go func() { drained <- DrainSpools(cacheDir) }()
	// The uploader that holds the spool stores it and deletes it.
	require.NoError(t, os.Remove(held))
	unlock()
	require.NoError(t, <-drained)

	require.Equal(t, []string{"go-buildcache/v1" + testAction("free")}, ss.keys(), "the free spool is stored, and the held one is left to its uploader")
	for _, path := range []string{free, held} {
		_, err := os.Stat(path)
		require.True(t, os.IsNotExist(err), "%s is gone", path)
	}
}

// The sweep restarts a spool whose uploader died, and leaves a fresh one alone.
func TestSweepRunsAnAbandonedSpool(t *testing.T) {
	handOffTest(t, RunUploadSpool)
	ss := newStoreServer(t)
	useStoreConfig(t, ss.srv.URL)
	dir := t.TempDir()

	abandoned, err := writeSpool(dir, spoolFile{Prefix: "go-buildcache/", Entries: []spoolEntry{
		{ActionID: testAction("abandoned"), OutputID: testOutputID("old"), Data: []byte("old")},
	}})
	require.NoError(t, err)
	old := time.Now().Add(-2 * spoolStaleAfter)
	require.NoError(t, os.Chtimes(abandoned, old, old))
	fresh, err := writeSpool(dir, spoolFile{Prefix: "go-buildcache/", Entries: []spoolEntry{
		{ActionID: testAction("fresh"), OutputID: testOutputID("new"), Data: []byte("new")},
	}})
	require.NoError(t, err)

	sweepSpools(dir)

	require.Equal(t, []string{"go-buildcache/v1" + testAction("abandoned")}, ss.keys())
	_, err = os.Stat(abandoned)
	require.True(t, os.IsNotExist(err), "the abandoned spool is done")
	_, err = os.Stat(fresh)
	require.NoError(t, err, "a fresh spool waits for its own uploader")
}

// The uploader re-runs this executable, so its environment names the spool and no broker.
func TestUploaderEnvironNamesTheSpool(t *testing.T) {
	t.Setenv(BrokerEnv, "gobuildcache-1-x")
	t.Setenv(UploadSpoolEnv, "/old")
	env := uploaderEnviron("/new")
	require.Contains(t, env, UploadSpoolEnv+"=/new")
	require.NotContains(t, env, UploadSpoolEnv+"=/old")
	require.NotContains(t, env, BrokerEnv+"=gobuildcache-1-x")
}

// A command that ends while the first index download runs hands the download
// off, so the next command finds a disk copy. The next command does not
// start over.
func TestCloseHandsOffAnUnfinishedIndexDownload(t *testing.T) {
	var spools []string
	handOffTest(t, func(spool string) error {
		spools = append(spools, spool)
		return nil
	})
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/testbucket/_index" {
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })

	store, err := NewWebBackend(WebConfig{Bucket: "testbucket", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: t.TempDir()})
	require.NoError(t, err)
	store.shortenIndexWait(IndexWaitDefault)
	disk, err := Open(t.TempDir())
	require.NoError(t, err)
	tier := layerStore(disk, store)
	var id ActionID
	id[0] = 0x42
	_, err = tier.Get(id)
	require.Error(t, err, "nothing is stored")

	require.NoError(t, tier.Close())
	require.Len(t, spools, 1, "Close starts an uploader even with no uploads")
	spool := readSpool(t, spools[0])
	require.True(t, spool.FetchIndex)
	require.Empty(t, spool.Entries)
}

// The uploader finishes the index download into the disk copy.
func TestRunUploadSpoolFetchesTheIndex(t *testing.T) {
	blob := marshalIndex(keySetToHashes(sevenKeys()))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/testbucket/_index" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("ETag", indexETag(blob))
		w.Write(blob)
	}))
	t.Cleanup(srv.Close)
	useStoreConfig(t, srv.URL)
	indexDir := t.TempDir()
	path, err := writeSpool(t.TempDir(), spoolFile{Prefix: "go-buildcache/", IndexDir: indexDir, FetchIndex: true})
	require.NoError(t, err)

	require.NoError(t, RunUploadSpool(path))

	reader, err := NewWebBackend(WebConfig{Bucket: "testbucket", Endpoint: srv.URL, AccessKey: "k", SecretKey: "s", IndexDir: indexDir, Prefix: "go-buildcache/"})
	require.NoError(t, err)
	_, err = os.Stat(reader.indexCachePath())
	require.NoError(t, err, "the uploader wrote the disk copy")
}
