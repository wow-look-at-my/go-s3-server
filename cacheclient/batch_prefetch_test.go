package cacheclient

import (
	"archive/tar"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// prefetchFixture is a server that answers a batch with the keys it was asked
// for PLUS a single the caller never named, which is what the real server's
// prefetch window looks like from the client.
type prefetchFixture struct {
	srv *httptest.Server
	// bodies are the stored objects, already compressed, by key.
	bodies map[string][]byte
	// outputIDs is what each key advertises, so a test can advertise a wrong
	// a single and make the body corrupt.
	outputIDs map[string]string
	// rawSizes is each body's uncompressed length.
	rawSizes map[string]int64
	// extra is the key the server volunteers on every batch.
	extra string

	mu       sync.Mutex
	batches  int
	askedFor []string
}

// testActionID is a full-length action ID, which is what ActionIDFromKey
// requires: a short a single is not a cache key at all.
func testActionID(fill byte) string {
	var h [hashSize]byte
	for i := range h {
		h[i] = fill
	}
	return hex.EncodeToString(h[:])
}

func newPrefetchFixture(t *testing.T, extra string) *prefetchFixture {
	t.Helper()
	f := &prefetchFixture{
		bodies:    map[string][]byte{},
		outputIDs: map[string]string{},
		rawSizes:  map[string]int64{},
		extra:     extra,
	}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/_batch/get") {
			// No index: the key set stays non-authoritative, so a cold key is
			// probed through the batch endpoint.
			w.WriteHeader(404)
			return
		}
		var req batchGetRequest
		json.NewDecoder(r.Body).Decode(&req)
		f.mu.Lock()
		f.batches++
		f.askedFor = append(f.askedFor, req.Keys...)
		f.mu.Unlock()

		keys := append([]string{}, req.Keys...)
		if f.extra != "" {
			keys = append(keys, f.extra)
		}
		var entries []batchGetManifestEntry
		for _, k := range keys {
			if _, ok := f.bodies[k]; !ok {
				continue
			}
			entries = append(entries, batchGetManifestEntry{
				Key:  k,
				Size: int64(len(f.bodies[k])),
				Metadata: map[string]string{
					"outputid":  f.outputIDs[k],
					"body-size": strconv.FormatInt(f.rawSizes[k], 10),
				},
				Prefetch: k == f.extra,
			})
		}
		w.Header().Set("Content-Type", "application/x-tar")
		w.WriteHeader(200)
		tw := tar.NewWriter(w)
		mdata, _ := json.Marshal(batchGetManifest{Entries: entries})
		tw.WriteHeader(&tar.Header{Name: "manifest.json", Size: int64(len(mdata)), Mode: 0644})
		tw.Write(mdata)
		for _, e := range entries {
			d := f.bodies[e.Key]
			tw.WriteHeader(&tar.Header{Name: "data/" + e.Key, Size: int64(len(d)), Mode: 0644})
			tw.Write(d)
		}
		tw.Close()
	}))
	t.Cleanup(f.srv.Close)
	return f
}

// store puts a body on the fixture under the action ID, advertising outputID.
func (f *prefetchFixture) store(prefix, actionID, body, outputID string) string {
	key := prefix + actionID
	compressed, _ := Compress([]byte(body))
	f.bodies[key] = compressed
	f.outputIDs[key] = outputID
	f.rawSizes[key] = int64(len(body))
	return key
}

func (f *prefetchFixture) batchCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.batches
}

func (f *prefetchFixture) asked(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, k := range f.askedFor {
		if k == key {
			return true
		}
	}
	return false
}

// sinkTier is the consumer's store, which is what OnBatchEntries feeds.
type sinkTier struct {
	mu      sync.Mutex
	entries map[string]BatchEntry
}

func newSinkTier() *sinkTier {
	return &sinkTier{entries: map[string]BatchEntry{}}
}

func (l *sinkTier) sink(entries []BatchEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range entries {
		l.entries[e.Key] = e
	}
}

func (l *sinkTier) get(key string) (BatchEntry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	e, ok := l.entries[key]
	return e, ok
}

func (l *sinkTier) len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// TestBatchStoresUnrequestedEntry is the whole point: a body the batch carried
// that nobody asked for lands in the local tier, verified, and the build can
// then have it without another request to the server.
func TestBatchStoresUnrequestedEntry(t *testing.T) {
	wantedID := testActionID(0x11)
	spareID := testActionID(0x22)
	f := newPrefetchFixture(t, "")

	b, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: f.srv.URL, AccessKey: "k", SecretKey: "s"})
	require.NoError(t, err)
	local := newSinkTier()
	b.OnBatchEntries = local.sink

	f.store(b.KeyPrefix(), wantedID, "the body the build asked for", testOutputID("the body the build asked for"))
	spareKey := f.store(b.KeyPrefix(), spareID, "the body it has not asked for yet", testOutputID("the body it has not asked for yet"))
	f.extra = spareKey

	_, data, _, miss := b.Get(wantedID)
	require.False(t, miss)
	require.Equal(t, "the body the build asked for", string(data))
	require.NoError(t, b.Close())

	require.Equal(t, uint32(1), b.PrefetchOffered.Load(), "the server volunteered one entry")
	require.Equal(t, uint32(1), b.PrefetchStored.Load(), "and it must be kept, not dropped")

	got, ok := local.get(spareKey)
	require.True(t, ok, "the unrequested body must reach the local tier")
	require.False(t, f.asked(spareKey), "it must be there without ever being requested")

	// What landed is usable: it passes the same gates and decompresses to the
	// object, so the local tier can serve it with no round trip.
	body, ok := b.Verify(got, spareID)
	require.True(t, ok)
	require.Equal(t, "the body it has not asked for yet", string(body))
	require.Equal(t, 1, f.batchCount(), "serving it must cost no second request")
}

// TestBatchRejectsCorruptUnrequestedEntry pins the gate.
func TestBatchRejectsCorruptUnrequestedEntry(t *testing.T) {
	wantedID := testActionID(0x33)
	spareID := testActionID(0x44)
	f := newPrefetchFixture(t, "")

	b, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: f.srv.URL, AccessKey: "k", SecretKey: "s"})
	require.NoError(t, err)
	local := newSinkTier()
	b.OnBatchEntries = local.sink

	f.store(b.KeyPrefix(), wantedID, "a good body", testOutputID("a good body"))
	// Advertised as a single object, stored as another.
	spareKey := f.store(b.KeyPrefix(), spareID, "a body that is not what it claims", testOutputID("what it claims to be"))
	f.extra = spareKey

	_, _, _, miss := b.Get(wantedID)
	require.False(t, miss)
	require.NoError(t, b.Close())

	require.Equal(t, uint32(1), b.PrefetchOffered.Load())
	require.Equal(t, uint32(0), b.PrefetchStored.Load(), "a body failing the checksum must never be stored")
	require.Equal(t, 0, local.len(), "nothing corrupt may reach the local tier")
	require.Equal(t, uint32(1), b.MissChecksum.Load(), "and it is counted as the integrity failure it is")
}

// TestBatchDropsUnrequestedEntryWithNoSink pins today's behaviour where there
// is nowhere to put the body: it is dropped as it always was, and nothing
// accumulates waiting for a consumer that does not exist.
func TestBatchDropsUnrequestedEntryWithNoSink(t *testing.T) {
	wantedID := testActionID(0x55)
	spareID := testActionID(0x66)
	f := newPrefetchFixture(t, "")

	b, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: f.srv.URL, AccessKey: "k", SecretKey: "s"})
	require.NoError(t, err)
	require.Nil(t, b.OnBatchEntries, "this test is about having no sink")

	f.store(b.KeyPrefix(), wantedID, "a good body", testOutputID("a good body"))
	spareKey := f.store(b.KeyPrefix(), spareID, "a spare body", testOutputID("a spare body"))
	f.extra = spareKey

	_, _, _, miss := b.Get(wantedID)
	require.False(t, miss)
	require.NoError(t, b.Close())

	require.Equal(t, uint32(1), b.PrefetchOffered.Load(), "the cost is still visible in the stats")
	require.Equal(t, uint32(0), b.PrefetchStored.Load())
	require.Zero(t, b.prefetchHold.held.Load(), "no sink must buffer nothing at all")
}

// TestPrefetchBudgetBoundsWhatIsHeld pins the byte accounting. The window is
// whatever the server chose to send, so what the client holds of it has to be
// a size, not a count.
func TestPrefetchBudgetBoundsWhatIsHeld(t *testing.T) {
	p := &prefetchBudget{limit: 1024}
	require.True(t, p.take(1000), "under the limit an entry is kept")
	require.False(t, p.take(25), "over the limit the next one is refused")
	require.Equal(t, int64(1000), p.held.Load(), "a refusal must reserve nothing")
	p.release(1000)
	require.Zero(t, p.held.Load())
	require.True(t, p.take(25), "a returned reservation admits the next entry")
	p.release(25)

	unbounded := &prefetchBudget{}
	require.True(t, unbounded.take(1<<30), "a zero limit is unbounded, as an unset budget asks for")
}

// TestPrefetchBudgetStopsCollectingOverTheLimit pins that the sink honours the
// budget rather than reading a whole window into memory.
func TestPrefetchBudgetStopsCollectingOverTheLimit(t *testing.T) {
	b := &WebBackend{OnBatchEntries: func([]BatchEntry) {}}
	b.prefetchHold.limit = 100
	s := &prefetchSink{b: b}

	s.collect(BatchEntry{Key: "a", Data: make([]byte, 60)})
	s.collect(BatchEntry{Key: "b", Data: make([]byte, 60)})
	require.Equal(t, uint32(2), b.PrefetchOffered.Load(), "both were offered")
	require.Len(t, s.entries, 1, "only what fits the budget is held")
	require.Equal(t, int64(60), s.held)
}
