package cacheclient

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// archiveWithBuildID builds a minimal Go ar archive whose __.PKGDEF header
// carries `build id "<action>/<content>"`, the same shape `go build` stamps.
// Only the build id line matters to the guard, so the export data is a stub.
func archiveWithBuildID(action string) []byte {
	body := "go object linux amd64 go1.24.7\n" +
		"build id \"" + action + "/Cw9xV7fakecontentid\"\n\n\n" +
		"$$B\nu\x00\x00\x00\n$$\n"
	return buildAr("__.PKGDEF", []byte(body))
}

// archivePkgdefNoBuildID builds an archive with NO build id line: corrupt, or stripped to
// slip export data under a foreign key.
func archivePkgdefNoBuildID() []byte {
	body := "go object linux amd64 go1.24.13\n\n\n$$B\nu\x00\x00\x00\n$$\n"
	return buildAr("__.PKGDEF", []byte(body))
}

// hermeticOTel clears the OTEL endpoint so the process-global tracer provider's sync.Once memoizes disabled.
func hermeticOTel(t *testing.T) { t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "") }

// TestExpectedBuildIDAction_Golden pins the encoding against a real `go build` stamp, verified with
// `go tool buildid`. If Go ever changes HashToString, this catches it.
func TestExpectedBuildIDAction_Golden(t *testing.T) {
	const actionID = "10f94fc02dcc245820dd861f4c6c25dee23ceb750f6be498fe84f67dfd2f1f9b"
	require.Equal(t, "EPlPwC3MJFgg3YYfTGwl", ExpectedBuildIDAction(actionID))
}

func TestExpectedBuildIDAction_ShortOrInvalid(t *testing.T) {
	require.Equal(t, "", ExpectedBuildIDAction(""))         // empty
	require.Equal(t, "", ExpectedBuildIDAction("aabbccdd")) // shorter than BuildIDHashSize
	require.Equal(t, "", ExpectedBuildIDAction("zzzz"))     // not hex
	// Exactly BuildIDHashSize bytes is the minimum that yields a value.
	require.NotEqual(t, "", ExpectedBuildIDAction(strings.Repeat("ab", 15)))
}

func TestArchiveBuildIDAction(t *testing.T) {
	require.Equal(t, "EPlPwC3MJFgg3YYfTGwl", archiveBuildIDAction(archiveWithBuildID("EPlPwC3MJFgg3YYfTGwl")))

	// Not an archive at all.
	require.Equal(t, "", archiveBuildIDAction([]byte("not an archive")))
	require.Equal(t, "", archiveBuildIDAction(nil))

	// A PKGDEF with no build id line (e.g. `go tool compile` output).
	noID := buildAr("__.PKGDEF", []byte("go object linux amd64 go1.24.7\n\n$$B\nu\x00\n$$\n"))
	require.Equal(t, "", archiveBuildIDAction(noID))
}

func TestBuildIDMatchesAction(t *testing.T) {
	const actionA = "10f94fc02dcc245820dd861f4c6c25dee23ceb750f6be498fe84f67dfd2f1f9b" // -> EPlPwC3MJFgg3YYfTGwl
	actionB := strings.Repeat("ab", 32)                                                // a different action
	wantA := ExpectedBuildIDAction(actionA)
	wantB := ExpectedBuildIDAction(actionB)
	require.NotEqual(t, wantA, wantB)

	// The correct object for A matches.
	_, ok := BuildIDMatchesAction(actionA, archiveWithBuildID(wantA))
	require.True(t, ok)

	// B's compiled object served under A's key: proven cross-contamination.
	got, ok := BuildIDMatchesAction(actionA, archiveWithBuildID(wantB))
	require.False(t, ok)
	require.Equal(t, wantB, got)

	// A non-archive body has nothing to verify and must never be reported as a mismatch.
	_, ok = BuildIDMatchesAction(actionA, []byte("vet facts, not an archive"))
	require.True(t, ok)

	// A too-short requested key (no derivable expectation) must not false-positive.
	_, ok = BuildIDMatchesAction("aabbccdd", archiveWithBuildID(wantB))
	require.True(t, ok)

	// A stripped build id under a real key must be refused, not pass as "no build id".
	_, ok = BuildIDMatchesAction(actionA, archivePkgdefNoBuildID())
	require.False(t, ok, "a package archive without a build id must be refused")

	// ...but with no derivable expectation (short key) it must not false-positive.
	_, ok = BuildIDMatchesAction("aabbccdd", archivePkgdefNoBuildID())
	require.True(t, ok)
}

func TestArchiveExportInfo(t *testing.T) {
	// A stamped package archive: isPkgArchive true, action extracted.
	isPkg, action := ArchiveExportInfo(archiveWithBuildID("EPlPwC3MJFgg3YYfTGwl"))
	require.True(t, isPkg)
	require.Equal(t, "EPlPwC3MJFgg3YYfTGwl", action)

	// A package archive with no build id: isPkgArchive true, action "".
	isPkg, action = ArchiveExportInfo(archivePkgdefNoBuildID())
	require.True(t, isPkg, "an ar archive with __.PKGDEF is a package archive even without a build id")
	require.Equal(t, "", action)

	// A non-archive: isPkgArchive false.
	isPkg, action = ArchiveExportInfo([]byte("vet facts, not an archive"))
	require.False(t, isPkg)
	require.Equal(t, "", action)
}

// TestWebBackend_GetRejectsBuildIDMismatch is the core regression guard: a body
// that hashes to its advertised outputID (so it passes the existing checksum
// gate) but whose build id belongs to a DIFFERENT action -- the exact
// "internal/reflectlite served for the runtime action" poisoning -- must be
// refused as a miss and its key evicted so a recompute re-uploads it clean.
func TestWebBackend_GetRejectsBuildIDMismatch(t *testing.T) {
	hermeticOTel(t)
	const actionID = "10f94fc02dcc245820dd861f4c6c25dee23ceb750f6be498fe84f67dfd2f1f9b"
	otherAction := strings.Repeat("ab", 32) // a different action's key
	poison := string(archiveWithBuildID(ExpectedBuildIDAction(otherAction)))
	outputID := testOutputID(poison) // self-consistent: body hashes to its id

	objectPath := "/testbucket/go-buildcache/v1" + actionID
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != objectPath {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("X-Cache-Meta-Outputid", outputID)
		w.WriteHeader(200)
		c, _ := Compress([]byte(poison))
		w.Write(c)
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "testkey", SecretKey: "testsecret",
	})
	require.NoError(t, err)
	defer b.Close()
	primeIndex(b, actionID)

	contains := func() bool {
		b.keysMu.RLock()
		defer b.keysMu.RUnlock()
		return b.keys.Contains(b.key(actionID))
	}
	require.True(t, contains(), "precondition: key is in the index")

	_, _, _, _, miss, err := b.Get(actionID)
	require.NoError(t, err)
	require.True(t, miss, "a build-id-mismatched object must be a miss, never served")
	require.Equal(t, uint32(1), b.MissBuildID.Load())
	require.Equal(t, uint32(0), b.MissChecksum.Load(), "must be reported as build-id, not checksum")
	require.Equal(t, uint32(1), b.Stats.Corrupt.Load())
	require.Equal(t, uint32(0), b.Stats.Hits.Load())
	require.False(t, contains(), "poisoned key must be evicted so a recompute re-uploads it clean")
}

// TestWebBackend_GetRejectsStrippedBuildID is the deliberate-evasion guard: a
// package archive (loadable __.PKGDEF export data) that hashes to its advertised
// outputID but carries NO build id -- a corrupt object, or an object crafted with the
// build id stripped to evade the cross-check -- must be refused as a miss and
// its key evicted, exactly like a mismatched build id.
func TestWebBackend_GetRejectsStrippedBuildID(t *testing.T) {
	hermeticOTel(t)
	const actionID = "10f94fc02dcc245820dd861f4c6c25dee23ceb750f6be498fe84f67dfd2f1f9b"
	poison := string(archivePkgdefNoBuildID())
	outputID := testOutputID(poison) // self-consistent: passes the hash gate

	objectPath := "/testbucket/go-buildcache/v1" + actionID
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != objectPath {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("X-Amz-Meta-Outputid", outputID)
		w.WriteHeader(200)
		c, _ := Compress([]byte(poison))
		w.Write(c)
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "testkey", SecretKey: "testsecret",
	})
	require.NoError(t, err)
	defer b.Close()
	primeIndex(b, actionID)

	_, _, _, _, miss, err := b.Get(actionID)
	require.NoError(t, err)
	require.True(t, miss, "a package archive with no build id must be a miss, never served")
	require.Equal(t, uint32(1), b.MissBuildID.Load())
	require.Equal(t, uint32(0), b.Stats.Hits.Load())

	b.keysMu.RLock()
	stillKnown := b.keys.Contains(b.key(actionID))
	b.keysMu.RUnlock()
	require.False(t, stillKnown, "the stripped object's key must be evicted")
}

// TestWebBackend_GetServesMatchingBuildID is the positive control: an object
// whose build id matches the requested action is served as a hit.
func TestWebBackend_GetServesMatchingBuildID(t *testing.T) {
	hermeticOTel(t)
	const actionID = "10f94fc02dcc245820dd861f4c6c25dee23ceb750f6be498fe84f67dfd2f1f9b"
	good := string(archiveWithBuildID(ExpectedBuildIDAction(actionID)))
	outputID := testOutputID(good)

	objectPath := "/testbucket/go-buildcache/v1" + actionID
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != objectPath {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("X-Cache-Meta-Outputid", outputID)
		w.WriteHeader(200)
		c, _ := Compress([]byte(good))
		w.Write(c)
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "testkey", SecretKey: "testsecret",
	})
	require.NoError(t, err)
	defer b.Close()
	primeIndex(b, actionID)

	gotOutputID, body, _, _, miss, err := b.Get(actionID)
	require.NoError(t, err)
	require.False(t, miss)
	require.Equal(t, outputID, gotOutputID)
	require.Equal(t, uint32(0), b.MissBuildID.Load())
	require.Equal(t, uint32(1), b.Stats.Hits.Load())
	data, _ := io.ReadAll(body)
	require.Equal(t, good, string(data))
}

// TestGetBatch_RejectsBuildIDMismatch verifies the batch serve path applies the
// same cross-contamination guard as individual GETs.
func TestGetBatch_RejectsBuildIDMismatch(t *testing.T) {
	hermeticOTel(t)
	store := make(map[string][]byte)
	meta := make(map[string]map[string]string)
	srv := fakeBatchServer(t, store, meta)
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "key", SecretKey: "secret",
	})
	require.NoError(t, err)
	defer b.Close()

	const actionID = "10f94fc02dcc245820dd861f4c6c25dee23ceb750f6be498fe84f67dfd2f1f9b"
	otherAction := strings.Repeat("cd", 32)
	poison := string(archiveWithBuildID(ExpectedBuildIDAction(otherAction)))
	key := "go-buildcache/v1" + actionID
	compressed, _ := Compress([]byte(poison))
	store[key] = compressed
	meta[key] = map[string]string{"outputid": testOutputID(poison)} // self-consistent hash

	_, _, _, _, miss, err := b.getBatch(actionID, key)
	require.NoError(t, err)
	require.True(t, miss, "a batched build-id-mismatched entry must be a miss")
	require.Equal(t, uint32(1), b.MissBuildID.Load())
	require.Equal(t, uint32(0), b.MissChecksum.Load())
	require.Equal(t, uint32(1), b.Stats.Corrupt.Load())
	require.Equal(t, uint32(0), b.Stats.Hits.Load())
}

// TestWebBackend_PutRefusesBuildIDMismatch verifies poison can never be written
// to the shared cache: a Put whose body's build id disagrees with the key is
// skipped (no upload) and the optimistic index claim is released.
func TestWebBackend_PutRefusesBuildIDMismatch(t *testing.T) {
	hermeticOTel(t)
	const actionID = "10f94fc02dcc245820dd861f4c6c25dee23ceb750f6be498fe84f67dfd2f1f9b"
	otherAction := strings.Repeat("ef", 32)
	poison := archiveWithBuildID(ExpectedBuildIDAction(otherAction))

	var putHits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "PUT" {
			putHits++
		}
		w.WriteHeader(404) // index fetch etc.
	}))
	defer srv.Close()

	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: srv.URL,
		AccessKey: "key", SecretKey: "secret",
	})
	require.NoError(t, err)
	defer b.Close()

	err = b.Put(actionID, testOutputID(string(poison)), strings.NewReader(string(poison)), int64(len(poison)))
	require.NoError(t, err, "a refused poison upload is a skip, not an error")
	require.Equal(t, 0, putHits, "poison must never be uploaded to the shared cache")
	require.Equal(t, uint32(0), b.Stats.Puts.Load())

	b.keysMu.RLock()
	claimed := b.keys.Contains(b.key(actionID))
	b.keysMu.RUnlock()
	require.False(t, claimed, "the optimistic claim must be released so a later correct Put can run")
}
