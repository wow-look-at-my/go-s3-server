package cacheclient

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// buildPkgbitsWithSources constructs a synthetic pkgbits V0 payload (no sync
// markers) that encodes importPath as the SectionPkg element's package path and each
// srcPaths entry as a SectionPosBase file name — enough structure for
// parseImportPath and parseSourceFiles to decode real values, so the
// provenance tests exercise the same extraction path a genuine `go build`
// archive takes.
func buildPkgbitsWithSources(importPath string, srcPaths []string) []byte {
	strs := append([]string{importPath}, srcPaths...)

	// Element bodies: strings, then a PosBase per source path, then Pkg.
	var elems [][]byte
	for _, s := range strs {
		elems = append(elems, []byte(s))
	}
	for i := range srcPaths {
		elems = append(elems, []byte{0x01, 0x00, byte(1 + i), 0x00})
	}
	elems = append(elems, []byte{0x01, 0x00, 0x00, 0x00})

	// Cumulative element counts per section, in section order: String, PosBase, Pkg.
	nStr := uint32(len(strs))
	nPos := uint32(len(srcPaths))
	eee := make([]uint32, pbNumSections)
	eee[0] = nStr
	eee[1] = nStr
	eee[2] = nStr + nPos
	for i := 3; i < pbNumSections; i++ {
		eee[i] = nStr + nPos + 1
	}

	var elemData []byte
	elemEnds := make([]uint32, 0, len(elems))
	for _, e := range elems {
		elemData = append(elemData, e...)
		elemEnds = append(elemEnds, uint32(len(elemData)))
	}

	var buf []byte
	le := func(v uint32) []byte {
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, v)
		return b
	}
	buf = append(buf, le(0)...) // the earliest version: no flags word, no sync markers
	for _, v := range eee {
		buf = append(buf, le(v)...)
	}
	for _, v := range elemEnds {
		buf = append(buf, le(v)...)
	}
	buf = append(buf, elemData...)
	buf = append(buf, make([]byte, 8)...) // fingerprint
	return buf
}

// provenanceAction returns a (hex actionID, build-id action) pair that agree:
// action is base64.RawURLEncoding of actionID's leading bytes, the stamp
// `go build` writes, so an archive carrying it passes the build-id guard.
func provenanceAction() (actionHex, action string) {
	raw15 := []byte("provenance-15bb") // exactly BuildIDHashSize bytes
	action = base64.RawURLEncoding.EncodeToString(raw15)
	actionHex = hex.EncodeToString(append(raw15, make([]byte, 17)...))
	return actionHex, action
}

// provenanceArchive builds a Go package archive whose __.PKGDEF carries a
// header line, a build id for the given action, and a decodable pkgbits
// payload — so every provenance metadata field (pkg, src, go-version, target)
// has a real value to extract.
func provenanceArchive(action string) []byte {
	pb := buildPkgbitsWithSources("example.com/provpkg",
		[]string{"/work/src/alpha.go", "/work/src/beta.go"})
	body := "go object linux amd64 go1.25.0\n" +
		"build id \"" + action + "/Cw9xV7fakecontentid\"\n\n\n" +
		"$$B\nu" + string(pb) + "\n$$\n"
	return buildAr("__.PKGDEF", []byte(body))
}

func TestProvenanceArchive_Decodes(t *testing.T) {
	_, action := provenanceAction()
	raw := provenanceArchive(action)
	require.Equal(t, "example.com/provpkg", parseImportPath(raw))
	require.Equal(t, []string{"alpha.go", "beta.go"}, parseSourceFiles(raw))
	goVer, target := parseArchiveHeader(raw)
	require.Equal(t, "go1.25.0", goVer)
	require.Equal(t, "linux/amd64", target)
	require.Equal(t, action, archiveBuildIDAction(raw))
}

// requireProvenanceMeta asserts the full provenance metadata set assembled by
// Put — identical between the batch manifest and single-PUT headers, since
// both derive from the same map.
func requireProvenanceMeta(t *testing.T, md map[string]string, outputID string) {
	t.Helper()
	require.Equal(t, outputID, md["outputid"])
	require.Equal(t, "go-archive", md["object-type"])
	require.Equal(t, "lz4", md["compression"])
	require.Equal(t, "example.com/provpkg", md["pkg"])
	require.Equal(t, "alpha.go beta.go", md["src"])
	require.Equal(t, "go1.25.0", md["go-version"])
	require.Equal(t, "linux/amd64", md["target"])
	require.Equal(t, "example.com/mymod", md["module"])
	require.Equal(t, "v-test", md["toolchain-version"])
	require.NotEmpty(t, md["body-size"])
	require.NotEmpty(t, md["created"])
}

func newProvenanceBackend(t *testing.T, url string) *WebBackend {
	t.Helper()
	b, err := NewWebBackend(WebConfig{
		Bucket: "testbucket", Endpoint: url,
		AccessKey: "k", SecretKey: "s",
		Version: "v-test", Module: "example.com/mymod",
	})
	require.NoError(t, err)
	return b
}

// TestPutProvenance_BatchManifestMetadata proves the full provenance metadata
// set (pkg / src / go-version / target / module / toolchain-version and the
// protocol fields) travels in the /_batch/put manifest — the primary upload
// path — by inspecting what the client actually shipped.
func TestPutProvenance_BatchManifestMetadata(t *testing.T) {
	hermeticOTel(t)
	t.Setenv("GO_TOOLCHAIN_CACHE_PUT_WINDOW_MS", "5000")

	var mu sync.Mutex
	var entries []batchPutManifestEntry
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/testbucket/_batch/put" {
			manifest, _ := parsePutTarSafe(r.Body)
			mu.Lock()
			entries = append(entries, manifest.Entries...)
			mu.Unlock()
			writePutResults(w, manifest, func(string) string { return "stored" })
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	b := newProvenanceBackend(t, srv.URL)

	actionHex, action := provenanceAction()
	raw := provenanceArchive(action)
	sum := sha256.Sum256(raw)
	outputID := hex.EncodeToString(sum[:])
	require.NoError(t, b.Put(actionHex, outputID, bytes.NewReader(raw), int64(len(raw))))
	require.NoError(t, b.Close()) // drains the coalescer: ships the batch

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, entries, 1)
	require.Equal(t, "go-buildcache/v1"+actionHex, entries[0].Key)
	requireProvenanceMeta(t, entries[0].Metadata, outputID)
}

// TestPutProvenance_SinglePutHeaders proves the same metadata set arrives as
// X-Cache-Meta-* headers on the single-PUT fallback path (server without
// /_batch/put), keeping both upload paths byte-equivalent.
func TestPutProvenance_SinglePutHeaders(t *testing.T) {
	hermeticOTel(t)

	var mu sync.Mutex
	var gotHeaders http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/testbucket/_batch/put" {
			w.WriteHeader(http.StatusMethodNotAllowed) // no batch endpoint
			return
		}
		if r.Method == "PUT" {
			mu.Lock()
			gotHeaders = r.Header.Clone()
			mu.Unlock()
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	b := newProvenanceBackend(t, srv.URL)
	defer b.Close()

	actionHex, action := provenanceAction()
	raw := provenanceArchive(action)
	sum := sha256.Sum256(raw)
	outputID := hex.EncodeToString(sum[:])
	require.NoError(t, b.Put(actionHex, outputID, bytes.NewReader(raw), int64(len(raw))))

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return gotHeaders != nil
	}, 2*time.Second, 10*time.Millisecond, "the object must arrive as a single PUT after the 405")

	mu.Lock()
	defer mu.Unlock()
	md := map[string]string{}
	for name, vals := range gotHeaders {
		if rest, ok := strings.CutPrefix(name, "X-Cache-Meta-"); ok && len(vals) > 0 {
			md[strings.ToLower(rest)] = vals[0]
		}
	}
	requireProvenanceMeta(t, md, outputID)
}

func TestCapSrcList(t *testing.T) {
	// Under both caps: unchanged.
	require.Equal(t, "a.go b.go", capSrcList([]string{"a.go", "b.go"}))
	require.Equal(t, "", capSrcList(nil))

	// More than srcMetaMaxFiles names: the leading ones plus a "+N more" summary.
	var many []string
	for i := 0; i < 20; i++ {
		many = append(many, fmt.Sprintf("f%02d.go", i))
	}
	got := capSrcList(many)
	require.Equal(t, "f00.go f01.go f02.go f03.go f04.go f05.go f06.go f07.go +12 more", got)
	require.LessOrEqual(t, len(got), srcMetaMaxBytes)

	// Long names: trimmed until the value fits the byte cap.
	long := strings.Repeat("x", 60) + "%d.go"
	var wide []string
	for i := 0; i < 8; i++ {
		wide = append(wide, fmt.Sprintf(long, i))
	}
	got = capSrcList(wide)
	require.LessOrEqual(t, len(got), srcMetaMaxBytes)
	require.Contains(t, got, "more")

	// A single name longer than the whole budget degrades to just the summary.
	huge := strings.Repeat("y", srcMetaMaxBytes+10) + ".go"
	require.Equal(t, "+1 more", capSrcList([]string{huge}))
}
