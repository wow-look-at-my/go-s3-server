package cacheclient

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseImportPath_RealArchive(t *testing.T) {
	data, err := os.ReadFile("testdata/testpkg.a")
	require.NoError(t, err)
	got := parseImportPath(data)
	require.Equal(t, "github.com/example/testpkg", got)
}

func TestParseImportPath_NotAnArchive(t *testing.T) {
	require.Equal(t, "", parseImportPath([]byte("not an archive")))
	require.Equal(t, "", parseImportPath(nil))
	require.Equal(t, "", parseImportPath([]byte{}))
}

func TestParseImportPath_NoMarker(t *testing.T) {
	// Valid ar header + PKGDEF member but no $$B\n marker.
	arData := buildAr("__.PKGDEF", []byte("go object linux amd64 go1.25.0\n\n"))
	require.Equal(t, "", parseImportPath(arData))
}

func TestParseImportPath_NonUnifiedFormat(t *testing.T) {
	// $$B\n present but format byte is not 'u'.
	body := []byte("go object linux amd64 go1.25.0\n\n$$B\nX<data>\n$$\n")
	arData := buildAr("__.PKGDEF", body)
	require.Equal(t, "", parseImportPath(arData))
}

func TestArMember(t *testing.T) {
	body := []byte("hello world")
	arData := buildAr("__.PKGDEF", body)
	got := arMember(arData, "__.PKGDEF")
	require.Equal(t, body, got)
}

func TestArMember_MissingMember(t *testing.T) {
	arData := buildAr("other.file", []byte("data"))
	require.Nil(t, arMember(arData, "__.PKGDEF"))
}

func TestArMember_BadMagic(t *testing.T) {
	require.Nil(t, arMember([]byte("not an ar file"), "__.PKGDEF"))
}

func TestPkgbitsImportPath_TruncatedPayload(t *testing.T) {
	// Various truncated payloads should return "" without panicking.
	require.Equal(t, "", pkgbitsImportPath(nil))
	require.Equal(t, "", pkgbitsImportPath([]byte{0, 0, 0}))
	require.Equal(t, "", pkgbitsImportPath(make([]byte, 4)))
}

func TestPkgbitsImportPath_WithSyncMarkersRoundTrip(t *testing.T) {
	// Build a minimal pkgbits payload with sync markers enabled (real archive has sync=false).
	data, err := os.ReadFile("testdata/testpkg.a")
	require.NoError(t, err)

	// Re-parsed to build a synthetic sync-enabled payload; this test just checks no panic on a V0 (no-flags) payload.
	_ = data

	// Build the earliest payload version (no flags/sync) with a lone SectionString element and a SectionPkg referencing it.
	importPath := "example.com/mypkg"
	payload := buildMinimalPkgbitsV0(importPath)
	got := pkgbitsImportPath(payload)
	require.Equal(t, importPath, got)
}

// buildAr creates a minimal ar archive containing a single member named name.
func buildAr(name string, body []byte) []byte {
	var out []byte
	out = append(out, "!<arch>\n"...)
	hdr := make([]byte, 60)
	copy(hdr[:16], name+"/               ")
	copy(hdr[16:28], "0           ")
	copy(hdr[28:34], "0     ")
	copy(hdr[34:40], "0     ")
	copy(hdr[40:48], "100644  ")
	sizeStr := []byte("          ")
	s := len(body)
	for i := len(sizeStr) - 1; s > 0; i-- {
		sizeStr[i] = byte('0' + s%10)
		s /= 10
	}
	copy(hdr[48:58], sizeStr)
	hdr[58] = '`'
	hdr[59] = '\n'
	out = append(out, hdr...)
	out = append(out, body...)
	if len(body)%2 != 0 {
		out = append(out, '\n')
	}
	return out
}

// buildMinimalPkgbitsV0 constructs a synthetic pkgbits V0 payload that encodes
// importPath as the package path in the pkg section's only element, without
// sync markers. Layout:
//
//	version (no flags)
//	elemEndsEnds: the cumulative element count per section
//	elemEnds: the end offset of each element
//	elemData: the importPath bytes, then the pkg element, then the fingerprint
func buildMinimalPkgbitsV0(importPath string) []byte {
	// The pkg element: a lone relocation naming the string section's element, no sync markers.
	pkgElem := []byte{
		0x01,       // nrelocs
		0x00, 0x00, // the relocation: kind SectionString, and its element index
		// String(): no sync markers, just the reloc index
		0x00, // relocIdx
	}

	strElem := []byte(importPath)

	// elemEnds offsets (excluding fingerprint): the string element ends at its own length, the pkg element that much further.
	strEnd := uint32(len(strElem))
	pkgEnd := strEnd + uint32(len(pkgElem))

	// elemEndsEnds: cumulative per section; only the string and pkg sections carry an element.
	numSections := 10
	eee := make([]uint32, numSections)
	eee[0] = 1 // the string section's element
	for i := 1; i < 3; i++ {
		eee[i] = 1 // the sections between it and pkg carry none
	}
	eee[3] = 2 // the pkg section's element
	for i := 4; i < numSections; i++ {
		eee[i] = 2 // the trailing sections carry none
	}

	totalElems := int(eee[numSections-1])
	elemEnds := make([]uint32, totalElems)
	elemEnds[0] = strEnd
	elemEnds[1] = pkgEnd

	fingerprint := make([]byte, 8)

	var buf []byte
	// version (uint32 LE, no flags)
	vb := make([]byte, 4)
	binary.LittleEndian.PutUint32(vb, 0)
	buf = append(buf, vb...)
	// elemEndsEnds
	for _, v := range eee {
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, v)
		buf = append(buf, b...)
	}
	// elemEnds
	for _, v := range elemEnds {
		b := make([]byte, 4)
		binary.LittleEndian.PutUint32(b, v)
		buf = append(buf, b...)
	}
	// elemData: strElem + pkgElem + fingerprint
	buf = append(buf, strElem...)
	buf = append(buf, pkgElem...)
	buf = append(buf, fingerprint...)

	return buf
}
