package main

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"io"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

// decodeLeading is the slow, authoritative answer the fast path must agree
// with: actually decompress, and take the leading bytes.
func decodeLeading(tb testing.TB, frame []byte, n int) []byte {
	buf := make([]byte, n)
	got, _ := io.ReadFull(lz4.NewReader(bytes.NewReader(frame)), buf)
	return buf[:got]
}

// TestLz4LeadingBytes_AgreesWithDecoder is the core claim: whenever the header
// walk says it understood a frame, the bytes it returns are the bytes a real
// decompression would produce. Bodies span the shapes the cache actually
// stores: an ar archive, a module index, highly compressible runs, and
// incompressible random data at several sizes.
func TestLz4LeadingBytes_AgreesWithDecoder(t *testing.T) {
	random := make([]byte, 512<<10)
	_, err := rand.Read(random)
	require.NoError(t, err)

	bodies := map[string][]byte{
		"ar-archive":     append([]byte("!<arch>\ndebug/deadcode"), bytes.Repeat([]byte("go func() {}\n"), 4096)...),
		"module-index":   append([]byte("go index v2\n"), bytes.Repeat([]byte("modindex payload "), 4096)...),
		"tiny":           []byte("go index v2\n"),
		"compressible":   bytes.Repeat([]byte{0}, 1<<20),
		"incompressible": random,
	}
	for name, body := range bodies {
		t.Run(name, func(t *testing.T) {
			frame := lz4Compress(t, body)
			lead, ok := lz4LeadingBytes(frame, len(goModuleIndexMagic))
			require.True(t, ok, "a frame the client wrote must be understood")
			require.NotEmpty(t, lead)
			want := decodeLeading(t, frame, len(lead))
			require.Equal(t, want, lead, "leading bytes must match a real decode")

			// And the verdict the guard actually asks for.
			match, decided := lz4HasPrefix(frame, goModuleIndexMagic)
			require.True(t, decided, "a well-formed frame must be decidable")
			require.Equal(t, bytes.HasPrefix(body, []byte(goModuleIndexMagic)), match)
		})
	}
}

// TestLz4LeadingBytes_UncompressedBlock: lz4 stores an incompressible block
// verbatim with the high bit set in its size, a shape with no token or literal
// run at all.
func TestLz4LeadingBytes_UncompressedBlock(t *testing.T) {
	body := make([]byte, 64<<10)
	_, err := rand.Read(body)
	require.NoError(t, err)
	copy(body, "go index v2\n") // magic in an otherwise incompressible block

	var buf bytes.Buffer
	zw := lz4.NewWriter(&buf)
	_, err = zw.Write(body)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	frame := buf.Bytes()

	match, decided := lz4HasPrefix(frame, goModuleIndexMagic)
	require.True(t, decided)
	require.True(t, match, "the magic must be found in a stored-uncompressed block")
	require.True(t, looksLikeGoModuleIndex(frame, "lz4"))
}

// TestLz4LeadingBytes_UndecidableShapes: everything the walk does not fully
// understand must report "cannot tell" so the caller decodes for real. A wrong
// "not an index" here would let poison through, so each of these is a
// fail-to-the-slow-path, never a verdict.
func TestLz4LeadingBytes_UndecidableShapes(t *testing.T) {
	frame := lz4Compress(t, append([]byte("go index v2\n"), bytes.Repeat([]byte("x"), 8192)...))

	t.Run("truncated", func(t *testing.T) {
		for _, n := range []int{0, 3, 6, 8, 12} {
			_, ok := lz4LeadingBytes(frame[:n], 10)
			require.False(t, ok, "a %d-byte head cannot be understood", n)
		}
	})
	t.Run("not-a-frame", func(t *testing.T) {
		_, ok := lz4LeadingBytes([]byte("!<arch>\nplain uncompressed bytes"), 10)
		require.False(t, ok)
	})
	t.Run("dictionary-frame", func(t *testing.T) {
		dict := append([]byte(nil), frame...)
		dict[4] |= 0x01 // FLG: DictID set
		_, ok := lz4LeadingBytes(dict, 10)
		require.False(t, ok, "a dict frame's first block may match into the dict")
	})
	t.Run("wrong-version", func(t *testing.T) {
		bad := append([]byte(nil), frame...)
		bad[4] = (bad[4] & 0x3f) | 0x80 // version bits = 10
		_, ok := lz4LeadingBytes(bad, 10)
		require.False(t, ok)
	})
	t.Run("endmark-only", func(t *testing.T) {
		empty := lz4Compress(t, nil)
		_, ok := lz4LeadingBytes(empty, 10)
		require.False(t, ok)
	})

	// Undecidable input must still reach the right answer through the fallback.
	require.True(t, looksLikeGoModuleIndex(frame, "lz4"))
	isIndex, err := readIsModuleIndex(bytes.NewReader(frame), "lz4")
	require.NoError(t, err)
	require.True(t, isIndex)
}

// TestLz4HasPrefix_ShortLiteralRunDecides: a literal run shorter than the magic
// still settles the common case, because a compiled object diverges from
// "go index v" within its first few bytes. Only a run that MATCHES so far is
// undecidable.
func TestLz4HasPrefix_ShortLiteralRunDecides(t *testing.T) {
	shortRun := func(t *testing.T, lead string, litLen int) []byte {
		t.Helper()
		var b []byte
		b = binary.LittleEndian.AppendUint32(b, lz4FrameMagic)
		b = append(b, 0x60, 0x70, 0x00) // FLG (version 1, B.Indep), BD, HC
		block := []byte{byte(litLen << 4)}
		block = append(block, lead...)
		b = binary.LittleEndian.AppendUint32(b, uint32(len(block)))
		return append(b, block...)
	}

	match, decided := lz4HasPrefix(shortRun(t, "!<ar", 4), goModuleIndexMagic)
	require.True(t, decided, "a diverging run is a verdict")
	require.False(t, match)

	_, decided = lz4HasPrefix(shortRun(t, "go i", 4), goModuleIndexMagic)
	require.False(t, decided, "a run that still matches cannot decide alone")
}

// TestModuleIndexGuard_FastPathMatchesSlowPath pins the property that matters
// operationally: for every body shape, the guard's verdict is the same whether
// it took the header walk or the decoder.
func TestModuleIndexGuard_FastPathMatchesSlowPath(t *testing.T) {
	for _, body := range [][]byte{
		[]byte("go index v2\n" + string(bytes.Repeat([]byte("a"), 5000))),
		[]byte("!<arch>\n" + string(bytes.Repeat([]byte("b"), 5000))),
		[]byte("go index"),   // a prefix of the magic, not the magic
		[]byte("go index v"), // exactly the magic
		[]byte("nope"),
		{},
	} {
		frame := lz4Compress(t, body)
		want := bytes.HasPrefix(body, []byte(goModuleIndexMagic))
		require.Equal(t, want, looksLikeGoModuleIndex(frame, "lz4"), "PUT guard on %q", body[:min(len(body), 12)])
		got, err := readIsModuleIndex(bytes.NewReader(frame), "lz4")
		require.NoError(t, err)
		require.Equal(t, want, got, "read guard on %q", body[:min(len(body), 12)])
	}
}
