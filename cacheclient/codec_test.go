package cacheclient

import (
	"bytes"
	"testing"

	"github.com/pierrec/lz4/v4"
	"github.com/stretchr/testify/require"
)

// lz4Frame is what this cache used to write, and what most of the objects in a
// live store still are.
func lz4Frame(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := lz4.NewWriter(&buf)
	_, err := w.Write(data)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	return buf.Bytes()
}

// The codec switch is not a migration. A store holds both kinds for as long as
// the old objects live, and a reader settles which is which from the body's own
// frame magic rather than from metadata that can be absent or wrong.
func TestDecompressReadsBothCodecs(t *testing.T) {
	body := bytes.Repeat([]byte("a go archive, mostly repetitive. "), 400)

	zstd, err := Compress(body)
	require.NoError(t, err)
	require.Equal(t, uint32(zstdMagic), codecMagic(zstd), "Compress must write zstd")

	old := lz4Frame(t, body)
	require.Equal(t, uint32(lz4Magic), codecMagic(old))

	for name, stored := range map[string][]byte{"zstd": zstd, "lz4": old} {
		t.Run(name, func(t *testing.T) {
			got, err := Decompress(stored)
			require.NoError(t, err)
			require.Equal(t, body, got)

			// The sized path is the one every hit takes, and it must agree.
			got, err = DecompressSized(stored, int64(len(body)))
			require.NoError(t, err)
			require.Equal(t, body, got)

			// A wrong declared size is a starting capacity, never a promise.
			got, err = DecompressSized(stored, 7)
			require.NoError(t, err)
			require.Equal(t, body, got)

			got, err = DecompressSized(stored, maxPresizedBody+1)
			require.NoError(t, err)
			require.Equal(t, body, got)
		})
	}
}

// The point of the switch: fewer bytes on a link the build is waiting on.
func TestZstdCarriesFewerBytesThanLz4(t *testing.T) {
	body := bytes.Repeat([]byte("package main\nimport \"fmt\"\nfunc main() {}\n"), 500)

	zstd, err := Compress(body)
	require.NoError(t, err)
	old := lz4Frame(t, body)

	require.Less(t, len(zstd), len(old),
		"zstd=%d lz4=%d: the codec exists to shrink the wire", len(zstd), len(old))
}

// A body that is neither frame must not be mistaken for one.
func TestDecompressRefusesGarbage(t *testing.T) {
	_, err := Decompress([]byte("not a compressed frame at all"))
	require.Error(t, err)
}
