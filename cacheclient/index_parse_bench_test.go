package cacheclient

import (
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"testing"
)

// The disk copy of the index is parsed by every go command a build starts,
// before its first lookup. This is that parse, for a short-form blob of a
// keys.
func BenchmarkParseShortIndex(b *testing.B) {
	const count = 1_000_000
	prefixes := make([]uint64, count)
	for idx := range prefixes {
		sum := sha256.Sum256(binary.BigEndian.AppendUint64(nil, uint64(idx)))
		prefixes[idx] = binary.BigEndian.Uint64(sum[:8])
	}
	slices.Sort(prefixes)
	prefixes = slices.Compact(prefixes)
	blob := make([]byte, gbciHeaderSize+len(prefixes)*8+sha256.Size)
	copy(blob[0:4], gbciMagic[:])
	blob[4] = gbciVersion
	blob[5] = 8
	binary.LittleEndian.PutUint64(blob[16:24], uint64(len(prefixes)))
	off := gbciHeaderSize
	for _, key := range prefixes {
		binary.BigEndian.PutUint64(blob[off:], key)
		off += 8
	}
	digest := sha256.Sum256(blob[:off])
	copy(blob[off:], digest[:])

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, _, err := parseIndexBlob(blob); err != nil {
			b.Fatal(err)
		}
	}
}
