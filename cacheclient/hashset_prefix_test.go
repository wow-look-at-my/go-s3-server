package cacheclient

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// shortBlob encodes the first gbciPrefixSize bytes of each sorted hash, the way
// the server's short form does.
func shortBlob(hashes ...actionHash) []byte {
	blob := make([]byte, gbciHeaderSize+len(hashes)*gbciPrefixSize+sha256.Size)
	copy(blob[0:4], gbciMagic[:])
	blob[4] = gbciVersion
	blob[5] = gbciPrefixSize
	binary.LittleEndian.PutUint64(blob[16:24], uint64(len(hashes)))
	off := gbciHeaderSize
	for _, h := range hashes {
		off += copy(blob[off:], h[:gbciPrefixSize])
	}
	digest := sha256.Sum256(blob[:off])
	copy(blob[off:], digest[:])
	return blob
}

func fullHash(lead, tail byte) actionHash {
	var h actionHash
	h[0], h[31] = lead, tail
	return h
}

// A set parsed from the short form answers lookups made with full hashes, by
// their first bytes.
func TestShortIndexAnswersFullHashLookups(t *testing.T) {
	keys, _, err := parseIndexBlob(shortBlob(fullHash(1, 0), fullHash(2, 0)))
	require.NoError(t, err)
	require.True(t, keys.prefixOnly)
	require.True(t, keys.Contains(fullHash(1, 0xaa)), "only the first bytes decide")
	require.False(t, keys.Contains(fullHash(3, 0)))

	keys.Add(fullHash(4, 0x55))
	require.True(t, keys.Contains(fullHash(4, 0x99)))
	keys.Remove(fullHash(1, 0x11))
	require.False(t, keys.Contains(fullHash(1, 0)))
}

// A short delta merged into a set from a full-width blob cuts that set first.
// The delta's keys are found by full-hash lookups.
func TestShortDeltaMergesIntoAFullSet(t *testing.T) {
	live := newHashSetFromSorted([]actionHash{fullHash(1, 7), fullHash(2, 7)})
	delta, _, err := parseIndexBlob(shortBlob(fullHash(3, 0)))
	require.NoError(t, err)

	live.addAll(delta)
	require.True(t, live.prefixOnly)
	for _, h := range []actionHash{fullHash(1, 7), fullHash(2, 7), fullHash(3, 9)} {
		require.True(t, live.Contains(h))
	}
	require.Equal(t, 3, live.Len())
}
