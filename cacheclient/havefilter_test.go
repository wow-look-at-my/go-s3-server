package cacheclient

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

// vectorHash is the test vector's input: a hash filled from a single byte, so
// both implementations of the filter can build the same one.
func vectorHash(fill byte) actionHash {
	var h actionHash
	for i := range h {
		h[i] = fill
	}
	return h
}

// haveFilterVectorBits is the filter over vectorHash(1..4) in 32 bytes at k=3,
// as a hex sha256 of the bit array.
//
// The server has its own copy of this filter, in havefilter.go at the repo
// root, and its own test asserting this same constant. A change to the bit
// arithmetic on one side and not the other fails one of the two tests instead
// of silently making suppression wrong on the wire.
const haveFilterVectorBits = "c2643f18fd57b6aa9bb7cb286f32b9ee7c655c83b95e78ca11591a166bd6658a"

func TestHaveFilterVector(t *testing.T) {
	f := &haveFilter{Bits: make([]byte, 32), K: 3}
	m := uint32(len(f.Bits) * 8)
	for _, fill := range []byte{1, 2, 3, 4} {
		h := vectorHash(fill)
		for i := 0; i < f.K; i++ {
			idx := haveFilterBit(h, i, m)
			f.Bits[idx/8] |= 1 << (idx % 8)
		}
	}
	sum := sha256.Sum256(f.Bits)
	require.Equal(t, haveFilterVectorBits, hex.EncodeToString(sum[:]),
		"the wire filter's bit arithmetic changed; the server's copy must change with it")
}

// TestHaveFilterNeverLosesAKey pins the direction the filter fails in. A key
// it was given must always read back as held: the error is a false positive,
// which costs one un-sent body, never a false negative, which would re-send a
// body the client already has.
func TestHaveFilterNeverLosesAKey(t *testing.T) {
	hashes := make([]actionHash, 500)
	for i := range hashes {
		hashes[i] = sha256.Sum256([]byte{byte(i), byte(i >> 8)})
	}
	f := newHaveFilter(len(hashes), slices.Values(hashes))
	require.NotNil(t, f)
	for _, h := range hashes {
		require.True(t, f.contains(h), "the filter must never lose a key it was given")
	}

	var falsePositives int
	const probes = 5000
	for i := range probes {
		h := sha256.Sum256([]byte{0xff, byte(i), byte(i >> 8)})
		if f.contains(h) {
			falsePositives++
		}
	}
	require.Less(t, falsePositives, probes/100,
		"at %d bits per key the rate must stay under a percent; got %d in %d", haveFilterBitsPerKey, falsePositives, probes)
}

// TestHaveFilterSizing pins the wire bounds. A tiny held set still gets a
// floor rather than a handful of saturated bytes, and a huge one is capped
// rather than sending a megabyte on every prefetch request.
func TestHaveFilterSizing(t *testing.T) {
	require.Equal(t, haveFilterMinBytes, haveFilterBytes(1))
	require.Equal(t, haveFilterMaxBytes, haveFilterBytes(1<<20))
	require.Equal(t, 10000*haveFilterBitsPerKey/8, haveFilterBytes(10000))

	var none *haveFilter
	require.Nil(t, newHaveFilter(0, slices.Values([]actionHash{})), "a client holding nothing states nothing")
	require.False(t, none.contains(vectorHash(1)))
}

// TestHeldFilterStatesWhatTheClientReceived pins what goes into the filter:
// the objects this process actually has, which is not the index.
func TestHeldFilterStatesWhatTheClientReceived(t *testing.T) {
	wantedID := testActionID(0x77)
	f := newPrefetchFixture(t, "")
	b, err := NewWebBackend(WebConfig{Bucket: "bk", Endpoint: f.srv.URL, AccessKey: "k", SecretKey: "s"})
	require.NoError(t, err)
	require.Nil(t, b.heldFilter(), "a process that has received nothing states nothing")

	f.store(b.KeyPrefix(), wantedID, "a body", testOutputID("a body"))
	_, _, _, miss := b.Get(wantedID)
	require.False(t, miss)
	require.NoError(t, b.Close())

	filter := b.heldFilter()
	require.NotNil(t, filter, "a received body is something the client holds")
	h, ok := parseActionHash(wantedID)
	require.True(t, ok)
	require.True(t, filter.contains(h))

	// It is what a prefetch request carries.
	body, err := json.Marshal(batchGetRequest{Keys: []string{"k"}, Prefetch: true, Have: filter})
	require.NoError(t, err)
	require.Contains(t, string(body), `"have"`)

	var round batchGetRequest
	require.NoError(t, json.Unmarshal(body, &round))
	require.NotNil(t, round.Have)
	require.True(t, round.Have.contains(h), "the filter must survive the wire")
}
