package main

import (
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompactKeyRoundTrip(t *testing.T) {
	for _, key := range []string{
		gbciKeyPrefix + hex64('a'),
		gbciKeyPrefix + hex64('0'),
		"plain-key",
		"go-buildcache/v1short",
		gbciKeyPrefix + hex64('z'), // 'z' is not hex: not a cacheprog key
	} {
		ck := newCompactKey(key)
		assert.Equal(t, key, ck.Key(), "a compact key must rebuild the key it was made from")
	}
}

func TestCompactKeyDistinguishesKinds(t *testing.T) {
	cacheprog := newCompactKey(gbciKeyPrefix + hex64('a'))
	_, ok := cacheprog.actionHash()
	assert.True(t, ok, "a well-formed cacheprog key carries its action hash inline")

	other := newCompactKey("plain-key")
	_, ok = other.actionHash()
	assert.False(t, ok, "a key outside the pattern has no action hash")

	assert.NotEqual(t, cacheprog, other)
	assert.Equal(t, cacheprog, newCompactKey(gbciKeyPrefix+hex64('a')),
		"equal keys must compare equal, since compactKey is used as a map key")
}

// TestCompactKeyCostsNothingPerKey pins the reason this type exists. The index,
// the eviction sweep and the access map each hold one key per stored object; at
// a million objects the previous string form cost ~80 bytes and one GC object
// apiece on top of the entry itself. Storing the action ID inline has to stay
// both allocation-free to build and small enough that a million of them is tens
// of megabytes, not hundreds.
func TestCompactKeyCostsNothingPerKey(t *testing.T) {
	key := gbciKeyPrefix + hex64('a')
	var sink compactKey
	allocs := testing.AllocsPerRun(100, func() { sink = newCompactKey(key) })
	require.Zero(t, allocs, "building a compact key must not allocate")
	require.Equal(t, key, sink.Key())

	assert.LessOrEqual(t, unsafe.Sizeof(compactKey{}), uintptr(48))
	assert.LessOrEqual(t, unsafe.Sizeof(indexEntry{}), uintptr(56),
		"one of these per indexed object: growth here is measured in hundreds of MiB")
	assert.LessOrEqual(t, unsafe.Sizeof(evictionVictim{}), uintptr(72))
}
