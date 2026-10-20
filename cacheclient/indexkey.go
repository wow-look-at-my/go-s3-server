package cacheclient

import (
	"bytes"
	"encoding/binary"
	"iter"
	"slices"
)

// indexKeySize is how many leading bytes of an action hash the index holds per key.
const indexKeySize = 8

// indexKey is the leading indexKeySize bytes of an action hash.
type indexKey = [indexKeySize]byte

// keyOf is the indexKey of h.
func keyOf(h actionHash) indexKey {
	var k indexKey
	copy(k[:], h[:])
	return k
}

func compareKey(a, b indexKey) int { return bytes.Compare(a[:], b[:]) }

// keySet is the index: the keys this client believes the remote has, each
// held as its action hash's indexKey.
//
// Every key is held left-aligned, with the low bits of the width its blob
// carried zeroed, so a lookup with a whole hash decides on those bits alone. A
// delta of a narrower width than the set already holds narrows the set rather
// than answering wrongly about the keys it parsed from a wider one.
type keySet struct {
	keys hashSet[indexKey]
	// The low shift bits of every key are zero.
	shift uint8
}

func newKeySet(n int) *keySet {
	return &keySet{keys: *newSortedSet(n, compareKey)}
}

// adoptSortedKeys adopts an index body's keys, which arrive ascending, as the
// whole indexKey of each.
func adoptSortedKeys(keys []indexKey) *keySet {
	return adoptSortedKeysAt(keys, indexKeySize*8)
}

// adoptSortedKeysAt adopts a body's keys, which arrive ascending and already
// masked to widthBits, the width the blob carried.
func adoptSortedKeysAt(keys []indexKey, widthBits int) *keySet {
	return &keySet{
		keys:  *adoptSorted(keys, compareKey),
		shift: uint8(indexKeySize*8 - widthBits),
	}
}

func (s *keySet) Len() int {
	if s == nil {
		return 0
	}
	return s.keys.Len()
}

// mask keeps the bits of a key this set holds.
func (s *keySet) mask() uint64 { return ^uint64(0) << s.shift }

// width is the prefix width in bits.
func (s *keySet) width() int { return indexKeySize*8 - int(s.shift) }

// maskKey zeroes the low shift bits of k, so a key of a narrower width and one
// of this set's own agree on the bits this set keeps.
func (s *keySet) maskKey(k indexKey) indexKey {
	binary.BigEndian.PutUint64(k[:], binary.BigEndian.Uint64(k[:])&s.mask())
	return k
}

// key is h as this set holds it.
func (s *keySet) key(h actionHash) indexKey { return s.maskKey(keyOf(h)) }

// Contains reports whether h's indexKey is a member, on the bits this set holds.
func (s *keySet) Contains(h actionHash) bool {
	if s == nil {
		return false
	}
	return s.keys.Contains(s.key(h))
}

// Add claims h's indexKey.
func (s *keySet) Add(h actionHash) {
	if s == nil {
		return
	}
	s.keys.Add(s.key(h))
}

// Remove drops h's indexKey.
func (s *keySet) Remove(h actionHash) {
	if s == nil {
		return
	}
	s.keys.Remove(s.key(h))
}

// addAll adds every member of other, masked to this set's width. A source of a
// narrower width narrows this set first, so every key keeps the same bits.
func (s *keySet) addAll(other *keySet) {
	if other.shift > s.shift {
		s.narrowTo(other.shift)
	}
	for k := range other.All() {
		s.keys.Add(s.maskKey(k))
	}
}

// narrowTo drops the low bits of every key down to shift. Keys that agree on
// what is left become one.
func (s *keySet) narrowTo(shift uint8) {
	s.keys.compact()
	s.shift = shift
	mask := s.mask()
	for i := range s.keys.sorted {
		binary.BigEndian.PutUint64(s.keys.sorted[i][:], binary.BigEndian.Uint64(s.keys.sorted[i][:])&mask)
	}
	s.keys.sorted = slices.CompactFunc(s.keys.sorted, func(a, b indexKey) bool { return a == b })
}

// All yields every member a single time, in no particular order.
func (s *keySet) All() iter.Seq[indexKey] {
	if s == nil {
		return func(func(indexKey) bool) {}
	}
	return s.keys.All()
}
