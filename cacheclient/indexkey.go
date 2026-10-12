package cacheclient

import (
	"bytes"
	"iter"
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
type keySet struct {
	keys hashSet[indexKey]
}

func newKeySet(n int) *keySet {
	return &keySet{keys: *newSortedSet(n, compareKey)}
}

// adoptSortedKeys adopts an index body's keys, which arrive ascending.
func adoptSortedKeys(keys []indexKey) *keySet {
	return &keySet{keys: *adoptSorted(keys, compareKey)}
}

func (s *keySet) Len() int {
	if s == nil {
		return 0
	}
	return s.keys.Len()
}

// Contains reports whether h's indexKey is a member.
func (s *keySet) Contains(h actionHash) bool {
	if s == nil {
		return false
	}
	return s.keys.Contains(keyOf(h))
}

// Add claims h's indexKey.
func (s *keySet) Add(h actionHash) {
	if s == nil {
		return
	}
	s.keys.Add(keyOf(h))
}

// Remove drops h's indexKey.
func (s *keySet) Remove(h actionHash) {
	if s == nil {
		return
	}
	s.keys.Remove(keyOf(h))
}

// addAll adds every member of other.
func (s *keySet) addAll(other *keySet) {
	for k := range other.All() {
		s.keys.Add(k)
	}
}

// All yields every member a single time, in no particular order.
func (s *keySet) All() iter.Seq[indexKey] {
	if s == nil {
		return func(func(indexKey) bool) {}
	}
	return s.keys.All()
}
