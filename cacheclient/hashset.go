package cacheclient

import (
	"encoding/binary"
	"iter"
	"slices"

	"github.com/wow-look-at-my/go-containers/set"
)

// hashSet holds the action hashes this client believes the remote has. It is a
// SORTED SLICE, not a map, because of what it costs the machine rather than
// what it costs a single process.
//
// `dist test` runs many of those at the same time, and the machine's total is
// that number times the count.
//
// The index is READ far more than it is written. It arrives whole from /_index
// at startup and gains a key per stored object after that. What a slice costs
// is the mutation, which is why the writes land in a small set earliest and
// are merged in a batch. Depth: docs/memory-limits.md.
type hashSet struct {
	// sorted is ascending and deduped: every lookup is a binary search over it.
	sorted []hashKey
	// pending holds keys added since the last compaction and absent.
	pending set.Set[hashKey]
	removed set.Set[hashKey]
	// shift is 64 minus the width in bits of the prefix the keys hold. The
	// low shift bits of every key are zero. A set built from a v2 blob holds
	// the blob's width; any other set holds all 64.
	shift uint8
}

// hashKey is the first gbciPrefixSize bytes of an action hash, big-endian.
type hashKey = uint64

// gbciPrefixSize is the hash width of the short v1 index form the client asks for.
const gbciPrefixSize = 8

func keyOf(h actionHash) hashKey { return binary.BigEndian.Uint64(h[:gbciPrefixSize]) }

// mask keeps the bits of a key this set holds.
func (s *hashSet) mask() hashKey { return ^hashKey(0) << s.shift }

// key is h as this set holds it.
func (s *hashSet) key(h actionHash) hashKey { return keyOf(h) & s.mask() }

// width is the prefix width in bits.
func (s *hashSet) width() int { return 64 - int(s.shift) }

// addAll adds every key in src. A src of a narrower width narrows this set
// first, so every key keeps the same bits.
func (s *hashSet) addAll(src *hashSet) {
	if src.shift > s.shift {
		s.narrowTo(src.shift)
	}
	for k := range src.All() {
		s.addKey(k & s.mask())
	}
}

// narrowTo drops the low bits of every key down to shift. Keys that agree on
// what is left become one.
func (s *hashSet) narrowTo(shift uint8) {
	s.compact()
	s.shift = shift
	mask := s.mask()
	for i := range s.sorted {
		s.sorted[i] &= mask
	}
	s.sorted = slices.Compact(s.sorted)
}

// compactAt is how many buffered mutations force a merge into the sorted slice.
// A build stores thousands of objects, so this is a handful of merges over a
// run rather than a single per Put.
const compactAt = 512

func newHashSet(n int) *hashSet {
	s := &hashSet{pending: set.New[hashKey](), removed: set.New[hashKey]()}
	if n > 0 {
		s.sorted = make([]hashKey, 0, n)
	}
	return s
}

// newHashSetFromSorted adopts hashes that are ALREADY ascending and deduped,
// which is what a GBCI blob's body is, holding widthBits of each. It costs a
// single linear check rather than a sort, and it falls back to sorting rather
// than trusting the claim: a blob that arrives out of order would otherwise
// make every later lookup wrong.
func newHashSetFromSorted(keys []hashKey, widthBits int) *hashSet {
	s := &hashSet{
		sorted:  keys,
		pending: set.New[hashKey](),
		removed: set.New[hashKey](),
		shift:   uint8(64 - widthBits),
	}
	if !slices.IsSorted(s.sorted) {
		slices.Sort(s.sorted)
	}
	s.sorted = slices.Compact(s.sorted)
	return s
}

func (s *hashSet) Len() int {
	if s == nil {
		return 0
	}
	return len(s.sorted) + s.pending.Len() - s.removed.Len()
}

func (s *hashSet) Contains(h actionHash) bool {
	if s == nil {
		return false
	}
	k := s.key(h)
	if s.removed.Contains(k) {
		return false
	}
	if s.pending.Contains(k) {
		return true
	}
	_, found := slices.BinarySearch(s.sorted, k)
	return found
}

func (s *hashSet) Add(h actionHash) {
	if s == nil {
		return
	}
	s.addKey(s.key(h))
}

func (s *hashSet) addKey(k hashKey) {
	// A key in removed is still in the sorted slice, so re-adding it is the
	// deletion going away rather than a new entry.
	if s.removed.Contains(k) {
		s.removed.Remove(k)
		return
	}
	if s.pending.Contains(k) {
		return
	}
	if _, found := slices.BinarySearch(s.sorted, k); found {
		return
	}
	s.pending.Add(k)
	if s.pending.Len() >= compactAt {
		s.compact()
	}
}

func (s *hashSet) Remove(h actionHash) {
	if s == nil {
		return
	}
	k := s.key(h)
	if s.pending.Contains(k) {
		s.pending.Remove(k)
		return
	}
	if _, found := slices.BinarySearch(s.sorted, k); !found {
		return
	}
	s.removed.Add(k)
	if s.removed.Len() >= compactAt {
		s.compact()
	}
}

// All yields every key a single time, in no particular order.
func (s *hashSet) All() iter.Seq[hashKey] {
	return func(yield func(hashKey) bool) {
		if s == nil {
			return
		}
		for _, h := range s.sorted {
			if s.removed.Contains(h) {
				continue
			}
			if !yield(h) {
				return
			}
		}
		for h := range s.pending.All() {
			if !yield(h) {
				return
			}
		}
	}
}

// compact folds the buffered mutations into the sorted slice. It filters the
// deletions out in place and then merges the additions in from the END, so the
// write cursor never overtakes the read cursor and the existing backing array
// is reused whenever it has the room. The point of this type is the array, so a
// merge that allocated another a single every time would hand back what it saved.
func (s *hashSet) compact() {
	if s.removed.Len() > 0 {
		s.sorted = slices.DeleteFunc(s.sorted, s.removed.Contains)
		s.removed.Clear()
	}
	if s.pending.Len() == 0 {
		return
	}
	add := s.pending.Values()
	s.pending.Clear()
	slices.Sort(add)

	n := len(s.sorted)
	s.sorted = slices.Grow(s.sorted, len(add))[:n+len(add)]
	i, j, w := n-1, len(add)-1, n+len(add)-1
	for j >= 0 {
		if i >= 0 && s.sorted[i] > add[j] {
			s.sorted[w] = s.sorted[i]
			i--
		} else {
			s.sorted[w] = add[j]
			j--
		}
		w--
	}
}
