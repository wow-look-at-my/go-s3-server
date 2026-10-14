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
	// drop names the low bits that are zero in every key of the set.
	drop uint
}

// hashKey is the first gbciPrefixSize bytes of an action hash, big-endian.
type hashKey = uint64

// gbciPrefixSize is the widest key a set holds, in bytes.
const gbciPrefixSize = 8

func keyOf(h actionHash) hashKey { return binary.BigEndian.Uint64(h[:gbciPrefixSize]) }

// key is h at the width of the set.
func (s *hashSet) key(h actionHash) hashKey { return keyOf(h) >> s.drop << s.drop }

// width is the key width of the set, in bytes.
func (s *hashSet) width() int { return int(64-s.drop) / 8 }

// narrow cuts every key to width bytes. A wider width changes nothing: a key
// cannot get back the bits it lost. Keys that become equal are one key.
func (s *hashSet) narrow(width int) {
	drop := uint(64 - 8*width)
	if drop <= s.drop {
		return
	}
	s.compact()
	s.drop = drop
	for i, k := range s.sorted {
		s.sorted[i] = k >> drop << drop
	}
	s.sorted = slices.Compact(s.sorted)
}

// addAll adds every key in src with a single merge into the sorted slice. A
// merge per compactAt keys moves the whole slice each time. When the widths
// differ, the result has the narrower width.
func (s *hashSet) addAll(src *hashSet) {
	s.narrow(src.width())
	for k := range src.All() {
		k = k >> s.drop << s.drop
		if s.removed.Contains(k) {
			s.removed.Remove(k)
			continue
		}
		if _, found := slices.BinarySearch(s.sorted, k); found {
			continue
		}
		s.pending.Add(k)
	}
	s.compact()
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
// which is what a GBCI blob's body is. It costs a single linear check rather
// than a sort, and it falls back to sorting rather than trusting the claim: a
// blob that arrives out of order would otherwise make every later lookup wrong.
func newHashSetFromSorted(keys []hashKey) *hashSet {
	s := &hashSet{
		sorted:  keys,
		pending: set.New[hashKey](),
		removed: set.New[hashKey](),
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
	s.addKey(keyOf(h))
}

// addKey adds k at the width of the set.
func (s *hashSet) addKey(k hashKey) {
	k = k >> s.drop << s.drop
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
