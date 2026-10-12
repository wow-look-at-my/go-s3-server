package cacheclient

import (
	"bytes"
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
//
// K is the key material: a whole action hash for the sets that must state
// every byte of one, an indexKey for the index.
type hashSet[K comparable] struct {
	// sorted is ascending and deduped: every lookup is a binary search over it.
	sorted []K
	// pending holds keys added since the last compaction and absent.
	pending set.Set[K]
	removed set.Set[K]
	// cmp orders K by its bytes.
	cmp func(a, b K) int
}

// compactAt is how many buffered mutations force a merge into the sorted slice.
const compactAt = 512

func newSortedSet[K comparable](n int, cmp func(a, b K) int) *hashSet[K] {
	s := &hashSet[K]{pending: set.New[K](), removed: set.New[K](), cmp: cmp}
	if n > 0 {
		s.sorted = make([]K, 0, n)
	}
	return s
}

// adoptSorted adopts keys that are ALREADY ascending and deduped, which is
// what a GBCI blob's body is. It costs a single linear check rather than a
// sort, and it falls back to sorting rather than trusting the claim. A blob
// that arrives out of order would otherwise make every later lookup wrong.
func adoptSorted[K comparable](keys []K, cmp func(a, b K) int) *hashSet[K] {
	s := &hashSet[K]{
		sorted:  keys,
		pending: set.New[K](),
		removed: set.New[K](),
		cmp:     cmp,
	}
	if !slices.IsSortedFunc(s.sorted, cmp) {
		slices.SortFunc(s.sorted, cmp)
	}
	s.sorted = slices.CompactFunc(s.sorted, func(a, b K) bool { return a == b })
	return s
}

// newHashSet is a set of whole action hashes.
func newHashSet(n int) *hashSet[actionHash] { return newSortedSet(n, compareHash) }

func compareHash(a, b actionHash) int { return bytes.Compare(a[:], b[:]) }

func (s *hashSet[K]) Len() int {
	if s == nil {
		return 0
	}
	return len(s.sorted) + s.pending.Len() - s.removed.Len()
}

func (s *hashSet[K]) Contains(h K) bool {
	if s == nil {
		return false
	}
	if s.removed.Contains(h) {
		return false
	}
	if s.pending.Contains(h) {
		return true
	}
	_, found := slices.BinarySearchFunc(s.sorted, h, s.cmp)
	return found
}

func (s *hashSet[K]) Add(h K) {
	if s == nil {
		return
	}
	// A key in removed is still in the sorted slice, so re-adding it is the
	// deletion going away rather than a new entry.
	if s.removed.Contains(h) {
		s.removed.Remove(h)
		return
	}
	if s.pending.Contains(h) {
		return
	}
	if _, found := slices.BinarySearchFunc(s.sorted, h, s.cmp); found {
		return
	}
	s.pending.Add(h)
	if s.pending.Len() >= compactAt {
		s.compact()
	}
}

func (s *hashSet[K]) Remove(h K) {
	if s == nil {
		return
	}
	if s.pending.Contains(h) {
		s.pending.Remove(h)
		return
	}
	if _, found := slices.BinarySearchFunc(s.sorted, h, s.cmp); !found {
		return
	}
	s.removed.Add(h)
	if s.removed.Len() >= compactAt {
		s.compact()
	}
}

// All yields every key a single time, in no particular order.
func (s *hashSet[K]) All() iter.Seq[K] {
	return func(yield func(K) bool) {
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
func (s *hashSet[K]) compact() {
	if s.removed.Len() > 0 {
		s.sorted = slices.DeleteFunc(s.sorted, s.removed.Contains)
		s.removed.Clear()
	}
	if s.pending.Len() == 0 {
		return
	}
	add := s.pending.Values()
	s.pending.Clear()
	slices.SortFunc(add, s.cmp)

	n := len(s.sorted)
	s.sorted = slices.Grow(s.sorted, len(add))[:n+len(add)]
	i, j, w := n-1, len(add)-1, n+len(add)-1
	for j >= 0 {
		if i >= 0 && s.cmp(s.sorted[i], add[j]) > 0 {
			s.sorted[w] = s.sorted[i]
			i--
		} else {
			s.sorted[w] = add[j]
			j--
		}
		w--
	}
}
