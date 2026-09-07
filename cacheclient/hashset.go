package cacheclient

import (
	"bytes"
	"iter"
	"slices"

	"github.com/wow-look-at-my/go-containers/set"
)

// hashSet holds the action hashes this client believes the remote has. It is a
// SORTED SLICE, not a map, because of what it costs the machine rather than
// what it costs one process.
//
// A hash is 32 bytes, so a million of them is 32 MiB of key material. A map of
// them held 88 MiB: buckets, growth slack, and a second set for the
// confirmed-absent keys. This slice holds 31 MiB. Over one `go install std`
// against a store of about 1,008,000 keys, that moved a single `go` process
// from a 463 MiB peak to 325 MiB. `dist test` runs many of those at once, and
// the machine's total is that number times the count.
//
// The index is READ far more than it is written. It arrives whole from /_index
// at startup and gains a key per stored object after that. What a slice costs
// is the mutation, which is why the writes land in a small set first and are
// merged in a batch. Depth: docs/memory-limits.md.
type hashSet struct {
	// sorted is ascending and deduped: every lookup is a binary search over it.
	sorted []actionHash
	// pending holds keys added since the last compaction and absent from
	// sorted. removed holds keys deleted since then and present in sorted. Both
	// are ordinary sets, and both are bounded by compactAt: the map overhead
	// this type exists to avoid is a few tens of kilobytes here, not 250 MB.
	pending set.Set[actionHash]
	removed set.Set[actionHash]
}

// compactAt is how many buffered mutations force a merge into the sorted slice.
// A build stores thousands of objects, so this is a handful of merges over a
// run rather than one per Put.
const compactAt = 512

func newHashSet(n int) *hashSet {
	s := &hashSet{pending: set.New[actionHash](), removed: set.New[actionHash]()}
	if n > 0 {
		s.sorted = make([]actionHash, 0, n)
	}
	return s
}

// newHashSetFromSorted adopts hashes that are ALREADY ascending and deduped,
// which is what a GBCI blob's body is. It costs one linear check rather than a
// sort, and it falls back to sorting rather than trusting the claim: a blob
// that arrives out of order would otherwise make every later lookup wrong.
func newHashSetFromSorted(hashes []actionHash) *hashSet {
	s := &hashSet{
		sorted:  hashes,
		pending: set.New[actionHash](),
		removed: set.New[actionHash](),
	}
	if !slices.IsSortedFunc(s.sorted, compareHash) {
		slices.SortFunc(s.sorted, compareHash)
	}
	s.sorted = slices.CompactFunc(s.sorted, func(a, b actionHash) bool { return a == b })
	return s
}

func compareHash(a, b actionHash) int { return bytes.Compare(a[:], b[:]) }

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
	if s.removed.Contains(h) {
		return false
	}
	if s.pending.Contains(h) {
		return true
	}
	_, found := slices.BinarySearchFunc(s.sorted, h, compareHash)
	return found
}

func (s *hashSet) Add(h actionHash) {
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
	if _, found := slices.BinarySearchFunc(s.sorted, h, compareHash); found {
		return
	}
	s.pending.Add(h)
	if s.pending.Len() >= compactAt {
		s.compact()
	}
}

func (s *hashSet) Remove(h actionHash) {
	if s == nil {
		return
	}
	if s.pending.Contains(h) {
		s.pending.Remove(h)
		return
	}
	if _, found := slices.BinarySearchFunc(s.sorted, h, compareHash); !found {
		return
	}
	s.removed.Add(h)
	if s.removed.Len() >= compactAt {
		s.compact()
	}
}

// All yields every key once, in no particular order.
func (s *hashSet) All() iter.Seq[actionHash] {
	return func(yield func(actionHash) bool) {
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
// is reused whenever it has the room. The point of this type is the array, so
// a merge that allocated a second one every time would hand back what it saved.
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
	slices.SortFunc(add, compareHash)

	n := len(s.sorted)
	s.sorted = slices.Grow(s.sorted, len(add))[:n+len(add)]
	i, j, w := n-1, len(add)-1, n+len(add)-1
	for j >= 0 {
		if i >= 0 && compareHash(s.sorted[i], add[j]) > 0 {
			s.sorted[w] = s.sorted[i]
			i--
		} else {
			s.sorted[w] = add[j]
			j--
		}
		w--
	}
}
