// What this client already holds, stated in the request.

package cacheclient

import "iter"

// What this client already holds, stated in the request. Only the client knows what it kept.

// haveFilterHashes is k: how many bit positions each action hash sets.
const haveFilterHashes = 6

// haveFilterBitsPerKey sizes the filter against what it holds.
const haveFilterBitsPerKey = 16

// haveFilterMinBytes and haveFilterMaxBytes bound the filter on the wire.
const (
	haveFilterMinBytes = 1 << 10
	haveFilterMaxBytes = 64 << 10
)

// haveFilter is the client's held set as it goes on the wire.
type haveFilter struct {
	Bits []byte `json:"bits"`
	K    int    `json:"k"`
}

// newHaveFilter builds a filter over the n hashes all yields. It answers nil for an empty set: a client holding nothing states nothing, and is sent the full window.
//
// It takes a sequence rather than a slice because the held set is held under a lock and can be large. Materializing it per request is the copy this avoids.
func newHaveFilter(n int, all iter.Seq[hashKey]) *haveFilter {
	if n == 0 {
		return nil
	}
	f := &haveFilter{Bits: make([]byte, haveFilterBytes(n)), K: haveFilterHashes}
	m := uint32(len(f.Bits) * 8)
	for key := range all {
		for i := 0; i < f.K; i++ {
			idx := haveFilterBit(key, i, m)
			f.Bits[idx/8] |= 1 << (idx % 8)
		}
	}
	return f
}

// haveFilterBytes sizes a filter for n keys, inside the wire bounds.
func haveFilterBytes(n int) int {
	size := n * haveFilterBitsPerKey / 8
	if size < haveFilterMinBytes {
		return haveFilterMinBytes
	}
	if size > haveFilterMaxBytes {
		return haveFilterMaxBytes
	}
	return size
}

// contains reports whether the filter states this hash, which is what the
// server asks of it.
func (f *haveFilter) contains(h actionHash) bool {
	if f == nil || len(f.Bits) == 0 {
		return false
	}
	m := uint32(len(f.Bits) * 8)
	key := keyOf64(h)
	for i := 0; i < f.K; i++ {
		idx := haveFilterBit(key, i, m)
		if f.Bits[idx/8]&(1<<(idx%8)) == 0 {
			return false
		}
	}
	return true
}

// haveFilterBit is position i of key in a filter of m bits, by double hashing
// over both halves of the key.
func haveFilterBit(key hashKey, i int, m uint32) uint32 {
	h1, h2 := uint32(key>>32), uint32(key)|1
	return (h1 + uint32(i)*h2) % m
}

// haveFilterKeys is the held set's hashes as the filter states them.
func haveFilterKeys(all iter.Seq[actionHash]) iter.Seq[hashKey] {
	return func(yield func(hashKey) bool) {
		for h := range all {
			if !yield(keyOf64(h)) {
				return
			}
		}
	}
}
