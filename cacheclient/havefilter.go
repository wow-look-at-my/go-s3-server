package cacheclient

import "iter"

// What this client already holds, stated in the request.
//
// Only the client knows what it kept.
//
// The statement is a Bloom filter over the first several bytes of each action
// hash. A hash is a sha256 and already uniformly distributed. The filter has
// no hash function of its own: the k bit positions come from double hashing
// over both halves of those bytes. The server's copy of this arithmetic is in
// havefilter.go at the repo root.
//
// It fails toward sending. The filter's error is a false positive, which says
// the client holds a key it does not: the server declines to send that body
// and the client asks for it by name on the blocking path. Nothing is lost
// and nothing is answered wrongly. A filter that erred the other way would
// re-send bodies the client already has, which is the waste this replaces.

// haveFilterHashes is k: how many bit positions each action hash sets.
const haveFilterHashes = 6

// haveFilterBitsPerKey sizes the filter against what it holds.
const haveFilterBitsPerKey = 16

// haveFilterMinBytes and haveFilterMaxBytes bound the filter on the wire. The
// floor keeps a nearly empty filter from being a handful of saturated bytes.
// The ceiling caps what a prefetch request carries; past it the rate degrades
// gracefully, and a degraded rate only means bodies the client fetches by name.
const (
	haveFilterMinBytes = 1 << 10
	haveFilterMaxBytes = 64 << 10
)

// haveFilter is the client's held set as it goes on the wire.
type haveFilter struct {
	Bits []byte `json:"bits"`
	K    int    `json:"k"`
}

// newHaveFilter builds a filter over the n hashes all yields. It answers nil
// for an empty set: a client holding nothing states nothing, and is sent the
// full window.
//
// It takes a sequence rather than a slice because the held set is held under
// a lock and can be large; materializing it per request is the copy this
// avoids.
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
	key := keyOf(h)
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
