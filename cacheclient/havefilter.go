package cacheclient

import "iter"

// What this client already holds, stated in the request.
//
// Only the client knows what it kept. The server used to remember what it had
// SENT each build, which is a different fact and an expensive one: per-build
// memory, a TTL, an LRU that evicts, and nothing at all after a restart.
//
// The statement is a Bloom filter over action hashes. A hash is a sha256 and
// already uniformly distributed, so the filter has no hash function of its
// own: each of the k bit positions is three bytes read straight out of the
// hash, modulo the bit count. The server's copy of this arithmetic is in
// havefilter.go at the repo root, and haveFilterVector pins the two together.
//
// It fails toward sending. The filter's error is a false positive, which says
// the client holds a key it does not: the server declines to send that one
// body and the client asks for it by name on the blocking path. Nothing is
// lost and nothing is answered wrongly. A filter that erred the other way
// would re-send bodies the client already has, which is the waste this
// replaces.

// haveFilterHashes is k: how many bit positions each action hash sets. Each
// one reads three of the hash's 32 bytes.
const haveFilterHashes = 6

// haveFilterBitsPerKey sizes the filter against what it holds. At 16 bits per
// key with k=6 the false-positive rate is under a tenth of a percent, and a
// false positive only costs one un-sent body.
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
func newHaveFilter(n int, all iter.Seq[actionHash]) *haveFilter {
	if n == 0 {
		return nil
	}
	f := &haveFilter{Bits: make([]byte, haveFilterBytes(n)), K: haveFilterHashes}
	m := uint32(len(f.Bits) * 8)
	for h := range all {
		for i := 0; i < f.K; i++ {
			idx := haveFilterBit(h, i, m)
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
	for i := 0; i < f.K; i++ {
		idx := haveFilterBit(h, i, m)
		if f.Bits[idx/8]&(1<<(idx%8)) == 0 {
			return false
		}
	}
	return true
}

// haveFilterBit is position i of hash h in a filter of m bits. The server
// computes this exact expression; neither side may change it alone.
func haveFilterBit(h actionHash, i int, m uint32) uint32 {
	off := i * 3
	v := uint32(h[off])<<16 | uint32(h[off+1])<<8 | uint32(h[off+2])
	return v % m
}
