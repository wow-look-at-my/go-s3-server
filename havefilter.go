package main

// The client's statement of what it already holds.
//
// A prefetch window is only worth sending for a key the caller does not have.
// The server used to guess at that from memory: a per-build record of what it
// had sent, held in a byte-bounded LRU with a five-minute TTL and lost on
// restart. That record was never a statement about the client. It was a
// statement about the wire, and while the client discarded every unrequested
// body it was tracking a fiction -- the server suppressed keys the client
// never kept, and they came back as ordinary blocking misses.
//
// So the client says what it holds, in the request, and the server keeps no
// per-client memory at all.
//
// The statement is a Bloom filter over ACTION HASHES. A cache key's action
// hash is a sha256, already uniformly distributed, so the filter needs no hash
// function of its own: each of the k bit positions is three bytes read
// straight out of the hash, modulo the bit count. That is what keeps the two
// implementations of this filter, here and in cacheclient, from drifting --
// there is no hash to agree on, only this arithmetic.
//
// It fails toward sending. A Bloom filter has false positives and no false
// negatives, and a positive here means "the client holds it", so the error is
// always a key the server declines to send. The client then asks for it by
// name on the blocking path and gets it: one body un-sent, never a wrong
// answer and never a lost object. The opposite arrangement, where the error
// claims the client LACKS a key, would re-send bodies it already has, which is
// the waste this whole change exists to remove.

// haveFilterMaxHashes bounds k. Each position reads three bytes of the hash,
// and a sha256 has 32 of them.
const haveFilterMaxHashes = 10

// haveFilter is a client's set of held action hashes, as it arrives on the
// wire. Bits is the bit array, K how many positions each hash sets.
type haveFilter struct {
	Bits []byte `json:"bits"`
	K    int    `json:"k"`
}

// usable reports whether the filter says anything at all. An absent, empty or
// malformed filter is read as a client that stated nothing, which is served
// the full window.
func (f *haveFilter) usable() bool {
	return f != nil && len(f.Bits) > 0 && f.K >= 1 && f.K <= haveFilterMaxHashes
}

// contains reports whether the client said it holds this action hash. It is
// false for an unusable filter: a client that states nothing is sent
// everything.
func (f *haveFilter) contains(h [gbciHashSize]byte) bool {
	if !f.usable() {
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

// haveFilterBit is position i of hash h in a filter of m bits. It is the whole
// of the filter's hashing, and it is the one piece that must read identically
// in the client.
func haveFilterBit(h [gbciHashSize]byte, i int, m uint32) uint32 {
	off := i * 3
	v := uint32(h[off])<<16 | uint32(h[off+1])<<8 | uint32(h[off+2])
	return v % m
}
