// The client's statement of what it already holds.

package main

import "encoding/binary"

// The client's statement of what it already holds.

// haveFilterMaxHashes bounds k.
const haveFilterMaxHashes = 10

// haveFilter is a client's set of held action hashes, as it arrives on the
// wire. Bits is the bit array, K how many positions each hash sets.
type haveFilter struct {
	Bits []byte `json:"bits"`
	K    int    `json:"k"`
}

// usable reports whether the filter says anything at all.
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

// haveFilterBit is position i of hash h in a filter of m bits, by double
// hashing over both halves of the hash's first several bytes. The client
// holds only those bytes.
func haveFilterBit(h [gbciHashSize]byte, i int, m uint32) uint32 {
	key := binary.BigEndian.Uint64(h[:8])
	h1, h2 := uint32(key>>32), uint32(key)|1
	return (h1 + uint32(i)*h2) % m
}
