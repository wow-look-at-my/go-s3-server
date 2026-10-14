package main

import (
	"crypto/sha256"
	"encoding/binary"
	"iter"
	"math/bits"
)

// The v2 index form: Rice-coded gaps between sorted hash prefixes. The
// decoder, and the format, are in cacheclient/indexv2.go. This encoder must
// write the same bytes as cacheclient.MarshalIndexV2, and a test holds it to
// that. The server stays off the client module so both meet only on the
// wire.

// gbciVersion2 is the wire-format version byte of the v2 form.
const gbciVersion2 = 2

// indexHashBitsMin and indexHashBitsMax bound index_hash_bits.
const (
	indexHashBitsMin = 32
	indexHashBitsMax = 64
)

func riceParam(widthBits, n int) uint {
	k := widthBits
	if n > 1 {
		k -= bits.Len64(uint64(n - 1))
	}
	return uint(max(0, min(k, 63)))
}

// It reads values twice: once to count them, once to write them.
func marshalGBCI2(widthBits int, values iter.Seq[uint64]) []byte {
	n := 0
	for range values {
		n++
	}
	k := riceParam(widthBits, n)
	w := bitWriter{buf: make([]byte, gbciHeaderSize, gbciHeaderSize+n*int(k+2)/8+sha256.Size)}
	var prev uint64
	i := 0
	for v := range values {
		g := v
		if i > 0 {
			g = v - prev - 1
		}
		w.writeUnary(g >> k)
		w.writeBits(g&(1<<k-1), k)
		prev = v
		i++
	}
	blob := w.flush()
	bodyEnd := len(blob)
	blob = append(blob, make([]byte, sha256.Size)...)
	copy(blob[0:4], gbciMagic[:])
	blob[4] = gbciVersion2
	blob[5] = byte(widthBits)
	blob[6] = byte(k)
	binary.LittleEndian.PutUint64(blob[16:24], uint64(n))
	bodyDigest := sha256.Sum256(blob[gbciHeaderSize:bodyEnd])
	copy(blob[8:16], bodyDigest[:8])
	digest := sha256.Sum256(blob[:bodyEnd])
	copy(blob[bodyEnd:], digest[:])
	return blob
}

// gbci2Size is the length of the blob marshalGBCI2 writes for values, without
// writing it.
func gbci2Size(widthBits int, values iter.Seq[uint64]) int {
	n := 0
	for range values {
		n++
	}
	k := riceParam(widthBits, n)
	var total uint64
	var prev uint64
	i := 0
	for v := range values {
		g := v
		if i > 0 {
			g = v - prev - 1
		}
		total += g>>k + uint64(k) + 1
		prev = v
		i++
	}
	return gbciHeaderSize + int((total+7)/8) + sha256.Size
}

// bitWriter appends bits most significant first. acc holds n pending bits.
type bitWriter struct {
	buf []byte
	acc uint64
	n   uint
}

// writeBits appends the low nbits of v.
func (w *bitWriter) writeBits(v uint64, nbits uint) {
	for nbits > 32 {
		w.writeBits(v>>(nbits-32), 32)
		nbits -= 32
		v &= 1<<32 - 1
	}
	w.acc = w.acc<<nbits | v&(1<<nbits-1)
	w.n += nbits
	for w.n >= 8 {
		w.n -= 8
		w.buf = append(w.buf, byte(w.acc>>w.n))
	}
}

// writeUnary appends q ones and a zero.
func (w *bitWriter) writeUnary(q uint64) {
	for q >= 32 {
		w.writeBits(1<<32-1, 32)
		q -= 32
	}
	w.writeBits((1<<q-1)<<1, uint(q)+1)
}

// flush pads the pending bits to a byte and answers the buffer.
func (w *bitWriter) flush() []byte {
	if w.n > 0 {
		w.buf = append(w.buf, byte(w.acc<<(8-w.n)))
		w.n = 0
	}
	return w.buf
}
