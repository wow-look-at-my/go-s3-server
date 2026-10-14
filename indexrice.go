package main

import (
	"crypto/sha256"
	"encoding/binary"
	"math/bits"
)

// The compact index form: sorted, distinct hash prefixes, written as the gaps between them in Rice code.

// gbciCompactWidth is the prefix width of the compact form, in bytes.
const gbciCompactWidth = 5

// gbciRiceVersion is the header version of the compact form.
const gbciRiceVersion = 2

// riceEncoding is the value of the enc query parameter that asks for the compact form.
const riceEncoding = "rice"

// prefixValue is the first width bytes of h as an integer.
func prefixValue(h *[gbciHashSize]byte, width int) uint64 {
	return binary.BigEndian.Uint64(h[:8]) >> (64 - 8*width)
}

// riceParam picks the Rice parameter for count values whose largest is top.
func riceParam(count int, top uint64) uint {
	if count == 0 {
		return 0
	}
	mean := top / uint64(count)
	scaled := mean - mean/4 - mean/16
	if scaled == 0 {
		return 0
	}
	return uint(bits.Len64(scaled) - 1)
}

// riceBodyBits is the length in bits of the Rice code of the sorted, distinct
// values that next yields, at parameter k.
func riceBodyBits(k uint, next func(yield func(uint64) bool)) uint64 {
	var total, prev uint64
	next(func(v uint64) bool {
		total += (v-prev)>>k + 1 + uint64(k)
		prev = v
		return true
	})
	return total
}

// bitWriter appends bits to buf, most significant bit first.
type bitWriter struct {
	buf []byte
	acc uint64
	n   uint
}

// write appends the low width bits of v.
func (w *bitWriter) write(v uint64, width uint) {
	for width > 0 {
		take := min(width, 32)
		width -= take
		w.acc = w.acc<<take | (v>>width)&(1<<take-1)
		w.n += take
		for w.n >= 8 {
			w.n -= 8
			w.buf = append(w.buf, byte(w.acc>>w.n))
		}
	}
}

// writeRice appends gap as q one bits, a zero bit, and the low k bits.
func (w *bitWriter) writeRice(gap uint64, k uint) {
	q := gap >> k
	for q >= 32 {
		w.write(1<<32-1, 32)
		q -= 32
	}
	w.write((1<<q-1)<<1, uint(q)+1)
	w.write(gap, k)
}

// flush pads the last byte with zero bits.
func (w *bitWriter) flush() {
	if w.n > 0 {
		w.buf = append(w.buf, byte(w.acc<<(8-w.n)))
		w.n = 0
	}
}

// marshalGBCIRice encodes the first width bytes of each sorted hash in the
// compact form. Hashes that share those bytes are written one time.
func marshalGBCIRice(hashes [][gbciHashSize]byte, width int) []byte {
	distinct := func(yield func(uint64) bool) {
		first := true
		var prev uint64
		for i := range hashes {
			v := prefixValue(&hashes[i], width)
			if !first && v == prev {
				continue
			}
			first, prev = false, v
			if !yield(v) {
				return
			}
		}
	}
	count := 0
	var top uint64
	distinct(func(v uint64) bool {
		count++
		top = v
		return true
	})
	k := riceParam(count, top)
	size := gbciHeaderSize + int((riceBodyBits(k, distinct)+7)/8) + sha256.Size
	w := bitWriter{buf: make([]byte, gbciHeaderSize, size)}
	var prev uint64
	distinct(func(v uint64) bool {
		w.writeRice(v-prev, k)
		prev = v
		return true
	})
	w.flush()
	blob := w.buf
	copy(blob[0:4], gbciMagic[:])
	blob[4] = gbciRiceVersion
	blob[5] = byte(width)
	blob[6] = byte(k)
	binary.LittleEndian.PutUint64(blob[16:24], uint64(count))
	bodyDigest := sha256.Sum256(blob[gbciHeaderSize:])
	binary.LittleEndian.PutUint64(blob[8:16], binary.LittleEndian.Uint64(bodyDigest[:8]))
	digest := sha256.Sum256(blob)
	return append(blob, digest[:]...)
}

// riceWireSize is the length of the compact form of the union of the sorted
// lists a and b, without the encode.
func riceWireSize(a, b [][gbciHashSize]byte, width int) int64 {
	merged := func(yield func(uint64) bool) {
		first := true
		var prev uint64
		i, j := 0, 0
		for i < len(a) || j < len(b) {
			var v uint64
			if j == len(b) || (i < len(a) && string(a[i][:]) <= string(b[j][:])) {
				v = prefixValue(&a[i], width)
				i++
			} else {
				v = prefixValue(&b[j], width)
				j++
			}
			if !first && v == prev {
				continue
			}
			first, prev = false, v
			if !yield(v) {
				return
			}
		}
	}
	count := 0
	var top uint64
	merged(func(v uint64) bool {
		count++
		top = v
		return true
	})
	k := riceParam(count, top)
	return int64(gbciHeaderSize) + int64((riceBodyBits(k, merged)+7)/8) + sha256.Size
}
