package cacheclient

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"iter"
	"math/bits"
)

// GBCI v2 carries the index as the gaps between its sorted hash prefixes.
//
// A sha256 prefix is incompressible on its own. A SORTED list of n of them
// is not: the gap between neighbours is about 2^width/n, so each entry
// costs about width-log2(n)+2 bits instead of width. The gaps are Rice
// coded: the high part of each gap in unary, the low k bits as they are.
//
// Header, 24 bytes: magic, version 2, prefix width in bits, k, a reserved
// zero, the content digest at [8:16], the count at [16:24]. The body is the
// coded gaps, padded to a byte. The trailer is the sha256 of header and body.

// IndexV2Version is the wire-format version byte of this form.
const IndexV2Version = 2

// IndexV2MinBits and IndexV2MaxBits bound the prefix width a v2 blob carries.
const (
	IndexV2MinBits = 32
	IndexV2MaxBits = 64
)

// riceParam is the Rice parameter for n values spread over widthBits bits: the
// floor of log2 of the mean gap, held under 64 so a shift by it is defined.
func riceParam(widthBits, n int) uint {
	k := widthBits
	if n > 1 {
		k -= bits.Len64(uint64(n - 1))
	}
	return uint(max(0, min(k, 63)))
}

// MarshalIndexV2 encodes values, ascending and distinct, each below
// 2^widthBits, as a GBCI v2 blob. An input that breaks those rules is a
// caller's bug and panics: a blob written from it would carry a valid trailer
// over wrong keys.
func MarshalIndexV2(widthBits int, values []uint64) []byte {
	if widthBits < IndexV2MinBits || widthBits > IndexV2MaxBits {
		panic(fmt.Sprintf("gbci v2: width %d bits is outside %d..%d", widthBits, IndexV2MinBits, IndexV2MaxBits))
	}
	k := riceParam(widthBits, len(values))
	limit := prefixLimit(widthBits)
	w := bitWriter{buf: make([]byte, gbciHeaderSize, gbciHeaderSize+len(values)*int(k+2)/8+sha256.Size)}
	var prev uint64
	for i, v := range values {
		if v > limit || (i > 0 && v <= prev) {
			panic(fmt.Sprintf("gbci v2: value %d at %d is out of order or over %d bits", v, i, widthBits))
		}
		g := v
		if i > 0 {
			g = v - prev - 1
		}
		w.writeUnary(g >> k)
		w.writeBits(g&(1<<k-1), k)
		prev = v
	}
	blob := w.flush()
	bodyEnd := len(blob)
	blob = append(blob, make([]byte, sha256.Size)...)
	copy(blob[0:4], gbciMagic[:])
	blob[4] = IndexV2Version
	blob[5] = byte(widthBits)
	blob[6] = byte(k)
	binary.LittleEndian.PutUint64(blob[16:24], uint64(len(values)))
	bodyDigest := sha256.Sum256(blob[gbciHeaderSize:bodyEnd])
	copy(blob[8:16], bodyDigest[:8])
	digest := sha256.Sum256(blob[:bodyEnd])
	copy(blob[bodyEnd:], digest[:])
	return blob
}

// IndexV2Size is the length of the blob MarshalIndexV2 writes for values,
// without writing it. values must be what MarshalIndexV2 would take.
func IndexV2Size(widthBits int, values iter.Seq[uint64]) int {
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

// prefixLimit is the largest value of widthBits bits.
func prefixLimit(widthBits int) uint64 {
	if widthBits >= 64 {
		return ^uint64(0)
	}
	return 1<<widthBits - 1
}

// ParseIndexV2 decodes a GBCI v2 blob into its values, ascending, and the
// prefix width they carry. It checks the magic, the version, the length and
// the trailer before it reads a gap.
func ParseIndexV2(blob []byte) (values []uint64, widthBits int, err error) {
	if len(blob) < gbciHeaderSize+sha256.Size {
		return nil, 0, fmt.Errorf("blob too small (%d bytes)", len(blob))
	}
	if !bytes.Equal(blob[0:4], gbciMagic[:]) {
		return nil, 0, fmt.Errorf("bad magic")
	}
	if blob[4] != IndexV2Version {
		return nil, 0, fmt.Errorf("unsupported version %d", blob[4])
	}
	widthBits = int(blob[5])
	if widthBits < IndexV2MinBits || widthBits > IndexV2MaxBits {
		return nil, 0, fmt.Errorf("unsupported hash width %d bits", widthBits)
	}
	k := uint(blob[6])
	if k > 63 {
		return nil, 0, fmt.Errorf("rice parameter %d is over 63", k)
	}
	bodyEnd := len(blob) - sha256.Size
	expected := sha256.Sum256(blob[:bodyEnd])
	if !bytes.Equal(expected[:], blob[bodyEnd:]) {
		return nil, 0, fmt.Errorf("trailer hash mismatch")
	}
	body := blob[gbciHeaderSize:bodyEnd]
	count := binary.LittleEndian.Uint64(blob[16:24])
	// Every value costs at least k+1 bits, so a count past that is a lie.
	if count > uint64(len(body))*8/uint64(k+1) {
		return nil, 0, fmt.Errorf("count %d does not fit a %d-byte body", count, len(body))
	}
	limit := prefixLimit(widthBits)
	r := bitReader{data: body}
	values = make([]uint64, count)
	var prev uint64
	for i := range values {
		q, ok := r.readUnary()
		if !ok {
			return nil, 0, fmt.Errorf("body ends inside value %d of %d", i, count)
		}
		low, ok := r.readBits(k)
		if !ok {
			return nil, 0, fmt.Errorf("body ends inside value %d of %d", i, count)
		}
		if k > 0 && q>>(64-k) != 0 {
			return nil, 0, fmt.Errorf("value %d overflows", i)
		}
		g := q<<k | low
		v := g
		if i > 0 {
			if prev >= limit || g > limit-prev-1 {
				return nil, 0, fmt.Errorf("value %d overflows %d bits", i, widthBits)
			}
			v = prev + g + 1
		}
		if v > limit {
			return nil, 0, fmt.Errorf("value %d overflows %d bits", i, widthBits)
		}
		values[i] = v
		prev = v
	}
	if r.pos < len(body) || r.n >= 8 {
		return nil, 0, fmt.Errorf("%d bytes follow the last value", len(body)-r.pos+int(r.n)/8)
	}
	return values, widthBits, nil
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

// bitReader reads bits most significant first. The low n bits of acc are the
// ones not yet read, and pos is the next byte of data to load.
type bitReader struct {
	data []byte
	pos  int
	acc  uint64
	n    uint
}

func (r *bitReader) refill() {
	for r.n <= 56 && r.pos < len(r.data) {
		r.acc = r.acc<<8 | uint64(r.data[r.pos])
		r.pos++
		r.n += 8
	}
}

// readBits reads nbits bits, up to 64. ok is false past the end of the data.
func (r *bitReader) readBits(nbits uint) (uint64, bool) {
	if nbits == 0 {
		return 0, true
	}
	if nbits > 32 {
		hi, ok := r.readBits(nbits - 32)
		if !ok {
			return 0, false
		}
		lo, ok := r.readBits(32)
		return hi<<32 | lo, ok
	}
	r.refill()
	if r.n < nbits {
		return 0, false
	}
	r.n -= nbits
	return r.acc >> r.n & (1<<nbits - 1), true
}

// readUnary counts the ones before the next zero and consumes that zero. ok is
// false when the data ends before a zero.
func (r *bitReader) readUnary() (uint64, bool) {
	var q uint64
	for {
		r.refill()
		if r.n == 0 {
			return 0, false
		}
		valid := r.acc << (64 - r.n)
		ones := uint(bits.LeadingZeros64(^valid))
		if ones < r.n {
			r.n -= ones + 1
			return q + uint64(ones), true
		}
		q += uint64(r.n)
		r.n = 0
	}
}
