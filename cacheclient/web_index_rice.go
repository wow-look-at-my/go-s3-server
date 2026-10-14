package cacheclient

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
)

// The compact index form. The server writes it in indexrice.go at the repo root.

// gbciCompactWidth is the key width the client asks the server for, in bytes.
const gbciCompactWidth = 5

// gbciRiceVersion is the header version of the compact form.
const gbciRiceVersion = 2

// riceQuery asks for the compact form. hash=8 stays in it, so a server that does not know enc answers the 8-byte form.
const riceQuery = "hash=8&enc=rice"

// riceParam picks the Rice parameter for count values whose largest is top.
// The server picks it the same way.
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

// bitReader reads bits most significant first. acc holds n bits at its top.
type bitReader struct {
	buf []byte
	pos int
	acc uint64
	n   uint
}

func (r *bitReader) refill() {
	for r.n <= 56 && r.pos < len(r.buf) {
		r.acc |= uint64(r.buf[r.pos]) << (56 - r.n)
		r.pos++
		r.n += 8
	}
}

var errRiceShort = errors.New("compact index: body ends inside a value")

// decodeRice decodes count values of width bytes, Rice-coded at k, into keys
// whose leading width bytes are the values.
func decodeRice(body []byte, count, width int, k uint) ([]hashKey, error) {
	if width < 1 || width > gbciPrefixSize || k > 63 {
		return nil, fmt.Errorf("compact index: width %d, k %d", width, k)
	}
	// Each value takes at least a single bit, so a count past the body is false.
	if count > len(body)*8 {
		return nil, fmt.Errorf("compact index: %d keys in %d bytes", count, len(body))
	}
	limit := uint64(1)<<(8*width) - 1
	shift := uint(64 - 8*width)
	keys := make([]hashKey, count)
	r := bitReader{buf: body}
	var prev uint64
	for idx := range keys {
		var quot uint64
		for {
			r.refill()
			if r.n == 0 {
				return nil, errRiceShort
			}
			ones := uint(bits.LeadingZeros64(^r.acc))
			if ones >= r.n {
				quot += uint64(r.n)
				r.acc, r.n = 0, 0
				continue
			}
			quot += uint64(ones)
			r.acc <<= ones + 1
			r.n -= ones + 1
			break
		}
		r.refill()
		if r.n < k {
			return nil, errRiceShort
		}
		low := r.acc >> (64 - k)
		r.acc <<= k
		r.n -= k
		if quot > limit>>k {
			return nil, fmt.Errorf("compact index: gap out of range at key %d", idx)
		}
		gap := quot<<k | low
		if idx > 0 && gap == 0 {
			return nil, fmt.Errorf("compact index: repeated key at %d", idx)
		}
		if gap > limit-prev {
			return nil, fmt.Errorf("compact index: key %d out of range", idx)
		}
		prev += gap
		keys[idx] = prev << shift
	}
	if r.pos != len(r.buf) || r.n >= 8 || r.acc != 0 {
		return nil, errors.New("compact index: bytes after the last key")
	}
	return keys, nil
}

// bitWriter appends bits most significant first. acc holds n bits at its bottom.
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
	quot := gap >> k
	for quot >= 32 {
		w.write(1<<32-1, 32)
		quot -= 32
	}
	w.write((1<<quot-1)<<1, uint(quot)+1)
	w.write(gap, k)
}

// marshalRiceIndex encodes ascending, distinct keys of width bytes in the
// compact form.
func marshalRiceIndex(sorted []hashKey, width int) []byte {
	shift := uint(64 - 8*width)
	var top uint64
	if len(sorted) > 0 {
		top = sorted[len(sorted)-1] >> shift
	}
	k := riceParam(len(sorted), top)
	w := bitWriter{buf: make([]byte, gbciHeaderSize, gbciHeaderSize+len(sorted)*3+sha256.Size)}
	var prev uint64
	for _, key := range sorted {
		value := key >> shift
		w.writeRice(value-prev, k)
		prev = value
	}
	if w.n > 0 {
		w.buf = append(w.buf, byte(w.acc<<(8-w.n)))
	}
	blob := w.buf
	copy(blob[0:4], gbciMagic[:])
	blob[4] = gbciRiceVersion
	blob[5] = byte(width)
	blob[6] = byte(k)
	binary.LittleEndian.PutUint64(blob[16:24], uint64(len(sorted)))
	digest := sha256.Sum256(blob)
	return append(blob, digest[:]...)
}
