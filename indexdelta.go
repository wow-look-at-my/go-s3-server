package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"iter"
	"time"

	"github.com/wow-look-at-my/go-s3-server/cacheclient"
)

// The add log lets a client that holds a copy of /_index fetch only the hashes stored since that copy.

// A full log starts a new epoch, and a client then makes one full fetch.
const maxAddLog = 1 << 20

// newIndexEpoch is random, so a client's position from an earlier server
// process never matches the log of a later one.
func newIndexEpoch() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("index: read random epoch: " + err.Error())
	}
	return binary.LittleEndian.Uint64(b[:])
}

// appendAddLocked records hash in the add log. The caller holds idx.mu.
func (idx *Index) appendAddLocked(hash [gbciHashSize]byte) {
	limit := idx.addLogLimit
	if limit == 0 {
		limit = maxAddLog
	}
	if len(idx.added) >= limit {
		idx.resetAddLogLocked()
	}
	idx.added = append(idx.added, hash)
}

// resetAddLogLocked starts a new epoch. Every cached blob names the old one,
// so each is rebuilt on its next read. The caller holds idx.mu.
func (idx *Index) resetAddLogLocked() {
	idx.addEpoch = newIndexEpoch()
	idx.added = nil
	idx.mutations++
	for _, c := range idx.forms {
		c.builtAt = time.Time{}
	}
}

// indexGen is a position in the add log.
type indexGen struct {
	epoch uint64
	seq   uint64
}

// Delta is DeltaForm in the full v1 form.
func (idx *Index) Delta(from indexGen) (blob []byte, to indexGen, ok bool) {
	return idx.DeltaForm(from, indexFormV1(gbciHashSize))
}

// DeltaForm answers a blob, in form, of the hashes added after from, and the
// position it brings the client to. ok is false when from is not in the
// current epoch or is ahead of the log: the client must make a full fetch.
func (idx *Index) DeltaForm(from indexGen, form indexForm) (blob []byte, to indexGen, ok bool) {
	idx.mu.RLock()
	if from.epoch != idx.addEpoch || from.seq > uint64(len(idx.added)) {
		idx.mu.RUnlock()
		return nil, indexGen{}, false
	}
	tail := append([][gbciHashSize]byte(nil), idx.added[from.seq:]...)
	to = indexGen{epoch: idx.addEpoch, seq: uint64(len(idx.added))}
	idx.mu.RUnlock()
	return marshalForm(sortDedupeHashes(tail), form), to, true
}

// gbciPrefixSize is the hash width of the short v1 form, in bytes.
const gbciPrefixSize = 8

// marshalForm encodes sorted, deduplicated hashes in form.
func marshalForm(hashes [][gbciHashSize]byte, form indexForm) []byte {
	if form.version == cacheclient.IndexV2Version {
		var values []uint64
		for v := range prefixValues(hashes, nil, form.width) {
			values = append(values, v)
		}
		return cacheclient.MarshalIndexV2(form.width, values)
	}
	return marshalGBCIWidth(hashes, form.width)
}

// prefixValues yields the prefixes of the hashes in the union of both sorted
// lists, ascending, each once.
func prefixValues(a, b [][gbciHashSize]byte, bits int) iter.Seq[uint64] {
	shift := 64 - bits
	return func(yield func(uint64) bool) {
		i, j := 0, 0
		first := true
		var prev uint64
		for i < len(a) || j < len(b) {
			var h [gbciHashSize]byte
			switch {
			case j == len(b) || (i < len(a) && bytes.Compare(a[i][:], b[j][:]) <= 0):
				h = a[i]
				i++
			default:
				h = b[j]
				j++
			}
			v := binary.BigEndian.Uint64(h[:8]) >> shift
			if !first && v == prev {
				continue
			}
			first, prev = false, v
			if !yield(v) {
				return
			}
		}
	}
}

// marshalGBCIWidth encodes the first width bytes of each sorted hash as a v1
// blob. Hashes that share those bytes are written a single time, so the body
// stays sorted and deduped.
func marshalGBCIWidth(hashes [][gbciHashSize]byte, width int) []byte {
	blob := make([]byte, gbciHeaderSize+len(hashes)*width+sha256.Size)
	off := gbciHeaderSize
	var prev []byte
	for i := range hashes {
		cur := hashes[i][:width]
		if prev != nil && string(prev) == string(cur) {
			continue
		}
		off += copy(blob[off:], cur)
		prev = cur
	}
	blob = blob[:off+sha256.Size]
	copy(blob[0:4], gbciMagic[:])
	blob[4] = gbciVersion
	blob[5] = byte(width)
	binary.LittleEndian.PutUint64(blob[16:24], uint64((off-gbciHeaderSize)/width))
	bodyDigest := sha256.Sum256(blob[gbciHeaderSize:off])
	binary.LittleEndian.PutUint64(blob[8:16], binary.LittleEndian.Uint64(bodyDigest[:8]))
	digest := sha256.Sum256(blob[:off])
	copy(blob[off:], digest[:])
	return blob
}

// WireSize is the length in bytes of the body GET /_index?v=2 sends right
// now, which is before any compression. It answers from the cached blob when
// the handler would serve it, and counts the merged list otherwise. It never
// serializes and never changes the index.
func (idx *Index) WireSize() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	bits := idx.hashBitsLocked()
	if c := idx.forms[indexForm{version: cacheclient.IndexV2Version, width: bits}]; idx.blobServableLocked(c) {
		return int64(len(c.blob))
	}
	var fresh [][gbciHashSize]byte
	if len(idx.pending) > 0 {
		fresh = sortDedupeHashes(append([][gbciHashSize]byte(nil), idx.pending...))
	}
	return int64(cacheclient.IndexV2Size(bits, prefixValues(idx.hashes, fresh, bits)))
}
