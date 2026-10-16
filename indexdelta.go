package main

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"time"
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

// resetAddLogLocked starts a new epoch. The cached blob names the epoch, so
// it is rebuilt on the next read. The caller holds idx.mu.
func (idx *Index) resetAddLogLocked() {
	idx.addEpoch = newIndexEpoch()
	idx.added = nil
	for f := range idx.served {
		idx.served[f].builtAt = time.Time{}
	}
	idx.markStaleLocked()
}

// indexGen is a position in the add log.
type indexGen struct {
	epoch uint64
	seq   uint64
}

// Delta answers a version 1 GBCI blob of the hashes added after from, and the
// position it brings the client to. ok is false.
func (idx *Index) Delta(from indexGen) (blob []byte, to indexGen, ok bool) {
	return idx.DeltaFormat(from, gbciFull)
}

// DeltaFormat is Delta with the blob in format f.
func (idx *Index) DeltaFormat(from indexGen, f gbciFormat) (blob []byte, to indexGen, ok bool) {
	idx.mu.RLock()
	if from.epoch != idx.addEpoch || from.seq > uint64(len(idx.added)) {
		idx.mu.RUnlock()
		return nil, indexGen{}, false
	}
	tail := append([][gbciHashSize]byte(nil), idx.added[from.seq:]...)
	to = indexGen{epoch: idx.addEpoch, seq: uint64(len(idx.added))}
	idx.mu.RUnlock()
	return marshalGBCI(sortDedupeHashes(tail), f), to, true
}

// marshalGBCI encodes sorted, deduplicated hashes as a GBCI blob in format f.
// Each entry is the leading f.entrySize() bytes of a hash. Neighbours that
// share those bytes are written once, so the body stays sorted and deduped.
func marshalGBCI(hashes [][gbciHashSize]byte, f gbciFormat) []byte {
	size := f.entrySize()
	blob := make([]byte, gbciHeaderSize+len(hashes)*size+sha256.Size)
	copy(blob[0:4], gbciMagic[:])
	blob[4] = f.version()
	blob[5] = byte(size)
	off := gbciHeaderSize
	for i := range hashes {
		entry := hashes[i][:size]
		if off > gbciHeaderSize && bytes.Equal(blob[off-size:off], entry) {
			continue
		}
		copy(blob[off:off+size], entry)
		off += size
	}
	binary.LittleEndian.PutUint64(blob[16:24], uint64((off-gbciHeaderSize)/size))
	bodyDigest := sha256.Sum256(blob[gbciHeaderSize:off])
	binary.LittleEndian.PutUint64(blob[8:16], binary.LittleEndian.Uint64(bodyDigest[:8]))
	digest := sha256.Sum256(blob[:off])
	copy(blob[off:], digest[:])
	return blob[:off+sha256.Size]
}

// WireSize is the length in bytes of the body a cacheclient gets from a full GET /_index right now, before any compression.
// The client asks for the gbciPrefixSize form, so this sizes that form and not the full-width one.
// It answers from the cached blob when the handler would serve it, and counts the merged list otherwise.
// It never serializes and never changes the index.
func (idx *Index) WireSize() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.prefixServableLocked() {
		return int64(len(idx.cachedPrefix))
	}
	fresh := sortDedupeHashes(append([][gbciHashSize]byte(nil), idx.pending...))
	n := countDistinctPrefixes(idx.hashes, fresh, gbciPrefixSize)
	return int64(gbciHeaderSize + n*gbciPrefixSize + sha256.Size)
}

// countDistinctPrefixes counts the distinct first width bytes over the union of sorted lists a and b.
// This is the hash count marshalGBCIWidth writes for the merged list. It does not allocate.
func countDistinctPrefixes(a, b [][gbciHashSize]byte, width int) int {
	n := 0
	var prev []byte
	i, j := 0, 0
	for i < len(a) || j < len(b) {
		var cur []byte
		switch {
		case j == len(b) || (i < len(a) && bytes.Compare(a[i][:], b[j][:]) <= 0):
			cur = a[i][:width]
			i++
		default:
			cur = b[j][:width]
			j++
		}
		if prev == nil || !bytes.Equal(prev, cur) {
			n++
			prev = cur
		}
	}
	return n
}
