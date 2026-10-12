package main

import (
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
	idx.builtAt = time.Time{}
	idx.dirty.Store(true)
}

// indexGen is a position in the add log.
type indexGen struct {
	epoch uint64
	seq   uint64
}

// BlobGenWidth is BlobGen in the form a client asked for: gbciPrefixSize-byte
// hashes, or the full gbciHashSize.
func (idx *Index) BlobGenWidth(width int) ([]byte, string, indexGen) {
	for {
		blob, etag, gen := idx.BlobGen()
		if width != gbciPrefixSize {
			return blob, etag, gen
		}
		idx.mu.RLock()
		prefix, prefixETag, prefixGen := idx.cachedPrefix, idx.cachedPrefixETag, idx.cachedGen
		idx.mu.RUnlock()
		// A rebuild between both locks clears the cache, so build it again.
		if prefix != nil {
			return prefix, prefixETag, prefixGen
		}
	}
}

// Delta is DeltaWidth at the full hash width.
func (idx *Index) Delta(from indexGen) (blob []byte, to indexGen, ok bool) {
	return idx.DeltaWidth(from, gbciHashSize)
}

// DeltaWidth answers a GBCI blob of the first width bytes of each hash added
// after from, and the position it brings the client to. ok is false when from
// is not in the current epoch or is ahead of the log: the client must make a
// full fetch.
func (idx *Index) DeltaWidth(from indexGen, width int) (blob []byte, to indexGen, ok bool) {
	idx.mu.RLock()
	if from.epoch != idx.addEpoch || from.seq > uint64(len(idx.added)) {
		idx.mu.RUnlock()
		return nil, indexGen{}, false
	}
	tail := append([][gbciHashSize]byte(nil), idx.added[from.seq:]...)
	to = indexGen{epoch: idx.addEpoch, seq: uint64(len(idx.added))}
	idx.mu.RUnlock()
	return marshalGBCIWidth(sortDedupeHashes(tail), width), to, true
}

// gbciPrefixSize is the hash width of the short index form.
const gbciPrefixSize = 8

// marshalGBCI encodes sorted hashes as a GBCI v1 blob.
func marshalGBCI(hashes [][gbciHashSize]byte) []byte {
	return marshalGBCIWidth(hashes, gbciHashSize)
}

// marshalGBCIWidth encodes the first width bytes of each sorted hash. Hashes
// that share those bytes are written a single time, so the body stays sorted
// and deduped.
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
