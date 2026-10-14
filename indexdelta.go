package main

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"sync/atomic"
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
	idx.prefixForm.stale()
	idx.riceForm.stale()
}

// formCache holds the last blob of a short index form.
type formCache struct {
	blob    []byte
	etag    string
	gen     indexGen
	builtAt time.Time
	dirty   atomic.Bool
}

// stale makes the next read rebuild the blob. The caller holds idx.mu.
func (c *formCache) stale() {
	c.builtAt = time.Time{}
	c.dirty.Store(true)
}

// drop discards the blob. The caller holds idx.mu.
func (c *formCache) drop() {
	c.blob = nil
	c.etag = ""
}

// servable is blobServableLocked for a short form. The caller holds idx.mu.
func (c *formCache) servable(interval time.Duration) bool {
	if c.blob == nil {
		return false
	}
	return !c.dirty.Load() || time.Since(c.builtAt) < interval
}

// indexForm names one of the encodings GET /_index serves.
type indexForm int

const (
	// formFull is gbciHashSize-byte hashes.
	formFull indexForm = iota
	// formPrefix is gbciPrefixSize-byte hashes.
	formPrefix
	// formRice is gbciCompactWidth-byte prefixes in Rice code.
	formRice
)

// marshal encodes sorted hashes in the form.
func (f indexForm) marshal(hashes [][gbciHashSize]byte) []byte {
	switch f {
	case formPrefix:
		return marshalGBCIWidth(hashes, gbciPrefixSize)
	case formRice:
		return marshalGBCIRice(hashes, gbciCompactWidth)
	default:
		return marshalGBCI(hashes)
	}
}

// formCacheLocked is the cache of a short form. The caller holds idx.mu.
func (idx *Index) formCacheLocked(form indexForm) *formCache {
	if form == formRice {
		return &idx.riceForm
	}
	return &idx.prefixForm
}

// indexGen is a position in the add log.
type indexGen struct {
	epoch uint64
	seq   uint64
}

// BlobGenForm is BlobGen in the form a client asked for.
func (idx *Index) BlobGenForm(form indexForm) ([]byte, string, indexGen) {
	if form == formFull {
		return idx.BlobGen()
	}
	interval := idx.BlobInterval()
	idx.mu.RLock()
	c := idx.formCacheLocked(form)
	blob, etag, gen, fresh := c.blob, c.etag, c.gen, c.servable(interval)
	idx.mu.RUnlock()
	if fresh {
		return blob, etag, gen
	}
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if c.servable(interval) {
		return c.blob, c.etag, c.gen
	}
	gen = idx.mergePendingLocked()
	c.blob = form.marshal(idx.hashes)
	c.etag = blobETag(c.blob)
	c.gen = gen
	c.builtAt = time.Now()
	c.dirty.Store(false)
	idx.updateGaugesLocked()
	return c.blob, c.etag, c.gen
}

// Delta is DeltaForm in the full form.
func (idx *Index) Delta(from indexGen) (blob []byte, to indexGen, ok bool) {
	return idx.DeltaForm(from, formFull)
}

// DeltaForm answers a GBCI blob in form of each hash added after from, and
// the position it brings the client to. ok is false when from is not in the
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
	return form.marshal(sortDedupeHashes(tail)), to, true
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

// WireSize is the length in bytes of the body a cacheclient gets from a full GET /_index right now, before any compression.
// GET /_index right now, before any compression. The client asks for the
// It answers from the cached blob when the handler would serve it, and counts the merged list otherwise.
// It never serializes and never changes the index.
func (idx *Index) WireSize() int64 {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	if idx.riceForm.servable(idx.BlobInterval()) {
		return int64(len(idx.riceForm.blob))
	}
	fresh := sortDedupeHashes(append([][gbciHashSize]byte(nil), idx.pending...))
	return riceWireSize(idx.hashes, fresh, gbciCompactWidth)
}
