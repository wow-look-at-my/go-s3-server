package main

import (
	"bytes"
	"errors"
	"io"
	"log"

	"github.com/pierrec/lz4/v4"
)

// goModuleIndexMagic is the leading bytes of a Go module index blob. cmd/go's
// modindex writer stamps a version line "go index vN\n" at the head of every
// index it stores through the build cache (indexVersion is "go index v2" in
// current Go, written verbatim followed by '\n'). Matching the version-less
// prefix keeps the check correct across index format bumps.
const goModuleIndexMagic = "go index v"

// indexMagicProbeBytes is how many DEcompressed bytes we read to recognize the
// magic: enough to cover "go index v" with a little slack. We never decode more
// of the body than this.
const indexMagicProbeBytes = 16

// indexPutPeekBytes bounds how many COMPRESSED leading bytes the PUT path reads
// before deciding whether an upload is a module index. It must be large enough
// to contain a real index's first lz4 block, because the lz4 reader needs the
// WHOLE first block to decode any output -- the bug this constant replaces was a
// fixed 512-byte peek (see the package note below) that truncated the
// single-block bodies the client actually sends, so the magic was never seen.
//
// The go-toolchain client packs each cached body into a single lz4 block whose
// max size is 4 MiB; real indexes observed on the production cache compress to
// ~600 bytes - ~36 KB. 1 MiB is comfortably above the observed range while still
// far below the 4 MiB block ceiling, so it reliably covers a real index's first
// block without buffering an arbitrarily large non-index upload. If a body's
// first block somehow exceeds this (it never does in practice), the decode comes
// up short and the upload is treated as "not an index" and stored -- fail-open,
// matching the historic default and bounded by the client-side guard + the v3
// purge.
const indexPutPeekBytes = 1 << 20

// The detection here used to peek only the first 512 COMPRESSED bytes of a body
// (const indexPeekBytes = 512) and lz4-decompress that prefix. That was wrong:
// the client stores each body as a SINGLE lz4 block, and pierrec/lz4's reader
// must have the entire block before it can decode any output. Real module
// indexes compress to 521 bytes - ~36 KB -- always more than 512 -- so the
// reader, handed a truncated 512-byte prefix, returned n=0 (unexpected EOF), the
// magic was never seen, and the poison was SERVED (and, on PUT, STORED). Both the
// read/evict path and the PUT guard were broken by the same truncation. The
// existing synthetic test passed only because its 2 KiB-of-'x' payload
// compressed to ~61 bytes, which fit in 512 -- non-representative. The fix feeds
// the detector enough input to decode the first block (where the magic lives):
// the read path streams an lz4.Reader straight off the open file (it pulls
// exactly one block); the PUT path reads a bounded but block-sized prefix.

// looksLikeGoModuleIndex reports whether the input (the leading bytes of an
// upload, possibly lz4-compressed per the compression hint) begins with the Go
// module index magic.
//
// IMPORTANT: when compression == "lz4", the input MUST contain at least the
// body's first complete lz4 block. pierrec/lz4's reader cannot decode any output
// from a partial block, so a prefix shorter than the first block decodes to
// nothing and the magic is missed. Callers pass either the whole body (read
// path) or a block-sized bounded prefix (PUT path, indexPutPeekBytes), never a
// fixed small peek.
//
// The module index is the one build-cache payload this server must refuse: it
// carries no build id and does not bind to its action key, so a mis-keyed one
// served for a std package's key breaks every consumer's build at package load
// ("package runtime is not in std" / "corrupt index") and neither the client's
// outputID hash nor its build-id guard can catch it. cmd/go recomputes an index
// locally for ~free, so dropping it from the shared cache costs nothing.
//
// A read/decompress error yields false (store it): the magic sits at the very
// start, so a well-formed index given a full first block is always recognized;
// failing open keeps a partial/garbled input from dropping a legitimate object.
// A false positive would only cost a recompute, but false negatives are the
// safer default here because the version-3 purge plus the client-side guard
// already bound any residual risk.
func looksLikeGoModuleIndex(input []byte, compression string) bool {
	data := input
	if compression == "lz4" {
		buf := make([]byte, indexMagicProbeBytes)
		zr := lz4.NewReader(bytes.NewReader(input))
		n, _ := io.ReadFull(zr, buf)
		// Return the reader's pooled buffers. pierrec/lz4 sizes its two internal
		// buffers from the frame header's BlockSizeIndex -- 4 MiB each for the
		// single-block frames the client writes -- and only releases them to the
		// pools on EOF or Reset. An abandoned 16-byte probe therefore leaked
		// ~8.4 MiB of allocation churn PER INSPECTED OBJECT (every PUT peek and
		// every read-path guard), keeping the pools empty so every peek malloc'd
		// fresh: the same GC-thrash -> admission-control-saturation failure mode
		// as the fixed 1 MiB PUT prealloc, ~8x larger. Reset(nil) puts both
		// buffers back so steady-state peeks allocate ~nothing.
		zr.Reset(nil)
		data = buf[:n]
	}
	return bytes.HasPrefix(data, []byte(goModuleIndexMagic))
}

// readIsModuleIndex reports whether r begins with the Go module-index magic,
// under the same `compression` metadata hint the PUT path consults. It is the
// shared detection core for every read path (the rewinding peek for a GET that
// keeps the file open, and the open-peek-close variant for the batch paths).
//
// For an lz4 body it streams an lz4.Reader straight over r and reads only the
// few decompressed bytes the magic needs: the reader pulls exactly as much
// COMPRESSED input from r as it takes to decode the first block, so the magic --
// which lives in that first block -- is always recovered regardless of how large
// the compressed block is. (This is the fix for the old fixed-512-byte peek,
// which truncated the single-block bodies the client sends and so never decoded
// the magic; see the package note above.) An uncompressed body is matched
// directly off its leading bytes.
//
// A read error yields (false, err): the magic is at the very start, so a
// well-formed index is always recognized, and the caller decides what a read
// failure means for it. The reader consumes from r, so a seekable caller that
// needs the stream intact afterward must rewind (the GET path does); the batch
// path opens a throwaway handle solely for this peek.
func readIsModuleIndex(r io.Reader, compression string) (bool, error) {
	if compression == "lz4" {
		buf := make([]byte, indexMagicProbeBytes)
		zr := lz4.NewReader(r)
		n, err := io.ReadFull(zr, buf)
		// Return the reader's two pooled 4 MiB buffers (see the matching Reset in
		// looksLikeGoModuleIndex): without this every read-path probe abandoned
		// them, costing ~8.4 MiB of allocation churn per inspected object on the
		// GET, batch, and prefetch paths.
		zr.Reset(nil)
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			// A decompression failure means the body is not a well-formed lz4
			// frame, hence not a module index we should evict; report "not an
			// index" so the caller serves/keeps the object (fail-open).
			return false, nil
		}
		return bytes.HasPrefix(buf[:n], []byte(goModuleIndexMagic)), nil
	}
	// Uncompressed: the magic is the very first bytes, so a tiny read suffices.
	buf := make([]byte, indexMagicProbeBytes)
	n, err := io.ReadFull(r, buf)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return false, err
	}
	return bytes.HasPrefix(buf[:n], []byte(goModuleIndexMagic)), nil
}

// evictModuleIndexOnRead is the read-path counterpart to the PutObject module-
// index guard, for a GET that already holds the object's open file. The PUT
// guard only stops a NEW index from being stored; a poisoned index already on
// disk (uploaded before that guard existed, after the one-time v3 startup purge
// had already run) is otherwise served verbatim on every GET and re-advertised
// in /_index on every restart -- and the client, which has no remote DELETE and
// never re-uploads an index, refuses and re-fetches it forever. This closes that
// loop: on read, detect such a blob, EVICT it (storage.Delete drops the file and
// the /_index entry, the same lever handleDeleteObject uses), and report a miss
// so the client recomputes the index locally. Each poisoned key is thus shed on
// its first post-deploy fetch -- a lazy, incremental self-heal that needs no
// cache-wide purge and works regardless of client version.
//
// It is scoped to indexed cacheprog keys (go-buildcache/v1<64-hex>, via
// extractActionHash) exactly like the outputid self-heal: those are the only
// keys advertised in /_index and the only ones that can carry a mis-keyed
// module index, so an arbitrary/non-cache object is never inspected or touched.
//
// The peek is non-destructive: f is SEEKED back to the start, so when this
// returns false the caller serves the body from byte 0 byte-for-byte unchanged.
// A read or seek error means we cannot guarantee an unchanged stream, so it is
// treated as "miss" (the object is left on disk -- Delete is not called -- and
// the caller reports a miss), which is safe because the client just re-fetches.
func evictModuleIndexOnRead(storage *Storage, key string, f io.ReadSeeker, meta *ObjectMeta) bool {
	// Only indexed cacheprog keys can be a poisoned module index; never inspect
	// or evict anything else.
	if _, ok := extractActionHash(key); !ok {
		return false
	}
	isIndex, readErr := readIsModuleIndex(f, meta.Metadata["compression"])
	// Rewind unconditionally so the non-index serve path reads from byte 0 --
	// identical to a no-peek read. A seek failure means we cannot promise an
	// unchanged stream, so report a miss rather than serve a consumed body.
	if _, seekErr := f.Seek(0, io.SeekStart); seekErr != nil {
		log.Printf("module-index guard: cannot rewind %q after peek (treated as miss, not evicted): %v", key, seekErr)
		return true
	}
	if readErr != nil {
		log.Printf("module-index guard: cannot inspect %q (treated as miss, not evicted): %v", key, readErr)
		return true
	}
	if !isIndex {
		return false
	}
	evictModuleIndex(storage, key)
	return true
}

// evictModuleIndexOnReadByKey is the batch-path counterpart: it OPENS the object
// at key, peeks it, and closes it, returning whether key holds a Go module-index
// blob (and evicting it when so). The batch paths collect entry metadata with
// Stat (no open file in hand) and must decide BEFORE building the manifest, so
// this self-contained open-peek-close variant detects without disturbing the
// later phase-2 streaming Open. Same scope (indexed cacheprog keys only) and
// same eviction (storage.Delete) as evictModuleIndexOnRead; no rewind is needed
// since the file is opened and closed solely for the peek. A key that vanished
// or cannot be opened is reported as "not an index" (false): the batch loops
// already treat a Stat/Open failure as a plain miss, so nothing regresses.
func evictModuleIndexOnReadByKey(storage *Storage, key string, meta *ObjectMeta) bool {
	if _, ok := extractActionHash(key); !ok {
		return false
	}
	f, _, err := storage.Open(key)
	if err != nil {
		return false
	}
	isIndex, readErr := readIsModuleIndex(f, meta.Metadata["compression"])
	f.Close()
	if readErr != nil {
		log.Printf("module-index guard: cannot inspect %q (treated as non-index): %v", key, readErr)
		return false
	}
	if !isIndex {
		return false
	}
	evictModuleIndex(storage, key)
	return true
}

// evictModuleIndex removes a confirmed module-index blob from the store and the
// /_index, counts it, and logs it once (the key is gone afterward, so it is
// self-limiting). storage.Delete is the same lever handleDeleteObject uses; a
// missing key (already evicted by a racing read) is not an error.
func evictModuleIndex(storage *Storage, key string) {
	if err := storage.Delete(key); err != nil && !errors.Is(err, ErrNotFound) {
		// Eviction failed; the caller still refuses to serve the poison. The next
		// read retries the eviction.
		log.Printf("module-index guard: failed to evict %q (still refused): %v", key, err)
		return
	}
	moduleIndexEvictionsTotal.Inc()
	log.Printf("module-index guard: evicted stored module-index blob %q on read (refused; client recomputes the index locally)", key)
}
