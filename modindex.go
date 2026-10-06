package main

import (
	"bytes"
	"errors"
	"io"
	"log"

	"github.com/pierrec/lz4/v4"
)

// goModuleIndexMagic is the leading bytes of a Go module index blob.
const goModuleIndexMagic = "go index v"

// indexMagicProbeBytes is how many DEcompressed bytes we read to recognize the magic.
const indexMagicProbeBytes = 16

const lz4HeadPeekBytes = 64

// indexPutPeekBytes bounds how many COMPRESSED leading bytes the PUT path.
const indexPutPeekBytes = 1 << 20

// That was wrong: the client stores each body as a SINGLE lz4 block.
// Pierrec/lz4's reader must have the entire block before it can decode any
// output. Both the read/evict path and the PUT guard were broken by the same
// truncation. The fix feeds the detector enough input to decode the earliest
// block (where the magic lives). The read path streams an lz4.Reader straight off
// the open file (it pulls exactly a single block). The PUT path reads a bounded
// but block-sized prefix.

// looksLikeGoModuleIndex reports whether the input (the leading bytes of an
// upload, possibly lz4-compressed per the compression hint) begins with the
// Go module index magic.
func looksLikeGoModuleIndex(input []byte, compression string) bool {
	// A zstd body settles through the shared decoder, which reads only the bytes
	// the magic needs.
	if frameCodec(input) == "zstd" {
		match, _ := readIsModuleIndex(bytes.NewReader(input), compression)
		return match
	}
	data := input
	if compression == "lz4" || frameCodec(input) == "lz4" {
		// Settle it from the frame header and earliest literal run when possible;
		// only an unusual frame shape falls through to a real decode (lz4head.go).
		if match, decided := lz4HasPrefix(input, goModuleIndexMagic); decided {
			return match
		}
		buf := make([]byte, indexMagicProbeBytes)
		zr := lz4.NewReader(bytes.NewReader(input))
		n, _ := io.ReadFull(zr, buf)
		// Return the reader's pooled buffers. Reset(nil) puts both buffers back so steady-state peeks allocate ~nothing.
		zr.Reset(nil)
		data = buf[:n]
	}
	return bytes.HasPrefix(data, []byte(goModuleIndexMagic))
}

// readIsModuleIndex reports whether r begins with the Go module-index magic,
// under the same `compression` metadata hint the PUT path consults. It is the
// shared detection core for every read path (the rewinding peek for a GET
// that keeps the file open, and the open-peek-close variant for the batch
// paths).
//
// For an lz4 body it streams an lz4.Reader straight over r. It reads only the
// few decompressed bytes the magic needs: the reader pulls exactly as much
// COMPRESSED input from r as it takes to decode the earliest block, so the
// magic -- which lives in that earliest block. It is always recovered
// regardless of how large the compressed block is. (This is the fix for the
// fixed-512-byte peek, which truncated the single-block bodies the client
// sends and so never decoded the magic; see the package note above.) An
// uncompressed body is matched directly off its leading bytes.
func readIsModuleIndex(r io.Reader, compression string) (bool, error) {
	// Peek far enough to name the codec AND to run lz4's header fast path.
	var head [lz4HeadPeekBytes]byte
	headN, headErr := io.ReadFull(r, head[:])
	if headErr != nil && !errors.Is(headErr, io.EOF) && !errors.Is(headErr, io.ErrUnexpectedEOF) {
		return false, headErr
	}
	codec := frameCodec(head[:headN])
	r = io.MultiReader(bytes.NewReader(head[:headN]), r)

	if codec == "zstd" {
		// zstd has no equivalent of lz4's readable earliest literal run, so the verdict costs a single decoded block.
		zr, release, _, err := decompressingReader(r)
		if err != nil {
			return false, nil // unreadable frame: not an index, fail open
		}
		defer release()
		buf := make([]byte, indexMagicProbeBytes)
		n, err := io.ReadFull(zr, buf)
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			return false, nil
		}
		return bytes.HasPrefix(buf[:n], []byte(goModuleIndexMagic)), nil
	}

	if compression == "lz4" || codec == "lz4" {
		// Fast path: the leading decompressed bytes are readable straight out of the frame's earliest literal run (lz4head.go).
		n := headN
		if match, decided := lz4HasPrefix(head[:n], goModuleIndexMagic); decided {
			return match, nil
		}
		// Undecided: decode. r already replays the consumed head.
		buf := make([]byte, indexMagicProbeBytes)
		src := &errTrackingReader{r: r}
		zr := lz4.NewReader(src)
		n, err := io.ReadFull(zr, buf)
		zr.Reset(nil)
		if src.err != nil {
			// The SOURCE failed: a genuine I/O error, not a format problem.
			return false, src.err
		}
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
			// The source read fine but the bytes are not a well-formed lz4 frame, hence not a module index we should evict; report "not an index".
			return false, nil
		}
		return bytes.HasPrefix(buf[:n], []byte(goModuleIndexMagic)), nil
	}
	// Uncompressed: the magic is the earliest bytes, so a tiny read suffices.
	buf := make([]byte, indexMagicProbeBytes)
	n, err := io.ReadFull(r, buf)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return false, err
	}
	return bytes.HasPrefix(buf[:n], []byte(goModuleIndexMagic)), nil
}

// errTrackingReader records the earliest non-EOF error returned by the
// wrapped reader.
type errTrackingReader struct {
	r   io.Reader
	err error
}

func (t *errTrackingReader) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		t.err = err
	}
	return n, err
}

// readGuardVerdict is the outcome of the GET-path module-index guard.
type readGuardVerdict int

const (
	// guardServe: not a module index — serve the body unchanged.
	guardServe readGuardVerdict = iota
	// guardEvicted: a stored module-index blob was detected and evicted.
	guardEvicted
	// guardPeekError: the body could not be inspected (or the stream could not be rewound).
	guardPeekError
)

// evictModuleIndexOnRead is the read-path counterpart to the PutObject
// module- index guard, for a GET that already holds the object's open file.
// This closes that loop: on read, detect such a blob, EVICT it
// (storage.Delete drops the file and the /_index entry, the same lever
// handleDeleteObject uses), and report a miss so the client recomputes the
// index locally. Each poisoned key is thus shed on its earliest post-deploy
// fetch -- a lazy, incremental self-heal that needs no cache-wide purge. It
// works regardless of client version.
func evictModuleIndexOnRead(storage *Storage, key string, f io.ReadSeeker, meta *ObjectMeta) readGuardVerdict {
	// Only indexed cacheprog keys can be a poisoned module index; never inspect or evict anything else.
	hash, ok := extractActionHash(key)
	if !ok {
		return guardServe
	}
	// Known-clean memo: this exact body already passed the probe on an earlier
	// read (and nothing has overwritten/deleted it since, or the memo entry.
	if storage.keyKnownClean(hash) {
		return guardServe
	}
	isIndex, readErr := readIsModuleIndex(f, meta.Metadata["compression"])
	// A seek failure means we cannot promise an unchanged stream, so report a
	// miss rather than serve a consumed body.
	if _, seekErr := f.Seek(0, io.SeekStart); seekErr != nil {
		log.Printf("module-index guard: cannot rewind %q after peek (treated as miss, not evicted): %v", key, seekErr)
		return guardPeekError
	}
	if readErr != nil {
		log.Printf("module-index guard: cannot inspect %q (treated as miss, not evicted): %v", key, readErr)
		return guardPeekError
	}
	if !isIndex {
		storage.markKeyClean(hash)
		return guardServe
	}
	evictModuleIndex(storage, key)
	return guardEvicted
}

// evictModuleIndexOnReadByKey is the batch-path counterpart. It OPENS the
// object at key, peeks it, and closes it, returning whether key holds a Go
// module-index blob (and evicting it when so). The batch paths collect entry
// metadata with Stat (no open file in hand) and must decide BEFORE building
// the manifest. This self-contained open-peek-close variant detects without
// disturbing the later phase-2 streaming Open.
func evictModuleIndexOnReadByKey(storage *Storage, key string, meta *ObjectMeta) bool {
	hash, ok := extractActionHash(key)
	if !ok {
		return false
	}
	// Known-clean memo: skip the open+decode when this body already passed the
	// probe on an earlier read (see evictModuleIndexOnRead).
	if storage.keyKnownClean(hash) {
		return false
	}
	f, err := storage.openRaw(key)
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
		storage.markKeyClean(hash)
		return false
	}
	evictModuleIndex(storage, key)
	return true
}

// evictModuleIndex removes a confirmed module-index blob from the store and the
// /_index, counts it, and logs it a single time (the key is gone afterward, so
// it is self-limiting). storage.Delete is the same lever handleDeleteObject
// uses; a missing key (already evicted by a racing read) is not an error.
func evictModuleIndex(storage *Storage, key string) {
	if err := storage.Delete(key); err != nil && !errors.Is(err, ErrNotFound) {
		// Eviction failed; the caller still refuses to serve the poison. The next read retries the eviction.
		log.Printf("module-index guard: failed to evict %q (still refused): %v", key, err)
		return
	}
	moduleIndexEvictionsTotal.Inc()
	log.Printf("module-index guard: evicted stored module-index blob %q on read (refused; client recomputes the index locally)", key)
}
