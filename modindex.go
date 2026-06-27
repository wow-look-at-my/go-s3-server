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

// indexPeekBytes is how many leading bytes of an upload we inspect. The magic is
// the very first thing in an index blob, and lz4 stores a small input's opening
// bytes as literals in the first block, so a few hundred compressed bytes always
// cover it. We never need the whole object, so the peek stays cheap and the rest
// of the body keeps streaming.
const indexPeekBytes = 512

// looksLikeGoModuleIndex reports whether prefix (the leading bytes of an upload,
// possibly lz4-compressed per the compression hint) begins with the Go module
// index magic.
//
// The module index is the one build-cache payload this server must refuse: it
// carries no build id and does not bind to its action key, so a mis-keyed one
// served for a std package's key breaks every consumer's build at package load
// ("package runtime is not in std" / "corrupt index") and neither the client's
// outputID hash nor its build-id guard can catch it. cmd/go recomputes an index
// locally for ~free, so dropping it from the shared cache costs nothing.
//
// A read/decompress error yields false (store it): the magic sits at the very
// start, so a well-formed index is always recognized; failing open keeps a
// truncated peek from dropping a legitimate object. A false positive would only
// cost a recompute, but false negatives are the safer default here because the
// version-3 purge plus the client-side guard already bound any residual risk.
func looksLikeGoModuleIndex(prefix []byte, compression string) bool {
	data := prefix
	if compression == "lz4" {
		buf := make([]byte, len(goModuleIndexMagic))
		n, _ := io.ReadFull(lz4.NewReader(bytes.NewReader(prefix)), buf)
		data = buf[:n]
	}
	return bytes.HasPrefix(data, []byte(goModuleIndexMagic))
}

// readIsModuleIndex reads at most indexPeekBytes leading bytes from r and
// reports whether they are the Go module-index magic, under the same
// `compression` metadata hint the PUT path consults. It is the shared detection
// core for every read path (the rewinding peek for a GET that keeps the file
// open, and the open-peek-close variant for the batch paths). A read error
// yields (false, err): the magic is at the very start, so a well-formed index is
// always recognized, and the caller decides what a read failure means for it.
func readIsModuleIndex(r io.Reader, compression string) (bool, error) {
	prefix := make([]byte, indexPeekBytes)
	n, err := io.ReadFull(r, prefix)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return false, err
	}
	return looksLikeGoModuleIndex(prefix[:n], compression), nil
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
