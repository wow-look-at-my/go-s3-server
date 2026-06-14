package main

import (
	"bytes"
	"io"

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
