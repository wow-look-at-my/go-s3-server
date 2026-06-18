package main

import (
	"errors"
	"log"
)

// outputIDMetaKey is the metadata field holding the GOCACHEPROG outputID -- the
// content address of a cached object. The go-toolchain client sends it on every
// PUT (as X-Cache-Meta-Outputid) and requires it on every GET: without it the
// client cannot verify the body, so it discards the download and rebuilds. Every
// legitimately stored object therefore carries one.
const outputIDMetaKey = "outputid"

// missingOutputID reports whether a stored object lacks a usable outputID.
//
// Such an object is a relic of an earlier cache-data iteration, or one whose
// xattrs were stripped by a data-dir copy/restore that did not preserve them. It
// can never satisfy a client (the outputID is mandatory), yet its key stays
// advertised in /_index, so every client skips re-uploading it -- turning each
// build that needs the action into a permanent cache miss. The fix is to stop
// treating it as a hit; see selfHeal.
func missingOutputID(meta *ObjectMeta) bool {
	return meta == nil || meta.Metadata[outputIDMetaKey] == ""
}

// selfHeal evicts an outputID-less object so the cache repairs itself as it is
// read. Deleting the file also drops the key from /_index (Storage.Delete ->
// Index.Remove), which is the crucial half: once the key leaves the index a
// client stops skipping it and re-uploads a correct object on its next build.
//
// The caller treats the read as a miss regardless of the outcome here, so a
// failed delete simply heals on a later read -- it is logged but never fails the
// request. Evictions are one-time per bad key (the object is gone afterward), so
// the log/metric volume is bounded by the number of stale objects and trends to
// zero as the cache heals.
func selfHeal(storage *Storage, key string) {
	if err := storage.Delete(key); err != nil && !errors.Is(err, ErrNotFound) {
		log.Printf("self-heal: evict outputid-less object %q: %v", key, err)
		return
	}
	selfHealEvictionsTotal.Inc()
	log.Printf("self-heal: evicted outputid-less cache object %q (clean miss; next PUT repopulates)", key)
}
