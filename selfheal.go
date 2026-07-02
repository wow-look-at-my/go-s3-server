package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"

	"github.com/pierrec/lz4/v4"
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
// build that needs the action into a permanent cache miss. The fix is to repair
// it, not evict it; see ensureOutputID.
func missingOutputID(meta *ObjectMeta) bool {
	return meta == nil || meta.Metadata[outputIDMetaKey] == ""
}

// ensureOutputID makes sure the object at key carries its outputid metadata,
// repairing it in place when it does not, and reports whether the object is
// usable as a cache hit. On success meta is updated to carry the outputid so the
// caller can serve it.
//
// Repair, not eviction, is deliberate. The GOCACHEPROG outputID is by definition
// sha256(decompressed body) -- the content address the client verifies on every
// GET -- so it can be reconstructed from the body itself, with no need for the
// original uploader. Deleting the object instead would throw away a perfectly
// good body, discard its audit xattrs (uploader/when/where), drop the key from
// /_index, and force a re-upload: an unauditable churn pipeline. Reconstructing
// the outputid in place keeps the bytes and the forensic trail, leaves the key
// indexed, and makes the object an immediate hit. The repair is one-time -- the
// next read sees the outputid and skips this path.
//
// If the body cannot be decompressed (genuinely corrupt or non-conforming, and
// unusable by the client regardless), it returns false WITHOUT deleting
// anything: the object is left on disk for the normal age/size eviction policy,
// and the caller reports a clean miss.
func ensureOutputID(storage *Storage, key string, meta *ObjectMeta) bool {
	if !missingOutputID(meta) {
		return true
	}
	// Self-heal applies only to GOCACHEPROG cache objects -- the
	// go-buildcache/v1<64-hex> keys that are advertised in /_index. Those are the
	// only keys with the permanent-miss problem: a client consults /_index and
	// skips re-uploading a key it sees there, so an indexed-but-outputid-less
	// object wedges forever. Any other key is not indexed, carries no cache-
	// protocol contract, and is served exactly as stored (an outputid is simply
	// not expected), so we never touch arbitrary objects.
	if _, ok := extractActionHash(key); !ok {
		return true
	}
	outputID, err := reconstructOutputID(storage, key)
	if err != nil {
		// The body is unusable (most often: cannot be lz4-decompressed) and the
		// outputid cannot be reconstructed, so this key can NEVER serve a hit.
		// Leaving it advertised in /_index would wedge it permanently: every
		// client is told to skip re-uploading an indexed key, yet every fetch is
		// a forced miss. Drop the key from the index (the FILE stays on disk for
		// forensics and the normal eviction policy) so the next consumer that
		// computes this action re-uploads a good body, overwriting the bad one.
		// Note the startup/sweep-end index rebuild re-advertises it from disk;
		// the next read then de-advertises it again — bounded churn, strictly
		// better than a permanent forced miss.
		if storage.Index != nil {
			storage.Index.Remove(key)
		}
		log.Printf("self-heal: cannot reconstruct outputid for %q (left on disk, de-advertised from index, treated as miss): %v", key, err)
		return false
	}
	selfHealRepairsTotal.Inc()
	if meta.Metadata == nil {
		meta.Metadata = map[string]string{}
	}
	meta.Metadata[outputIDMetaKey] = outputID
	log.Printf("self-heal: reconstructed outputid for %q in place (body and audit preserved, no eviction)", key)
	return true
}

// reconstructOutputID recomputes a stored object's outputID from its body and
// persists it as metadata, returning the value. The body is stored as an lz4
// frame (the client compresses every PUT and lz4-decompresses every GET), so the
// outputID is hex(sha256(lz4-decompressed body)). Decompression is streamed
// straight into the hash, so even this rare repair path never buffers a whole
// object in memory. Only the outputid xattr is written; the body and every other
// xattr (audit included) are left exactly as they were.
func reconstructOutputID(storage *Storage, key string) (string, error) {
	f, _, err := storage.Open(key)
	if err != nil {
		return "", err
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, lz4.NewReader(f)); err != nil {
		return "", fmt.Errorf("decompress body: %w", err)
	}
	outputID := hex.EncodeToString(h.Sum(nil))

	if err := storage.SetMeta(key, map[string]string{outputIDMetaKey: outputID}); err != nil {
		return "", fmt.Errorf("persist reconstructed outputid: %w", err)
	}
	return outputID, nil
}
