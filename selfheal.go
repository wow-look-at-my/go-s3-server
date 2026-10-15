package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"os"
)

// outputIDMetaKey is the metadata field holding the GOCACHEPROG outputID -- the content address of a cached object.
const outputIDMetaKey = "outputid"

// missingOutputID reports whether a stored object lacks a usable outputID.
func missingOutputID(meta *ObjectMeta) bool {
	return meta == nil || meta.Metadata[outputIDMetaKey] == ""
}

// ensureOutputID makes sure the object at key carries its outputid metadata,
// repairing it in place. This happens when it does not, and reports whether
// the object is usable as a cache hit. On success meta is updated to carry
// the outputid so the caller can serve it.
//
// Callers without an open handle (the batch paths) pass nil and a private
// handle is used — hashed and stamped through the same fd.
func ensureOutputID(storage *Storage, key string, meta *ObjectMeta, f *os.File) bool {
	if !missingOutputID(meta) {
		return true
	}
	// Self-heal applies only to GOCACHEPROG cache objects -- the
	// go-buildcache/v1<64-hex> keys that are advertised in /_index.
	if _, ok := extractActionHash(key); !ok {
		return true
	}
	outputID, err := reconstructOutputID(storage, key, f)
	if err != nil {
		// The body is unusable (most often: cannot be decompressed) and the
		// outputid cannot be reconstructed, so this key can NEVER serve a hit.
		if storage.Index != nil {
			storage.Index.Remove(key)
		}
		selfHealFailuresTotal.Inc()
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
// persists it as metadata, returning the value. The body is stored compressed
// (the client compresses every PUT and decompresses every GET), so the
// outputID is hex(sha256(decompressed body)). Decompression is streamed
// straight into the hash, so even this rare repair path never buffers a whole
// object in memory. Only the outputid xattr is written; the body and every other
// xattr (audit included) are left exactly as they were.
func reconstructOutputID(storage *Storage, key string, f *os.File) (string, error) {
	callerOwned := f != nil
	if !callerOwned {
		// openRaw, not Open: the repair read is not a client-visible serve.
		opened, err := storage.openRaw(key)
		if err != nil {
			return "", err
		}
		defer opened.Close()
		f = opened
	}

	// The digest recorded with the body decides whether these bytes are the ones
	// that were stored. A body disagreeing with it yields no content address.
	if ok, err := bodyMatchesStoredDigest(f, getMetadataValueFd(f, storedDigestMetaKey)); err != nil {
		return "", fmt.Errorf("check stored digest: %w", err)
	} else if !ok {
		storedDigestMismatchTotal.WithLabelValues("selfheal").Inc()
		return "", fmt.Errorf("%w: body no longer matches the digest stored with it", ErrStoredDigestMismatch)
	}

	h := sha256.New()
	zr, release, codec, decErr := decompressingReader(f)
	if decErr != nil {
		return "", fmt.Errorf("decompress body: %w", decErr)
	}
	if codec == "" {
		// The body opens with neither frame magic, so it is not something this cache stored.
		release()
		return "", fmt.Errorf("decompress body: not a compressed frame")
	}
	_, copyErr := io.Copy(h, zr)
	release()
	if callerOwned {
		// If the rewind fails the stream is poisoned, so the repair fails (the
		// caller reports a miss rather than serving a consumed fd).
		if _, seekErr := f.Seek(0, io.SeekStart); seekErr != nil {
			return "", fmt.Errorf("rewind after hash: %w", seekErr)
		}
	}
	if copyErr != nil {
		return "", fmt.Errorf("decompress body: %w", copyErr)
	}
	outputID := hex.EncodeToString(h.Sum(nil))

	// Mismatch tripwire. This repair only runs when the metadata read reported
	// no outputid. Finding a DIFFERENT a single on the inode now means someone
	// stamped a value. That disagrees with the body hash — the historical
	// stale-stamp corruption replaying.
	if current := getMetadataValueFd(f, outputIDMetaKey); current != "" && current != outputID {
		outputIDMismatchTotal.Inc()
		log.Printf("self-heal: outputid on %q disagrees with its body hash (found %.8s..., recomputed %.8s...); repairing", key, current, outputID)
	}

	if err := setMetadataFd(f, map[string]string{outputIDMetaKey: outputID}); err != nil {
		return "", fmt.Errorf("persist reconstructed outputid: %w", err)
	}
	// An fsetxattr leaves the inode's mtime and size alone.
	storage.forgetMeta(key)
	return outputID, nil
}
