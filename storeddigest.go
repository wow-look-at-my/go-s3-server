package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// storedDigestMetaKey names the sha256 of an object's bytes AS STORED. It
// travels as ordinary metadata, so it reaches a reader in the usual header.
//
// outputid is the other hash an object carries, and the two answer different
// questions. outputid is the content address of the DECOMPRESSED body, so only
// a reader that decompresses can check it, and only the client does. This
// digest describes the exact bytes on disk. So it covers a key the cache
// protocol says nothing about, it catches a body that changed under a key that
// still carries the right outputid, and it costs a hash of what is already
// being copied rather than a decompression pass.
const storedDigestMetaKey = "storedsha256"

// ErrStoredDigestMismatch reports an upload whose bytes disagree with the
// digest its own metadata claims. The body is refused rather than stored: the
// alternative is a key that every reader has to discover is corrupt.
var ErrStoredDigestMismatch = errors.New("stored digest mismatch")

// storedDigest is the digest form every producer and checker here writes.
func storedDigest(sum []byte) string { return hex.EncodeToString(sum) }

// checkStoredDigest compares a claim against what arrived. An empty claim is
// no claim: an uploader that names no digest gets the computed one stored for
// it, which is what puts a hash on every cached item.
func checkStoredDigest(claimed, computed string) error {
	if claimed == "" || strings.EqualFold(claimed, computed) {
		return nil
	}
	return fmt.Errorf("%w: claimed %s, received %s", ErrStoredDigestMismatch, claimed, computed)
}

// verifyStoredDigest reads f whole and reports whether it still hashes to the
// digest recorded for it. A file with no recorded digest predates the stamp and
// reports true: an object stored before this existed is not evidence of rot.
// The file is rewound afterwards, so a caller that goes on to serve it streams
// the same bytes this read.
func verifyStoredDigest(f *os.File, meta map[string]string) (bool, error) {
	want := meta[storedDigestMetaKey]
	if want == "" {
		return true, nil
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return false, err
	}
	sum := sha256.New()
	if _, err := io.Copy(sum, f); err != nil {
		return false, err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return false, err
	}
	return strings.EqualFold(want, storedDigest(sum.Sum(nil))), nil
}
