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

// storedDigestMetaKey names the sha256 of an object's bytes AS STORED.
const storedDigestMetaKey = "storedsha256"

// ErrStoredDigestMismatch reports an upload whose bytes disagree with the digest its own metadata claims.
var ErrStoredDigestMismatch = errors.New("stored digest mismatch")

// storedDigest is the digest form every producer and checker here writes.
func storedDigest(sum []byte) string { return hex.EncodeToString(sum) }

// checkStoredDigest compares a claim against what arrived. An empty claim is
// no claim. An uploader that names no digest gets the computed a single
// stored for it, which is what puts a hash on every cached item.
func checkStoredDigest(claimed, computed string) error {
	if claimed == "" || strings.EqualFold(claimed, computed) {
		return nil
	}
	return fmt.Errorf("%w: claimed %s, received %s", ErrStoredDigestMismatch, claimed, computed)
}

// verifyStoredDigest reads f whole and reports whether it still hashes to the
// digest recorded for it.
func verifyStoredDigest(f *os.File, meta map[string]string) (bool, error) {
	return bodyMatchesStoredDigest(f, meta[storedDigestMetaKey])
}

// bodyMatchesStoredDigest is verifyStoredDigest for a caller that holds the
// recorded digest on its own rather than in a metadata map.
func bodyMatchesStoredDigest(f *os.File, want string) (bool, error) {
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
