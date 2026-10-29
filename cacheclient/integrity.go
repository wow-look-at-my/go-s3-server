// StoredDigestMetaKey names the metadata field carrying the sha256 of the bytes an upload puts on the wire.

package cacheclient

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

// StoredDigestMetaKey names the metadata field carrying the sha256 of the bytes an upload puts on the wire.
const StoredDigestMetaKey = "storedsha256"

// OutputIDMatches verifies body's sha256 against outputID (the GOCACHEPROG
// contract), so remote corruption is never cached and served as valid.
func OutputIDMatches(outputID string, body []byte) (got string, ok bool) {
	sum := sha256.Sum256(body)
	got = hex.EncodeToString(sum[:])
	return got, strings.EqualFold(got, outputID)
}

// StoredDigest is the digest an upload claims for its own bytes.
func StoredDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
