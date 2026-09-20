package cacheclient

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
)

// bytesReader answers a reader over data, for a body that is already in hand.
func bytesReader(data []byte) io.ReadSeeker { return bytes.NewReader(data) }

// joinErrors is errors.Join, named here so the tier reads the same whatever
// the module's Go version allows.
func joinErrors(errs ...error) error { return errors.Join(errs...) }

// decodeOutputID reads an output ID the store reported.
func decodeOutputID(text string) (OutputID, error) {
	var out OutputID
	raw, err := hex.DecodeString(text)
	if err != nil {
		return out, err
	}
	if len(raw) != len(out) {
		return out, fmt.Errorf("output id %q is %d bytes, not %d", text, len(raw), len(out))
	}
	copy(out[:], raw)
	return out, nil
}

// countBytes reports a count and what it weighs.
func countBytes(count, bytes int64) string {
	return fmt.Sprintf("%d (%s)", count, formatMB(bytes))
}

// formatMB reports a byte count in megabytes.
func formatMB(bytes int64) string {
	return fmt.Sprintf("%.1f MB", float64(bytes)/(1<<20))
}
