//go:build !js

package cachedisk

import (
	"errors"
	"syscall"
)

// isETXTBSY reports whether a running program holds the file, which unix
// refuses to replace. That is a miss rather than a failure.
func isETXTBSY(err error) bool {
	return errors.Is(err, syscall.ETXTBSY)
}
