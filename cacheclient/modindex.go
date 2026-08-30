package cacheclient

import "bytes"

// Leading bytes of a Go module index blob; the version-less prefix matches every index format (v1, v2, future vN).
const goModuleIndexMagic = "go index v"

// A module index blob binds to no action key and carries no build id, so a
// mis-keyed one silently breaks a build. The client refuses it on GET and PUT.
func IsGoModuleIndex(body []byte) bool {
	return bytes.HasPrefix(body, []byte(goModuleIndexMagic))
}
