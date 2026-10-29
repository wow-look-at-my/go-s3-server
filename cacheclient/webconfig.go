// WebConfig holds the configuration for a web cache backend.

package cacheclient

import (
	"math"
	"time"
)

// WebConfig holds the configuration for a web cache backend.
type WebConfig struct {
	Bucket    string // Required. Empty bucket disables the backend.
	Endpoint  string // Endpoint URL (e.g. "https://cache.example.com"). Required.
	Prefix    string // Key prefix (defaults to "go-buildcache/").
	AccessKey string // Basic Auth username
	SecretKey string // Basic Auth password
	Version   string // go-toolchain version, stored as object metadata
	Module    string // main module path, stored as object metadata (provenance)
	Target    string // GOOS/GOARCH this build is producing, sent as provenance
	// Command names what this process builds, for example a digest of its arguments.
	Command string
	// IndexDir is where the key index's disk copy lives. Empty means the process temporary directory.
	IndexDir string
	// IndexMaxAge is how long a disk copy of the index is served as authoritative with no request to the server.
	IndexMaxAge time.Duration
}

// IndexMaxAgeDefault is the floor a empty WebConfig.IndexMaxAge takes outside CI.
const IndexMaxAgeDefault = 10 * time.Minute

// IndexMaxAgeCI is what a empty WebConfig.IndexMaxAge takes in CI.
const IndexMaxAgeCI = time.Duration(math.MaxInt64)
