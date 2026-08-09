package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// ConfigString is a string that can be specified as a literal or as an
// environment variable reference: {"type": "envvar", "name": "VAR_NAME"}.
type ConfigString struct {
	Value  string // resolved value
	EnvVar string // non-empty if sourced from an env var
}

func (cs *ConfigString) UnmarshalJSON(data []byte) error {
	// Try plain string first.
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		cs.Value = s
		return nil
	}

	// Try envvar object.
	var obj struct {
		Type string `json:"type"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(data, &obj); err != nil {
		return fmt.Errorf("config value must be a string or {\"type\": \"envvar\", \"name\": \"...\"}")
	}
	if obj.Type != "envvar" {
		return fmt.Errorf("config value object type must be \"envvar\", got %q", obj.Type)
	}
	if obj.Name == "" {
		return fmt.Errorf("config envvar name must not be empty")
	}
	cs.EnvVar = obj.Name
	cs.Value = os.Getenv(obj.Name)
	return nil
}

func (cs ConfigString) MarshalJSON() ([]byte, error) {
	return json.Marshal(cs.Value)
}

// String returns the resolved value.
func (cs ConfigString) String() string {
	return cs.Value
}

type Credential struct {
	Username ConfigString `json:"username"`
	Password ConfigString `json:"password"`
}

type WriteOnceConfig struct {
	Action       string `json:"action"`       // "allow" or "deny"
	Notification string `json:"notification"` // "never", "always", "content_differs"
}

// Duration is a time.Duration that unmarshals from a Go duration string
// ("720h", "30m", "0") or from a JSON number interpreted as seconds. It
// marshals back to the canonical Go duration string.
type Duration time.Duration

func (d *Duration) UnmarshalJSON(data []byte) error {
	// JSON null means "not set", which for a duration is zero. Erroring on it
	// would reject a config that spells an absent field out explicitly.
	if string(data) == "null" {
		*d = 0
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		if s == "" {
			*d = 0
			return nil
		}
		parsed, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("invalid duration %q (use a Go duration like \"720h\" or \"30m\"): %w", s, err)
		}
		*d = Duration(parsed)
		return nil
	}
	var n float64
	if err := json.Unmarshal(data, &n); err != nil {
		return fmt.Errorf("duration must be a string like \"720h\" or a number of seconds")
	}
	*d = Duration(time.Duration(n * float64(time.Second)))
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// Std returns the underlying time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// EvictionConfig controls automatic pruning of cache entries so the data_dir
// does not grow without bound.
//
// The cache is an LRU: by default it is bounded by SIZE (max_bytes), and when
// it is over budget the least recently used entries go first. Age eviction
// (max_age) is a separate, off-by-default TTL -- a build cache entry that is
// still being read is still useful however old it is, so nothing is dropped
// for age alone unless an operator asks for it.
//
// "Last used" is the latest of an entry's on-disk mtime (when it was written),
// its filesystem access time (advanced by the kernel whenever its body is
// read, and durable across restarts), and any read this process recorded in
// memory. mtime itself is never rewritten on read, so the prefetch system's
// "same build" grouping (which keys on mtime) is unaffected.
type EvictionConfig struct {
	// MaxAge removes entries not used within this window. Absent or 0 leaves
	// age eviction off; negative is a config error.
	MaxAge Duration `json:"max_age"`
	// MaxBytes is the total-size budget for the data_dir: over budget, the
	// least-recently-used entries are evicted until the total is back under it.
	// A pointer so an absent field can take the built-in default (or the
	// CACHE_MAX_BYTES env var) while an explicit 0 disables size eviction.
	MaxBytes *int64 `json:"max_bytes"`
	// Interval is how often the background sweeper runs. 0 → default.
	Interval Duration `json:"interval"`
}

// AgeLimit returns the configured max-age as a time.Duration (0 if disabled).
func (e EvictionConfig) AgeLimit() time.Duration {
	return e.MaxAge.Std()
}

// SizeLimit returns the configured size budget in bytes (0 if disabled). It is
// only meaningful after LoadConfig has applied the default.
func (e EvictionConfig) SizeLimit() int64 {
	if e.MaxBytes == nil {
		return 0
	}
	return *e.MaxBytes
}

// Enabled reports whether any eviction policy is active.
func (e EvictionConfig) Enabled() bool {
	return e.AgeLimit() > 0 || e.SizeLimit() > 0
}

type Config struct {
	Listen        string          `json:"listen"`
	MetricsListen string          `json:"metrics_listen"`
	Bucket        string          `json:"bucket"`
	DataDir       string          `json:"data_dir"`
	WriteOnce     WriteOnceConfig `json:"write_once"`
	DisableAuth   bool            `json:"disable_auth"`
	Credentials   []Credential    `json:"credentials"`

	// MaxConcurrentRequests bounds in-flight requests; excess requests are shed
	// with 503 + Retry-After instead of piling up until the process OOMs (which
	// a fronting proxy then surfaces as a 502). 0 → default.
	MaxConcurrentRequests int `json:"max_concurrent_requests"`
	// MaxObjectBytes caps a single PUT body. 0 → default. The body is streamed
	// to disk, so this guards disk, not memory.
	MaxObjectBytes int64 `json:"max_object_bytes"`

	// Eviction bounds the on-disk cache so it does not grow until the disk
	// fills. See EvictionConfig. Enabled by default with a size budget; set
	// eviction.max_bytes to 0 to opt out.
	Eviction EvictionConfig `json:"eviction"`
}

// Resource-limit defaults. Both are generous: under normal CI load the server
// never approaches them, but they bound the worst case so a load spike degrades
// (503 / 413) instead of OOM-killing the process.
const (
	defaultMaxConcurrentRequests = 128
	defaultMaxObjectBytes        = 1 << 30 // 1 GiB
	// defaultEvictionMaxBytes is the cache's size budget when neither the
	// config nor CACHE_MAX_BYTES sets one. A bound has to exist by default:
	// unbounded, the cache grows until the disk fills, and every stored object
	// also costs index memory in this process.
	defaultEvictionMaxBytes = 50 << 30 // 50 GiB
	// maxBytesEnvVar overrides defaultEvictionMaxBytes without a config edit,
	// which is how the deployment sizes the cache to the volume it mounted.
	// An explicit eviction.max_bytes in the config still wins.
	maxBytesEnvVar = "CACHE_MAX_BYTES"
	// defaultEvictionInterval is how often the sweeper runs when eviction is on.
	// Daily: each sweep walks the whole data_dir, and a day bounds how far the
	// cache can overshoot its size budget between sweeps. The sweeper also runs
	// at startup when the recorded last sweep is at least this old, so a
	// frequently-restarted deployment still sweeps.
	defaultEvictionInterval = 24 * time.Hour
)

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if cfg.Listen == "" {
		cfg.Listen = ":9000"
	}
	if cfg.WriteOnce.Action == "" {
		cfg.WriteOnce.Action = "allow"
	}
	if cfg.WriteOnce.Notification == "" {
		cfg.WriteOnce.Notification = "never"
	}
	switch cfg.WriteOnce.Action {
	case "allow", "deny":
	default:
		return nil, fmt.Errorf("config: write_once.action must be \"allow\" or \"deny\"")
	}
	switch cfg.WriteOnce.Notification {
	case "never", "always", "content_differs":
	default:
		return nil, fmt.Errorf("config: write_once.notification must be \"never\", \"always\", or \"content_differs\"")
	}
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("config: bucket is required")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("config: data_dir is required")
	}
	if cfg.MaxConcurrentRequests <= 0 {
		cfg.MaxConcurrentRequests = defaultMaxConcurrentRequests
	}
	if cfg.MaxObjectBytes <= 0 {
		cfg.MaxObjectBytes = defaultMaxObjectBytes
	}
	// Eviction: an absent max_bytes takes CACHE_MAX_BYTES or the built-in
	// default; an explicit 0 disables the size budget. max_age is off unless
	// asked for -- the cache is an LRU, not a TTL.
	if cfg.Eviction.AgeLimit() < 0 {
		return nil, fmt.Errorf("config: eviction.max_age must not be negative")
	}
	if cfg.Eviction.MaxBytes == nil {
		n, err := envMaxBytes()
		if err != nil {
			return nil, err
		}
		cfg.Eviction.MaxBytes = &n
	}
	if *cfg.Eviction.MaxBytes < 0 {
		return nil, fmt.Errorf("config: eviction.max_bytes must not be negative")
	}
	if cfg.Eviction.Interval <= 0 {
		cfg.Eviction.Interval = Duration(defaultEvictionInterval)
	}
	if cfg.DisableAuth {
		if len(cfg.Credentials) != 0 {
			return nil, fmt.Errorf("config: disable_auth is true but credentials are set; remove credentials or set disable_auth to false")
		}
	} else {
		if len(cfg.Credentials) == 0 {
			return nil, fmt.Errorf("config: at least one credential is required (or set disable_auth: true to run without authentication)")
		}
		for i, c := range cfg.Credentials {
			if c.Username.Value == "" || c.Password.Value == "" {
				return nil, fmt.Errorf("config: credential %d: username and password must both be non-empty (set disable_auth: true to run without authentication)", i)
			}
		}
	}
	return &cfg, nil
}

// envMaxBytes resolves the cache size budget from CACHE_MAX_BYTES, falling back
// to the built-in default when it is unset or empty. A value that is SET but
// unparseable is a typo the operator needs to hear about, so it fails the load
// rather than silently reverting to the default.
func envMaxBytes() (int64, error) {
	raw := strings.TrimSpace(os.Getenv(maxBytesEnvVar))
	if raw == "" {
		return defaultEvictionMaxBytes, nil
	}
	n, err := parseByteSize(raw)
	if err != nil {
		return 0, fmt.Errorf("config: %s=%q: %w", maxBytesEnvVar, raw, err)
	}
	return n, nil
}

// byteSizeUnits are the suffixes parseByteSize accepts, longest first so "KiB"
// is matched before "K". Both the binary (KiB) and the decimal-looking (KB, K)
// spellings mean powers of 1024: a cache budget is disk space, and nobody
// writing "50GB" for one means 50,000,000,000 bytes exactly.
var byteSizeUnits = []struct {
	suffix string
	mult   int64
}{
	{"KIB", 1 << 10}, {"MIB", 1 << 20}, {"GIB", 1 << 30}, {"TIB", 1 << 40},
	{"KB", 1 << 10}, {"MB", 1 << 20}, {"GB", 1 << 30}, {"TB", 1 << 40},
	{"K", 1 << 10}, {"M", 1 << 20}, {"G", 1 << 30}, {"T", 1 << 40},
	{"B", 1},
}

// parseByteSize parses a byte count, with or without a size suffix: "50GB",
// "50 GiB", "512M", "1073741824".
func parseByteSize(s string) (int64, error) {
	up := strings.ToUpper(strings.TrimSpace(s))
	mult := int64(1)
	for _, u := range byteSizeUnits {
		if rest, ok := strings.CutSuffix(up, u.suffix); ok {
			up, mult = rest, u.mult
			break
		}
	}
	up = strings.TrimSpace(up)
	n, err := strconv.ParseInt(up, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("not a byte size (want e.g. \"50GB\" or a plain byte count)")
	}
	if n < 0 {
		return 0, fmt.Errorf("must not be negative")
	}
	if mult > 1 && n > (1<<62)/mult {
		return 0, fmt.Errorf("byte size overflows int64")
	}
	return n * mult, nil
}
