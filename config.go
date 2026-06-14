package main

import (
	"encoding/json"
	"fmt"
	"os"
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
// does not grow without bound. Both limits are independent and may be combined.
//
// Eviction is driven by an entry's "last used" time, defined as the later of
// its on-disk mtime (when it was written) and its last-access time (tracked
// in memory while the server runs). This keeps frequently-read but rarely-
// rewritten entries alive, which is exactly what "not accessed in a while"
// means for a write-once content-addressed cache. mtime itself is never
// rewritten on read, so the prefetch system's "same build" grouping (which
// keys on mtime) is unaffected.
type EvictionConfig struct {
	// MaxAge removes entries not used within this window. A pointer so an
	// absent field can take the built-in default while an explicit 0 / "0"
	// disables age-based eviction. Negative is a config error.
	MaxAge *Duration `json:"max_age"`
	// MaxBytes is a total-size budget for the data_dir. When exceeded, the
	// least-recently-used entries are evicted until the total is back under
	// budget. 0 disables size-based eviction.
	MaxBytes int64 `json:"max_bytes"`
	// Interval is how often the background sweeper runs. 0 → default.
	Interval Duration `json:"interval"`
}

// AgeLimit returns the configured max-age as a time.Duration (0 if disabled).
func (e EvictionConfig) AgeLimit() time.Duration {
	if e.MaxAge == nil {
		return 0
	}
	return e.MaxAge.Std()
}

// Enabled reports whether any eviction policy is active.
func (e EvictionConfig) Enabled() bool {
	return e.AgeLimit() > 0 || e.MaxBytes > 0
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
	// fills. See EvictionConfig. Enabled by default with a conservative
	// max_age; set eviction.max_age to 0 to opt out.
	Eviction EvictionConfig `json:"eviction"`
}

// Resource-limit defaults. Both are generous: under normal CI load the server
// never approaches them, but they bound the worst case so a load spike degrades
// (503 / 413) instead of OOM-killing the process.
const (
	defaultMaxConcurrentRequests = 128
	defaultMaxObjectBytes        = 1 << 30 // 1 GiB
	// defaultEvictionMaxAge is the idle window after which an unused cache
	// entry is pruned by default. 30 days is generous: anything not touched in
	// a month is almost certainly a stale action ID from code that has since
	// changed, and re-fetching a wrongly-evicted entry only costs one rebuild.
	defaultEvictionMaxAge = 30 * 24 * time.Hour
	// defaultEvictionInterval is how often the sweeper runs when eviction is on.
	defaultEvictionInterval = time.Hour
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
	// Eviction: an absent max_age takes the default; an explicit 0 disables it.
	if cfg.Eviction.MaxAge == nil {
		d := Duration(defaultEvictionMaxAge)
		cfg.Eviction.MaxAge = &d
	}
	if cfg.Eviction.AgeLimit() < 0 {
		return nil, fmt.Errorf("config: eviction.max_age must not be negative")
	}
	if cfg.Eviction.MaxBytes < 0 {
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
