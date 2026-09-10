package cacheclient

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"strings"
)

// ConfigEnv holds the client's whole configuration, as base64-encoded JSON.
const ConfigEnv = "GO_BUILDCACHE_CONFIG"

// envConfig is the JSON inside ConfigEnv.
//
// The server authenticates with HTTP Basic Auth, so the credential fields are
// username and password. The S3-era spellings are still read, because a
// consumer's CI configuration outlives any one release.
type envConfig struct {
	Endpoint string `json:"endpoint"`
	Bucket   string `json:"bucket"`

	Username string `json:"username"`
	Password string `json:"password"`

	KeyID     string `json:"key_id"`     // deprecated spelling of username
	AccessKey string `json:"access_key"` // deprecated spelling of password
	Region    string `json:"region"`     // S3-only, ignored
}

// DefaultBucket is where objects land when the configuration names no bucket.
const DefaultBucket = "gobuildcache"

// ConfigFromEnv reads the client's configuration from ConfigEnv. A returned
// config with an empty Bucket means "no remote": every consumer treats that as
// local-only rather than as a failure, since a build without the shared cache
// is slower and still correct.
//
// Every consumer reads the same variable and the same JSON, so the contract
// lives here rather than once per consumer.
func ConfigFromEnv() WebConfig {
	raw := os.Getenv(ConfigEnv)
	if raw == "" {
		return WebConfig{}
	}
	// Standard or URL-safe base64, padded or not, wrapped or not.
	normalized := strings.NewReplacer("-", "+", "_", "/", "\n", "", "\r", "", " ", "").Replace(raw)
	if m := len(normalized) % 4; m != 0 {
		normalized += strings.Repeat("=", 4-m)
	}
	data, err := base64.StdEncoding.DecodeString(normalized)
	if err != nil {
		logging.Debugf("%s: base64 decode: %v", ConfigEnv, err)
		return WebConfig{}
	}
	var cfg envConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		logging.Debugf("%s: json unmarshal: %v", ConfigEnv, err)
		return WebConfig{}
	}
	if cfg.Endpoint == "" {
		logging.Debugf("%s: missing endpoint field", ConfigEnv)
		return WebConfig{}
	}

	var deprecated []string
	username := cfg.Username
	if username == "" && cfg.KeyID != "" {
		username = cfg.KeyID
		deprecated = append(deprecated, "key_id (use username)")
	}
	password := cfg.Password
	if password == "" && cfg.AccessKey != "" {
		password = cfg.AccessKey
		deprecated = append(deprecated, "access_key (use password)")
	}
	if cfg.Region != "" {
		deprecated = append(deprecated, "region (ignored; the cache server is not S3)")
	}
	if len(deprecated) > 0 {
		logging.Warnf("%s: deprecated S3-style field(s): %s; these will be removed in a future release",
			ConfigEnv, strings.Join(deprecated, ", "))
	}
	if username == "" || password == "" {
		logging.Warnf("%s: missing username or password", ConfigEnv)
		return WebConfig{}
	}

	bucket := cfg.Bucket
	if bucket == "" {
		bucket = DefaultBucket
	}
	return WebConfig{
		Bucket:    bucket,
		Endpoint:  cfg.Endpoint,
		AccessKey: username,
		SecretKey: password,
	}
}
