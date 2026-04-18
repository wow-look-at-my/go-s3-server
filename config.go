package main

import (
	"encoding/json"
	"fmt"
	"os"
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

type Config struct {
	Listen        string          `json:"listen"`
	MetricsListen string          `json:"metrics_listen"`
	Bucket        string          `json:"bucket"`
	DataDir       string          `json:"data_dir"`
	WriteOnce     WriteOnceConfig `json:"write_once"`
	DisableAuth   bool            `json:"disable_auth"`
	Credentials   []Credential    `json:"credentials"`
}

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
