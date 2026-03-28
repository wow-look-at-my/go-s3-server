package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type Credential struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
}

type WriteOnceConfig struct {
	Action       string `json:"action"`       // "allow" or "deny"
	Notification string `json:"notification"` // "never", "always", "content_differs"
}

type Config struct {
	Listen      string          `json:"listen"`
	Bucket      string          `json:"bucket"`
	Region      string          `json:"region"`
	DataDir     string          `json:"data_dir"`
	WriteOnce   WriteOnceConfig `json:"write_once"`
	Credentials []Credential    `json:"credentials"`
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
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
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
	if len(cfg.Credentials) == 0 {
		return nil, fmt.Errorf("config: at least one credential is required")
	}
	for i, c := range cfg.Credentials {
		if c.AccessKey == "" || c.SecretKey == "" {
			return nil, fmt.Errorf("config: credential %d: access_key and secret_key are required", i)
		}
	}
	return &cfg, nil
}

func (cfg *Config) FindCredential(accessKey string) *Credential {
	for i := range cfg.Credentials {
		if cfg.Credentials[i].AccessKey == accessKey {
			return &cfg.Credentials[i]
		}
	}
	return nil
}
