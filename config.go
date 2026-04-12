package main

import (
	"encoding/json"
	"fmt"
	"os"
)

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
	return &cfg, nil
}
