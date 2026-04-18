package main

import (
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "go-s3-server",
	Short: "Minimal S3-compatible server",
	RunE:  run,
}

func init() {
	rootCmd.Flags().String("config", "", "path to JSON config file (required)")
	rootCmd.MarkFlagRequired("config")
	rootCmd.Flags().String("listen", "", "override listen address (e.g. :9000)")
	rootCmd.Flags().String("bucket", "", "override bucket name")
	rootCmd.Flags().String("data-dir", "", "override data directory")
	rootCmd.Flags().String("metrics-listen", "", "address for Prometheus metrics server (e.g. :9090)")
}

func run(cmd *cobra.Command, args []string) error {
	configPath, _ := cmd.Flags().GetString("config")
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	if v, _ := cmd.Flags().GetString("listen"); v != "" {
		cfg.Listen = v
	}
	if v, _ := cmd.Flags().GetString("bucket"); v != "" {
		cfg.Bucket = v
	}
	if v, _ := cmd.Flags().GetString("data-dir"); v != "" {
		cfg.DataDir = v
	}
	if v, _ := cmd.Flags().GetString("metrics-listen"); v != "" {
		cfg.MetricsListen = v
	}

	storage, err := NewStorage(cfg.DataDir, cfg.WriteOnce)
	if err != nil {
		return fmt.Errorf("init storage: %w", err)
	}
	defer storage.Close()

	srv := NewServer(cfg, storage)

	if cfg.MetricsListen != "" {
		go startMetricsServer(cfg.MetricsListen)
		log.Printf("metrics server listening on %s", cfg.MetricsListen)
	}

	log.Printf("listening on %s bucket=%s data_dir=%s write_once.action=%s write_once.notification=%s",
		cfg.Listen, cfg.Bucket, cfg.DataDir, cfg.WriteOnce.Action, cfg.WriteOnce.Notification)

	if cfg.DisableAuth {
		log.Printf("WARNING: authentication is DISABLED (disable_auth=true). All requests will be accepted without credentials. Only use this behind a trusted reverse proxy.")
	}

	return http.ListenAndServe(cfg.Listen, srv)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
