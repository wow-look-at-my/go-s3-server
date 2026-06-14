package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
)

// HTTP server timeouts. ReadHeaderTimeout is the important slowloris guard
// (request lines/headers must arrive promptly); Read/Write are generous
// backstops so a stuck connection cannot pin a concurrency slot forever, while
// still allowing CI-sized object uploads and batch streams to complete. Idle
// reaps unused keep-alive connections from many CI runners.
const (
	httpReadHeaderTimeout = 15 * time.Second
	httpReadTimeout       = 5 * time.Minute
	httpWriteTimeout      = 5 * time.Minute
	httpIdleTimeout       = 120 * time.Second
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

	if cfg.Eviction.Enabled() {
		storage.EnableAccessTracking()
		maxAge := cfg.Eviction.AgeLimit()
		go storage.RunEvictionLoop(maxAge, cfg.Eviction.MaxBytes, cfg.Eviction.Interval.Std())
		log.Printf("cache eviction: enabled max_age=%s max_bytes=%d interval=%s",
			maxAge, cfg.Eviction.MaxBytes, cfg.Eviction.Interval.Std())
	} else {
		log.Printf("WARNING: cache eviction is DISABLED (eviction.max_age=0 and eviction.max_bytes=0); the cache will grow without bound until the disk fills. Set eviction.max_age (e.g. \"720h\") and/or eviction.max_bytes to enable automatic pruning.")
	}

	if cfg.MetricsListen != "" {
		go startMetricsServer(cfg.MetricsListen)
		log.Printf("metrics server listening on %s", cfg.MetricsListen)
	}

	log.Printf("listening on %s bucket=%s data_dir=%s write_once.action=%s write_once.notification=%s",
		cfg.Listen, cfg.Bucket, cfg.DataDir, cfg.WriteOnce.Action, cfg.WriteOnce.Notification)

	if cfg.DisableAuth {
		log.Printf("WARNING: authentication is DISABLED (disable_auth=true). All requests will be accepted without credentials. Only use this behind a trusted reverse proxy.")
	}

	log.Printf("limits: max_concurrent_requests=%d max_object_bytes=%d", cfg.MaxConcurrentRequests, cfg.MaxObjectBytes)

	httpSrv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
	}
	return httpSrv.ListenAndServe()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
