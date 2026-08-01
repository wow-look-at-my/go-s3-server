package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
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

	// shutdownTimeout bounds how long graceful shutdown waits for in-flight
	// requests to finish after SIGINT/SIGTERM. Kept under a typical orchestrator
	// stop grace period (docker-updater issues ContainerStop with a 300s timeout)
	// so the process drains and exits cleanly before a SIGKILL would arrive.
	shutdownTimeout = 280 * time.Second
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

	// Memory: the in-memory caches are already sized from this budget; starting
	// the controller adds the feedback half, shrinking them when memory gets
	// tight and letting them grow back when it does not. It never touches
	// request handling -- a cache that stops answering is not a cache.
	if memoryBudget > 0 {
		log.Printf("memory: budget %d MiB (from %s); in-memory caches sized against it and shrunk above %d%% in use",
			memoryBudget>>20, memoryBudgetSource, int(memShrinkFraction*100))
		stopController := make(chan struct{})
		defer close(stopController)
		go srv.mem.Run(stopController)
	} else {
		log.Printf("memory: no process limit discovered (no GOMEMLIMIT, no cgroup limit); in-memory caches use fixed default budgets. Set GOMEMLIMIT or a container memory limit to have them sized and adjusted automatically.")
	}

	// Bodies are already compressed when they arrive and this server never
	// compresses anything, so a compressing dataset underneath is a second
	// pass for no gain -- said once, here, where the other costly-config
	// warnings are.
	logCompressionAdvisory(cfg.DataDir, log.Printf)

	httpSrv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
	}

	// Serve in a goroutine so the main goroutine can wait for a termination
	// signal and drain in-flight requests before exiting. Without this, the
	// SIGTERM that `docker stop` sends during a rolling update kills the process
	// immediately and cuts off in-flight GET/PUT streams; draining lets the
	// orchestrator's stop grace period be spent finishing those requests.
	serveErr := make(chan error, 1)
	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serveErr <- err
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-serveErr:
		return fmt.Errorf("http server: %w", err)
	case sig := <-sigCh:
		log.Printf("received signal %v, draining in-flight requests (up to %s)", sig, shutdownTimeout)
		srv.BeginShutdown()
		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := httpSrv.Shutdown(ctx); err != nil {
			return fmt.Errorf("graceful shutdown: %w", err)
		}
		log.Printf("drain complete, exiting")
		return nil
	}
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
