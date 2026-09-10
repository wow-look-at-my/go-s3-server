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
	Short: "Minimal cache server",
	RunE:  run,
}

func init() {
	rootCmd.Flags().String("config", "", "path to JSON config file (required)")
	rootCmd.MarkFlagRequired("config")
	rootCmd.Flags().String("listen", "", "override listen address (e.g. :9000)")
	rootCmd.Flags().String("bucket", "", "override bucket name")
	rootCmd.Flags().String("data-dir", "", "override data directory")
	rootCmd.Flags().String("metrics-listen", "", "address for Prometheus metrics server (e.g. :9090)")
	rootCmd.Flags().String("dashboard-listen", "", "address for the operator dashboard (e.g. :9002); \"off\" disables it")
	rootCmd.Flags().String("log-mode", "", "access log shape: \"normal\" (one aggregated line per active second) or \"verbose\" (one line per request)")
}

func run(cmd *cobra.Command, args []string) error {
	// Every init() has run by now, including the GOMEMLIMIT guard go-toolchain
	// injects, so this reads the ceiling the GC is actually enforcing.
	resolveMemoryBudget()

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
	// An empty flag value cannot mean "turn the dashboard off": an unset flag
	// is empty too. "off" is the spelling that disables it from the command
	// line; the config file uses an explicit empty dashboard_listen.
	if v, _ := cmd.Flags().GetString("dashboard-listen"); v != "" {
		if v == "off" {
			v = ""
		}
		cfg.DashboardListen = &v
	}

	if v, _ := cmd.Flags().GetString("log-mode"); v != "" {
		switch v {
		case logModeNormal, logModeVerbose:
			cfg.LogMode = v
		default:
			return fmt.Errorf("--log-mode must be %q or %q, got %q", logModeNormal, logModeVerbose, v)
		}
	}

	storage, err := NewStorage(cfg.DataDir, cfg.WriteOnce)
	if err != nil {
		return fmt.Errorf("init storage: %w", err)
	}
	defer storage.Close()

	srv := NewServer(cfg, storage)

	// Normal mode's access log IS the aggregator: one line per second in which
	// the cache moved anything. Verbose mode leaves it nil and every request
	// prints itself instead.
	if srv.logAgg != nil {
		go srv.logAgg.Run()
		defer srv.logAgg.Stop()
		log.Printf("access log: normal mode, one aggregated line per active second. Use log_mode=%q (or --log-mode %s) for one line per request.", logModeVerbose, logModeVerbose)
	} else {
		log.Printf("access log: verbose mode, one line per request")
	}

	if cfg.Eviction.Enabled() {
		configureLastUseTracking(storage, cfg.DataDir)
		maxAge := cfg.Eviction.AgeLimit()
		maxBytes := cfg.Eviction.SizeLimit()
		go storage.RunEvictionLoop(maxAge, maxBytes, cfg.Eviction.Interval.Std())
		log.Printf("cache eviction: enabled max_bytes=%d (%d MiB) max_age=%s interval=%s; over budget, the least recently used entries are evicted first",
			maxBytes, maxBytes>>20, maxAge, cfg.Eviction.Interval.Std())
	} else {
		log.Printf("WARNING: cache eviction is DISABLED (eviction.max_bytes=0 and eviction.max_age=0); the cache will grow without bound until the disk fills. Set eviction.max_bytes (or the %s env var) to enable automatic pruning.", maxBytesEnvVar)
	}

	if cfg.MetricsListen != "" {
		go startMetricsServer(cfg.MetricsListen)
		log.Printf("metrics server listening on %s", cfg.MetricsListen)
	}

	if addr := cfg.DashboardListenAddr(); addr != "" {
		go startDashboardServer(addr, newDashboard(srv, cfg, time.Now()))
		log.Printf("dashboard listening on %s; it answers without credentials, so publish that port through an access proxy (e.g. Cloudflare Zero Trust) rather than directly", addr)
	} else {
		log.Printf("dashboard disabled (dashboard_listen is empty)")
	}

	log.Printf("listening on %s bucket=%s data_dir=%s write_once.action=%s write_once.notification=%s",
		cfg.Listen, cfg.Bucket, cfg.DataDir, cfg.WriteOnce.Action, cfg.WriteOnce.Notification)

	if cfg.DisableAuth {
		log.Printf("WARNING: authentication is DISABLED (disable_auth=true). All requests will be accepted without credentials. Only use this behind a trusted reverse proxy.")
	}

	if cfg.IndexBlobInterval != nil {
		storage.Index.SetBlobInterval(time.Duration(*cfg.IndexBlobInterval))
	}
	log.Printf("limits: max_concurrent_requests=%d max_object_bytes=%d index_blob_interval=%v", cfg.MaxConcurrentRequests, cfg.MaxObjectBytes, storage.Index.BlobInterval())

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

// configureLastUseTracking decides where eviction's last-use times come from.
// The filesystem's own access times are preferred: the kernel maintains them
// for free on every body read, and they survive restarts. Only when the
// data_dir turns out not to record them does the server keep its own in-memory
// map -- accurate while it runs, empty again after every restart, and one entry
// per key read, which is the memory this avoids paying at a million keys.
func configureLastUseTracking(storage *Storage, dataDir string) {
	recorded, err := atimeIsRecorded(dataDir)
	switch {
	case err != nil:
		log.Printf("eviction: could not test whether %s records file access times (%v); tracking reads in memory instead", dataDir, err)
	case recorded:
		log.Printf("eviction: last use comes from the filesystem's access times, so reads counted against eviction survive restarts")
		return
	default:
		log.Printf("WARNING: %s does not record file access times (mounted noatime, or a platform without them), so reads are tracked in memory and forgotten on restart: an entry written long ago but read constantly can be evicted by the first sweep after a restart. Mount the data_dir with relatime (the Linux default) to avoid that.", dataDir)
	}
	storage.EnableAccessTracking()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
