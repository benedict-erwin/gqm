// Example: Config-driven Setup
//
// Demonstrates YAML-based server configuration using LoadConfigFile
// and NewServerFromConfig. Zero pool/queue setup in code — everything
// is defined in gqm.yaml.
//
// Features shown:
//   - LoadConfigFile() for YAML config loading
//   - NewServerFromConfig() for config-driven server creation
//   - Code options override config values (WithLogLevel shown)
//   - Handler registration (still done in code)
//
// Usage:
//
//	go run ./_examples/06-config-driven
//
// Prerequisites: Redis on localhost:6379 (or set redis.addr in gqm.yaml)
package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"time"

	gqm "github.com/benedict-erwin/gqm"
)

func main() {
	// Load config from YAML file (resolved relative to this source file).
	cfg, err := gqm.LoadConfigFile(configPath("gqm.yaml"))
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	// Create server from config. Code options override YAML values.
	server, err := gqm.NewServerFromConfig(cfg,
		gqm.WithLogLevel("info"), // override log_level from config
	)
	if err != nil {
		slog.Error("failed to create server", "error", err)
		os.Exit(1)
	}

	// Register handlers — job types must match config pool definitions.
	server.Handle("email:send", handleEmail)
	server.Handle("email:bulk", handleBulkEmail)
	server.Handle("report:generate", handleReport)
	server.Handle("cleanup:temp", handleCleanup)

	// Enqueue some sample jobs.
	addr := cfg.Redis.Addr
	if addr == "" {
		addr = "localhost:6379"
	}
	client, err := gqm.NewClient(gqm.WithRedisAddr(addr))
	if err != nil {
		slog.Error("failed to create client", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	ctx := context.Background()
	for i := range 5 {
		client.Enqueue(ctx, "email:send", gqm.Payload{
			"to":      fmt.Sprintf("user%d@example.com", i),
			"subject": "Config-driven example",
		}, gqm.Queue("email:high"))

		client.Enqueue(ctx, "email:bulk", gqm.Payload{
			"campaign": fmt.Sprintf("campaign-%d", i),
			"count":    1000 + i*500,
		}, gqm.Queue("email:low"))
	}

	for i := range 3 {
		client.Enqueue(ctx, "report:generate", gqm.Payload{
			"type":   "analytics",
			"period": fmt.Sprintf("2026-Q%d", i+1),
		}, gqm.Queue("report:generate"))
	}
	slog.Info("sample jobs enqueued")

	// ── Start server ──────────────────────────────────────────────

	fmt.Println("\n=== Config-driven Setup ===")
	fmt.Println("Config loaded from: gqm.yaml")
	fmt.Println("Pools and queues defined in YAML — zero setup in code.")
	fmt.Println("Press Ctrl+C to stop.\n")

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	if err := server.Start(ctx); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}

func handleEmail(ctx context.Context, job *gqm.Job) error {
	to := job.Payload["to"]
	time.Sleep(time.Duration(200+rand.Intn(500)) * time.Millisecond)
	slog.Info("email sent", "to", to, "job_id", job.ID)
	return nil
}

func handleBulkEmail(ctx context.Context, job *gqm.Job) error {
	campaign := job.Payload["campaign"]
	time.Sleep(time.Duration(1000+rand.Intn(3000)) * time.Millisecond)
	slog.Info("bulk email sent", "campaign", campaign, "job_id", job.ID)
	return nil
}

func handleReport(ctx context.Context, job *gqm.Job) error {
	period := job.Payload["period"]
	time.Sleep(time.Duration(2000+rand.Intn(3000)) * time.Millisecond)
	slog.Info("report generated", "period", period, "job_id", job.ID)
	return nil
}

func handleCleanup(ctx context.Context, job *gqm.Job) error {
	time.Sleep(time.Duration(500+rand.Intn(1000)) * time.Millisecond)
	slog.Info("temp files cleaned", "job_id", job.ID)
	return nil
}

// configPath resolves a filename relative to this source file's directory,
// so `go run` works from any working directory.
func configPath(name string) string {
	_, src, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(src), name)
}
