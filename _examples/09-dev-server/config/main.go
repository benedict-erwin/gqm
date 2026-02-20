// Example: Dev Server (YAML Config Variant)
//
// Same full-featured development server as the programmatic variant,
// but pools, queues, cron entries, and auth are all defined in gqm.yaml.
// Code only registers handlers and seeds sample data.
//
// Usage:
//
//	# First, generate a real bcrypt hash for the admin password:
//	go run ./cmd/gqm hash-password
//	# Enter "admin", copy the hash, replace placeholder in gqm.yaml
//
//	go run ./_examples/09-dev-server/config
//	open http://localhost:8080/dashboard/
//
// Prerequisites: Redis on localhost:6379
package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"time"

	gqm "github.com/benedict-erwin/gqm"
)

func main() {
	// Load config from YAML (resolved relative to this source file).
	cfg, err := gqm.LoadConfigFile(configPath("gqm.yaml"))
	if err != nil {
		slog.Error("failed to load config", "error", err)
		fmt.Println()
		fmt.Println("Hint: You need to replace the placeholder password_hash in gqm.yaml")
		fmt.Println("      Run: go run ../../../cmd/gqm hash-password")
		fmt.Println("      Enter 'admin' as password, then paste the hash into gqm.yaml")
		os.Exit(1)
	}

	// Create server from config — override Redis addr from env if set.
	var serverOpts []gqm.ServerOption
	if addr := os.Getenv("GQM_TEST_REDIS_ADDR"); addr != "" {
		serverOpts = append(serverOpts, gqm.WithServerRedis(addr))
	}
	s, err := gqm.NewServerFromConfig(cfg, serverOpts...)
	if err != nil {
		slog.Error("failed to create server", "error", err)
		os.Exit(1)
	}

	// ── Register handlers (same as programmatic variant) ──────────

	s.Handle("email:send", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(300+rand.Intn(1500)) * time.Millisecond)
		if rand.Intn(10) == 0 {
			return fmt.Errorf("smtp connection timeout")
		}
		return nil
	})

	s.Handle("report:generate", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(1000+rand.Intn(3000)) * time.Millisecond)
		if rand.Intn(5) == 0 {
			return fmt.Errorf("report generation failed")
		}
		return nil
	})

	s.Handle("notification:push", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(100+rand.Intn(500)) * time.Millisecond)
		if rand.Intn(20) == 0 {
			return fmt.Errorf("push service unavailable")
		}
		return nil
	})

	s.Handle("image:resize", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(200+rand.Intn(300)) * time.Millisecond)
		return fmt.Errorf("unsupported image format: %v", job.Payload["format"])
	})

	s.Handle("data:export", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(500+rand.Intn(2000)) * time.Millisecond)
		if rand.Intn(2) == 0 {
			return fmt.Errorf("database connection reset")
		}
		return nil
	})

	s.Handle("invoice:generate", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(500+rand.Intn(1000)) * time.Millisecond)
		return nil
	})

	s.Handle("payment:process", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(300+rand.Intn(700)) * time.Millisecond)
		return nil
	})

	s.Handle("shipping:schedule", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(200+rand.Intn(500)) * time.Millisecond)
		return nil
	})

	s.Handle("analytics:aggregate", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(400+rand.Intn(600)) * time.Millisecond)
		return nil
	})

	// ── Seed data ─────────────────────────────────────────────────

	go func() {
		time.Sleep(2 * time.Second)

		addr := os.Getenv("GQM_TEST_REDIS_ADDR")
		if addr == "" {
			addr = cfg.Redis.Addr
		}
		if addr == "" {
			addr = "localhost:6379"
		}
		client, err := gqm.NewClient(gqm.WithRedisAddr(addr))
		if err != nil {
			slog.Error("failed to create client", "err", err)
			return
		}
		defer client.Close()

		ctx := context.Background()
		seedData(ctx, client)

		// Continuous enqueue.
		allTypes := []string{"email:send", "report:generate", "notification:push", "image:resize", "data:export"}
		for {
			time.Sleep(time.Duration(3+rand.Intn(5)) * time.Second)
			jt := allTypes[rand.Intn(len(allTypes))]
			maxRetry := 3
			if jt == "image:resize" {
				maxRetry = 1
			}
			client.Enqueue(ctx, jt, gqm.Payload{
				"time": time.Now().Format(time.RFC3339), "auto": true,
			}, gqm.Queue(jt), gqm.MaxRetry(maxRetry))
		}
	}()

	// ── Banner ────────────────────────────────────────────────────

	fmt.Println("=== GQM Dev Server (YAML Config) ===")
	fmt.Println("Config:    gqm.yaml")
	fmt.Println("Dashboard: http://localhost:8080/dashboard/")
	fmt.Println("Health:    http://localhost:8080/health")
	fmt.Println()

	if err := s.Start(context.Background()); err != nil {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}

func seedData(ctx context.Context, client *gqm.Client) {
	normalTypes := []string{"email:send", "report:generate", "notification:push"}
	for i := range 15 {
		jt := normalTypes[rand.Intn(len(normalTypes))]
		client.Enqueue(ctx, jt, gqm.Payload{
			"batch": "normal", "index": i,
		}, gqm.Queue(jt), gqm.MaxRetry(3))
	}

	for i := range 8 {
		client.Enqueue(ctx, "image:resize", gqm.Payload{
			"batch": "always-fail", "index": i, "format": "bmp",
		}, gqm.Queue("image:resize"), gqm.MaxRetry(1))
	}

	for i := range 5 {
		delay := time.Duration(30+rand.Intn(270)) * time.Second
		client.EnqueueIn(ctx, delay, "email:send", gqm.Payload{
			"batch": "scheduled", "index": i,
		}, gqm.Queue("email:send"), gqm.MaxRetry(3))
	}

	slog.Info("seed data complete", "normal", 15, "fail", 8, "scheduled", 5)
}

// configPath resolves a filename relative to this source file's directory,
// so `go run` works from any working directory.
func configPath(name string) string {
	_, src, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(src), name)
}
