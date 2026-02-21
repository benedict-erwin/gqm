// Example: Dev Server (YAML Config Variant)
//
// Same full-featured development server as the programmatic variant,
// but pools, queues, cron entries, and auth are all defined in gqm.yaml.
// Code only registers handlers and seeds sample data.
//
// Demonstrates:
//   - Multi-queue pool (billing-pool: payment + invoice)
//   - Varied dequeue strategies (strict, round_robin, weighted)
//   - All job statuses simulated
//   - 4 DAG patterns (linear, diamond, fan-out, deep pipeline)
//   - Scheduled and canceled jobs
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

	// email:send — moderate speed, ~10% failure
	s.Handle("email:send", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(300+rand.Intn(1500)) * time.Millisecond)
		if rand.Intn(10) == 0 {
			return fmt.Errorf("smtp connection timeout")
		}
		return nil
	})

	// report:generate — slow, ~20% failure
	s.Handle("report:generate", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(1000+rand.Intn(3000)) * time.Millisecond)
		if rand.Intn(5) == 0 {
			return fmt.Errorf("report generation failed")
		}
		return nil
	})

	// notification:push — fast, rarely fails
	s.Handle("notification:push", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(100+rand.Intn(500)) * time.Millisecond)
		if rand.Intn(20) == 0 {
			return fmt.Errorf("push service unavailable")
		}
		return nil
	})

	// image:resize — always fails (MaxRetry 1 → quickly goes to DLQ)
	s.Handle("image:resize", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(200+rand.Intn(300)) * time.Millisecond)
		return fmt.Errorf("unsupported image format: %v", job.Payload["format"])
	})

	// data:export — 50% failure rate, good for retry testing
	s.Handle("data:export", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(500+rand.Intn(2000)) * time.Millisecond)
		if rand.Intn(2) == 0 {
			return fmt.Errorf("database connection reset")
		}
		return nil
	})

	// invoice:generate — billing domain, reliable
	s.Handle("invoice:generate", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(500+rand.Intn(1000)) * time.Millisecond)
		return nil
	})

	// payment:process — billing domain, reliable
	s.Handle("payment:process", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(300+rand.Intn(700)) * time.Millisecond)
		return nil
	})

	// shipping:schedule — logistics
	s.Handle("shipping:schedule", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(200+rand.Intn(500)) * time.Millisecond)
		return nil
	})

	// analytics:aggregate — background processing
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
		seedAllData(ctx, client, s)

		// Continuous: random jobs every 3-8 seconds.
		allTypes := []string{
			"email:send", "report:generate", "notification:push",
			"image:resize", "data:export", "payment:process", "invoice:generate",
		}
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
	fmt.Println("Pools:")
	fmt.Println("  comms-pool      (5w, strict)   — email:send + notification:push")
	fmt.Println("  report-pool     (2w, rr)       — report:generate")
	fmt.Println("  image-pool      (2w)           — image:resize (always fail)")
	fmt.Println("  export-pool     (2w, weighted)  — data:export (50% fail)")
	fmt.Println("  billing-pool    (3w, weighted)  — payment:process + invoice:generate")
	fmt.Println("  logistics-pool  (1w)           — shipping:schedule")
	fmt.Println("  analytics-pool  (1w)           — analytics:aggregate")
	fmt.Println()
	fmt.Println("DAG patterns: linear (ETL), diamond (order), fan-out, deep pipeline")
	fmt.Println("Continuous: 1 random job every 3-8s")
	fmt.Println()

	if err := s.Start(context.Background()); err != nil {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}

func seedAllData(ctx context.Context, client *gqm.Client, s *gqm.Server) {
	// Normal jobs across all types (→ ready → processing → completed)
	normalTypes := []string{"email:send", "report:generate", "notification:push"}
	for i := range 15 {
		jt := normalTypes[rand.Intn(len(normalTypes))]
		client.Enqueue(ctx, jt, gqm.Payload{
			"batch": "normal", "index": i, "user_id": 1000 + i,
		}, gqm.Queue(jt), gqm.MaxRetry(3))
	}
	slog.Info("seeded: 15 normal jobs")

	// Always-fail jobs (→ DLQ)
	formats := []string{"bmp", "tiff", "webp", "svg"}
	for i := range 8 {
		client.Enqueue(ctx, "image:resize", gqm.Payload{
			"batch": "always-fail", "index": i, "format": formats[rand.Intn(len(formats))],
		}, gqm.Queue("image:resize"), gqm.MaxRetry(1))
	}
	slog.Info("seeded: 8 always-fail jobs -> DLQ")

	// High-failure jobs (50% fail, MaxRetry 5)
	for i := range 10 {
		client.Enqueue(ctx, "data:export", gqm.Payload{
			"batch": "high-fail", "index": i, "rows": rand.Intn(100000),
		}, gqm.Queue("data:export"), gqm.MaxRetry(5))
	}
	slog.Info("seeded: 10 high-failure jobs")

	// Billing jobs — demonstrates multi-queue pool
	for i := range 6 {
		client.Enqueue(ctx, "payment:process", gqm.Payload{
			"batch": "billing", "index": i, "amount": 100 + rand.Intn(9900),
			"currency": "USD", "customer_id": 2000 + i,
		}, gqm.Queue("payment:process"), gqm.MaxRetry(3))
	}
	for i := range 4 {
		client.Enqueue(ctx, "invoice:generate", gqm.Payload{
			"batch": "billing", "index": i, "invoice_no": fmt.Sprintf("INV-2026-%04d", i+1),
			"customer_id": 2000 + i,
		}, gqm.Queue("invoice:generate"), gqm.MaxRetry(3))
	}
	slog.Info("seeded: 10 billing jobs (6 payment + 4 invoice)")

	// Shipping + analytics jobs
	for i := range 3 {
		client.Enqueue(ctx, "shipping:schedule", gqm.Payload{
			"batch": "logistics", "index": i, "tracking": fmt.Sprintf("TRK%06d", rand.Intn(999999)),
		}, gqm.Queue("shipping:schedule"))
	}
	for i := range 3 {
		client.Enqueue(ctx, "analytics:aggregate", gqm.Payload{
			"batch": "analytics", "index": i, "period": "daily",
		}, gqm.Queue("analytics:aggregate"), gqm.MaxRetry(2))
	}
	slog.Info("seeded: 6 logistics + analytics jobs")

	// Scheduled jobs (future execution)
	for i := range 5 {
		delay := time.Duration(30+rand.Intn(270)) * time.Second
		client.EnqueueIn(ctx, delay, "email:send", gqm.Payload{
			"batch": "scheduled", "index": i, "to": fmt.Sprintf("scheduled-%d@example.com", i),
		}, gqm.Queue("email:send"), gqm.MaxRetry(3))
	}
	slog.Info("seeded: 5 scheduled jobs (30s-5min delay)")

	// Jobs to cancel
	var cancelIDs []string
	for i := range 3 {
		job, err := client.EnqueueIn(ctx, 10*time.Minute, "notification:push", gqm.Payload{
			"batch": "to-cancel", "index": i,
		}, gqm.Queue("notification:push"), gqm.MaxRetry(1))
		if err == nil {
			cancelIDs = append(cancelIDs, job.ID)
		}
	}
	time.Sleep(1 * time.Second)
	for _, id := range cancelIDs {
		s.CancelJob(ctx, id)
	}
	slog.Info("seeded: 3 canceled jobs")

	// DAG patterns
	seedDAGPatterns(ctx, client)

	slog.Info("all seed data complete")
}

func seedDAGPatterns(ctx context.Context, client *gqm.Client) {
	// DAG 1: Linear chain (ETL) — extract → transform → load
	extract, _ := client.Enqueue(ctx, "data:export", gqm.Payload{
		"dag": "etl", "step": "extract",
	}, gqm.Queue("data:export"), gqm.MaxRetry(2))

	transform, _ := client.Enqueue(ctx, "report:generate", gqm.Payload{
		"dag": "etl", "step": "transform",
	}, gqm.Queue("report:generate"), gqm.MaxRetry(2), gqm.DependsOn(extract.ID))

	client.Enqueue(ctx, "data:export", gqm.Payload{
		"dag": "etl", "step": "load",
	}, gqm.Queue("data:export"), gqm.MaxRetry(2), gqm.DependsOn(transform.ID))
	slog.Info("seeded DAG: linear chain (ETL)")

	// DAG 2: Diamond — validate → [payment + invoice] → notify
	validate, _ := client.Enqueue(ctx, "data:export", gqm.Payload{
		"dag": "order", "step": "validate", "order_id": 12345,
	}, gqm.Queue("data:export"), gqm.MaxRetry(2))

	payment, _ := client.Enqueue(ctx, "payment:process", gqm.Payload{
		"dag": "order", "step": "payment",
	}, gqm.Queue("payment:process"), gqm.MaxRetry(3), gqm.DependsOn(validate.ID))

	invoice, _ := client.Enqueue(ctx, "invoice:generate", gqm.Payload{
		"dag": "order", "step": "invoice",
	}, gqm.Queue("invoice:generate"), gqm.MaxRetry(2), gqm.DependsOn(validate.ID))

	client.Enqueue(ctx, "notification:push", gqm.Payload{
		"dag": "order", "step": "notify",
	}, gqm.Queue("notification:push"), gqm.MaxRetry(3), gqm.DependsOn(payment.ID, invoice.ID))
	slog.Info("seeded DAG: diamond (order)")

	// DAG 3: Fan-out — ingest → [analytics, report, notify, archive]
	ingest, _ := client.Enqueue(ctx, "data:export", gqm.Payload{
		"dag": "ingest", "step": "ingest",
	}, gqm.Queue("data:export"), gqm.MaxRetry(2))

	fanOuts := []struct{ jt, step, q string }{
		{"analytics:aggregate", "analytics", "analytics:aggregate"},
		{"report:generate", "report", "report:generate"},
		{"notification:push", "notify", "notification:push"},
		{"data:export", "archive", "data:export"},
	}
	for _, f := range fanOuts {
		client.Enqueue(ctx, f.jt, gqm.Payload{
			"dag": "ingest", "step": f.step,
		}, gqm.Queue(f.q), gqm.MaxRetry(2), gqm.DependsOn(ingest.ID))
	}
	slog.Info("seeded DAG: fan-out (ingest)")

	// DAG 4: Deep pipeline with AllowFailure — step-0 → ... → step-4
	var prevID string
	for i := range 5 {
		opts := []gqm.EnqueueOption{gqm.Queue("data:export"), gqm.MaxRetry(1)}
		if i > 0 {
			opts = append(opts, gqm.DependsOn(prevID))
		}
		if i == 2 {
			opts = append(opts, gqm.AllowFailure(true))
		}
		j, _ := client.Enqueue(ctx, "data:export", gqm.Payload{
			"dag": "deep", "step": fmt.Sprintf("step-%d", i),
		}, opts...)
		prevID = j.ID
	}
	slog.Info("seeded DAG: deep pipeline (5 steps, step-2 allow_failure)")
}

// configPath resolves a filename relative to this source file's directory,
// so `go run` works from any working directory.
func configPath(name string) string {
	_, src, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(src), name)
}
