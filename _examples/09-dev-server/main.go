// Example: Dev Server (Programmatic)
//
// Full-featured development server that demonstrates ALL GQM capabilities
// with dummy data. Simulates every job status (ready, processing, completed,
// failed, dead_letter, scheduled, deferred, canceled) and includes 4 DAG
// patterns, 4 cron entries, and continuous background job enqueue.
//
// This is the best starting point for exploring the dashboard and TUI.
//
// Features shown:
//   - All job statuses simulated
//   - 9 handlers with varying failure rates
//   - 4 DAG patterns (linear, diamond, fan-out, deep pipeline)
//   - 4 cron entries (enabled + disabled)
//   - Dashboard + auth + API key
//   - Continuous background enqueue
//
// For YAML config variant with multi-queue pools and varied dequeue
// strategies, see: config/ subdirectory.
//
// Usage:
//
//	go run ./_examples/09-dev-server
//	open http://localhost:8080/dashboard/
//
// Login: admin / admin
//
// Prerequisites: Redis on localhost:6379 (or set GQM_TEST_REDIS_ADDR)
package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"time"

	gqm "github.com/benedict-erwin/gqm"
	"golang.org/x/crypto/bcrypt"
)

func main() {
	hash, _ := bcrypt.GenerateFromPassword([]byte("admin"), bcrypt.DefaultCost)

	redisAddr := envOr("GQM_TEST_REDIS_ADDR", "localhost:6379")

	s, err := gqm.NewServer(
		gqm.WithServerRedis(redisAddr),
		gqm.WithAPI(true, ":8080"),
		gqm.WithDashboard(true),
		gqm.WithAuthEnabled(true),
		gqm.WithAuthUsers([]gqm.AuthUser{
			{Username: "admin", PasswordHash: string(hash), Role: "admin"},
		}),
		gqm.WithAPIKeys([]gqm.AuthAPIKey{
			{Name: "dev-key", Key: "gqm_ak_dev_server_example_key_for_testing_only", Role: "admin"},
		}),
		gqm.WithLogLevel("debug"),
	)
	if err != nil {
		slog.Error("failed to create server", "err", err)
		os.Exit(1)
	}

	// ── Handlers ──────────────────────────────────────────────────

	// email:send — moderate speed, ~10% failure
	s.Handle("email:send", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(300+rand.Intn(1500)) * time.Millisecond)
		if rand.Intn(10) == 0 {
			return fmt.Errorf("smtp connection timeout")
		}
		return nil
	}, gqm.Workers(3))

	// report:generate — slow, ~20% failure
	s.Handle("report:generate", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(1000+rand.Intn(3000)) * time.Millisecond)
		if rand.Intn(5) == 0 {
			return fmt.Errorf("report generation failed: out of memory")
		}
		return nil
	}, gqm.Workers(2))

	// notification:push — fast, rarely fails
	s.Handle("notification:push", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(100+rand.Intn(500)) * time.Millisecond)
		if rand.Intn(20) == 0 {
			return fmt.Errorf("push service unavailable")
		}
		return nil
	}, gqm.Workers(5))

	// image:resize — always fails (MaxRetry 1 → quickly goes to DLQ)
	s.Handle("image:resize", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(200+rand.Intn(300)) * time.Millisecond)
		return fmt.Errorf("unsupported image format: %v", job.Payload["format"])
	}, gqm.Workers(2))

	// data:export — 50% failure rate, good for retry testing
	s.Handle("data:export", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(500+rand.Intn(2000)) * time.Millisecond)
		if rand.Intn(2) == 0 {
			return fmt.Errorf("database connection reset during export")
		}
		return nil
	}, gqm.Workers(2))

	// DAG demo handlers
	s.Handle("invoice:generate", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(500+rand.Intn(1000)) * time.Millisecond)
		return nil
	}, gqm.Workers(1))

	s.Handle("payment:process", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(300+rand.Intn(700)) * time.Millisecond)
		return nil
	}, gqm.Workers(1))

	s.Handle("shipping:schedule", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(200+rand.Intn(500)) * time.Millisecond)
		return nil
	}, gqm.Workers(1))

	s.Handle("analytics:aggregate", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(time.Duration(400+rand.Intn(600)) * time.Millisecond)
		return nil
	}, gqm.Workers(1))

	// ── Cron entries ──────────────────────────────────────────────

	s.Schedule(gqm.CronEntry{
		ID: "daily-report", Name: "Daily Summary Report",
		CronExpr: "0 8 * * *", JobType: "report:generate", Queue: "report:generate",
		Payload: gqm.Payload{"type": "daily_summary"}, MaxRetry: 3, Enabled: true,
	})
	s.Schedule(gqm.CronEntry{
		ID: "hourly-cleanup", Name: "Hourly Cleanup",
		CronExpr: "0 * * * *", JobType: "data:export", Queue: "data:export",
		Payload: gqm.Payload{"type": "cleanup"}, MaxRetry: 1, Enabled: true,
	})
	s.Schedule(gqm.CronEntry{
		ID: "weekly-digest", Name: "Weekly Email Digest",
		CronExpr: "0 9 * * 1", JobType: "email:send", Queue: "email:send",
		Payload: gqm.Payload{"type": "weekly_digest"}, MaxRetry: 5, Enabled: true,
	})
	s.Schedule(gqm.CronEntry{
		ID: "disabled-task", Name: "Disabled Maintenance Task",
		CronExpr: "30 3 * * *", JobType: "data:export", Queue: "data:export",
		Payload: gqm.Payload{"type": "full_maintenance"}, MaxRetry: 1, Enabled: false,
	})

	// ── Seed data ─────────────────────────────────────────────────

	go func() {
		time.Sleep(2 * time.Second)

		client, err := gqm.NewClient(gqm.WithRedisAddr(redisAddr))
		if err != nil {
			slog.Error("failed to create client", "err", err)
			return
		}
		defer client.Close()

		ctx := context.Background()
		seedAllData(ctx, client, s)

		// Continuous: random jobs every 3-8 seconds.
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

	fmt.Println("=== GQM Dev Server ===")
	fmt.Println("Dashboard: http://localhost:8080/dashboard/")
	fmt.Println("Login:     admin / admin")
	fmt.Println("API Key:   gqm_ak_dev_server_example_key_for_testing_only")
	fmt.Println("Health:    http://localhost:8080/health")
	fmt.Println()
	fmt.Println("Handlers:")
	fmt.Println("  email:send            (3w, ~10% fail)")
	fmt.Println("  report:generate       (2w, ~20% fail)")
	fmt.Println("  notification:push     (5w, ~5% fail)")
	fmt.Println("  image:resize          (2w, always fail → DLQ)")
	fmt.Println("  data:export           (2w, ~50% fail)")
	fmt.Println("  invoice:generate      (1w, DAG)")
	fmt.Println("  payment:process       (1w, DAG)")
	fmt.Println("  shipping:schedule     (1w, DAG)")
	fmt.Println("  analytics:aggregate   (1w, DAG)")
	fmt.Println()
	fmt.Println("Simulated: ready, processing, completed, failed, dead_letter,")
	fmt.Println("           scheduled, deferred, canceled")
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
	// Normal jobs (→ ready → processing → completed)
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
	slog.Info("seeded: 8 always-fail jobs → DLQ")

	// High-failure jobs (50% fail, MaxRetry 3)
	for i := range 10 {
		client.Enqueue(ctx, "data:export", gqm.Payload{
			"batch": "high-fail", "index": i, "rows": rand.Intn(100000),
		}, gqm.Queue("data:export"), gqm.MaxRetry(3))
	}
	slog.Info("seeded: 10 high-failure jobs")

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

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
