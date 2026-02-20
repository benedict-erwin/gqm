// Example: Scheduled Reports
//
// Demonstrates cron scheduling and delayed job execution.
// Sets up recurring cron entries and one-off delayed jobs.
//
// Features shown:
//   - Schedule() with CronEntry (recurring jobs)
//   - EnqueueAt() for specific time execution
//   - EnqueueIn() for relative delay execution
//   - 6-field cron expressions (second minute hour dom month dow)
//   - Overlap policies (skip, allow)
//   - Timezone support
//
// Usage:
//
//	go run ./_examples/03-scheduled-reports
//
// Prerequisites: Redis on localhost:6379 (or set GQM_TEST_REDIS_ADDR)
package main

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"os/signal"
	"time"

	gqm "github.com/benedict-erwin/gqm"
)

func main() {
	redisAddr := envOr("GQM_TEST_REDIS_ADDR", "localhost:6379")

	client, err := gqm.NewClient(gqm.WithRedisAddr(redisAddr))
	if err != nil {
		slog.Error("failed to create client", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	server, err := gqm.NewServer(
		gqm.WithServerRedis(redisAddr),
		gqm.WithLogLevel("info"),
		gqm.WithSchedulerEnabled(true), // enabled by default, shown for clarity
	)
	if err != nil {
		slog.Error("failed to create server", "error", err)
		os.Exit(1)
	}

	// Register report handlers.
	server.Handle("report:daily", handleDailyReport, gqm.Workers(1))
	server.Handle("report:hourly", handleHourlyStats, gqm.Workers(2))
	server.Handle("report:weekly", handleWeeklyDigest, gqm.Workers(1))
	server.Handle("report:ondemand", handleOnDemandReport, gqm.Workers(2))

	// ── Cron entries (recurring schedules) ─────────────────────────

	// Daily summary at 8:00 AM (skip if previous run still going)
	server.Schedule(gqm.CronEntry{
		ID:            "daily-summary",
		Name:          "Daily Summary Report",
		CronExpr:      "0 8 * * *", // 5-field: minute hour dom month dow
		JobType:       "report:daily",
		Queue:         "report:daily",
		Payload:       gqm.Payload{"type": "daily_summary", "format": "pdf"},
		MaxRetry:      3,
		OverlapPolicy: gqm.OverlapSkip, // skip if previous run not done
		Enabled:       true,
	})

	// Hourly stats every hour (allow overlap — fast job)
	server.Schedule(gqm.CronEntry{
		ID:            "hourly-stats",
		Name:          "Hourly Stats Collection",
		CronExpr:      "0 * * * *",
		JobType:       "report:hourly",
		Queue:         "report:hourly",
		Payload:       gqm.Payload{"type": "stats", "granularity": "1h"},
		MaxRetry:      1,
		OverlapPolicy: gqm.OverlapAllow,
		Enabled:       true,
	})

	// Weekly digest every Monday at 9 AM (Asia/Jakarta timezone)
	server.Schedule(gqm.CronEntry{
		ID:       "weekly-digest",
		Name:     "Weekly Email Digest",
		CronExpr: "0 9 * * 1",
		Timezone: "Asia/Jakarta",
		JobType:  "report:weekly",
		Queue:    "report:weekly",
		Payload:  gqm.Payload{"type": "weekly_digest", "recipients": "all"},
		MaxRetry: 5,
		Enabled:  true,
	})

	// ── One-off delayed jobs ──────────────────────────────────────

	ctx := context.Background()

	// Schedule a report 30 seconds from now
	job1, err := client.EnqueueIn(ctx, 30*time.Second,
		"report:ondemand", gqm.Payload{
			"type":      "custom",
			"requester": "admin",
			"period":    "last-7-days",
		},
		gqm.Queue("report:ondemand"),
		gqm.MaxRetry(2),
	)
	if err != nil {
		slog.Error("enqueue delayed job failed", "error", err)
	} else {
		slog.Info("scheduled delayed report", "job_id", job1.ID, "runs_in", "30s")
	}

	// Schedule a report at a specific time (2 minutes from now)
	runAt := time.Now().Add(2 * time.Minute)
	job2, err := client.EnqueueAt(ctx, runAt,
		"report:ondemand", gqm.Payload{
			"type":      "quarterly",
			"requester": "cfo",
			"quarter":   "Q1-2026",
		},
		gqm.Queue("report:ondemand"),
		gqm.MaxRetry(3),
	)
	if err != nil {
		slog.Error("enqueue scheduled job failed", "error", err)
	} else {
		slog.Info("scheduled report at specific time",
			"job_id", job2.ID,
			"run_at", runAt.Format(time.RFC3339),
		)
	}

	// ── Start server ──────────────────────────────────────────────

	fmt.Println("\n=== Scheduled Reports Example ===")
	fmt.Println("Cron entries:")
	fmt.Println("  daily-summary  — 0 8 * * *    (skip overlap)")
	fmt.Println("  hourly-stats   — 0 * * * *    (allow overlap)")
	fmt.Println("  weekly-digest  — 0 9 * * 1    (Asia/Jakarta)")
	fmt.Println()
	fmt.Println("Delayed jobs:")
	fmt.Println("  custom report  — runs in 30s")
	fmt.Println("  quarterly      — runs in 2min")
	fmt.Println()
	fmt.Println("Press Ctrl+C to stop.\n")

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	if err := server.Start(ctx); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}

func handleDailyReport(ctx context.Context, job *gqm.Job) error {
	reportType := job.Payload["type"]
	time.Sleep(time.Duration(2000+rand.Intn(3000)) * time.Millisecond)
	slog.Info("daily report generated", "type", reportType, "job_id", job.ID)
	return nil
}

func handleHourlyStats(ctx context.Context, job *gqm.Job) error {
	time.Sleep(time.Duration(500+rand.Intn(1000)) * time.Millisecond)
	slog.Info("hourly stats collected", "job_id", job.ID)
	return nil
}

func handleWeeklyDigest(ctx context.Context, job *gqm.Job) error {
	time.Sleep(time.Duration(3000+rand.Intn(5000)) * time.Millisecond)
	slog.Info("weekly digest sent", "job_id", job.ID)
	return nil
}

func handleOnDemandReport(ctx context.Context, job *gqm.Job) error {
	reportType := job.Payload["type"]
	requester := job.Payload["requester"]
	time.Sleep(time.Duration(1000+rand.Intn(2000)) * time.Millisecond)
	slog.Info("on-demand report generated",
		"type", reportType, "requester", requester, "job_id", job.ID)
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
