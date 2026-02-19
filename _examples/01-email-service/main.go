// Example: Email Service
//
// Demonstrates basic GQM usage: enqueue jobs, process with retry and backoff,
// handle dead-letter queue (DLQ) for permanently failed deliveries.
//
// Features shown:
//   - NewClient / NewServer (zero-config)
//   - Handle with Workers(N)
//   - MaxRetry, RetryIntervals
//   - Automatic DLQ after retries exhausted
//   - Graceful shutdown
//
// Usage:
//
//	go run ./_examples/01-email-service
//
// Prerequisites: Redis on localhost:6379 (or set GQM_REDIS_ADDR)
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
	redisAddr := envOr("GQM_REDIS_ADDR", "localhost:6379")

	// ── Producer ──────────────────────────────────────────────────

	client, err := gqm.NewClient(gqm.WithRedisAddr(redisAddr))
	if err != nil {
		slog.Error("failed to create client", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	// ── Consumer ──────────────────────────────────────────────────

	server, err := gqm.NewServer(
		gqm.WithServerRedis(redisAddr),
		gqm.WithLogLevel("info"),
	)
	if err != nil {
		slog.Error("failed to create server", "error", err)
		os.Exit(1)
	}

	// Register handler: 3 concurrent workers, max 5 retries.
	// RetryIntervals defines progressive backoff: 1s, 5s, 15s, 30s, 60s.
	server.Handle("email:send", handleEmailSend, gqm.Workers(3))

	// ── Enqueue sample jobs ───────────────────────────────────────

	ctx := context.Background()
	recipients := []string{
		"alice@example.com",
		"bob@example.com",
		"carol@example.com",
		"dave@example.com",
		"eve@example.com",
	}

	for _, to := range recipients {
		job, err := client.Enqueue(ctx, "email:send", gqm.Payload{
			"to":      to,
			"subject": "Welcome to GQM!",
			"body":    "Thanks for signing up.",
		},
			gqm.Queue("email:send"),
			gqm.MaxRetry(5),
			gqm.RetryIntervals(1, 5, 15, 30, 60), // progressive backoff in seconds
		)
		if err != nil {
			slog.Error("enqueue failed", "to", to, "error", err)
			continue
		}
		slog.Info("enqueued", "job_id", job.ID, "to", to)
	}

	// ── Start server (blocks until signal) ────────────────────────

	fmt.Println("\n=== Email Service Example ===")
	fmt.Println("Handlers: email:send (3 workers, ~30% failure rate)")
	fmt.Println("Press Ctrl+C to stop.\n")

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	if err := server.Start(ctx); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}

// handleEmailSend simulates sending an email.
// ~30% failure rate to demonstrate retry and DLQ behavior.
func handleEmailSend(ctx context.Context, job *gqm.Job) error {
	to := job.Payload["to"]
	subject := job.Payload["subject"]

	// Simulate work
	time.Sleep(time.Duration(200+rand.Intn(800)) * time.Millisecond)

	// Simulate transient failures (~30%)
	if rand.Intn(10) < 3 {
		return fmt.Errorf("SMTP connection timeout for %s", to)
	}

	slog.Info("email sent", "to", to, "subject", subject, "job_id", job.ID)
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
