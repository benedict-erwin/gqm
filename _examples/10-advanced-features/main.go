// Example: Advanced Features (Phase 8)
//
// Demonstrates ErrSkipRetry, IsFailure predicate, middleware chain,
// job callbacks (OnSuccess/OnFailure/OnComplete), and bulk enqueue.
//
// Features shown:
//   - ErrSkipRetry: permanent errors skip retry and go directly to DLQ
//   - IsFailure: classify transient vs permanent errors
//   - Server.Use(): global middleware (logging, metrics)
//   - OnSuccess/OnFailure/OnComplete: per-handler callbacks
//   - EnqueueBatch: bulk enqueue via single Redis pipeline
//
// Usage:
//
//	go run ./_examples/10-advanced-features
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
	"strings"
	"sync/atomic"
	"time"

	gqm "github.com/benedict-erwin/gqm"
)

func main() {
	addr := envOr("GQM_REDIS_ADDR", "localhost:6379")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	// --- Client: enqueue jobs ---
	client, err := gqm.NewClient(gqm.WithRedisAddr(addr))
	if err != nil {
		slog.Error("client", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	// --- Server ---
	srv, err := gqm.NewServer(
		gqm.WithServerRedis(addr),
		gqm.WithShutdownTimeout(3*time.Second),
		gqm.WithLogLevel("info"),
	)
	if err != nil {
		slog.Error("server", "error", err)
		os.Exit(1)
	}

	// --- Middleware: timing wrapper ---
	if err := srv.Use(func(next gqm.Handler) gqm.Handler {
		return func(ctx context.Context, job *gqm.Job) error {
			slog.Info("[middleware] job starting", "id", job.ID, "type", job.Type)
			start := time.Now()
			err := next(ctx, job)
			slog.Info("[middleware] job finished",
				"id", job.ID, "duration", time.Since(start), "error", err,
			)
			return err
		}
	}); err != nil {
		slog.Error("middleware", "error", err)
		os.Exit(1)
	}

	// --- Handler: payment processing with error classification ---
	var processed, failed atomic.Int32

	srv.Handle("payment.process", func(ctx context.Context, job *gqm.Job) error {
		amount := job.Payload["amount"]

		// Simulate different error types
		r := rand.Intn(10)
		switch {
		case r < 4:
			return nil // Success
		case r < 7:
			// Transient error — IsFailure returns false, retries without counting
			return fmt.Errorf("gateway timeout for amount %v", amount)
		case r < 9:
			// Permanent error — ErrSkipRetry bypasses all retries
			return fmt.Errorf("invalid card: %w", gqm.ErrSkipRetry)
		default:
			// Regular failure — retries with counter increment
			return fmt.Errorf("insufficient funds for amount %v", amount)
		}
	},
		gqm.Workers(2),

		// IsFailure: gateway timeouts are transient (return false = don't count).
		// Everything else is a real failure (return true = count toward retry limit).
		gqm.IsFailure(func(err error) bool {
			return !strings.Contains(err.Error(), "gateway timeout")
		}),

		// Callbacks
		gqm.OnSuccess(func(ctx context.Context, job *gqm.Job) {
			processed.Add(1)
			slog.Info("[callback] payment succeeded",
				"id", job.ID, "total_processed", processed.Load(),
			)
		}),
		gqm.OnFailure(func(ctx context.Context, job *gqm.Job, err error) {
			failed.Add(1)
			slog.Warn("[callback] payment failed",
				"id", job.ID, "error", err, "total_failed", failed.Load(),
			)
		}),
		gqm.OnComplete(func(ctx context.Context, job *gqm.Job, err error) {
			status := "success"
			if err != nil {
				status = "failure"
			}
			slog.Info("[callback] payment complete", "id", job.ID, "status", status)
		}),
	)

	// --- Bulk Enqueue: 5 payment jobs in a single pipeline ---
	items := make([]gqm.BatchItem, 5)
	for i := range items {
		items[i] = gqm.BatchItem{
			JobType: "payment.process",
			Payload: gqm.Payload{"amount": (i + 1) * 100, "currency": "USD"},
			Options: []gqm.EnqueueOption{gqm.MaxRetry(3)},
		}
	}

	jobs, err := client.EnqueueBatch(ctx, items)
	if err != nil {
		slog.Error("batch enqueue", "error", err)
		os.Exit(1)
	}
	slog.Info("batch enqueued", "count", len(jobs))

	// --- Start server (auto-shutdown after 10s) ---
	go func() {
		time.Sleep(10 * time.Second)
		slog.Info("auto-shutdown after 10s")
		cancel()
	}()

	if err := srv.Start(ctx); err != nil {
		slog.Error("server stopped", "error", err)
	}

	slog.Info("final stats", "processed", processed.Load(), "failed", failed.Load())
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
