// Example: Idempotent Webhook Delivery
//
// Demonstrates unique job IDs and custom retry intervals for reliable
// webhook delivery. Uses Unique() to prevent duplicate deliveries and
// JobID() for deterministic idempotency keys.
//
// Features shown:
//   - Unique() for idempotent enqueue (no duplicates)
//   - JobID() for deterministic job IDs
//   - RetryIntervals() with custom backoff schedule
//   - Meta() for attaching metadata
//   - EnqueuedBy() for audit trail
//   - Context cancellation in handlers
//
// Usage:
//
//	go run ./_examples/07-webhook-delivery
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

	client, err := gqm.NewClient(gqm.WithRedisAddr(redisAddr))
	if err != nil {
		slog.Error("failed to create client", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	server, err := gqm.NewServer(
		gqm.WithServerRedis(redisAddr),
		gqm.WithLogLevel("info"),
	)
	if err != nil {
		slog.Error("failed to create server", "error", err)
		os.Exit(1)
	}

	server.Handle("webhook:deliver", handleWebhookDeliver, gqm.Workers(5))

	// ── Enqueue webhook deliveries ────────────────────────────────

	ctx := context.Background()

	webhooks := []struct {
		eventID  string
		eventType string
		url      string
	}{
		{"evt_001", "order.created", "https://partner-a.example.com/hooks"},
		{"evt_002", "payment.completed", "https://partner-b.example.com/webhook"},
		{"evt_003", "shipment.shipped", "https://partner-a.example.com/hooks"},
		{"evt_004", "order.created", "https://partner-c.example.com/notify"},
		{"evt_005", "refund.issued", "https://partner-b.example.com/webhook"},
	}

	for _, wh := range webhooks {
		// JobID = deterministic key based on event + endpoint hash.
		// Uses only safe characters (alphanumeric, hyphen, underscore, dot).
		jobID := fmt.Sprintf("webhook.%s.%s", wh.eventID, wh.eventType)

		job, err := client.Enqueue(ctx, "webhook:deliver", gqm.Payload{
			"event_id":   wh.eventID,
			"event_type": wh.eventType,
			"url":        wh.url,
			"body":       map[string]any{"event": wh.eventType, "id": wh.eventID},
		},
			gqm.Queue("webhook:deliver"),
			gqm.JobID(jobID),       // deterministic ID for idempotency
			gqm.Unique(),           // prevent duplicate enqueue
			gqm.MaxRetry(8),
			// Webhook retry schedule: 10s, 30s, 1m, 5m, 15m, 30m, 1h, 2h
			gqm.RetryIntervals(10, 30, 60, 300, 900, 1800, 3600, 7200),
			gqm.EnqueuedBy("webhook-service"),
			gqm.Meta(gqm.Payload{
				"source":     "payment-api",
				"partner_id": "partner-a",
			}),
		)
		if err != nil {
			slog.Warn("enqueue webhook", "event_id", wh.eventID, "error", err)
			continue
		}
		slog.Info("webhook enqueued", "job_id", job.ID, "event_id", wh.eventID, "url", wh.url)
	}

	// Try to enqueue a duplicate — should be silently skipped due to Unique().
	dup, err := client.Enqueue(ctx, "webhook:deliver", gqm.Payload{
		"event_id": "evt_001",
		"url":      "https://partner-a.example.com/hooks",
	},
		gqm.Queue("webhook:deliver"),
		gqm.JobID("webhook.evt_001.order.created"),
		gqm.Unique(),
	)
	if err != nil {
		slog.Info("duplicate correctly rejected", "error", err)
	} else {
		slog.Info("duplicate enqueued (already existed)", "job_id", dup.ID)
	}

	// ── Start server ──────────────────────────────────────────────

	fmt.Println("\n=== Idempotent Webhook Delivery ===")
	fmt.Println("Features:")
	fmt.Println("  - Unique() prevents duplicate deliveries")
	fmt.Println("  - JobID() for deterministic idempotency keys")
	fmt.Println("  - Custom retry: 10s, 30s, 1m, 5m, 15m, 30m, 1h, 2h")
	fmt.Println("  - Meta() for audit trail")
	fmt.Println()
	fmt.Println("Press Ctrl+C to stop.\n")

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	if err := server.Start(ctx); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}

func handleWebhookDeliver(ctx context.Context, job *gqm.Job) error {
	url := job.Payload["url"]
	eventID := job.Payload["event_id"]
	eventType := job.Payload["event_type"]

	// Check context before making the HTTP call.
	if ctx.Err() != nil {
		return ctx.Err()
	}

	// Simulate HTTP POST to webhook URL.
	time.Sleep(time.Duration(200+rand.Intn(800)) * time.Millisecond)

	// Simulate various HTTP response codes.
	// ~20% failure to demonstrate retry behavior.
	roll := rand.Intn(10)
	switch {
	case roll == 0:
		return fmt.Errorf("webhook %s: HTTP 500 Internal Server Error", url)
	case roll == 1:
		return fmt.Errorf("webhook %s: HTTP 502 Bad Gateway", url)
	default:
		slog.Info("webhook delivered",
			"event_id", eventID,
			"event_type", eventType,
			"url", url,
			"status", 200,
			"job_id", job.ID,
		)
		return nil
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
