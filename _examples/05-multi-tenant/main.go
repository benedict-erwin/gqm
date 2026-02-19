// Example: Multi-tenant Worker Pools
//
// Demonstrates explicit pool configuration with priority queues and different
// dequeue strategies. Simulates a SaaS platform where premium tenants get
// priority processing over free-tier users.
//
// Features shown:
//   - Server.Pool() for explicit pool configuration
//   - Priority queues (multiple queues per pool, ordered by priority)
//   - DequeueStrategy: strict, round-robin, weighted
//   - Pool-level RetryPolicy with backoff types
//   - Per-pool concurrency and timeout settings
//
// Usage:
//
//	go run ./_examples/05-multi-tenant
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

	// ── Pool 1: API requests (strict priority) ────────────────────
	// Premium queue always processed first before standard queue.
	server.Pool(gqm.PoolConfig{
		Name:     "api-pool",
		JobTypes: []string{"api:request"},
		Queues: []string{
			"api:premium", // highest priority (processed first)
			"api:standard",
		},
		Concurrency:     5,
		JobTimeout:       10 * time.Second,
		DequeueStrategy: gqm.StrategyStrict, // always drain higher-priority queue first
		RetryPolicy: &gqm.RetryPolicy{
			MaxRetry:    3,
			Backoff:     gqm.BackoffExponential,
			BackoffBase: 1 * time.Second,
			BackoffMax:  30 * time.Second,
		},
	})

	// ── Pool 2: Background jobs (weighted) ────────────────────────
	// Premium gets ~70% throughput, standard ~30% (via position-based weights).
	server.Pool(gqm.PoolConfig{
		Name:     "background-pool",
		JobTypes: []string{"bg:process"},
		Queues: []string{
			"bg:premium",
			"bg:standard",
		},
		Concurrency:     3,
		JobTimeout:       5 * time.Minute,
		DequeueStrategy: gqm.StrategyWeighted, // weighted round-robin
		RetryPolicy: &gqm.RetryPolicy{
			MaxRetry:    5,
			Backoff:     gqm.BackoffFixed,
			BackoffBase: 10 * time.Second,
		},
	})

	// ── Pool 3: Notifications (round-robin) ───────────────────────
	// Fair processing across all notification channels.
	server.Pool(gqm.PoolConfig{
		Name:     "notification-pool",
		JobTypes: []string{"notify:send"},
		Queues: []string{
			"notify:email",
			"notify:sms",
			"notify:push",
		},
		Concurrency:     4,
		JobTimeout:       30 * time.Second,
		DequeueStrategy: gqm.StrategyRoundRobin, // fair: rotate across queues
		RetryPolicy: &gqm.RetryPolicy{
			MaxRetry:  2,
			Backoff:   gqm.BackoffCustom,
			Intervals: []int{5, 30}, // 5s, then 30s
		},
	})

	// Register handlers.
	server.Handle("api:request", handleAPIRequest)
	server.Handle("bg:process", handleBackgroundJob)
	server.Handle("notify:send", handleNotification)

	// ── Enqueue sample jobs ───────────────────────────────────────

	ctx := context.Background()

	// API requests: mix of premium and standard
	for i := range 20 {
		queue := "api:standard"
		tenant := "free-user"
		if i%3 == 0 {
			queue = "api:premium"
			tenant = "premium-user"
		}
		client.Enqueue(ctx, "api:request", gqm.Payload{
			"tenant":   tenant,
			"endpoint": fmt.Sprintf("/api/v1/resource/%d", i),
			"index":    i,
		}, gqm.Queue(queue))
	}
	slog.Info("enqueued: 20 API requests (mixed premium/standard)")

	// Background jobs: premium and standard
	for i := range 10 {
		queue := "bg:standard"
		tenant := "free-corp"
		if i%2 == 0 {
			queue = "bg:premium"
			tenant = "enterprise-corp"
		}
		client.Enqueue(ctx, "bg:process", gqm.Payload{
			"tenant": tenant,
			"task":   fmt.Sprintf("data-sync-%d", i),
		}, gqm.Queue(queue))
	}
	slog.Info("enqueued: 10 background jobs (mixed premium/standard)")

	// Notifications: across all channels
	channels := []struct {
		queue   string
		channel string
	}{
		{"notify:email", "email"},
		{"notify:sms", "sms"},
		{"notify:push", "push"},
	}
	for i := range 12 {
		ch := channels[i%len(channels)]
		client.Enqueue(ctx, "notify:send", gqm.Payload{
			"channel": ch.channel,
			"user_id": fmt.Sprintf("user-%d", 100+i),
			"message": "Your order has been shipped!",
		}, gqm.Queue(ch.queue))
	}
	slog.Info("enqueued: 12 notifications (email/sms/push)")

	// ── Start server ──────────────────────────────────────────────

	fmt.Println("\n=== Multi-tenant Worker Pools ===")
	fmt.Println("Pools:")
	fmt.Println("  api-pool          — 5 workers, strict priority (premium first)")
	fmt.Println("  background-pool   — 3 workers, weighted (~70/30 premium/standard)")
	fmt.Println("  notification-pool — 4 workers, round-robin (email/sms/push)")
	fmt.Println()
	fmt.Println("Press Ctrl+C to stop.\n")

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	if err := server.Start(ctx); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}

func handleAPIRequest(ctx context.Context, job *gqm.Job) error {
	tenant := job.Payload["tenant"]
	endpoint := job.Payload["endpoint"]
	time.Sleep(time.Duration(50+rand.Intn(200)) * time.Millisecond)
	slog.Info("API request processed", "tenant", tenant, "endpoint", endpoint)
	return nil
}

func handleBackgroundJob(ctx context.Context, job *gqm.Job) error {
	tenant := job.Payload["tenant"]
	task := job.Payload["task"]
	time.Sleep(time.Duration(1000+rand.Intn(3000)) * time.Millisecond)
	slog.Info("background job done", "tenant", tenant, "task", task)
	return nil
}

func handleNotification(ctx context.Context, job *gqm.Job) error {
	channel := job.Payload["channel"]
	userID := job.Payload["user_id"]
	time.Sleep(time.Duration(100+rand.Intn(500)) * time.Millisecond)
	slog.Info("notification sent", "channel", channel, "user_id", userID)
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
