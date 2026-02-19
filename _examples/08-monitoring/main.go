// Example: Monitoring Setup
//
// Demonstrates the HTTP API, web dashboard, authentication, and API key
// configuration. Starts a server with monitoring enabled so you can
// browse the dashboard and interact with the REST API.
//
// Features shown:
//   - WithAPI() to enable HTTP monitoring endpoints
//   - WithDashboard() for the embedded web dashboard
//   - WithAuthEnabled() + WithAuthUsers() for session-based auth
//   - WithAPIKeys() for programmatic API access
//   - Health endpoint (no auth required)
//   - All 32 REST API endpoints available
//
// Usage:
//
//	go run ./_examples/08-monitoring
//	open http://localhost:8080/dashboard/
//
// Login: admin / secret123
// API key: X-API-Key: gqm_ak_monitoring_example_key_do_not_use_in_production
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
	"golang.org/x/crypto/bcrypt"
)

func main() {
	redisAddr := envOr("GQM_REDIS_ADDR", "localhost:6379")

	// Hash password for the admin user.
	hash, err := bcrypt.GenerateFromPassword([]byte("secret123"), bcrypt.DefaultCost)
	if err != nil {
		slog.Error("failed to hash password", "error", err)
		os.Exit(1)
	}

	server, err := gqm.NewServer(
		gqm.WithServerRedis(redisAddr),
		gqm.WithLogLevel("info"),

		// ── Monitoring ────────────────────────────────────────
		gqm.WithAPI(true, ":8080"),   // enable HTTP API on port 8080
		gqm.WithDashboard(true),       // enable embedded web dashboard

		// ── Authentication ────────────────────────────────────
		gqm.WithAuthEnabled(true),
		gqm.WithAuthUsers([]gqm.AuthUser{
			{
				Username:     "admin",
				PasswordHash: string(hash),
				Role:         "admin", // full access
			},
			{
				Username:     "viewer",
				PasswordHash: string(hash), // same password for demo
				Role:         "viewer",     // read-only access
			},
		}),
		gqm.WithAPIKeys([]gqm.AuthAPIKey{
			{
				Name: "monitoring-bot",
				Key:  "gqm_ak_monitoring_example_key_do_not_use_in_production",
				Role: "viewer", // API key with read-only access
			},
		}),
	)
	if err != nil {
		slog.Error("failed to create server", "error", err)
		os.Exit(1)
	}

	// Register some handlers so the dashboard has data to show.
	server.Handle("task:process", handleTask, gqm.Workers(3))
	server.Handle("task:notify", handleNotify, gqm.Workers(2))

	// Enqueue some sample jobs.
	go func() {
		time.Sleep(2 * time.Second) // wait for server to start

		client, err := gqm.NewClient(gqm.WithRedisAddr(redisAddr))
		if err != nil {
			slog.Error("failed to create client", "error", err)
			return
		}
		defer client.Close()

		ctx := context.Background()

		for i := range 10 {
			client.Enqueue(ctx, "task:process", gqm.Payload{
				"index": i, "data": fmt.Sprintf("item-%d", i),
			}, gqm.Queue("task:process"), gqm.MaxRetry(3))

			client.Enqueue(ctx, "task:notify", gqm.Payload{
				"user": fmt.Sprintf("user-%d", i),
			}, gqm.Queue("task:notify"), gqm.MaxRetry(2))
		}
		slog.Info("sample jobs enqueued", "count", 20)

		// Continuous enqueue for live dashboard demo.
		types := []string{"task:process", "task:notify"}
		for {
			time.Sleep(time.Duration(2+rand.Intn(4)) * time.Second)
			jt := types[rand.Intn(len(types))]
			client.Enqueue(ctx, jt, gqm.Payload{
				"auto": true, "time": time.Now().Format(time.RFC3339),
			}, gqm.Queue(jt), gqm.MaxRetry(2))
		}
	}()

	// ── Start server ──────────────────────────────────────────────

	fmt.Println("=== Monitoring Setup Example ===")
	fmt.Println()
	fmt.Println("Dashboard:  http://localhost:8080/dashboard/")
	fmt.Println("Health:     http://localhost:8080/health")
	fmt.Println()
	fmt.Println("Login:      admin / secret123   (admin role)")
	fmt.Println("            viewer / secret123  (viewer role)")
	fmt.Println()
	fmt.Println("API key:    X-API-Key: gqm_ak_monitoring_example_key_do_not_use_in_production")
	fmt.Println()
	fmt.Println("Try the API:")
	fmt.Println("  curl http://localhost:8080/health")
	fmt.Println("  curl -H 'X-API-Key: gqm_ak_monitoring_example_key_do_not_use_in_production' http://localhost:8080/queues")
	fmt.Println("  curl -H 'X-API-Key: gqm_ak_monitoring_example_key_do_not_use_in_production' http://localhost:8080/stats")
	fmt.Println()
	fmt.Println("Press Ctrl+C to stop.")
	fmt.Println()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	if err := server.Start(ctx); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}

func handleTask(ctx context.Context, job *gqm.Job) error {
	time.Sleep(time.Duration(300+rand.Intn(1500)) * time.Millisecond)
	if rand.Intn(8) == 0 {
		return fmt.Errorf("task processing failed")
	}
	return nil
}

func handleNotify(ctx context.Context, job *gqm.Job) error {
	time.Sleep(time.Duration(100+rand.Intn(500)) * time.Millisecond)
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
