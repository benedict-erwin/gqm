// Example: Custom Dashboard
//
// Demonstrates how to override the embedded dashboard with your own
// HTML/CSS/JS files using WithDashboardDir(). This lets you customize
// the look and feel, add pages, or build an entirely new dashboard
// while keeping GQM's REST API backend.
//
// How to customize:
//
//  1. Export the built-in dashboard:
//     go run ./cmd/gqm dashboard export ./my-dashboard
//
//  2. Edit the exported files (HTML, CSS, JS) to your liking.
//
//  3. Point your server to the custom directory:
//     gqm.WithDashboardDir("./my-dashboard")
//
// Features shown:
//   - WithDashboardDir() to serve custom dashboard files
//   - WithDashboardPathPrefix() to change the dashboard URL path
//   - Full API backend still works with custom frontend
//
// Usage:
//
//	go run ./_examples/11-custom-dashboard
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
	"os"
	"path/filepath"
	"runtime"
	"time"

	gqm "github.com/benedict-erwin/gqm"
	"golang.org/x/crypto/bcrypt"
)

func main() {
	hash, _ := bcrypt.GenerateFromPassword([]byte("admin"), bcrypt.DefaultCost)
	redisAddr := envOr("GQM_TEST_REDIS_ADDR", "localhost:6379")

	// Resolve the custom dashboard directory relative to this source file.
	dashDir := localPath("dashboard")

	s, err := gqm.NewServer(
		gqm.WithServerRedis(redisAddr),
		gqm.WithAPI(true, ":8080"),
		gqm.WithDashboard(true),
		gqm.WithDashboardDir(dashDir), // <-- serve from local directory instead of embedded
		gqm.WithAuthEnabled(true),
		gqm.WithAuthUsers([]gqm.AuthUser{
			{Username: "admin", PasswordHash: string(hash), Role: "admin"},
		}),
		gqm.WithAPIKeys([]gqm.AuthAPIKey{
			{Name: "dev-key", Key: "gqm_ak_custom_dashboard_example_key_testing", Role: "admin"},
		}),
		gqm.WithLogLevel("debug"),
	)
	if err != nil {
		slog.Error("failed to create server", "err", err)
		os.Exit(1)
	}

	// Register a simple handler for demo purposes.
	s.Handle("demo:task", func(ctx context.Context, job *gqm.Job) error {
		time.Sleep(500 * time.Millisecond)
		return nil
	}, gqm.Workers(2))

	// Seed a few jobs so the dashboard has something to show.
	go func() {
		time.Sleep(2 * time.Second)
		client, err := gqm.NewClient(gqm.WithRedisAddr(redisAddr))
		if err != nil {
			return
		}
		defer client.Close()
		ctx := context.Background()
		for i := range 5 {
			client.Enqueue(ctx, "demo:task", gqm.Payload{
				"index": i, "msg": "hello from custom dashboard example",
			}, gqm.Queue("demo:task"), gqm.MaxRetry(2))
		}
		slog.Info("seeded 5 demo jobs")
	}()

	fmt.Println("=== GQM Custom Dashboard Example ===")
	fmt.Println()
	fmt.Println("Dashboard: http://localhost:8080/dashboard/")
	fmt.Println("Login:     admin / admin")
	fmt.Println()
	fmt.Println("Custom dashboard served from:", dashDir)
	fmt.Println()
	fmt.Println("To customize the built-in dashboard:")
	fmt.Println("  1. go run ./cmd/gqm dashboard export ./my-dashboard")
	fmt.Println("  2. Edit files in ./my-dashboard/")
	fmt.Println("  3. Use gqm.WithDashboardDir(\"./my-dashboard\")")
	fmt.Println()

	if err := s.Start(context.Background()); err != nil {
		slog.Error("server error", "err", err)
		os.Exit(1)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// localPath resolves a path relative to this source file's directory.
func localPath(name string) string {
	_, src, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(src), name)
}
