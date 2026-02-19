// Example: Image Processing Pipeline
//
// Demonstrates DAG (Directed Acyclic Graph) job dependencies with per-job
// timeouts. Jobs form a linear chain: download → resize → watermark → upload.
// Each step depends on the previous one completing successfully.
//
// Features shown:
//   - DependsOn() for linear DAG chains
//   - Timeout() per job
//   - Multiple handlers with different worker counts
//   - Job payload passing between steps
//
// Usage:
//
//	go run ./_examples/02-image-pipeline
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

	// Register handlers for each pipeline stage.
	server.Handle("image:download", handleDownload, gqm.Workers(2))
	server.Handle("image:resize", handleResize, gqm.Workers(3))
	server.Handle("image:watermark", handleWatermark, gqm.Workers(2))
	server.Handle("image:upload", handleUpload, gqm.Workers(2))

	// ── Build the pipeline (DAG chain) ────────────────────────────
	ctx := context.Background()

	images := []string{"photo_001.jpg", "photo_002.jpg", "photo_003.jpg"}

	for _, img := range images {
		// Step 1: Download (root — no dependencies)
		download, err := client.Enqueue(ctx, "image:download", gqm.Payload{
			"url":      fmt.Sprintf("https://cdn.example.com/%s", img),
			"filename": img,
		},
			gqm.Queue("image:download"),
			gqm.Timeout(30*time.Second),
			gqm.MaxRetry(3),
		)
		if err != nil {
			slog.Error("enqueue download failed", "image", img, "error", err)
			continue
		}

		// Step 2: Resize (depends on download)
		resize, err := client.Enqueue(ctx, "image:resize", gqm.Payload{
			"filename": img,
			"width":    1920,
			"height":   1080,
		},
			gqm.Queue("image:resize"),
			gqm.Timeout(60*time.Second), // resize can be slow
			gqm.MaxRetry(2),
			gqm.DependsOn(download.ID),
		)
		if err != nil {
			slog.Error("enqueue resize failed", "image", img, "error", err)
			continue
		}

		// Step 3: Watermark (depends on resize)
		watermark, err := client.Enqueue(ctx, "image:watermark", gqm.Payload{
			"filename":  img,
			"watermark": "© 2026 Example Corp",
			"position":  "bottom-right",
		},
			gqm.Queue("image:watermark"),
			gqm.Timeout(15*time.Second),
			gqm.MaxRetry(2),
			gqm.DependsOn(resize.ID),
		)
		if err != nil {
			slog.Error("enqueue watermark failed", "image", img, "error", err)
			continue
		}

		// Step 4: Upload (depends on watermark)
		upload, err := client.Enqueue(ctx, "image:upload", gqm.Payload{
			"filename":    img,
			"destination": "s3://output-bucket/processed/",
		},
			gqm.Queue("image:upload"),
			gqm.Timeout(45*time.Second),
			gqm.MaxRetry(3),
			gqm.DependsOn(watermark.ID),
		)
		if err != nil {
			slog.Error("enqueue upload failed", "image", img, "error", err)
			continue
		}

		slog.Info("pipeline created",
			"image", img,
			"download", download.ID,
			"resize", resize.ID,
			"watermark", watermark.ID,
			"upload", upload.ID,
		)
	}

	// ── Start server ──────────────────────────────────────────────

	fmt.Println("\n=== Image Processing Pipeline ===")
	fmt.Println("Chain: download → resize → watermark → upload")
	fmt.Println("Press Ctrl+C to stop.\n")

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	if err := server.Start(ctx); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}

func handleDownload(ctx context.Context, job *gqm.Job) error {
	url := job.Payload["url"]
	time.Sleep(time.Duration(500+rand.Intn(1000)) * time.Millisecond)
	slog.Info("downloaded", "url", url, "job_id", job.ID)
	return nil
}

func handleResize(ctx context.Context, job *gqm.Job) error {
	filename := job.Payload["filename"]
	time.Sleep(time.Duration(1000+rand.Intn(2000)) * time.Millisecond)
	slog.Info("resized", "filename", filename, "job_id", job.ID)
	return nil
}

func handleWatermark(ctx context.Context, job *gqm.Job) error {
	filename := job.Payload["filename"]
	time.Sleep(time.Duration(300+rand.Intn(700)) * time.Millisecond)
	slog.Info("watermarked", "filename", filename, "job_id", job.ID)
	return nil
}

func handleUpload(ctx context.Context, job *gqm.Job) error {
	filename := job.Payload["filename"]
	dest := job.Payload["destination"]
	time.Sleep(time.Duration(800+rand.Intn(1200)) * time.Millisecond)
	slog.Info("uploaded", "filename", filename, "destination", dest, "job_id", job.ID)
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
