// Example: Order Fulfillment
//
// Demonstrates a diamond DAG pattern for e-commerce order processing.
// After validation, payment and invoice run in parallel, then shipping
// and notification depend on both completing.
//
//	     validate
//	     /      \
//	payment    invoice
//	     \      /
//	     shipping
//	        |
//	     notify
//
// Features shown:
//   - DependsOn() with multiple parents (diamond join)
//   - AllowFailure() for non-critical steps
//   - DAG failure propagation (if payment fails, shipping is canceled)
//   - Multiple job types in a single workflow
//
// Usage:
//
//	go run ./_examples/04-order-fulfillment
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
	)
	if err != nil {
		slog.Error("failed to create server", "error", err)
		os.Exit(1)
	}

	// Register handlers for each fulfillment step.
	server.Handle("order:validate", handleValidate, gqm.Workers(3))
	server.Handle("order:payment", handlePayment, gqm.Workers(2))
	server.Handle("order:invoice", handleInvoice, gqm.Workers(2))
	server.Handle("order:shipping", handleShipping, gqm.Workers(2))
	server.Handle("order:notify", handleNotify, gqm.Workers(3))

	// ── Create order workflows ────────────────────────────────────

	ctx := context.Background()

	orders := []struct {
		orderID string
		amount  float64
		items   int
	}{
		{"ORD-1001", 149.99, 3},
		{"ORD-1002", 29.99, 1},
		{"ORD-1003", 599.00, 7},
	}

	for _, order := range orders {
		if err := createOrderWorkflow(ctx, client, order.orderID, order.amount, order.items); err != nil {
			slog.Error("failed to create order workflow", "order_id", order.orderID, "error", err)
		}
	}

	// ── Start server ──────────────────────────────────────────────

	fmt.Println("\n=== Order Fulfillment Example ===")
	fmt.Println("DAG: validate → [payment + invoice] → shipping → notify")
	fmt.Println("Press Ctrl+C to stop.\n")

	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt)
	defer cancel()

	if err := server.Start(ctx); err != nil {
		slog.Error("server error", "error", err)
		os.Exit(1)
	}
}

func createOrderWorkflow(ctx context.Context, client *gqm.Client, orderID string, amount float64, items int) error {
	payload := gqm.Payload{"order_id": orderID, "amount": amount, "items": items}

	// Step 1: Validate order (root — no dependencies)
	validate, err := client.Enqueue(ctx, "order:validate", payload,
		gqm.Queue("order:validate"),
		gqm.MaxRetry(2),
	)
	if err != nil {
		return fmt.Errorf("enqueue validate: %w", err)
	}

	// Step 2a: Process payment (depends on validation)
	payment, err := client.Enqueue(ctx, "order:payment", payload,
		gqm.Queue("order:payment"),
		gqm.MaxRetry(3),
		gqm.DependsOn(validate.ID),
	)
	if err != nil {
		return fmt.Errorf("enqueue payment: %w", err)
	}

	// Step 2b: Generate invoice (depends on validation, AllowFailure)
	// Invoice failure should NOT block shipping — it can be regenerated later.
	invoice, err := client.Enqueue(ctx, "order:invoice", payload,
		gqm.Queue("order:invoice"),
		gqm.MaxRetry(2),
		gqm.DependsOn(validate.ID),
		gqm.AllowFailure(true), // shipping proceeds even if invoice fails
	)
	if err != nil {
		return fmt.Errorf("enqueue invoice: %w", err)
	}

	// Step 3: Schedule shipping (depends on BOTH payment AND invoice)
	shipping, err := client.Enqueue(ctx, "order:shipping", payload,
		gqm.Queue("order:shipping"),
		gqm.MaxRetry(2),
		gqm.DependsOn(payment.ID, invoice.ID), // diamond join
	)
	if err != nil {
		return fmt.Errorf("enqueue shipping: %w", err)
	}

	// Step 4: Send notification (depends on shipping)
	notify, err := client.Enqueue(ctx, "order:notify", payload,
		gqm.Queue("order:notify"),
		gqm.MaxRetry(5),
		gqm.DependsOn(shipping.ID),
	)
	if err != nil {
		return fmt.Errorf("enqueue notify: %w", err)
	}

	slog.Info("order workflow created",
		"order_id", orderID,
		"validate", validate.ID,
		"payment", payment.ID,
		"invoice", invoice.ID,
		"shipping", shipping.ID,
		"notify", notify.ID,
	)
	return nil
}

func handleValidate(ctx context.Context, job *gqm.Job) error {
	orderID := job.Payload["order_id"]
	time.Sleep(time.Duration(200+rand.Intn(500)) * time.Millisecond)
	slog.Info("order validated", "order_id", orderID, "job_id", job.ID)
	return nil
}

func handlePayment(ctx context.Context, job *gqm.Job) error {
	orderID := job.Payload["order_id"]
	amount := job.Payload["amount"]
	time.Sleep(time.Duration(500+rand.Intn(1500)) * time.Millisecond)

	// ~10% payment failure
	if rand.Intn(10) == 0 {
		return fmt.Errorf("payment gateway timeout for order %s", orderID)
	}

	slog.Info("payment processed", "order_id", orderID, "amount", amount, "job_id", job.ID)
	return nil
}

func handleInvoice(ctx context.Context, job *gqm.Job) error {
	orderID := job.Payload["order_id"]
	time.Sleep(time.Duration(300+rand.Intn(700)) * time.Millisecond)

	// ~20% invoice failure (but AllowFailure so workflow continues)
	if rand.Intn(5) == 0 {
		return fmt.Errorf("PDF generation failed for order %s", orderID)
	}

	slog.Info("invoice generated", "order_id", orderID, "job_id", job.ID)
	return nil
}

func handleShipping(ctx context.Context, job *gqm.Job) error {
	orderID := job.Payload["order_id"]
	items := job.Payload["items"]
	time.Sleep(time.Duration(400+rand.Intn(600)) * time.Millisecond)
	slog.Info("shipping scheduled", "order_id", orderID, "items", items, "job_id", job.ID)
	return nil
}

func handleNotify(ctx context.Context, job *gqm.Job) error {
	orderID := job.Payload["order_id"]
	time.Sleep(time.Duration(100+rand.Intn(300)) * time.Millisecond)
	slog.Info("customer notified", "order_id", orderID, "job_id", job.ID)
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
