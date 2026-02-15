package gqm

import (
	"context"
	"testing"
	"time"
)

func TestClient_Enqueue(t *testing.T) {
	rc := testRedisClient(t)
	ctx := context.Background()

	client := &Client{rc: rc}

	job, err := client.Enqueue(ctx, "email.send", Payload{
		"to":      "user@example.com",
		"subject": "Hello",
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if job.ID == "" {
		t.Error("expected non-empty job ID")
	}
	if job.Type != "email.send" {
		t.Errorf("Type = %q, want %q", job.Type, "email.send")
	}
	if job.Queue != "default" {
		t.Errorf("Queue = %q, want %q", job.Queue, "default")
	}

	// Verify job hash exists in Redis
	jobKey := rc.Key("job", job.ID)
	exists, err := rc.rdb.Exists(ctx, jobKey).Result()
	if err != nil {
		t.Fatalf("checking job exists: %v", err)
	}
	if exists != 1 {
		t.Error("job hash not found in Redis")
	}

	// Verify job is in ready queue
	queueKey := rc.Key("queue", "default", "ready")
	members, err := rc.rdb.LRange(ctx, queueKey, 0, -1).Result()
	if err != nil {
		t.Fatalf("checking queue: %v", err)
	}
	found := false
	for _, m := range members {
		if m == job.ID {
			found = true
			break
		}
	}
	if !found {
		t.Error("job ID not found in ready queue")
	}

	// Cleanup
	rc.rdb.Del(ctx, jobKey, queueKey, rc.Key("queues"))
}

func TestClient_Enqueue_WithOptions(t *testing.T) {
	rc := testRedisClient(t)
	ctx := context.Background()

	client := &Client{rc: rc}

	job, err := client.Enqueue(ctx, "payment.process", Payload{"amount": 99.99},
		Queue("payment"),
		MaxRetry(5),
		Timeout(2*time.Minute),
		RetryIntervals(10, 30, 60),
		EnqueuedBy("test-suite"),
		Meta(Payload{"priority": "high"}),
	)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	if job.Queue != "payment" {
		t.Errorf("Queue = %q, want %q", job.Queue, "payment")
	}
	if job.MaxRetry != 5 {
		t.Errorf("MaxRetry = %d, want 5", job.MaxRetry)
	}
	if job.Timeout != 120 {
		t.Errorf("Timeout = %d, want 120", job.Timeout)
	}
	if len(job.RetryIntervals) != 3 {
		t.Errorf("RetryIntervals len = %d, want 3", len(job.RetryIntervals))
	}
	if job.EnqueuedBy != "test-suite" {
		t.Errorf("EnqueuedBy = %q, want %q", job.EnqueuedBy, "test-suite")
	}

	// Cleanup
	rc.rdb.Del(ctx, rc.Key("job", job.ID), rc.Key("queue", "payment", "ready"), rc.Key("queues"))
}

func TestClient_Enqueue_InvalidJobType(t *testing.T) {
	rc := testRedisClient(t)
	ctx := context.Background()

	client := &Client{rc: rc}

	_, err := client.Enqueue(ctx, "", Payload{})
	if err != ErrInvalidJobType {
		t.Errorf("expected ErrInvalidJobType, got %v", err)
	}
}

func TestClient_GetJob(t *testing.T) {
	rc := testRedisClient(t)
	ctx := context.Background()

	client := &Client{rc: rc}

	original, err := client.Enqueue(ctx, "test.job", Payload{"key": "value"})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	fetched, err := client.GetJob(ctx, original.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}

	if fetched.ID != original.ID {
		t.Errorf("ID = %q, want %q", fetched.ID, original.ID)
	}
	if fetched.Type != "test.job" {
		t.Errorf("Type = %q, want %q", fetched.Type, "test.job")
	}

	// Cleanup
	rc.rdb.Del(ctx, rc.Key("job", original.ID), rc.Key("queue", "default", "ready"), rc.Key("queues"))
}

func TestClient_GetJob_NotFound(t *testing.T) {
	rc := testRedisClient(t)
	ctx := context.Background()

	client := &Client{rc: rc}

	_, err := client.GetJob(ctx, "nonexistent-id")
	if err != ErrJobNotFound {
		t.Errorf("expected ErrJobNotFound, got %v", err)
	}
}
