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

func TestClient_EnqueueAt(t *testing.T) {
	rc := testRedisClient(t)
	ctx := context.Background()

	client := &Client{rc: rc}

	at := time.Now().Add(1 * time.Hour)
	job, err := client.EnqueueAt(ctx, at, "email.send", Payload{"to": "user@example.com"},
		Queue("email"),
	)
	if err != nil {
		t.Fatalf("EnqueueAt: %v", err)
	}

	if job.Status != StatusScheduled {
		t.Errorf("Status = %q, want %q", job.Status, StatusScheduled)
	}
	if job.ScheduledAt == 0 {
		t.Error("ScheduledAt should be set")
	}

	// Verify job hash exists.
	jobKey := rc.Key("job", job.ID)
	status, err := rc.rdb.HGet(ctx, jobKey, "status").Result()
	if err != nil {
		t.Fatalf("HGet status: %v", err)
	}
	if status != StatusScheduled {
		t.Errorf("redis status = %q, want %q", status, StatusScheduled)
	}

	// Verify job is in scheduled sorted set (not in ready queue).
	scheduledKey := rc.Key("scheduled")
	score, err := rc.rdb.ZScore(ctx, scheduledKey, job.ID).Result()
	if err != nil {
		t.Fatalf("ZScore: %v", err)
	}
	if int64(score) != at.Unix() {
		t.Errorf("scheduled score = %v, want %v", int64(score), at.Unix())
	}

	// Verify NOT in ready queue.
	queueKey := rc.Key("queue", "email", "ready")
	len, err := rc.rdb.LLen(ctx, queueKey).Result()
	if err != nil {
		t.Fatalf("LLen: %v", err)
	}
	if len != 0 {
		t.Errorf("ready queue len = %d, want 0 (delayed job should not be in ready queue)", len)
	}

	// Cleanup
	rc.rdb.Del(ctx, jobKey, scheduledKey, rc.Key("queues"))
}

func TestClient_EnqueueAt_InvalidJobType(t *testing.T) {
	rc := testRedisClient(t)
	ctx := context.Background()

	client := &Client{rc: rc}

	_, err := client.EnqueueAt(ctx, time.Now().Add(time.Hour), "", Payload{})
	if err != ErrInvalidJobType {
		t.Errorf("expected ErrInvalidJobType, got %v", err)
	}
}

func TestClient_EnqueueIn(t *testing.T) {
	rc := testRedisClient(t)
	ctx := context.Background()

	client := &Client{rc: rc}

	before := time.Now()
	job, err := client.EnqueueIn(ctx, 30*time.Minute, "report.generate", Payload{})
	if err != nil {
		t.Fatalf("EnqueueIn: %v", err)
	}

	if job.Status != StatusScheduled {
		t.Errorf("Status = %q, want %q", job.Status, StatusScheduled)
	}

	// Verify scheduled time is approximately 30 minutes from now.
	scheduledKey := rc.Key("scheduled")
	score, err := rc.rdb.ZScore(ctx, scheduledKey, job.ID).Result()
	if err != nil {
		t.Fatalf("ZScore: %v", err)
	}

	expectedUnix := before.Add(30 * time.Minute).Unix()
	if int64(score) < expectedUnix-1 || int64(score) > expectedUnix+1 {
		t.Errorf("scheduled score = %v, expected ~%v", int64(score), expectedUnix)
	}

	// Cleanup
	rc.rdb.Del(ctx, rc.Key("job", job.ID), scheduledKey, rc.Key("queues"))
}

func TestClient_EnqueueAt_RejectsDependsOn(t *testing.T) {
	rc := testRedisClient(t)
	ctx := context.Background()

	client := &Client{rc: rc}

	_, err := client.EnqueueAt(ctx, time.Now().Add(time.Hour), "test.job", Payload{},
		DependsOn("parent-1"),
	)
	if err == nil {
		t.Fatal("expected error for DependsOn with EnqueueAt, got nil")
	}
}

func TestClient_EnqueueIn_RejectsDependsOn(t *testing.T) {
	rc := testRedisClient(t)
	ctx := context.Background()

	client := &Client{rc: rc}

	_, err := client.EnqueueIn(ctx, 30*time.Minute, "test.job", Payload{},
		DependsOn("parent-1"),
	)
	if err == nil {
		t.Fatal("expected error for DependsOn with EnqueueIn, got nil")
	}
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
