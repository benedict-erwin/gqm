package gqm

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func testAdminServer(t *testing.T) (*Server, *redis.Client) {
	t.Helper()
	addr := testRedisAddr()
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	prefix := "gqmadmintest:" + t.Name() + ":"

	s, err := NewServer(
		WithServerRedis(addr),
		WithServerRedisOpts(WithPrefix(prefix)),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	t.Cleanup(func() {
		ctx := context.Background()
		iter := rdb.Scan(ctx, 0, prefix+"*", 100).Iterator()
		for iter.Next(ctx) {
			rdb.Del(ctx, iter.Val())
		}
		s.rc.Close()
		rdb.Close()
	})

	return s, rdb
}

// --- Queue Pause ---

func TestPauseQueue(t *testing.T) {
	s, _ := testAdminServer(t)
	ctx := context.Background()

	if err := s.PauseQueue(ctx, "email"); err != nil {
		t.Fatalf("PauseQueue: %v", err)
	}

	paused, err := s.IsQueuePaused(ctx, "email")
	if err != nil {
		t.Fatalf("IsQueuePaused: %v", err)
	}
	if !paused {
		t.Error("queue should be paused")
	}

	if err := s.ResumeQueue(ctx, "email"); err != nil {
		t.Fatalf("ResumeQueue: %v", err)
	}

	paused, err = s.IsQueuePaused(ctx, "email")
	if err != nil {
		t.Fatalf("IsQueuePaused: %v", err)
	}
	if paused {
		t.Error("queue should not be paused after resume")
	}
}

func TestIsQueuePaused_DefaultFalse(t *testing.T) {
	s, _ := testAdminServer(t)
	ctx := context.Background()

	paused, err := s.IsQueuePaused(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("IsQueuePaused: %v", err)
	}
	if paused {
		t.Error("unpaused queue should return false")
	}
}

// --- Job Retry ---

func TestRetryJob_FromDLQ(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	// Seed a DLQ job
	rdb.HSet(ctx, s.rc.Key("job", "j1"),
		"id", "j1", "type", "test", "queue", "default", "status", "dead_letter",
		"error", "some error", "retry_count", "3")
	rdb.ZAdd(ctx, s.rc.Key("queue", "default", "dead_letter"),
		redis.Z{Score: float64(time.Now().Unix()), Member: "j1"})

	if err := s.RetryJob(ctx, "j1"); err != nil {
		t.Fatalf("RetryJob: %v", err)
	}

	// Verify job is now ready
	status, _ := rdb.HGet(ctx, s.rc.Key("job", "j1"), "status").Result()
	if status != "ready" {
		t.Errorf("status = %q, want ready", status)
	}

	// Verify retry_count reset
	rc, _ := rdb.HGet(ctx, s.rc.Key("job", "j1"), "retry_count").Result()
	if rc != "0" {
		t.Errorf("retry_count = %q, want 0", rc)
	}

	// Verify in ready list
	len, _ := rdb.LLen(ctx, s.rc.Key("queue", "default", "ready")).Result()
	if len != 1 {
		t.Errorf("ready list len = %d, want 1", len)
	}

	// Verify removed from DLQ
	dlqLen, _ := rdb.ZCard(ctx, s.rc.Key("queue", "default", "dead_letter")).Result()
	if dlqLen != 0 {
		t.Errorf("DLQ len = %d, want 0", dlqLen)
	}
}

func TestRetryJob_NotFound(t *testing.T) {
	s, _ := testAdminServer(t)
	ctx := context.Background()

	err := s.RetryJob(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent job")
	}
}

func TestRetryJob_WrongStatus(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	rdb.HSet(ctx, s.rc.Key("job", "j1"),
		"id", "j1", "type", "test", "queue", "default", "status", "processing")

	err := s.RetryJob(ctx, "j1")
	if err == nil {
		t.Fatal("expected error for processing job")
	}
}

// --- Job Cancel ---

func TestCancelJob_Ready(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	rdb.HSet(ctx, s.rc.Key("job", "j1"),
		"id", "j1", "type", "test", "queue", "default", "status", "ready")
	rdb.LPush(ctx, s.rc.Key("queue", "default", "ready"), "j1")

	if err := s.CancelJob(ctx, "j1"); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}

	status, _ := rdb.HGet(ctx, s.rc.Key("job", "j1"), "status").Result()
	if status != "canceled" {
		t.Errorf("status = %q, want canceled", status)
	}

	len, _ := rdb.LLen(ctx, s.rc.Key("queue", "default", "ready")).Result()
	if len != 0 {
		t.Errorf("ready list len = %d, want 0", len)
	}
}

func TestCancelJob_Scheduled(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	rdb.HSet(ctx, s.rc.Key("job", "j1"),
		"id", "j1", "type", "test", "queue", "default", "status", "scheduled")
	rdb.ZAdd(ctx, s.rc.Key("scheduled"),
		redis.Z{Score: float64(time.Now().Add(time.Hour).Unix()), Member: "j1"})

	if err := s.CancelJob(ctx, "j1"); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}

	status, _ := rdb.HGet(ctx, s.rc.Key("job", "j1"), "status").Result()
	if status != "canceled" {
		t.Errorf("status = %q, want canceled", status)
	}
}

func TestCancelJob_Processing(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	rdb.HSet(ctx, s.rc.Key("job", "j1"),
		"id", "j1", "type", "test", "queue", "default", "status", "processing")

	err := s.CancelJob(ctx, "j1")
	if err == nil {
		t.Fatal("expected error for processing job")
	}
}

// --- Job Delete ---

func TestDeleteJob_Ready(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	rdb.HSet(ctx, s.rc.Key("job", "j1"),
		"id", "j1", "type", "test", "queue", "default", "status", "ready")
	rdb.LPush(ctx, s.rc.Key("queue", "default", "ready"), "j1")

	if err := s.DeleteJob(ctx, "j1"); err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}

	exists, _ := rdb.Exists(ctx, s.rc.Key("job", "j1")).Result()
	if exists != 0 {
		t.Error("job hash should be deleted")
	}
}

func TestDeleteJob_Processing(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	rdb.HSet(ctx, s.rc.Key("job", "j1"),
		"id", "j1", "type", "test", "queue", "default", "status", "processing")

	err := s.DeleteJob(ctx, "j1")
	if err == nil {
		t.Fatal("expected error for processing job")
	}
}

func TestDeleteJob_NotFound(t *testing.T) {
	s, _ := testAdminServer(t)
	ctx := context.Background()

	err := s.DeleteJob(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent job")
	}
}

// --- Empty Queue ---

func TestEmptyQueue(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	// Seed 3 ready jobs
	for _, id := range []string{"j1", "j2", "j3"} {
		rdb.HSet(ctx, s.rc.Key("job", id),
			"id", id, "type", "test", "queue", "default", "status", "ready")
		rdb.LPush(ctx, s.rc.Key("queue", "default", "ready"), id)
	}

	removed, err := s.EmptyQueue(ctx, "default")
	if err != nil {
		t.Fatalf("EmptyQueue: %v", err)
	}
	if removed != 3 {
		t.Errorf("removed = %d, want 3", removed)
	}

	len, _ := rdb.LLen(ctx, s.rc.Key("queue", "default", "ready")).Result()
	if len != 0 {
		t.Errorf("ready list len = %d, want 0", len)
	}

	// Jobs should be canceled
	status, _ := rdb.HGet(ctx, s.rc.Key("job", "j1"), "status").Result()
	if status != "canceled" {
		t.Errorf("status = %q, want canceled", status)
	}
}

func TestEmptyQueue_AlreadyEmpty(t *testing.T) {
	s, _ := testAdminServer(t)
	ctx := context.Background()

	removed, err := s.EmptyQueue(ctx, "empty-queue")
	if err != nil {
		t.Fatalf("EmptyQueue: %v", err)
	}
	if removed != 0 {
		t.Errorf("removed = %d, want 0", removed)
	}
}

// --- DLQ Operations ---

func TestRetryAllDLQ(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	for _, id := range []string{"j1", "j2"} {
		rdb.HSet(ctx, s.rc.Key("job", id),
			"id", id, "type", "test", "queue", "default", "status", "dead_letter")
		rdb.ZAdd(ctx, s.rc.Key("queue", "default", "dead_letter"),
			redis.Z{Score: float64(time.Now().Unix()), Member: id})
	}

	retried, err := s.RetryAllDLQ(ctx, "default")
	if err != nil {
		t.Fatalf("RetryAllDLQ: %v", err)
	}
	if retried != 2 {
		t.Errorf("retried = %d, want 2", retried)
	}

	readyLen, _ := rdb.LLen(ctx, s.rc.Key("queue", "default", "ready")).Result()
	if readyLen != 2 {
		t.Errorf("ready list len = %d, want 2", readyLen)
	}
}

func TestClearDLQ(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	for _, id := range []string{"j1", "j2"} {
		rdb.HSet(ctx, s.rc.Key("job", id),
			"id", id, "type", "test", "queue", "default", "status", "dead_letter")
		rdb.ZAdd(ctx, s.rc.Key("queue", "default", "dead_letter"),
			redis.Z{Score: float64(time.Now().Unix()), Member: id})
	}

	cleared, err := s.ClearDLQ(ctx, "default")
	if err != nil {
		t.Fatalf("ClearDLQ: %v", err)
	}
	if cleared != 2 {
		t.Errorf("cleared = %d, want 2", cleared)
	}

	// Job hashes should be deleted
	exists, _ := rdb.Exists(ctx, s.rc.Key("job", "j1")).Result()
	if exists != 0 {
		t.Error("job hash should be deleted")
	}

	// DLQ should be empty
	dlqLen, _ := rdb.ZCard(ctx, s.rc.Key("queue", "default", "dead_letter")).Result()
	if dlqLen != 0 {
		t.Errorf("DLQ len = %d, want 0", dlqLen)
	}
}

// --- Cron Operations ---

func TestTriggerCron(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	entry := map[string]any{
		"id":        "daily",
		"name":      "Daily Report",
		"cron_expr": "0 0 3 * * *",
		"job_type":  "report.generate",
		"queue":     "default",
		"enabled":   true,
	}
	data, _ := json.Marshal(entry)
	rdb.HSet(ctx, s.rc.Key("cron", "entries"), "daily", string(data))

	jobID, err := s.TriggerCron(ctx, "daily")
	if err != nil {
		t.Fatalf("TriggerCron: %v", err)
	}
	if jobID == "" {
		t.Fatal("expected non-empty job ID")
	}

	// Job should exist and be ready
	status, _ := rdb.HGet(ctx, s.rc.Key("job", jobID), "status").Result()
	if status != "ready" {
		t.Errorf("status = %q, want ready", status)
	}

	jobType, _ := rdb.HGet(ctx, s.rc.Key("job", jobID), "type").Result()
	if jobType != "report.generate" {
		t.Errorf("type = %q, want report.generate", jobType)
	}

	// Should be in ready queue
	readyLen, _ := rdb.LLen(ctx, s.rc.Key("queue", "default", "ready")).Result()
	if readyLen != 1 {
		t.Errorf("ready list len = %d, want 1", readyLen)
	}

	// Should be in history
	histLen, _ := rdb.ZCard(ctx, s.rc.Key("cron", "history", "daily")).Result()
	if histLen != 1 {
		t.Errorf("history len = %d, want 1", histLen)
	}
}

func TestTriggerCron_NotFound(t *testing.T) {
	s, _ := testAdminServer(t)
	ctx := context.Background()

	_, err := s.TriggerCron(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent cron entry")
	}
}

func TestEnableCron(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	entry := map[string]any{
		"id":      "daily",
		"enabled": false,
	}
	data, _ := json.Marshal(entry)
	rdb.HSet(ctx, s.rc.Key("cron", "entries"), "daily", string(data))

	if err := s.EnableCron(ctx, "daily"); err != nil {
		t.Fatalf("EnableCron: %v", err)
	}

	// Read back and verify
	raw, _ := rdb.HGet(ctx, s.rc.Key("cron", "entries"), "daily").Result()
	var updated map[string]any
	json.Unmarshal([]byte(raw), &updated)
	if updated["enabled"] != true {
		t.Errorf("enabled = %v, want true", updated["enabled"])
	}
}

func TestDisableCron(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	entry := map[string]any{
		"id":      "daily",
		"enabled": true,
	}
	data, _ := json.Marshal(entry)
	rdb.HSet(ctx, s.rc.Key("cron", "entries"), "daily", string(data))

	if err := s.DisableCron(ctx, "daily"); err != nil {
		t.Fatalf("DisableCron: %v", err)
	}

	raw, _ := rdb.HGet(ctx, s.rc.Key("cron", "entries"), "daily").Result()
	var updated map[string]any
	json.Unmarshal([]byte(raw), &updated)
	if updated["enabled"] != false {
		t.Errorf("enabled = %v, want false", updated["enabled"])
	}
}

func TestEnableCron_NotFound(t *testing.T) {
	s, _ := testAdminServer(t)
	ctx := context.Background()

	err := s.EnableCron(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent cron entry")
	}
}

// --- Pause mechanism integration: dequeueFromQueue ---

func TestDequeueFromQueue_SkipsPaused(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	// Create a pool with a single queue
	pcfg := newDefaultPoolConfig("test-pool", "default")
	p := newPool(pcfg, s)

	// Seed a ready job
	rdb.HSet(ctx, s.rc.Key("job", "j1"),
		"id", "j1", "type", "test", "queue", "default", "status", "ready")
	rdb.LPush(ctx, s.rc.Key("queue", "default", "ready"), "j1")

	// Dequeue should succeed before pause
	jobID, err := p.dequeueFromQueue(ctx, "default")
	if err != nil {
		t.Fatalf("dequeueFromQueue: %v", err)
	}
	if jobID == "" {
		t.Fatal("expected job to be dequeued")
	}

	// Re-seed a job (first one was consumed)
	rdb.HSet(ctx, s.rc.Key("job", "j2"),
		"id", "j2", "type", "test", "queue", "default", "status", "ready")
	rdb.LPush(ctx, s.rc.Key("queue", "default", "ready"), "j2")

	// Pause the queue
	if err := s.PauseQueue(ctx, "default"); err != nil {
		t.Fatalf("PauseQueue: %v", err)
	}

	// Dequeue should return empty (skipped due to pause)
	jobID, err = p.dequeueFromQueue(ctx, "default")
	if err != nil {
		t.Fatalf("dequeueFromQueue after pause: %v", err)
	}
	if jobID != "" {
		t.Errorf("expected empty jobID from paused queue, got %q", jobID)
	}

	// Job should still be in ready list (not consumed)
	readyLen, _ := rdb.LLen(ctx, s.rc.Key("queue", "default", "ready")).Result()
	if readyLen != 1 {
		t.Errorf("ready list len = %d, want 1 (job should remain)", readyLen)
	}

	// Resume the queue
	if err := s.ResumeQueue(ctx, "default"); err != nil {
		t.Fatalf("ResumeQueue: %v", err)
	}

	// Dequeue should succeed again
	jobID, err = p.dequeueFromQueue(ctx, "default")
	if err != nil {
		t.Fatalf("dequeueFromQueue after resume: %v", err)
	}
	if jobID == "" {
		t.Fatal("expected job to be dequeued after resume")
	}
	if jobID != "j2" {
		t.Errorf("jobID = %q, want j2", jobID)
	}
}

func TestDequeue_PausedQueueFallsThrough(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	// Create a pool with two queues: "high" and "low"
	pcfg := newDefaultPoolConfig("test-pool", "high")
	pcfg.queues = []string{"high", "low"}
	p := newPool(pcfg, s)

	// Seed jobs in both queues
	rdb.HSet(ctx, s.rc.Key("job", "h1"),
		"id", "h1", "type", "test", "queue", "high", "status", "ready")
	rdb.LPush(ctx, s.rc.Key("queue", "high", "ready"), "h1")

	rdb.HSet(ctx, s.rc.Key("job", "l1"),
		"id", "l1", "type", "test", "queue", "low", "status", "ready")
	rdb.LPush(ctx, s.rc.Key("queue", "low", "ready"), "l1")

	// Pause the high-priority queue
	if err := s.PauseQueue(ctx, "high"); err != nil {
		t.Fatalf("PauseQueue: %v", err)
	}

	// Dequeue should skip "high" and dequeue from "low"
	jobID, queue, err := p.dequeue(ctx)
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if jobID != "l1" {
		t.Errorf("jobID = %q, want l1", jobID)
	}
	if queue != "low" {
		t.Errorf("queue = %q, want low", queue)
	}

	// "high" job should still be there
	highLen, _ := rdb.LLen(ctx, s.rc.Key("queue", "high", "ready")).Result()
	if highLen != 1 {
		t.Errorf("high queue len = %d, want 1", highLen)
	}
}

func TestCancelJob_Deferred(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	// Create parent and deferred child.
	rdb.HSet(ctx, s.rc.Key("job", "parent-1"),
		"id", "parent-1", "type", "test", "queue", "default", "status", "processing")
	rdb.HSet(ctx, s.rc.Key("job", "child-1"),
		"id", "child-1", "type", "test", "queue", "default", "status", "deferred")
	rdb.SAdd(ctx, s.rc.Key("deferred"), "child-1")
	rdb.SAdd(ctx, s.rc.Key("job", "child-1", "pending_deps"), "parent-1")
	rdb.SAdd(ctx, s.rc.Key("job", "parent-1", "dependents"), "child-1")

	err := s.CancelJob(ctx, "child-1")
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}

	// Verify status changed.
	status, _ := rdb.HGet(ctx, s.rc.Key("job", "child-1"), "status").Result()
	if status != StatusCanceled {
		t.Errorf("status = %q, want canceled", status)
	}
}

func TestCancelJob_NotFound(t *testing.T) {
	s, _ := testAdminServer(t)
	ctx := context.Background()

	err := s.CancelJob(ctx, "nonexistent-cancel")
	if err != ErrJobNotFound {
		t.Errorf("expected ErrJobNotFound, got %v", err)
	}
}

func TestDeleteJob_Completed(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	rdb.HSet(ctx, s.rc.Key("job", "del-comp-1"),
		"id", "del-comp-1", "type", "test", "queue", "default", "status", "completed")
	rdb.ZAdd(ctx, s.rc.Key("queue", "default", "completed"),
		redis.Z{Score: float64(time.Now().Unix()), Member: "del-comp-1"})

	err := s.DeleteJob(ctx, "del-comp-1")
	if err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}

	// Verify deleted.
	exists, _ := rdb.Exists(ctx, s.rc.Key("job", "del-comp-1")).Result()
	if exists != 0 {
		t.Error("job should be deleted")
	}
}

func TestDeleteJob_DeadLetter(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	rdb.HSet(ctx, s.rc.Key("job", "del-dl-1"),
		"id", "del-dl-1", "type", "test", "queue", "default", "status", "dead_letter")
	rdb.ZAdd(ctx, s.rc.Key("queue", "default", "dead_letter"),
		redis.Z{Score: float64(time.Now().Unix()), Member: "del-dl-1"})

	err := s.DeleteJob(ctx, "del-dl-1")
	if err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}
}

func TestRetryJob_EmptyQueue(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	// Job with empty queue field.
	rdb.HSet(ctx, s.rc.Key("job", "retry-empty-q"),
		"id", "retry-empty-q", "type", "test", "queue", "", "status", "dead_letter")
	rdb.ZAdd(ctx, s.rc.Key("queue", "default", "dead_letter"),
		redis.Z{Score: float64(time.Now().Unix()), Member: "retry-empty-q"})

	err := s.RetryJob(ctx, "retry-empty-q")
	if err != nil {
		t.Fatalf("RetryJob: %v", err)
	}
}

func TestTriggerCron_WithTimeoutAndMaxRetry(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	entry := map[string]any{
		"id":        "custom",
		"job_type":  "job.custom",
		"queue":     "special",
		"timeout":   120,
		"max_retry": 5,
		"enabled":   true,
	}
	data, _ := json.Marshal(entry)
	rdb.HSet(ctx, s.rc.Key("cron", "entries"), "custom", string(data))

	jobID, err := s.TriggerCron(ctx, "custom")
	if err != nil {
		t.Fatalf("TriggerCron: %v", err)
	}

	timeout, _ := rdb.HGet(ctx, s.rc.Key("job", jobID), "timeout").Result()
	if timeout != "120" {
		t.Errorf("timeout = %q, want 120", timeout)
	}
	maxRetry, _ := rdb.HGet(ctx, s.rc.Key("job", jobID), "max_retry").Result()
	if maxRetry != "5" {
		t.Errorf("max_retry = %q, want 5", maxRetry)
	}
}

func TestSetCronEnabled_InMemoryUpdate(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	// Register a cron entry in-memory.
	s.cronEntries["mem-test"] = &CronEntry{
		ID:      "mem-test",
		Enabled: true,
	}

	// Also register in Redis.
	entry := map[string]any{"id": "mem-test", "enabled": true}
	data, _ := json.Marshal(entry)
	rdb.HSet(ctx, s.rc.Key("cron", "entries"), "mem-test", string(data))

	if err := s.DisableCron(ctx, "mem-test"); err != nil {
		t.Fatal(err)
	}

	// In-memory entry should be updated.
	if s.cronEntries["mem-test"].Enabled {
		t.Error("in-memory entry should be disabled")
	}
}

func TestDisableCron_NotFound(t *testing.T) {
	s, _ := testAdminServer(t)
	ctx := context.Background()

	err := s.DisableCron(ctx, "nonexistent-disable")
	if err == nil {
		t.Error("expected error for nonexistent cron entry")
	}
}

func TestCancelJob_EmptyQueue(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	rdb.HSet(ctx, s.rc.Key("job", "cancel-emptyq"),
		"id", "cancel-emptyq", "type", "test", "queue", "", "status", "ready")
	rdb.LPush(ctx, s.rc.Key("queue", "default", "ready"), "cancel-emptyq")

	err := s.CancelJob(ctx, "cancel-emptyq")
	if err != nil {
		t.Fatalf("CancelJob: %v", err)
	}
}

func TestDeleteJob_EmptyQueue(t *testing.T) {
	s, rdb := testAdminServer(t)
	ctx := context.Background()

	rdb.HSet(ctx, s.rc.Key("job", "del-emptyq"),
		"id", "del-emptyq", "type", "test", "queue", "", "status", "ready")
	rdb.LPush(ctx, s.rc.Key("queue", "default", "ready"), "del-emptyq")

	err := s.DeleteJob(ctx, "del-emptyq")
	if err != nil {
		t.Fatalf("DeleteJob: %v", err)
	}
}
