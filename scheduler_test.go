package gqm

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"
)

func testSchedulerEngine(t *testing.T) (*schedulerEngine, *RedisClient, string) {
	t.Helper()
	skipWithoutRedis(t)

	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	rc, err := NewRedisClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("creating redis client: %v", err)
	}
	t.Cleanup(func() {
		cleanupRedis(t, prefix)
		rc.Close()
	})

	sr := newScriptRegistry()
	if err := sr.load(); err != nil {
		t.Fatalf("loading scripts: %v", err)
	}

	s := &Server{
		cfg:         &serverConfig{globalTimeout: 30 * time.Minute, gracePeriod: 10 * time.Second},
		rc:          rc,
		scripts:     sr,
		handlers:    make(map[string]Handler),
		jobTypePool: make(map[string]string),
		poolNames:   make(map[string]bool),
		cronEntries: make(map[string]*CronEntry),
		logger:      slog.Default(),
	}

	se := newSchedulerEngine(s)
	return se, rc, prefix
}

func TestCheckOverlap_Allow(t *testing.T) {
	se, _, _ := testSchedulerEngine(t)

	entry := &CronEntry{
		ID:            "test-allow",
		OverlapPolicy: OverlapAllow,
	}

	// Allow policy always returns true, regardless of previous job state.
	if !se.checkOverlap(context.Background(), entry) {
		t.Error("OverlapAllow should always return true")
	}
}

func TestCheckOverlap_Skip_NoPrevious(t *testing.T) {
	se, _, _ := testSchedulerEngine(t)

	entry := &CronEntry{
		ID:            "test-skip-none",
		OverlapPolicy: OverlapSkip,
	}

	// No previous job tracked — should allow enqueue.
	if !se.checkOverlap(context.Background(), entry) {
		t.Error("OverlapSkip with no previous job should return true")
	}
}

func TestCheckOverlap_Skip_PreviousCompleted(t *testing.T) {
	se, rc, _ := testSchedulerEngine(t)
	ctx := context.Background()

	entry := &CronEntry{
		ID:            "test-skip-done",
		OverlapPolicy: OverlapSkip,
	}

	// Create a completed previous job and track it.
	jobID := NewUUID()
	jobKey := rc.Key("job", jobID)
	currentKey := rc.Key("cron", "current", entry.ID)
	rc.rdb.HSet(ctx, jobKey, "status", StatusCompleted)
	rc.rdb.Set(ctx, currentKey, jobID, 0)

	if !se.checkOverlap(ctx, entry) {
		t.Error("OverlapSkip with completed previous job should return true")
	}
}

func TestCheckOverlap_Skip_PreviousActive(t *testing.T) {
	se, rc, _ := testSchedulerEngine(t)
	ctx := context.Background()

	activeStatuses := []string{StatusReady, StatusScheduled, StatusProcessing, StatusRetry}

	for _, status := range activeStatuses {
		t.Run(status, func(t *testing.T) {
			entry := &CronEntry{
				ID:            fmt.Sprintf("test-skip-%s", status),
				OverlapPolicy: OverlapSkip,
			}

			jobID := NewUUID()
			jobKey := rc.Key("job", jobID)
			currentKey := rc.Key("cron", "current", entry.ID)
			rc.rdb.HSet(ctx, jobKey, "status", status)
			rc.rdb.Set(ctx, currentKey, jobID, 0)

			if se.checkOverlap(ctx, entry) {
				t.Errorf("OverlapSkip with %s previous job should return false", status)
			}
		})
	}
}

func TestCheckOverlap_Skip_PreviousTerminal(t *testing.T) {
	se, rc, _ := testSchedulerEngine(t)
	ctx := context.Background()

	terminalStatuses := []string{StatusCompleted, StatusFailed, StatusDeadLetter, StatusStopped}

	for _, status := range terminalStatuses {
		t.Run(status, func(t *testing.T) {
			entry := &CronEntry{
				ID:            fmt.Sprintf("test-skip-term-%s", status),
				OverlapPolicy: OverlapSkip,
			}

			jobID := NewUUID()
			jobKey := rc.Key("job", jobID)
			currentKey := rc.Key("cron", "current", entry.ID)
			rc.rdb.HSet(ctx, jobKey, "status", status)
			rc.rdb.Set(ctx, currentKey, jobID, 0)

			if !se.checkOverlap(ctx, entry) {
				t.Errorf("OverlapSkip with %s previous job should return true", status)
			}
		})
	}
}

func TestCheckOverlap_Skip_PreviousJobDeleted(t *testing.T) {
	se, rc, _ := testSchedulerEngine(t)
	ctx := context.Background()

	entry := &CronEntry{
		ID:            "test-skip-deleted",
		OverlapPolicy: OverlapSkip,
	}

	// Track a job ID but don't create the job hash — simulates expired/deleted job.
	currentKey := rc.Key("cron", "current", entry.ID)
	rc.rdb.Set(ctx, currentKey, "nonexistent-job-id", 0)

	if !se.checkOverlap(ctx, entry) {
		t.Error("OverlapSkip with deleted previous job should return true")
	}
}

func TestCheckOverlap_Replace_PreviousActive(t *testing.T) {
	se, rc, _ := testSchedulerEngine(t)
	ctx := context.Background()

	entry := &CronEntry{
		ID:            "test-replace",
		OverlapPolicy: OverlapReplace,
	}

	// Create an active previous job.
	jobID := NewUUID()
	jobKey := rc.Key("job", jobID)
	currentKey := rc.Key("cron", "current", entry.ID)
	rc.rdb.HSet(ctx, jobKey, "status", StatusProcessing)
	rc.rdb.Set(ctx, currentKey, jobID, 0)

	if !se.checkOverlap(ctx, entry) {
		t.Error("OverlapReplace should return true (cancel previous, enqueue new)")
	}

	// Verify previous job was cancelled.
	status, err := rc.rdb.HGet(ctx, jobKey, "status").Result()
	if err != nil {
		t.Fatalf("getting job status: %v", err)
	}
	if status != StatusStopped {
		t.Errorf("previous job status = %q, want %q", status, StatusStopped)
	}
}

func TestCheckOverlap_Replace_NoPrevious(t *testing.T) {
	se, _, _ := testSchedulerEngine(t)

	entry := &CronEntry{
		ID:            "test-replace-none",
		OverlapPolicy: OverlapReplace,
	}

	// No previous job — should allow enqueue without error.
	if !se.checkOverlap(context.Background(), entry) {
		t.Error("OverlapReplace with no previous job should return true")
	}
}

func TestEnqueueCronJob_TracksCurrentJob(t *testing.T) {
	se, rc, _ := testSchedulerEngine(t)
	ctx := context.Background()

	expr, err := ParseCronExpr("* * * * *")
	if err != nil {
		t.Fatalf("parsing cron expr: %v", err)
	}

	entry := &CronEntry{
		ID:            "test-track",
		JobType:       "test.job",
		Queue:         "default",
		OverlapPolicy: OverlapAllow,
		Enabled:       true,
		expr:          expr,
	}

	now := time.Now()
	se.enqueueCronJob(ctx, entry, now)

	// Verify current job is tracked.
	currentKey := rc.Key("cron", "current", entry.ID)
	jobID, err := rc.rdb.Get(ctx, currentKey).Result()
	if err != nil {
		t.Fatalf("getting current job: %v", err)
	}
	if jobID == "" {
		t.Error("current job ID should not be empty")
	}

	// Verify the job exists in Redis.
	jobKey := rc.Key("job", jobID)
	status, err := rc.rdb.HGet(ctx, jobKey, "status").Result()
	if err != nil {
		t.Fatalf("getting job status: %v", err)
	}
	if status != StatusReady {
		t.Errorf("job status = %q, want %q", status, StatusReady)
	}
}

func TestEnqueueCronJob_SkipUpdatesSchedule(t *testing.T) {
	se, rc, _ := testSchedulerEngine(t)
	ctx := context.Background()

	expr, err := ParseCronExpr("* * * * *")
	if err != nil {
		t.Fatalf("parsing cron expr: %v", err)
	}

	entry := &CronEntry{
		ID:            "test-skip-advance",
		JobType:       "test.job",
		Queue:         "default",
		OverlapPolicy: OverlapSkip,
		Enabled:       true,
		expr:          expr,
	}

	// Create an active previous job so skip triggers.
	jobID := NewUUID()
	jobKey := rc.Key("job", jobID)
	currentKey := rc.Key("cron", "current", entry.ID)
	rc.rdb.HSet(ctx, jobKey, "status", StatusProcessing)
	rc.rdb.Set(ctx, currentKey, jobID, 0)

	now := time.Now()
	se.enqueueCronJob(ctx, entry, now)

	// Verify schedule was advanced despite skip.
	if entry.LastStatus != "skipped" {
		t.Errorf("LastStatus = %q, want %q", entry.LastStatus, "skipped")
	}
	if entry.LastRun == 0 {
		t.Error("LastRun should be set after skip")
	}
	if entry.NextRun == 0 {
		t.Error("NextRun should be set after skip")
	}
}

func TestCronLock_Contention(t *testing.T) {
	skipWithoutRedis(t)

	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())

	// Create two Redis clients sharing the same prefix so they access the same keys.
	rc1, err := NewRedisClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("creating redis client 1: %v", err)
	}
	rc2, err := NewRedisClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("creating redis client 2: %v", err)
	}
	t.Cleanup(func() {
		cleanupRedis(t, prefix)
		rc1.Close()
		rc2.Close()
	})

	sr := newScriptRegistry()
	if err := sr.load(); err != nil {
		t.Fatalf("loading scripts: %v", err)
	}

	// Build two Server structs with distinct serverIDs.
	s1 := &Server{
		cfg:            &serverConfig{globalTimeout: 30 * time.Minute, gracePeriod: 10 * time.Second},
		rc:             rc1,
		scripts:        sr,
		handlers:       make(map[string]Handler),
		handlerConfigs: make(map[string]*handlerConfig),
		jobTypePool:    make(map[string]string),
		poolNames:      make(map[string]bool),
		cronEntries:    make(map[string]*CronEntry),
		logger:         slog.Default(),
		serverID:       "instance-1",
	}
	s2 := &Server{
		cfg:            &serverConfig{globalTimeout: 30 * time.Minute, gracePeriod: 10 * time.Second},
		rc:             rc2,
		scripts:        sr,
		handlers:       make(map[string]Handler),
		handlerConfigs: make(map[string]*handlerConfig),
		jobTypePool:    make(map[string]string),
		poolNames:      make(map[string]bool),
		cronEntries:    make(map[string]*CronEntry),
		logger:         slog.Default(),
		serverID:       "instance-2",
	}

	se1 := newSchedulerEngine(s1)
	se2 := newSchedulerEngine(s2)

	ctx := context.Background()
	entryID := "test-contention"

	// Both engines try to acquire the same cron lock.
	got1 := se1.acquireCronLock(ctx, entryID)
	got2 := se2.acquireCronLock(ctx, entryID)

	// Exactly one should succeed.
	if got1 == got2 {
		t.Fatalf("both engines returned %v; exactly one should acquire the lock", got1)
	}

	// Identify winner and loser.
	var winner, loser *schedulerEngine
	if got1 {
		winner = se1
		loser = se2
	} else {
		winner = se2
		loser = se1
	}

	// Loser cannot acquire the lock while it is held.
	if loser.acquireCronLock(ctx, entryID) {
		t.Error("loser acquired the lock while it is still held by winner")
	}

	// Winner releases the lock.
	winner.releaseCronLock(ctx, entryID)

	// After release, the loser should be able to acquire it.
	if !loser.acquireCronLock(ctx, entryID) {
		t.Error("loser should acquire the lock after winner releases it")
	}

	// Clean up: release the lock held by loser.
	loser.releaseCronLock(ctx, entryID)
}
