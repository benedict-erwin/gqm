package gqm

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// testReaper builds a scheduler engine whose server owns a single pool with the
// given config, which is what reapStale walks to find queues.
func testReaper(t *testing.T, pcfg *poolConfig) (*schedulerEngine, *RedisClient) {
	t.Helper()
	skipWithoutRedis(t)

	prefix := fmt.Sprintf("gqm:test:reap:%d:", time.Now().UnixNano())
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
		cfg:            &serverConfig{globalTimeout: 30 * time.Minute, gracePeriod: defaultGracePeriod},
		rc:             rc,
		scripts:        sr,
		handlers:       make(map[string]Handler),
		handlerConfigs: make(map[string]*handlerConfig),
		jobTypePool:    make(map[string]string),
		poolNames:      make(map[string]bool),
		cronEntries:    make(map[string]*CronEntry),
		logger:         slog.Default(),
	}
	s.pools = []*pool{newPool(pcfg, s)}

	return newSchedulerEngine(s), rc
}

// seedProcessing writes a job hash and places it in the queue's processing set
// with the given deadline, which is exactly the state a worker leaves behind
// when its process dies mid-job.
func seedProcessing(t *testing.T, rc *RedisClient, queue, jobID string, deadline int64, extra map[string]any) {
	t.Helper()
	ctx := context.Background()

	fields := map[string]any{
		"id":         jobID,
		"type":       "test.job",
		"queue":      queue,
		"payload":    "{}",
		"status":     StatusProcessing,
		"created_at": time.Now().Unix() - 600,
		"started_at": time.Now().Unix() - 600,
	}
	for k, v := range extra {
		fields[k] = v
	}

	if err := rc.rdb.HSet(ctx, rc.Key("job", jobID), fields).Err(); err != nil {
		t.Fatalf("seeding job hash: %v", err)
	}
	if err := rc.rdb.ZAdd(ctx, rc.Key("queue", queue, "processing"),
		redis.Z{Score: float64(deadline), Member: jobID}).Err(); err != nil {
		t.Fatalf("seeding processing set: %v", err)
	}
}

func TestReapStale_RetriesWhenRetriesRemain(t *testing.T) {
	tests := []struct {
		name           string
		fields         map[string]any
		wantRetryCount string
	}{
		{
			name:           "first attempt",
			fields:         map[string]any{"max_retry": 3},
			wantRetryCount: "1",
		},
		{
			name:           "mid way through the budget",
			fields:         map[string]any{"max_retry": 3, "retry_count": 1},
			wantRetryCount: "2",
		},
		{
			name:           "last allowed attempt",
			fields:         map[string]any{"max_retry": 3, "retry_count": 2},
			wantRetryCount: "3",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			se, rc := testReaper(t, newDefaultPoolConfig("p", "q"))
			ctx := context.Background()

			now := time.Now()
			seedProcessing(t, rc, "q", "zombie", now.Add(-time.Hour).Unix(), tt.fields)

			se.reapStale(ctx, now)

			data := rc.rdb.HGetAll(ctx, rc.Key("job", "zombie")).Val()
			if data["status"] != StatusRetry {
				t.Errorf("status = %q, want %q", data["status"], StatusRetry)
			}
			if data["retry_count"] != tt.wantRetryCount {
				t.Errorf("retry_count = %q, want %q", data["retry_count"], tt.wantRetryCount)
			}
			if data["error"] != reapErrorMessage {
				t.Errorf("error = %q, want %q", data["error"], reapErrorMessage)
			}

			if n := rc.rdb.ZCard(ctx, rc.Key("queue", "q", "processing")).Val(); n != 0 {
				t.Errorf("processing set card = %d, want 0", n)
			}
			score, err := rc.rdb.ZScore(ctx, rc.Key("scheduled"), "zombie").Result()
			if err != nil {
				t.Fatalf("job not in scheduled set: %v", err)
			}
			wantAt := now.Add(defaultRetryDelay).Unix()
			if delta := int64(score) - wantAt; delta < -2 || delta > 2 {
				t.Errorf("retry_at = %d, want ~%d", int64(score), wantAt)
			}
		})
	}
}

func TestReapStale_DeadLettersWhenRetriesExhausted(t *testing.T) {
	tests := []struct {
		name   string
		fields map[string]any
	}{
		{name: "budget spent", fields: map[string]any{"max_retry": 2, "retry_count": 2}},
		{name: "retry disabled", fields: map[string]any{"max_retry": 0}},
		{name: "counter beyond budget", fields: map[string]any{"max_retry": 1, "retry_count": 5}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			se, rc := testReaper(t, newDefaultPoolConfig("p", "q"))
			ctx := context.Background()

			now := time.Now()
			seedProcessing(t, rc, "q", "zombie", now.Add(-time.Hour).Unix(), tt.fields)

			se.reapStale(ctx, now)

			data := rc.rdb.HGetAll(ctx, rc.Key("job", "zombie")).Val()
			if data["status"] != StatusDeadLetter {
				t.Errorf("status = %q, want %q", data["status"], StatusDeadLetter)
			}
			if data["error"] != reapErrorMessage {
				t.Errorf("error = %q, want %q", data["error"], reapErrorMessage)
			}

			if n := rc.rdb.ZCard(ctx, rc.Key("queue", "q", "processing")).Val(); n != 0 {
				t.Errorf("processing set card = %d, want 0", n)
			}
			if _, err := rc.rdb.ZScore(ctx, rc.Key("queue", "q", "dead_letter"), "zombie").Result(); err != nil {
				t.Fatalf("job not in dead letter queue: %v", err)
			}

			date := time.Now().UTC().Format("2006-01-02")
			if v := rc.rdb.Get(ctx, rc.Key("stats", "q", "failed", date)).Val(); v != "1" {
				t.Errorf("daily failed stat = %q, want 1", v)
			}
			if v := rc.rdb.Get(ctx, rc.Key("stats", "q", "failed_total")).Val(); v != "1" {
				t.Errorf("total failed stat = %q, want 1", v)
			}
		})
	}
}

func TestReapStale_HonorsPoolRetryPolicy(t *testing.T) {
	pcfg := newDefaultPoolConfig("p", "q")
	pcfg.retryPolicy = &RetryPolicy{MaxRetry: 2, Backoff: BackoffFixed, BackoffBase: 90 * time.Second}

	se, rc := testReaper(t, pcfg)
	ctx := context.Background()

	// No job-level max_retry: the pool policy has to supply both the budget and
	// the delay, exactly as it does for a live worker's failure path.
	now := time.Now()
	seedProcessing(t, rc, "q", "zombie", now.Add(-time.Hour).Unix(), nil)

	se.reapStale(ctx, now)

	if status := rc.rdb.HGet(ctx, rc.Key("job", "zombie"), "status").Val(); status != StatusRetry {
		t.Fatalf("status = %q, want %q", status, StatusRetry)
	}
	score, err := rc.rdb.ZScore(ctx, rc.Key("scheduled"), "zombie").Result()
	if err != nil {
		t.Fatalf("job not in scheduled set: %v", err)
	}
	wantAt := now.Add(90 * time.Second).Unix()
	if delta := int64(score) - wantAt; delta < -2 || delta > 2 {
		t.Errorf("retry_at = %d, want ~%d", int64(score), wantAt)
	}
}

func TestReapStale_LeavesLiveClaims(t *testing.T) {
	tests := []struct {
		name       string
		deadlineIn time.Duration
	}{
		{name: "deadline still ahead", deadlineIn: 10 * time.Minute},
		{name: "just past the deadline", deadlineIn: -1 * time.Second},
		{name: "inside the grace period", deadlineIn: -(defaultGracePeriod - time.Second)},
		{name: "inside the reaper slack", deadlineIn: -(defaultGracePeriod + reaperSlack - 2*time.Second)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			se, rc := testReaper(t, newDefaultPoolConfig("p", "q"))
			ctx := context.Background()

			now := time.Now()
			seedProcessing(t, rc, "q", "live", now.Add(tt.deadlineIn).Unix(),
				map[string]any{"max_retry": 3})

			se.reapStale(ctx, now)

			if status := rc.rdb.HGet(ctx, rc.Key("job", "live"), "status").Val(); status != StatusProcessing {
				t.Errorf("status = %q, want %q", status, StatusProcessing)
			}
			if n := rc.rdb.ZCard(ctx, rc.Key("queue", "q", "processing")).Val(); n != 1 {
				t.Errorf("processing set card = %d, want 1", n)
			}
			if n := rc.rdb.ZCard(ctx, rc.Key("scheduled")).Val(); n != 0 {
				t.Errorf("scheduled set card = %d, want 0", n)
			}
		})
	}
}

func TestReapStale_ScansEveryPoolQueue(t *testing.T) {
	se, rc := testReaper(t, newDefaultPoolConfig("p", "high", "low"))
	ctx := context.Background()

	now := time.Now()
	seedProcessing(t, rc, "high", "zombie-high", now.Add(-time.Hour).Unix(), map[string]any{"max_retry": 3})
	seedProcessing(t, rc, "low", "zombie-low", now.Add(-time.Hour).Unix(), map[string]any{"max_retry": 3})

	se.reapStale(ctx, now)

	for _, id := range []string{"zombie-high", "zombie-low"} {
		if status := rc.rdb.HGet(ctx, rc.Key("job", id), "status").Val(); status != StatusRetry {
			t.Errorf("%s status = %q, want %q", id, status, StatusRetry)
		}
	}
}

func TestReapStale_SharedQueueUsesLongestGracePeriod(t *testing.T) {
	impatient := newDefaultPoolConfig("impatient", "q")
	impatient.gracePeriod = time.Second
	patient := newDefaultPoolConfig("patient", "q")
	patient.gracePeriod = time.Hour

	se, rc := testReaper(t, impatient)
	se.server.pools = append(se.server.pools, newPool(patient, se.server))
	ctx := context.Background()

	// Past the impatient pool's margin, well inside the patient one's: a worker
	// of the patient pool could still be finishing this job.
	now := time.Now()
	seedProcessing(t, rc, "q", "slow", now.Add(-time.Minute).Unix(), map[string]any{"max_retry": 3})

	se.reapStale(ctx, now)

	if status := rc.rdb.HGet(ctx, rc.Key("job", "slow"), "status").Val(); status != StatusProcessing {
		t.Errorf("status = %q, want %q", status, StatusProcessing)
	}
}

func TestReapStale_RemovesOrphanEntry(t *testing.T) {
	se, rc := testReaper(t, newDefaultPoolConfig("p", "q"))
	ctx := context.Background()

	// Processing entry whose job hash is gone. Nothing can ever act on it, and
	// leaving it in place means rescanning it on every tick forever.
	now := time.Now()
	rc.rdb.ZAdd(ctx, rc.Key("queue", "q", "processing"),
		redis.Z{Score: float64(now.Add(-time.Hour).Unix()), Member: "orphan"})

	se.reapStale(ctx, now)

	if n := rc.rdb.ZCard(ctx, rc.Key("queue", "q", "processing")).Val(); n != 0 {
		t.Errorf("processing set card = %d, want 0", n)
	}
}

func TestReapStale_PropagatesFailureToDependents(t *testing.T) {
	se, rc := testReaper(t, newDefaultPoolConfig("p", "q"))
	ctx := context.Background()

	now := time.Now()
	seedProcessing(t, rc, "q", "parent", now.Add(-time.Hour).Unix(), map[string]any{"max_retry": 0})

	rc.rdb.HSet(ctx, rc.Key("job", "child"),
		"id", "child", "type", "test.job", "queue", "q", "status", StatusDeferred)
	rc.rdb.SAdd(ctx, rc.Key("deferred"), "child")
	rc.rdb.SAdd(ctx, rc.Key("job", "child", "pending_deps"), "parent")
	rc.rdb.SAdd(ctx, rc.Key("job", "parent", "dependents"), "child")

	se.reapStale(ctx, now)

	if status := rc.rdb.HGet(ctx, rc.Key("job", "parent"), "status").Val(); status != StatusDeadLetter {
		t.Fatalf("parent status = %q, want %q", status, StatusDeadLetter)
	}
	if status := rc.rdb.HGet(ctx, rc.Key("job", "child"), "status").Val(); status != StatusCanceled {
		t.Errorf("child status = %q, want %q", status, StatusCanceled)
	}
}

// TestReapScript_SkipsReclaimedJob covers the window the plain ZREM guard used
// by retry.lua and deadletter.lua cannot see: between the reaper's scan and its
// script call the job may have completed, been re-enqueued and re-claimed by
// another worker. Reaping it then would abandon a claim that is very much
// alive, running the job twice.
func TestReapScript_SkipsReclaimedJob(t *testing.T) {
	se, rc := testReaper(t, newDefaultPoolConfig("p", "q"))
	ctx := context.Background()

	now := time.Now()
	threshold := now.Add(-(defaultGracePeriod + reaperSlack)).Unix()

	// Fresh claim: its deadline sits above the threshold the reaper scanned with.
	seedProcessing(t, rc, "q", "reclaimed", now.Add(time.Hour).Unix(), map[string]any{"max_retry": 3})

	result := se.server.scripts.run(ctx, rc.rdb, "reap",
		[]string{
			rc.Key("queue", "q", "processing"),
			rc.Key("scheduled"),
			rc.Key("job", "reclaimed"),
			rc.Key("queue", "q", "dead_letter"),
			rc.Key("job", "reclaimed", "dependents"),
			rc.Key("stats", "q", "failed", now.UTC().Format("2006-01-02")),
			rc.Key("stats", "q", "failed_total"),
		},
		"reclaimed", threshold, "retry", now.Unix(), reapErrorMessage,
		now.Add(defaultRetryDelay).Unix(), 1, 3600, defaultFailureTTLSeconds,
	)
	if result.Err() != nil {
		t.Fatalf("reap script: %v", result.Err())
	}
	if val, _ := result.Int64(); val != 0 {
		t.Errorf("reap returned %d, want 0 (skipped)", val)
	}

	if status := rc.rdb.HGet(ctx, rc.Key("job", "reclaimed"), "status").Val(); status != StatusProcessing {
		t.Errorf("status = %q, want %q", status, StatusProcessing)
	}
	if n := rc.rdb.ZCard(ctx, rc.Key("queue", "q", "processing")).Val(); n != 1 {
		t.Errorf("processing set card = %d, want 1", n)
	}
}

func TestReapScript_SkipsJobAlreadyOutOfProcessing(t *testing.T) {
	se, rc := testReaper(t, newDefaultPoolConfig("p", "q"))
	ctx := context.Background()

	now := time.Now()
	rc.rdb.HSet(ctx, rc.Key("job", "gone"), "id", "gone", "queue", "q", "status", StatusCompleted)

	result := se.server.scripts.run(ctx, rc.rdb, "reap",
		[]string{
			rc.Key("queue", "q", "processing"),
			rc.Key("scheduled"),
			rc.Key("job", "gone"),
			rc.Key("queue", "q", "dead_letter"),
			rc.Key("job", "gone", "dependents"),
			rc.Key("stats", "q", "failed", now.UTC().Format("2006-01-02")),
			rc.Key("stats", "q", "failed_total"),
		},
		"gone", now.Unix(), "deadletter", now.Unix(), reapErrorMessage,
		now.Unix(), 1, 3600, defaultFailureTTLSeconds,
	)
	if result.Err() != nil {
		t.Fatalf("reap script: %v", result.Err())
	}
	if val, _ := result.Int64(); val != 0 {
		t.Errorf("reap returned %d, want 0 (skipped)", val)
	}
	if status := rc.rdb.HGet(ctx, rc.Key("job", "gone"), "status").Val(); status != StatusCompleted {
		t.Errorf("status = %q, want %q", status, StatusCompleted)
	}
}

func TestReapStale_NoPoolsIsNoop(t *testing.T) {
	se, rc := testReaper(t, newDefaultPoolConfig("p", "q"))
	se.server.pools = nil
	ctx := context.Background()

	now := time.Now()
	seedProcessing(t, rc, "q", "zombie", now.Add(-time.Hour).Unix(), map[string]any{"max_retry": 3})

	se.reapStale(ctx, now)

	if status := rc.rdb.HGet(ctx, rc.Key("job", "zombie"), "status").Val(); status != StatusProcessing {
		t.Errorf("status = %q, want %q", status, StatusProcessing)
	}
}
