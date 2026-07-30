package gqm

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// rawTTL reads a key's TTL as the integer Redis itself reports: a positive
// number of seconds, -1 when the key exists without an expiry, or -2 when the
// key does not exist. Going through Do avoids go-redis's duration wrapping,
// which blurs those two negative cases together.
func rawTTL(t *testing.T, rc *RedisClient, key string) int64 {
	t.Helper()
	ttl, err := rc.rdb.Do(context.Background(), "TTL", key).Int64()
	if err != nil {
		t.Fatalf("TTL %s: %v", key, err)
	}
	return ttl
}

// retentionFixture wires a server and a client onto a private key prefix and
// returns them along with the prefix, ready for a retention assertion.
type retentionFixture struct {
	server *Server
	client *Client
	rc     *RedisClient
	prefix string
}

func newRetentionFixture(t *testing.T, opts ...ServerOption) *retentionFixture {
	t.Helper()
	skipWithoutRedis(t)

	prefix := fmt.Sprintf("gqm:test:retention:%d:", time.Now().UnixNano())
	t.Cleanup(func() { cleanupRedis(t, prefix) })

	base := []ServerOption{
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(10 * time.Second),
		WithGracePeriod(time.Second),
		WithShutdownTimeout(5 * time.Second),
	}
	server, err := NewServer(append(base, opts...)...)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { client.Close() })

	// A probe client of its own: server shutdown closes the server's Redis
	// client, and assertions run after shutdown.
	probe, err := NewRedisClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewRedisClient: %v", err)
	}
	t.Cleanup(func() { probe.Close() })

	return &retentionFixture{server: server, client: client, rc: probe, prefix: prefix}
}

// runUntil starts the server and blocks until cond returns true or the deadline
// passes, then shuts the server down.
func (f *retentionFixture) runUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.server.Start(ctx) }()

	deadline := time.After(timeout)
	for !cond() {
		select {
		case <-deadline:
			cancel()
			<-done
			t.Fatal("timeout waiting for job to reach its terminal state")
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Let the terminal Lua script finish writing before assertions read TTLs.
	time.Sleep(200 * time.Millisecond)
	cancel()
	<-done
}

func (f *retentionFixture) jobKey(jobID string) string {
	return f.rc.Key("job", jobID)
}

func (f *retentionFixture) statusOf(t *testing.T, jobID string) string {
	t.Helper()
	status, err := f.rc.rdb.HGet(context.Background(), f.jobKey(jobID), "status").Result()
	if err != nil {
		t.Fatalf("HGet status: %v", err)
	}
	return status
}

// assertTTLNear checks a TTL is within a tolerance of the expected window.
// Exactness is impossible: seconds elapse between the EXPIRE and the read.
func assertTTLNear(t *testing.T, got int64, want int64) {
	t.Helper()
	const tolerance = 60
	if got < want-tolerance || got > want {
		t.Errorf("TTL = %d, want within %d..%d", got, want-tolerance, want)
	}
}

func TestRetention_CompletedJobGetsResultTTL(t *testing.T) {
	f := newRetentionFixture(t, WithResultTTL(2*time.Hour))

	processed := make(chan struct{}, 1)
	if err := f.server.Handle("ttl.ok", func(ctx context.Context, job *Job) error {
		processed <- struct{}{}
		return nil
	}, Workers(1)); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	job, err := f.client.Enqueue(context.Background(), "ttl.ok", nil, Queue("ttl.ok"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	f.runUntil(t, 10*time.Second, func() bool {
		select {
		case <-processed:
			return true
		default:
			return false
		}
	})

	if status := f.statusOf(t, job.ID); status != StatusCompleted {
		t.Fatalf("status = %q, want %q", status, StatusCompleted)
	}
	assertTTLNear(t, rawTTL(t, f.rc, f.jobKey(job.ID)), 7200)
}

func TestRetention_NonTerminalJobHasNoTTL(t *testing.T) {
	// The single most important assertion in the retention change: a job that
	// has not reached a terminal state must never carry an expiry. If it did,
	// retention would silently turn into work loss.
	f := newRetentionFixture(t, WithResultTTL(time.Hour), WithFailureTTL(time.Hour))

	ctx := context.Background()
	ready, err := f.client.Enqueue(ctx, "ttl.pending", nil, Queue("ttl.pending"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	scheduled, err := f.client.EnqueueIn(ctx, time.Hour, "ttl.pending", nil, Queue("ttl.pending"))
	if err != nil {
		t.Fatalf("EnqueueIn: %v", err)
	}

	for _, job := range []*Job{ready, scheduled} {
		if ttl := rawTTL(t, f.rc, f.jobKey(job.ID)); ttl != -1 {
			t.Errorf("job %s (status %s) TTL = %d, want -1 (no expiry)",
				job.ID, f.statusOf(t, job.ID), ttl)
		}
	}
}

func TestRetention_DeadLetterJobGetsFailureTTL(t *testing.T) {
	f := newRetentionFixture(t, WithFailureTTL(3*time.Hour))

	if err := f.server.Handle("ttl.fail", func(ctx context.Context, job *Job) error {
		return errors.New("always fails")
	}, Workers(1)); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	job, err := f.client.Enqueue(context.Background(), "ttl.fail", nil,
		Queue("ttl.fail"), MaxRetry(0))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	f.runUntil(t, 15*time.Second, func() bool {
		status, err := f.rc.rdb.HGet(context.Background(), f.jobKey(job.ID), "status").Result()
		return err == nil && status == StatusDeadLetter
	})

	assertTTLNear(t, rawTTL(t, f.rc, f.jobKey(job.ID)), 10800)
}

func TestRetention_RetriedDeadLetterJobLosesTTL(t *testing.T) {
	// Guard for the inherited-TTL trap: HSET does not clear an expiry, so
	// resurrecting a dead-lettered job without PERSIST would leave it counting
	// down to deletion while queued or running.
	f := newRetentionFixture(t, WithFailureTTL(2*time.Hour))

	if err := f.server.Handle("ttl.revive", func(ctx context.Context, job *Job) error {
		return errors.New("always fails")
	}, Workers(1)); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	ctx := context.Background()
	job, err := f.client.Enqueue(ctx, "ttl.revive", nil, Queue("ttl.revive"), MaxRetry(0))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	f.runUntil(t, 15*time.Second, func() bool {
		status, err := f.rc.rdb.HGet(ctx, f.jobKey(job.ID), "status").Result()
		return err == nil && status == StatusDeadLetter
	})

	// Precondition: the dead-lettered job really is on a countdown.
	if ttl := rawTTL(t, f.rc, f.jobKey(job.ID)); ttl <= 0 {
		t.Fatalf("dead-letter job TTL = %d, want a positive countdown before retry", ttl)
	}

	// Retry through a second server that runs no pools: the worker server has
	// shut down (which closed its Redis client), and keeping it alive would let
	// its worker immediately re-process and re-dead-letter the job, putting the
	// expiry back before the assertion could read it.
	admin, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(f.prefix)),
	)
	if err != nil {
		t.Fatalf("NewServer (admin): %v", err)
	}
	defer admin.rc.Close()

	if err := admin.RetryJob(ctx, job.ID); err != nil {
		t.Fatalf("RetryJob: %v", err)
	}

	if ttl := rawTTL(t, f.rc, f.jobKey(job.ID)); ttl != -1 {
		t.Errorf("retried job TTL = %d, want -1; a requeued job must not expire", ttl)
	}
	if status := f.statusOf(t, job.ID); status != StatusReady {
		t.Errorf("status after retry = %q, want %q", status, StatusReady)
	}
}

func TestRetention_PermanentLeavesNoTTL(t *testing.T) {
	f := newRetentionFixture(t, WithResultTTL(TTLPermanent))

	processed := make(chan struct{}, 1)
	if err := f.server.Handle("ttl.forever", func(ctx context.Context, job *Job) error {
		processed <- struct{}{}
		return nil
	}, Workers(1)); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	job, err := f.client.Enqueue(context.Background(), "ttl.forever", nil, Queue("ttl.forever"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	f.runUntil(t, 10*time.Second, func() bool {
		select {
		case <-processed:
			return true
		default:
			return false
		}
	})

	if ttl := rawTTL(t, f.rc, f.jobKey(job.ID)); ttl != -1 {
		t.Errorf("TTL = %d, want -1 (retained permanently)", ttl)
	}
}

func TestRetention_ZeroDeletesRecordAndSkipsZsetEntry(t *testing.T) {
	f := newRetentionFixture(t, WithResultTTL(0))

	processed := make(chan struct{}, 1)
	if err := f.server.Handle("ttl.gone", func(ctx context.Context, job *Job) error {
		processed <- struct{}{}
		return nil
	}, Workers(1)); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	job, err := f.client.Enqueue(context.Background(), "ttl.gone", nil, Queue("ttl.gone"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	f.runUntil(t, 10*time.Second, func() bool {
		select {
		case <-processed:
			return true
		default:
			return false
		}
	})

	ctx := context.Background()
	if ttl := rawTTL(t, f.rc, f.jobKey(job.ID)); ttl != -2 {
		t.Errorf("TTL = %d, want -2 (key deleted)", ttl)
	}

	// The zset entry must go too, otherwise the dashboard lists a job whose
	// details no longer exist.
	score := f.rc.rdb.ZScore(ctx, f.rc.Key("queue", "ttl.gone", "completed"), job.ID)
	if score.Err() == nil {
		t.Error("completed zset still holds the job, but its hash was deleted")
	}
}

func TestRetention_PerJobOverrideWinsOverServerDefault(t *testing.T) {
	f := newRetentionFixture(t, WithResultTTL(30*24*time.Hour))

	processed := make(chan struct{}, 1)
	if err := f.server.Handle("ttl.override", func(ctx context.Context, job *Job) error {
		processed <- struct{}{}
		return nil
	}, Workers(1)); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	job, err := f.client.Enqueue(context.Background(), "ttl.override", nil,
		Queue("ttl.override"), ResultTTL(time.Hour))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	f.runUntil(t, 10*time.Second, func() bool {
		select {
		case <-processed:
			return true
		default:
			return false
		}
	})

	assertTTLNear(t, rawTTL(t, f.rc, f.jobKey(job.ID)), 3600)
}

func TestRetention_CanceledJobGetsFailureTTL(t *testing.T) {
	f := newRetentionFixture(t, WithFailureTTL(4*time.Hour))

	// No pool is started: the job stays in ready so it can be canceled.
	ctx := context.Background()
	job, err := f.client.Enqueue(ctx, "ttl.cancel", nil, Queue("ttl.cancel"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if ttl := rawTTL(t, f.rc, f.jobKey(job.ID)); ttl != -1 {
		t.Fatalf("ready job TTL = %d, want -1 before cancel", ttl)
	}

	if err := f.server.CancelJob(ctx, job.ID); err != nil {
		t.Fatalf("CancelJob: %v", err)
	}

	if status := f.statusOf(t, job.ID); status != StatusCanceled {
		t.Fatalf("status = %q, want %q", status, StatusCanceled)
	}
	assertTTLNear(t, rawTTL(t, f.rc, f.jobKey(job.ID)), 14400)
}

func TestRetention_EmptyQueueCanceledJobsGetTTL(t *testing.T) {
	f := newRetentionFixture(t, WithFailureTTL(2*time.Hour))

	ctx := context.Background()
	var ids []string
	for i := 0; i < 3; i++ {
		job, err := f.client.Enqueue(ctx, "ttl.empty", nil, Queue("ttl.empty"))
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		ids = append(ids, job.ID)
	}

	n, err := f.server.EmptyQueue(ctx, "ttl.empty")
	if err != nil {
		t.Fatalf("EmptyQueue: %v", err)
	}
	if n != 3 {
		t.Fatalf("EmptyQueue removed %d, want 3", n)
	}

	for _, id := range ids {
		if status := f.statusOf(t, id); status != StatusCanceled {
			t.Errorf("job %s status = %q, want %q", id, status, StatusCanceled)
		}
		assertTTLNear(t, rawTTL(t, f.rc, f.jobKey(id)), 7200)
	}
}

func TestRetention_CompletedZsetTrimmedByScore(t *testing.T) {
	// A short window makes the trim observable: an entry backdated beyond it
	// must be gone once a fresh completion runs the trim.
	f := newRetentionFixture(t, WithResultTTL(10*time.Second))

	processed := make(chan struct{}, 1)
	if err := f.server.Handle("ttl.trim", func(ctx context.Context, job *Job) error {
		processed <- struct{}{}
		return nil
	}, Workers(1)); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	ctx := context.Background()
	completedKey := f.rc.Key("queue", "ttl.trim", "completed")

	// Stand in for a job completed long ago, whose hash Redis already expired.
	stale := "stale-job-id"
	if err := f.rc.rdb.ZAdd(ctx, completedKey,
		redis.Z{Score: float64(time.Now().Add(-time.Hour).Unix()), Member: stale}).Err(); err != nil {
		t.Fatalf("seeding stale entry: %v", err)
	}

	job, err := f.client.Enqueue(ctx, "ttl.trim", nil, Queue("ttl.trim"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	f.runUntil(t, 10*time.Second, func() bool {
		select {
		case <-processed:
			return true
		default:
			return false
		}
	})

	if err := f.rc.rdb.ZScore(ctx, completedKey, stale).Err(); err == nil {
		t.Error("stale entry survived the trim; hash and zset would diverge")
	}
	// The just-completed job is inside the window and must remain listable.
	if err := f.rc.rdb.ZScore(ctx, completedKey, job.ID).Err(); err != nil {
		t.Errorf("fresh entry was trimmed away: %v", err)
	}
}

func TestRetention_DeadLetterZsetTrimmedByScore(t *testing.T) {
	f := newRetentionFixture(t, WithFailureTTL(10*time.Second))

	if err := f.server.Handle("ttl.dlqtrim", func(ctx context.Context, job *Job) error {
		return errors.New("always fails")
	}, Workers(1)); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	ctx := context.Background()
	dlqKey := f.rc.Key("queue", "ttl.dlqtrim", "dead_letter")

	stale := "stale-dlq-id"
	if err := f.rc.rdb.ZAdd(ctx, dlqKey,
		redis.Z{Score: float64(time.Now().Add(-time.Hour).Unix()), Member: stale}).Err(); err != nil {
		t.Fatalf("seeding stale DLQ entry: %v", err)
	}

	job, err := f.client.Enqueue(ctx, "ttl.dlqtrim", nil,
		Queue("ttl.dlqtrim"), MaxRetry(0))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	f.runUntil(t, 15*time.Second, func() bool {
		status, err := f.rc.rdb.HGet(ctx, f.jobKey(job.ID), "status").Result()
		return err == nil && status == StatusDeadLetter
	})

	if err := f.rc.rdb.ZScore(ctx, dlqKey, stale).Err(); err == nil {
		t.Error("stale DLQ entry survived the trim")
	}
	if err := f.rc.rdb.ZScore(ctx, dlqKey, job.ID).Err(); err != nil {
		t.Errorf("fresh DLQ entry was trimmed away: %v", err)
	}
}

func TestRetention_VolatileLRUCannotEvictNonTerminalJobs(t *testing.T) {
	// Setting a TTL on terminal jobs changes their eviction class: under a
	// volatile-* maxmemory policy Redis only considers keys that carry an
	// expiry, so terminal jobs become evictable where they previously were not.
	// That is intended. What must stay true is the other half — a job still
	// queued or running carries no expiry, and so remains outside the candidate
	// set no matter how much memory pressure Redis is under.
	f := newRetentionFixture(t, WithResultTTL(time.Hour), WithFailureTTL(time.Hour))

	ctx := context.Background()
	original, err := f.rc.rdb.ConfigGet(ctx, "maxmemory-policy").Result()
	if err != nil {
		t.Skipf("cannot read maxmemory-policy: %v", err)
	}
	previous := original["maxmemory-policy"]
	if err := f.rc.rdb.ConfigSet(ctx, "maxmemory-policy", "volatile-lru").Err(); err != nil {
		t.Skipf("cannot set maxmemory-policy: %v", err)
	}
	t.Cleanup(func() {
		f.rc.rdb.ConfigSet(context.Background(), "maxmemory-policy", previous)
	})

	processed := make(chan struct{}, 1)
	if err := f.server.Handle("ttl.lru", func(ctx context.Context, job *Job) error {
		processed <- struct{}{}
		return nil
	}, Workers(1)); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// A job that never runs: it stays in a non-terminal state throughout.
	queued, err := f.client.EnqueueIn(ctx, time.Hour, "ttl.never", nil, Queue("ttl.never"))
	if err != nil {
		t.Fatalf("EnqueueIn: %v", err)
	}
	done, err := f.client.Enqueue(ctx, "ttl.lru", nil, Queue("ttl.lru"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	f.runUntil(t, 10*time.Second, func() bool {
		select {
		case <-processed:
			return true
		default:
			return false
		}
	})

	if ttl := rawTTL(t, f.rc, f.jobKey(queued.ID)); ttl != -1 {
		t.Errorf("scheduled job TTL = %d, want -1; with an expiry volatile-lru could evict live work", ttl)
	}
	if ttl := rawTTL(t, f.rc, f.jobKey(done.ID)); ttl <= 0 {
		t.Errorf("completed job TTL = %d, want a positive expiry so it is reclaimable", ttl)
	}
}

func TestRetention_JobRecordCost(t *testing.T) {
	// The retention analysis estimated ~500 B per completed job from the shape
	// of the structures without ever measuring it. This reports the real cost
	// so the growth arithmetic rests on a number, and fails only on a gross
	// regression rather than on normal variation.
	//
	// The count must stay above zset-max-listpack-entries (128 by default):
	// below it Redis stores the completed set as a listpack at roughly half the
	// per-entry cost, which would understate what a real retained queue costs.
	const jobs = 150
	f := newRetentionFixture(t, WithResultTTL(time.Hour))

	if err := f.server.Handle("ttl.cost", func(ctx context.Context, job *Job) error {
		return nil
	}, Workers(4)); err != nil {
		t.Fatalf("Handle: %v", err)
	}

	ctx := context.Background()
	var ids []string
	for i := 0; i < jobs; i++ {
		// An ID-only payload, the shape a pipeline that passes references
		// rather than documents produces.
		job, err := f.client.Enqueue(ctx, "ttl.cost", Payload{
			"conversation_id": "019fb109-39d2-7000-bc86-43a0d93d744b",
			"agent_id":        "019fb109-39d0-7000-b8bc-b2e4137f9335",
			"segment_id":      "019fb109-39ce-7000-a304-24ec987bfaae",
		}, Queue("ttl.cost"))
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
		ids = append(ids, job.ID)
	}

	completedKey := f.rc.Key("queue", "ttl.cost", "completed")
	f.runUntil(t, 30*time.Second, func() bool {
		n, err := f.rc.rdb.ZCard(ctx, completedKey).Result()
		return err == nil && n >= jobs
	})

	var hashTotal int64
	for _, id := range ids {
		usage, err := f.rc.rdb.MemoryUsage(ctx, f.jobKey(id)).Result()
		if err != nil {
			t.Fatalf("MEMORY USAGE: %v", err)
		}
		hashTotal += usage
	}
	zsetUsage, err := f.rc.rdb.MemoryUsage(ctx, completedKey).Result()
	if err != nil {
		t.Fatalf("MEMORY USAGE zset: %v", err)
	}

	encoding, err := f.rc.rdb.ObjectEncoding(ctx, completedKey).Result()
	if err != nil {
		t.Fatalf("OBJECT ENCODING: %v", err)
	}
	if encoding == "listpack" {
		t.Fatalf("completed set is still a listpack at %d entries; the measurement "+
			"would understate the real per-entry cost", len(ids))
	}

	perJob := (hashTotal + zsetUsage) / int64(len(ids))
	t.Logf("measured retained cost: %d B/job (hash %d B avg + zset %d B total over %d jobs, encoding %s)",
		perJob, hashTotal/int64(len(ids)), zsetUsage, len(ids), encoding)
	t.Logf("projected growth: %.1f MB/year at 100 jobs/day, %.1f MB/year at 1000 jobs/day",
		float64(perJob)*100*365/(1024*1024), float64(perJob)*1000*365/(1024*1024))

	if perJob > 4096 {
		t.Errorf("retained cost = %d B/job, above the 4 KB sanity ceiling", perJob)
	}
}
