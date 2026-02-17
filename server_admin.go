package gqm

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// --- Queue Pause ---

// PauseQueue pauses a queue. Workers will stop dequeuing from the paused queue
// but in-flight jobs continue to completion. New jobs can still be enqueued.
func (s *Server) PauseQueue(ctx context.Context, queue string) error {
	return s.rc.rdb.SAdd(ctx, s.rc.Key("paused"), queue).Err()
}

// ResumeQueue resumes a paused queue. Workers will start dequeuing again.
func (s *Server) ResumeQueue(ctx context.Context, queue string) error {
	return s.rc.rdb.SRem(ctx, s.rc.Key("paused"), queue).Err()
}

// IsQueuePaused returns whether the given queue is paused.
func (s *Server) IsQueuePaused(ctx context.Context, queue string) (bool, error) {
	return s.rc.rdb.SIsMember(ctx, s.rc.Key("paused"), queue).Result()
}

// --- Job Operations ---

// RetryJob moves a dead-letter job back to the ready queue.
// Only jobs with status "dead_letter" can be retried.
func (s *Server) RetryJob(ctx context.Context, jobID string) error {
	jobKey := s.rc.Key("job", jobID)
	data, err := s.rc.rdb.HGetAll(ctx, jobKey).Result()
	if err != nil {
		return fmt.Errorf("fetching job %s: %w", jobID, err)
	}
	if len(data) == 0 {
		return ErrJobNotFound
	}

	status := data["status"]
	if status != StatusDeadLetter {
		return fmt.Errorf("gqm: cannot retry job with status %q (must be %q)", status, StatusDeadLetter)
	}

	queue := data["queue"]
	if queue == "" {
		queue = "default"
	}

	dlqKey := s.rc.Key("queue", queue, "dead_letter")
	readyKey := s.rc.Key("queue", queue, "ready")

	now := time.Now().Unix()
	result := s.scripts.run(ctx, s.rc.rdb, "admin_retry",
		[]string{dlqKey, readyKey, jobKey},
		jobID, now,
	)
	if result.Err() != nil {
		return fmt.Errorf("retrying job %s: %w", jobID, result.Err())
	}

	val, _ := result.Int64()
	if val == 0 {
		return fmt.Errorf("gqm: job %s not found in dead letter queue", jobID)
	}

	return nil
}

// CancelJob cancels a job that is ready, scheduled, or deferred.
// Processing jobs cannot be canceled (handler is already running).
func (s *Server) CancelJob(ctx context.Context, jobID string) error {
	jobKey := s.rc.Key("job", jobID)
	data, err := s.rc.rdb.HGetAll(ctx, jobKey).Result()
	if err != nil {
		return fmt.Errorf("fetching job %s: %w", jobID, err)
	}
	if len(data) == 0 {
		return ErrJobNotFound
	}

	status := data["status"]
	if status != StatusReady && status != StatusScheduled && status != StatusDeferred {
		return fmt.Errorf("gqm: cannot cancel job with status %q (must be ready, scheduled, or deferred)", status)
	}

	queue := data["queue"]
	if queue == "" {
		queue = "default"
	}

	readyKey := s.rc.Key("queue", queue, "ready")
	scheduledKey := s.rc.Key("scheduled")
	deferredKey := s.rc.Key("deferred")

	now := time.Now().Unix()
	result := s.scripts.run(ctx, s.rc.rdb, "admin_cancel",
		[]string{jobKey, readyKey, scheduledKey, deferredKey},
		jobID, now, status,
	)
	if result.Err() != nil {
		return fmt.Errorf("canceling job %s: %w", jobID, result.Err())
	}

	val, _ := result.Int64()
	if val == 0 {
		return fmt.Errorf("gqm: job %s not found in expected location for status %q", jobID, status)
	}

	// DAG: if deferred, propagate cancellation to dependents.
	if status == StatusDeferred {
		if err := propagateFailure(ctx, s.rc, s.scripts, jobID); err != nil {
			s.logger.Error("failed to propagate cancellation to dependents",
				"job_id", jobID,
				"error", err,
			)
		}
	}

	return nil
}

// DeleteJob removes a job completely from Redis.
// Processing jobs cannot be deleted (return error).
// Uses a Lua script for atomicity to prevent TOCTOU races where a job
// transitions to "processing" between the status check and deletion.
func (s *Server) DeleteJob(ctx context.Context, jobID string) error {
	// We need the queue name to build the correct keys for the Lua script.
	jobKey := s.rc.Key("job", jobID)
	queue, err := s.rc.rdb.HGet(ctx, jobKey, "queue").Result()
	if err == redis.Nil {
		return ErrJobNotFound
	}
	if err != nil {
		return fmt.Errorf("fetching job %s: %w", jobID, err)
	}
	if queue == "" {
		queue = "default"
	}

	result := s.scripts.run(ctx, s.rc.rdb, "admin_delete",
		[]string{
			jobKey,
			s.rc.Key("queue", queue, "ready"),
			s.rc.Key("scheduled"),
			s.rc.Key("deferred"),
			s.rc.Key("queue", queue, "completed"),
			s.rc.Key("queue", queue, "dead_letter"),
			s.rc.Key("job", jobID, "deps"),
			s.rc.Key("job", jobID, "pending_deps"),
			s.rc.Key("job", jobID, "dependents"),
		},
		jobID,
	)
	if result.Err() != nil {
		return fmt.Errorf("deleting job %s: %w", jobID, result.Err())
	}

	val, _ := result.Int64()
	switch val {
	case -1:
		return ErrJobNotFound
	case -2:
		return fmt.Errorf("gqm: cannot delete job with status %q (currently processing)", StatusProcessing)
	}

	return nil
}

// --- Queue + DLQ Operations ---

// maxBulkOps is the maximum number of jobs processed in a single bulk
// operation (EmptyQueue, RetryAllDLQ, ClearDLQ). This prevents OOM when
// queues contain millions of jobs. Callers can invoke repeatedly until
// the returned count is 0.
const maxBulkOps = 1000

// EmptyQueue removes pending (ready) jobs from a queue, up to maxBulkOps at a time.
// Processing, scheduled, and DLQ jobs are unaffected.
// Returns the number of jobs removed. Call repeatedly until 0 to drain fully.
func (s *Server) EmptyQueue(ctx context.Context, queue string) (int64, error) {
	readyKey := s.rc.Key("queue", queue, "ready")

	// Atomically pop up to maxBulkOps job IDs from the ready list.
	jobIDs, err := s.rc.rdb.LPopCount(ctx, readyKey, maxBulkOps).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("listing ready jobs for queue %s: %w", queue, err)
	}

	if len(jobIDs) == 0 {
		return 0, nil
	}

	pipe := s.rc.rdb.Pipeline()
	for _, id := range jobIDs {
		pipe.HSet(ctx, s.rc.Key("job", id), "status", StatusCanceled, "completed_at", time.Now().Unix())
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("emptying queue %s: %w", queue, err)
	}

	return int64(len(jobIDs)), nil
}

// RetryAllDLQ retries jobs in the dead letter queue, up to maxBulkOps at a time.
// Returns the number of jobs retried. Call repeatedly until 0 to drain fully.
func (s *Server) RetryAllDLQ(ctx context.Context, queue string) (int64, error) {
	dlqKey := s.rc.Key("queue", queue, "dead_letter")
	readyKey := s.rc.Key("queue", queue, "ready")

	jobIDs, err := s.rc.rdb.ZRange(ctx, dlqKey, 0, int64(maxBulkOps-1)).Result()
	if err != nil {
		return 0, fmt.Errorf("listing DLQ jobs for queue %s: %w", queue, err)
	}

	if len(jobIDs) == 0 {
		return 0, nil
	}

	now := time.Now().Unix()
	var retried int64
	for _, jobID := range jobIDs {
		jobKey := s.rc.Key("job", jobID)
		result := s.scripts.run(ctx, s.rc.rdb, "admin_retry",
			[]string{dlqKey, readyKey, jobKey},
			jobID, now,
		)
		if result.Err() != nil {
			s.logger.Warn("failed to retry DLQ job", "job_id", jobID, "error", result.Err())
			continue
		}
		val, _ := result.Int64()
		if val == 1 {
			retried++
		}
	}

	return retried, nil
}

// ClearDLQ deletes jobs from the dead letter queue, up to maxBulkOps at a time.
// Job hashes and DAG keys are also removed.
// Returns the number of jobs deleted. Call repeatedly until 0 to drain fully.
func (s *Server) ClearDLQ(ctx context.Context, queue string) (int64, error) {
	dlqKey := s.rc.Key("queue", queue, "dead_letter")

	jobIDs, err := s.rc.rdb.ZRange(ctx, dlqKey, 0, int64(maxBulkOps-1)).Result()
	if err != nil {
		return 0, fmt.Errorf("listing DLQ jobs for queue %s: %w", queue, err)
	}

	if len(jobIDs) == 0 {
		return 0, nil
	}

	pipe := s.rc.rdb.Pipeline()
	for _, id := range jobIDs {
		pipe.Del(ctx, s.rc.Key("job", id))
		pipe.Del(ctx, s.rc.Key("job", id, "deps"))
		pipe.Del(ctx, s.rc.Key("job", id, "pending_deps"))
		pipe.Del(ctx, s.rc.Key("job", id, "dependents"))
		pipe.ZRem(ctx, dlqKey, id)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("clearing DLQ for queue %s: %w", queue, err)
	}

	return int64(len(jobIDs)), nil
}

// --- Cron Operations ---

// TriggerCron manually triggers a cron entry, enqueuing a job immediately.
// Returns the new job ID. Bypasses overlap checks and cron locks (explicit user intent).
func (s *Server) TriggerCron(ctx context.Context, cronID string) (string, error) {
	entriesKey := s.rc.Key("cron", "entries")
	raw, err := s.rc.rdb.HGet(ctx, entriesKey, cronID).Result()
	if err == redis.Nil {
		return "", fmt.Errorf("gqm: cron entry %q not found", cronID)
	}
	if err != nil {
		return "", fmt.Errorf("fetching cron entry %s: %w", cronID, err)
	}

	var entry CronEntry
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		return "", fmt.Errorf("parsing cron entry %s: %w", cronID, err)
	}

	// Create and enqueue a job using the entry's config
	job := NewJob(entry.JobType, entry.Payload)
	job.Queue = entry.Queue
	if job.Queue == "" {
		job.Queue = "default"
	}
	if entry.Timeout > 0 {
		job.Timeout = entry.Timeout
	}
	if entry.MaxRetry > 0 {
		job.MaxRetry = entry.MaxRetry
	}

	jobMap, err := job.ToMap()
	if err != nil {
		return "", fmt.Errorf("converting triggered job to map: %w", err)
	}

	jobKey := s.rc.Key("job", job.ID)
	queueKey := s.rc.Key("queue", job.Queue, "ready")
	queuesKey := s.rc.Key("queues")
	historyKey := s.rc.Key("cron", "history", cronID)
	currentKey := s.rc.Key("cron", "current", cronID)

	now := time.Now()
	pipe := s.rc.rdb.Pipeline()
	pipe.HSet(ctx, jobKey, jobMap)
	pipe.LPush(ctx, queueKey, job.ID)
	pipe.SAdd(ctx, queuesKey, job.Queue)
	pipe.ZAdd(ctx, historyKey, redis.Z{Score: float64(now.Unix()), Member: job.ID})
	pipe.ZRemRangeByRank(ctx, historyKey, 0, -1001) // Trim to last 1000 entries
	pipe.Set(ctx, currentKey, job.ID, 0)
	if _, err := pipe.Exec(ctx); err != nil {
		return "", fmt.Errorf("enqueuing triggered cron job: %w", err)
	}

	return job.ID, nil
}

// EnableCron enables a cron entry.
func (s *Server) EnableCron(ctx context.Context, cronID string) error {
	return s.setCronEnabled(ctx, cronID, true)
}

// DisableCron disables a cron entry.
func (s *Server) DisableCron(ctx context.Context, cronID string) error {
	return s.setCronEnabled(ctx, cronID, false)
}

func (s *Server) setCronEnabled(ctx context.Context, cronID string, enabled bool) error {
	entriesKey := s.rc.Key("cron", "entries")
	raw, err := s.rc.rdb.HGet(ctx, entriesKey, cronID).Result()
	if err == redis.Nil {
		return fmt.Errorf("gqm: cron entry %q not found", cronID)
	}
	if err != nil {
		return fmt.Errorf("fetching cron entry %s: %w", cronID, err)
	}

	var entry map[string]any
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		return fmt.Errorf("parsing cron entry %s: %w", cronID, err)
	}

	entry["enabled"] = enabled
	entry["updated_at"] = time.Now().Unix()

	updated, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshaling cron entry %s: %w", cronID, err)
	}

	if err := s.rc.rdb.HSet(ctx, entriesKey, cronID, string(updated)).Err(); err != nil {
		return fmt.Errorf("updating cron entry %s: %w", cronID, err)
	}

	// Update in-memory entry if it exists (for live scheduler).
	// Must hold cronMu to avoid data race with scheduler's evalCron.
	s.cronMu.Lock()
	if e, ok := s.cronEntries[cronID]; ok {
		e.Enabled = enabled
		e.UpdatedAt = time.Now().Unix()
	}
	s.cronMu.Unlock()

	return nil
}
