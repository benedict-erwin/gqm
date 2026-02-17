package gqm

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	defaultSchedulerPollInterval = 1 * time.Second
	schedulerBatchSize           = 100
	cronLockTTL                  = 60 * time.Second
)

// schedulerEngine handles both the scheduled job polling (retry + delayed jobs)
// and the cron evaluation loop. It uses Redis TIME as a single clock source
// for consistency across multiple instances.
type schedulerEngine struct {
	server     *Server
	logger     *slog.Logger
	instanceID string // unique ID for distributed lock ownership
}

func newSchedulerEngine(server *Server) *schedulerEngine {
	return &schedulerEngine{
		server:     server,
		logger:     server.logger.With("component", "scheduler"),
		instanceID: NewUUID(),
	}
}

// run starts the scheduler loop. Each tick:
// 1. Gets current time from Redis TIME
// 2. Polls the scheduled sorted set (retry + delayed jobs)
// 3. Evaluates cron entries and enqueues due jobs
func (se *schedulerEngine) run(ctx context.Context) {
	ticker := time.NewTicker(se.server.cfg.schedulerPollInterval)
	defer ticker.Stop()

	se.logger.Debug("scheduler engine started", "instance_id", se.instanceID)
	defer se.logger.Debug("scheduler engine stopped")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			se.tick(ctx)
		}
	}
}

func (se *schedulerEngine) tick(ctx context.Context) {
	now, err := se.redisTime(ctx)
	if err != nil {
		if ctx.Err() == nil {
			se.logger.Error("getting redis time", "error", err)
		}
		return
	}

	se.pollScheduled(ctx, now)
	se.evalCron(ctx, now)
}

// redisTime returns the current time from Redis, used as a single source of
// truth across all instances to avoid clock skew issues.
func (se *schedulerEngine) redisTime(ctx context.Context) (time.Time, error) {
	return se.server.rc.rdb.Time(ctx).Result()
}

// pollScheduled checks for scheduled/retry jobs that are ready and moves them
// to their queues via the schedule_poll Lua script.
func (se *schedulerEngine) pollScheduled(ctx context.Context, now time.Time) {
	rc := se.server.rc

	scheduledKey := rc.Key("scheduled")
	jobPrefix := rc.Key("job") + ":"
	queuePrefix := rc.Key("queue") + ":"

	result := se.server.scripts.run(ctx, rc.rdb, "schedule_poll",
		[]string{scheduledKey, jobPrefix, queuePrefix},
		now.Unix(), schedulerBatchSize,
	)
	if result.Err() != nil {
		if ctx.Err() == nil {
			se.logger.Error("schedule poll failed", "error", result.Err())
		}
		return
	}

	moved, _ := result.Int64()
	if moved > 0 {
		se.logger.Info("scheduled jobs moved to ready", "count", moved)
	}
}

// evalCron iterates all registered cron entries and enqueues jobs that are due.
func (se *schedulerEngine) evalCron(ctx context.Context, now time.Time) {
	// Snapshot entries and their Enabled state under cronMu to avoid racing
	// with setCronEnabled which writes Enabled from HTTP handler goroutines.
	type cronSnap struct {
		entry   *CronEntry
		enabled bool
	}
	se.server.cronMu.Lock()
	snaps := make([]cronSnap, 0, len(se.server.cronEntries))
	for _, e := range se.server.cronEntries {
		snaps = append(snaps, cronSnap{entry: e, enabled: e.Enabled})
	}
	se.server.cronMu.Unlock()

	for _, snap := range snaps {
		entry := snap.entry
		if !snap.enabled {
			continue
		}

		// Initialize NextRun on first evaluation. A value of -1 means no
		// valid next run was found; skip permanently to avoid re-evaluating
		// every tick.
		if entry.NextRun == 0 {
			loc := se.resolveLocation(entry)
			next := entry.expr.Next(now.In(loc))
			if next.IsZero() {
				entry.NextRun = -1
				continue
			}
			entry.NextRun = next.Unix()
			se.updateCronEntryState(ctx, entry)
			continue
		}
		if entry.NextRun < 0 {
			continue
		}

		// Not yet due.
		if now.Unix() < entry.NextRun {
			continue
		}

		// Acquire distributed lock to prevent double enqueue.
		if !se.acquireCronLock(ctx, entry.ID) {
			continue
		}

		se.enqueueCronJob(ctx, entry, now)
		se.releaseCronLock(ctx, entry.ID)
	}
}

// acquireCronLock attempts to acquire a distributed lock for a cron entry.
// Returns true if the lock was acquired (this instance should enqueue the job).
func (se *schedulerEngine) acquireCronLock(ctx context.Context, entryID string) bool {
	lockKey := se.server.rc.Key("cron", "lock", entryID)
	ok, err := se.server.rc.rdb.SetNX(ctx, lockKey, se.instanceID, cronLockTTL).Result()
	if err != nil {
		if ctx.Err() == nil {
			se.logger.Error("acquiring cron lock", "entry_id", entryID, "error", err)
		}
		return false
	}
	return ok
}

// releaseCronLock releases the distributed lock after the enqueue operation
// completes. The lock's TTL acts as a crash-safety fallback; under normal
// operation the lock is released immediately so the next scheduled fire is
// not blocked.
func (se *schedulerEngine) releaseCronLock(ctx context.Context, entryID string) {
	lockKey := se.server.rc.Key("cron", "lock", entryID)
	// Atomic check-and-delete via Lua to prevent TOCTOU race.
	result := se.server.scripts.run(ctx, se.server.rc.rdb, "cron_unlock",
		[]string{lockKey},
		se.instanceID,
	)
	if result.Err() != nil {
		se.server.logger.Warn("failed to release cron lock, will expire via TTL",
			"entry_id", entryID,
			"error", result.Err(),
		)
	}
}

// checkOverlap checks whether a new cron job should be enqueued based on the
// entry's overlap policy. Returns true if the job should be enqueued.
//
// Policies:
//   - OverlapAllow: always enqueue (no check).
//   - OverlapSkip: skip if the previous job is still active.
//   - OverlapReplace: cancel the previous job, then enqueue.
func (se *schedulerEngine) checkOverlap(ctx context.Context, entry *CronEntry) bool {
	if entry.OverlapPolicy == OverlapAllow {
		return true
	}

	rc := se.server.rc
	currentKey := rc.Key("cron", "current", entry.ID)

	prevJobID, err := rc.rdb.Get(ctx, currentKey).Result()
	if err == redis.Nil {
		// No previous job tracked — safe to enqueue.
		return true
	}
	if err != nil {
		if ctx.Err() == nil {
			se.logger.Error("checking cron overlap", "entry_id", entry.ID, "error", err)
		}
		return false
	}

	// Check if the previous job is still active.
	jobKey := rc.Key("job", prevJobID)
	status, err := rc.rdb.HGet(ctx, jobKey, "status").Result()
	if err == redis.Nil {
		// Job hash no longer exists — safe to enqueue.
		return true
	}
	if err != nil {
		if ctx.Err() == nil {
			se.logger.Error("checking previous cron job status",
				"entry_id", entry.ID, "job_id", prevJobID, "error", err)
		}
		return false
	}

	active := status == StatusReady || status == StatusScheduled ||
		status == StatusProcessing || status == StatusRetry

	if !active {
		return true
	}

	switch entry.OverlapPolicy {
	case OverlapSkip:
		se.logger.Info("cron job skipped (overlap)",
			"entry_id", entry.ID, "previous_job", prevJobID, "status", status)
		return false

	case OverlapReplace:
		se.cancelPreviousCronJob(ctx, entry.ID, prevJobID)
		return true

	default:
		return true
	}
}

// cancelPreviousCronJob marks the previous cron job as stopped and removes it from queues.
// Note: if the job is already in StatusProcessing, the worker will not be interrupted.
// The status is set to "stopped" in the hash but the worker already loaded the job data.
// OverlapReplace only fully prevents overlap for jobs in ready/scheduled states.
func (se *schedulerEngine) cancelPreviousCronJob(ctx context.Context, entryID, jobID string) {
	rc := se.server.rc
	jobKey := rc.Key("job", jobID)

	// Read the job's queue to know which ready list to remove from.
	queue, _ := rc.rdb.HGet(ctx, jobKey, "queue").Result()
	if queue == "" {
		queue = "default"
	}

	pipe := rc.rdb.Pipeline()
	pipe.HSet(ctx, jobKey, "status", StatusStopped)
	// Remove from ready queue (LREM removes all occurrences).
	pipe.LRem(ctx, rc.Key("queue", queue, "ready"), 0, jobID)
	// Remove from scheduled set (in case it's a delayed/retry job).
	pipe.ZRem(ctx, rc.Key("scheduled"), jobID)
	// Remove from processing set (best-effort; worker may still be running).
	pipe.ZRem(ctx, rc.Key("queue", queue, "processing"), jobID)
	if _, err := pipe.Exec(ctx); err != nil {
		if ctx.Err() == nil {
			se.logger.Error("cancelling previous cron job",
				"entry_id", entryID, "job_id", jobID, "error", err)
		}
		return
	}

	se.logger.Info("previous cron job cancelled (replace)",
		"entry_id", entryID, "job_id", jobID)
}

// enqueueCronJob creates and enqueues a job for a cron entry, then updates
// the entry state (last_run, next_run, last_status) in Redis.
func (se *schedulerEngine) enqueueCronJob(ctx context.Context, entry *CronEntry, now time.Time) {
	// Check overlap policy before enqueuing.
	if !se.checkOverlap(ctx, entry) {
		se.advanceCronSchedule(ctx, entry, now, "skipped")
		return
	}

	rc := se.server.rc

	job := NewJob(entry.JobType, entry.Payload)
	job.Queue = entry.Queue
	if entry.Timeout > 0 {
		job.Timeout = entry.Timeout
	}
	if entry.MaxRetry > 0 {
		job.MaxRetry = entry.MaxRetry
	}

	jobMap, err := job.ToMap()
	if err != nil {
		se.logger.Error("converting cron job to map", "entry_id", entry.ID, "error", err)
		entry.LastStatus = "error"
		return
	}

	jobKey := rc.Key("job", job.ID)
	queueKey := rc.Key("queue", job.Queue, "ready")
	queuesKey := rc.Key("queues")
	historyKey := rc.Key("cron", "history", entry.ID)
	currentKey := rc.Key("cron", "current", entry.ID)

	pipe := rc.rdb.Pipeline()
	pipe.HSet(ctx, jobKey, jobMap)
	pipe.LPush(ctx, queueKey, job.ID)
	pipe.SAdd(ctx, queuesKey, job.Queue)
	pipe.ZAdd(ctx, historyKey, redis.Z{Score: float64(now.Unix()), Member: job.ID})
	pipe.ZRemRangeByRank(ctx, historyKey, 0, -1001) // Trim to last 1000 entries
	pipe.Set(ctx, currentKey, job.ID, 0)             // Track current job for overlap detection
	if _, err := pipe.Exec(ctx); err != nil {
		if ctx.Err() == nil {
			se.logger.Error("enqueuing cron job", "entry_id", entry.ID, "error", err)
		}
		entry.LastStatus = "error"
		return
	}

	se.advanceCronSchedule(ctx, entry, now, "triggered")

	se.logger.Info("cron job enqueued",
		"entry_id", entry.ID,
		"job_id", job.ID,
		"overlap_policy", string(entry.OverlapPolicy),
	)
}

// advanceCronSchedule updates the entry's next_run, last_run, and last_status,
// then persists to Redis. Used after both successful enqueue and skip.
func (se *schedulerEngine) advanceCronSchedule(ctx context.Context, entry *CronEntry, now time.Time, status string) {
	entry.LastRun = now.Unix()
	entry.LastStatus = status

	loc := se.resolveLocation(entry)
	next := entry.expr.Next(now.In(loc))
	if !next.IsZero() {
		entry.NextRun = next.Unix()
	}
	entry.UpdatedAt = now.Unix()

	se.updateCronEntryState(ctx, entry)
}

// resolveLocation returns the timezone location for a cron entry.
// Fallback chain: entry.Timezone → server defaultTimezone → UTC.
func (se *schedulerEngine) resolveLocation(entry *CronEntry) *time.Location {
	if entry.Timezone != "" {
		loc, err := time.LoadLocation(entry.Timezone)
		if err == nil {
			return loc
		}
		// Timezone was validated at registration, this shouldn't happen.
		se.logger.Warn("invalid timezone in cron entry, falling back",
			"entry_id", entry.ID, "timezone", entry.Timezone)
	}

	if tz := se.server.cfg.defaultTimezone; tz != "" {
		loc, err := time.LoadLocation(tz)
		if err == nil {
			return loc
		}
		se.logger.Warn("invalid default timezone, falling back to UTC",
			"timezone", tz)
	}

	return time.UTC
}

// updateCronEntryState persists the entry's runtime state to Redis.
func (se *schedulerEngine) updateCronEntryState(ctx context.Context, entry *CronEntry) {
	data, err := json.Marshal(entry)
	if err != nil {
		se.logger.Error("marshaling cron entry", "entry_id", entry.ID, "error", err)
		return
	}

	entriesKey := se.server.rc.Key("cron", "entries")
	if err := se.server.rc.rdb.HSet(ctx, entriesKey, entry.ID, string(data)).Err(); err != nil {
		if ctx.Err() == nil {
			se.logger.Error("updating cron entry in redis", "entry_id", entry.ID, "error", err)
		}
	}
}
