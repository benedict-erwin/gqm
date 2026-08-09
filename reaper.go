package gqm

import (
	"context"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// reaperSlack is added to a pool's grace period before a job still sitting
	// in the processing set counts as abandoned.
	//
	// A live worker always leaves the processing set by deadline + grace: at the
	// deadline it cancels the handler context, waits at most the grace period,
	// and then runs the completion, retry, or dead-letter path regardless of
	// whether the handler came back. Anything still there past deadline + grace
	// + slack therefore belongs to a process that is no longer running, and the
	// slack absorbs clock jitter and a slow final Redis round trip.
	reaperSlack = 15 * time.Second

	// reaperBatchSize caps how many expired jobs a single queue yields per tick,
	// so one very large backlog cannot stall the scheduler loop.
	reaperBatchSize = schedulerBatchSize

	// reapErrorMessage is recorded on jobs the reaper reclaims. It states what
	// was observed — the claim outlived its deadline by more than any live
	// worker could — rather than guessing at a cause.
	reapErrorMessage = "worker presumed dead: processing deadline exceeded"

	// reaperStatsTTL is the retention window for the daily failed counter,
	// matching the one used on the worker's dead-letter path.
	reaperStatsTTL = 90 * 24 * 3600 // 90 days in seconds
)

// reapStale reclaims jobs whose processing deadline has passed by more than the
// owning pool's grace period plus reaperSlack.
//
// dequeue.lua stamps every claim with a deadline score; this is the half that
// reads it. Without it, a worker process that dies mid-job strands its in-flight
// jobs in "processing" permanently, because every other exit from that state is
// driven by the claiming worker itself.
//
// It runs on every instance and takes no lock. reap.lua's score guard makes
// concurrent reapers safe: whichever call lands first removes the job from the
// processing set, and the rest see the entry gone and do nothing.
func (se *schedulerEngine) reapStale(ctx context.Context, now time.Time) {
	// A queue may be listed by more than one pool, and nothing in the processing
	// set records which of them claimed a given job. Each queue is therefore
	// scanned once, under the most patient pool listening on it: waiting out the
	// longest grace period at worst leaves a zombie a little longer, while
	// waiting out the shortest could abandon a claim a slower pool's worker is
	// still legitimately finishing.
	owners := make(map[string]*pool, len(se.server.pools))
	for _, p := range se.server.pools {
		for _, queue := range p.cfg.queues {
			if prev, ok := owners[queue]; !ok || poolGracePeriod(p) > poolGracePeriod(prev) {
				owners[queue] = p
			}
		}
	}

	for queue, p := range owners {
		threshold := now.Add(-(poolGracePeriod(p) + reaperSlack)).Unix()
		se.reapQueue(ctx, p, queue, now, threshold)
	}
}

// trimDeadLetter drops dead-letter entries whose job hash no longer exists.
//
// deadletter.lua puts the job's id in the queue's dead-letter set and stamps
// the hash with the failure retention TTL, but a sorted set member cannot
// expire on its own. Its only other trim runs inside deadletter.lua itself, so
// a queue that never dead-letters again keeps the member forever: the count
// reports a dead job the listing cannot show, because the listing joins to a
// hash Redis has already removed.
//
// The test is existence-based rather than score-based on purpose. Failure
// retention is resolved per job (Job.FailureTTL overrides the server default,
// and a negative value keeps the hash forever), so no single retention window
// is right: it would drop members whose hash is still alive and keep members
// whose hash expired long ago. EXISTS is exact under every setting, and it also
// heals orphans left behind by anything else that removed a hash.
func (se *schedulerEngine) trimDeadLetter(ctx context.Context) {
	rc := se.server.rc

	// The queue registry rather than the pools: a queue no pool listens on any
	// more still holds whatever dead-lettered before, and nothing else would
	// ever visit it.
	queues, err := rc.rdb.SMembers(ctx, rc.Key("queues")).Result()
	if err != nil {
		if ctx.Err() == nil {
			se.logger.Error("listing queues for dead letter trim", "error", err)
		}
		return
	}

	for _, queue := range queues {
		se.trimQueueDeadLetter(ctx, queue)
	}
}

// trimQueueDeadLetter sweeps one queue's dead-letter set and removes every
// member whose job hash is gone.
func (se *schedulerEngine) trimQueueDeadLetter(ctx context.Context, queue string) {
	rc := se.server.rc
	dlqKey := rc.Key("queue", queue, "dead_letter")

	// One tick walks the whole set instead of stopping after a batch: the
	// dead-letter set is an exception path and expected to stay small, and a
	// phantom carried over to the next tick is the very thing this prevents.
	for start := int64(0); ; {
		jobIDs, err := rc.rdb.ZRange(ctx, dlqKey, start, start+reaperBatchSize-1).Result()
		if err != nil {
			if ctx.Err() == nil {
				se.logger.Error("scanning dead letter set", "queue", queue, "error", err)
			}
			return
		}
		if len(jobIDs) == 0 {
			return
		}

		pipe := rc.rdb.Pipeline()
		exists := make([]*redis.IntCmd, len(jobIDs))
		for i, jobID := range jobIDs {
			exists[i] = pipe.Exists(ctx, rc.Key("job", jobID))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			if ctx.Err() == nil {
				se.logger.Error("checking dead letter job hashes", "queue", queue, "error", err)
			}
			return
		}

		orphans := make([]any, 0, len(jobIDs))
		for i, cmd := range exists {
			if cmd.Val() == 0 {
				orphans = append(orphans, jobIDs[i])
			}
		}

		if len(orphans) > 0 {
			// A hash cannot reappear between the check and the removal: ids are
			// never reused, and both the admin retry path and dequeue.lua refuse
			// a job that has no hash. Losing the race only means removing on this
			// tick what the next one would have removed anyway.
			if err := rc.rdb.ZRem(ctx, dlqKey, orphans...).Err(); err != nil {
				if ctx.Err() == nil {
					se.logger.Error("removing orphaned dead letter entries", "queue", queue, "error", err)
				}
				return
			}
			se.logger.Warn("removed orphaned dead letter entries: job hashes no longer exist",
				"queue", queue, "count", len(orphans))
		}

		if len(jobIDs) < reaperBatchSize {
			return
		}
		// Removing members shifts every later member left by that many places,
		// so the next page starts where this one's survivors end.
		start += int64(len(jobIDs) - len(orphans))
	}
}

// poolGracePeriod returns the pool's effective grace period: the window a
// worker gives a canceled handler before it stops waiting and writes the job's
// outcome itself.
func poolGracePeriod(p *pool) time.Duration {
	if p.cfg.gracePeriod <= 0 {
		return defaultGracePeriod
	}
	return p.cfg.gracePeriod
}

// reapQueue scans one queue's processing set for claims older than threshold and
// reclaims each one.
func (se *schedulerEngine) reapQueue(ctx context.Context, p *pool, queue string, now time.Time, threshold int64) {
	rc := se.server.rc
	processingKey := rc.Key("queue", queue, "processing")

	jobIDs, err := rc.rdb.ZRangeByScore(ctx, processingKey, &redis.ZRangeBy{
		Min:   "-inf",
		Max:   strconv.FormatInt(threshold, 10),
		Count: reaperBatchSize,
	}).Result()
	if err != nil {
		if ctx.Err() == nil {
			se.logger.Error("scanning processing set for stale jobs",
				"queue", queue, "error", err)
		}
		return
	}

	for _, jobID := range jobIDs {
		se.reapJob(ctx, p, queue, jobID, now, threshold)
	}
}

// reapJob decides what should happen to a single expired claim and hands the
// decision to reap.lua, which re-checks the deadline before acting on it.
func (se *schedulerEngine) reapJob(ctx context.Context, p *pool, queue, jobID string, now time.Time, threshold int64) {
	rc := se.server.rc
	processingKey := rc.Key("queue", queue, "processing")
	jobKey := rc.Key("job", jobID)

	data, err := rc.rdb.HGetAll(ctx, jobKey).Result()
	if err != nil {
		if ctx.Err() == nil {
			se.logger.Error("reading stale job hash", "job_id", jobID, "queue", queue, "error", err)
		}
		return
	}
	if len(data) == 0 {
		// The hash is gone, so no transition is possible and no worker can ever
		// re-claim the job — dequeue.lua refuses a job without a hash. Dropping
		// the entry is the only way it stops being rescanned every tick.
		if err := rc.rdb.ZRem(ctx, processingKey, jobID).Err(); err != nil && ctx.Err() == nil {
			se.logger.Error("removing orphaned processing entry",
				"job_id", jobID, "queue", queue, "error", err)
			return
		}
		se.logger.Warn("removed orphaned processing entry: job hash no longer exists",
			"job_id", jobID, "queue", queue)
		return
	}

	job, err := JobFromMap(data)
	if err != nil {
		// A field that will not decode must not make the job unreapable: fall
		// back to the handful of values the decision actually needs.
		se.logger.Warn("stale job hash could not be fully parsed, reaping on raw fields",
			"job_id", jobID, "queue", queue, "error", err)
		job = &Job{
			ID:         jobID,
			Queue:      queue,
			RetryCount: parseInt(data["retry_count"]),
			MaxRetry:   parseInt(data["max_retry"]),
		}
		if v := data["failure_ttl"]; v != "" {
			ttl := parseInt(v)
			job.FailureTTL = &ttl
		}
	}

	newRetryCount := job.RetryCount + 1
	maxRetry := p.resolveMaxRetry(job)

	// The reaped attempt counts against the retry budget, so a job that keeps
	// killing the process running it eventually dead-letters instead of cycling
	// through workers forever.
	action := "deadletter"
	retryAt := now.Unix()
	if maxRetry > 0 && newRetryCount <= maxRetry {
		action = "retry"
		retryAt = now.Add(p.retryDelay(job, newRetryCount)).Unix()
	}

	date := now.UTC().Format("2006-01-02")
	result := se.server.scripts.run(ctx, rc.rdb, "reap",
		[]string{
			processingKey,
			rc.Key("scheduled"),
			jobKey,
			rc.Key("queue", queue, "dead_letter"),
			rc.Key("job", jobID, "dependents"),
			rc.Key("stats", queue, "failed", date),
			rc.Key("stats", queue, "failed_total"),
		},
		jobID, threshold, action, now.Unix(), reapErrorMessage,
		retryAt, newRetryCount, reaperStatsTTL,
		se.server.cfg.failureRetention(job.FailureTTL),
	)
	if result.Err() != nil {
		if ctx.Err() == nil {
			se.logger.Error("reaping stale job", "job_id", jobID, "queue", queue, "error", result.Err())
		}
		return
	}

	val, err := result.Int64()
	if err != nil {
		se.logger.Error("reading reap result", "job_id", jobID, "queue", queue, "error", err)
		return
	}

	switch val {
	case 0:
		// The job left processing or was re-claimed between the scan and the
		// call. Nothing was touched.
		se.logger.Debug("stale job no longer eligible for reaping",
			"job_id", jobID, "queue", queue)

	case 1:
		se.logger.Warn("reclaimed stale job",
			"job_id", jobID, "queue", queue, "action", "retried",
			"retry_count", newRetryCount, "retry_at", time.Unix(retryAt, 0))

	case 2, 3:
		se.logger.Warn("reclaimed stale job",
			"job_id", jobID, "queue", queue, "action", "dead_lettered",
			"retry_count", job.RetryCount, "max_retry", maxRetry)

		// DAG: propagate failure to dependents only if Lua found the dependents
		// set, mirroring the worker's dead-letter path.
		if val == 3 {
			if err := propagateFailure(ctx, rc, se.server.scripts, jobID, se.server.cfg.failureRetention); err != nil {
				se.logger.Error("failed to propagate failure to dependents",
					"job_id", jobID, "error", err)
			}
		}
	}
}
