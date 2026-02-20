package gqm

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client is used to enqueue jobs into the queue system.
type Client struct {
	rc         *RedisClient
	knownQueues sync.Map // tracks queues already registered via SADD
}

// NewClient creates a new Client with the given Redis options.
func NewClient(opts ...RedisOption) (*Client, error) {
	rc, err := NewRedisClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating client: %w", err)
	}
	return &Client{rc: rc}, nil
}

// Close closes the client's Redis connection.
func (c *Client) Close() error {
	return c.rc.Close()
}

// Enqueue creates a new job and adds it to the queue.
// If DependsOn is set, the job is created with status "deferred" and will be
// moved to the ready queue only after all dependencies are resolved.
// Returns the created job or an error.
func (c *Client) Enqueue(ctx context.Context, jobType string, payload Payload, opts ...EnqueueOption) (*Job, error) {
	if jobType == "" {
		return nil, ErrInvalidJobType
	}

	job := NewJob(jobType, payload)
	for _, opt := range opts {
		opt(job)
	}

	// Validate queue name and custom job ID.
	if err := validateJobInputs(job); err != nil {
		return nil, err
	}

	// DAG: if dependencies are specified, validate and create as deferred.
	if len(job.DependsOn) > 0 {
		return c.enqueueDeferred(ctx, job)
	}

	jobMap, err := job.ToMap()
	if err != nil {
		return nil, fmt.Errorf("converting job to map: %w", err)
	}

	jobKey := c.rc.Key("job", job.ID)

	// Unique check: atomically claim the job key if it doesn't exist.
	// Note: there is a small TOCTOU window between HSetNX and the pipeline
	// Exec below. A process crash in between leaves an orphaned stub key.
	// This is an accepted limitation; full atomicity would require a Lua script.
	if job.unique {
		ok, err := c.rc.rdb.HSetNX(ctx, jobKey, "id", job.ID).Result()
		if err != nil {
			return nil, fmt.Errorf("checking uniqueness for job %s: %w", job.ID, err)
		}
		if !ok {
			return nil, ErrDuplicateJobID
		}
	}

	queueKey := c.rc.Key("queue", job.Queue, "ready")

	pipe := c.rc.rdb.Pipeline()
	pipe.HSet(ctx, jobKey, jobMap)
	pipe.LPush(ctx, queueKey, job.ID)
	if _, loaded := c.knownQueues.LoadOrStore(job.Queue, struct{}{}); !loaded {
		pipe.SAdd(ctx, c.rc.Key("queues"), job.Queue)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		// Clean up orphaned key from HSetNX if pipeline fails.
		if job.unique {
			c.rc.rdb.Del(ctx, jobKey)
		}
		return nil, fmt.Errorf("enqueuing job %s: %w", job.ID, err)
	}

	return job, nil
}

// enqueueDeferred creates a job with dependencies. The job starts as "deferred"
// and is moved to the ready queue when all parent jobs complete.
func (c *Client) enqueueDeferred(ctx context.Context, job *Job) (*Job, error) {
	// Cycle detection: DFS from each parent to check if jobID appears in ancestor chain.
	if err := detectCycle(ctx, c.rc, job.ID, job.DependsOn); err != nil {
		return nil, fmt.Errorf("enqueueing deferred job %s: %w", job.ID, err)
	}

	job.Status = StatusDeferred

	jobMap, err := job.ToMap()
	if err != nil {
		return nil, fmt.Errorf("converting job to map: %w", err)
	}

	jobKey := c.rc.Key("job", job.ID)

	// Unique check: atomically claim the job key if it doesn't exist.
	if job.unique {
		ok, err := c.rc.rdb.HSetNX(ctx, jobKey, "id", job.ID).Result()
		if err != nil {
			return nil, fmt.Errorf("checking uniqueness for deferred job %s: %w", job.ID, err)
		}
		if !ok {
			return nil, ErrDuplicateJobID
		}
	}

	// Single pipeline: job hash + queue registration + DAG setup.
	queuesKey := c.rc.Key("queues")
	depsKey := c.rc.Key("job", job.ID, "deps")
	pendingDepsKey := c.rc.Key("job", job.ID, "pending_deps")
	deferredKey := c.rc.Key("deferred")

	pipe := c.rc.rdb.Pipeline()
	pipe.HSet(ctx, jobKey, jobMap)
	pipe.SAdd(ctx, queuesKey, job.Queue)

	// DAG: deps, pending_deps, dependents, deferred set.
	parentIDs := make([]any, len(job.DependsOn))
	for i, id := range job.DependsOn {
		parentIDs[i] = id
	}
	pipe.SAdd(ctx, depsKey, parentIDs...)
	pipe.SAdd(ctx, pendingDepsKey, parentIDs...)
	for _, parentID := range job.DependsOn {
		dependentsKey := c.rc.Key("job", parentID, "dependents")
		pipe.SAdd(ctx, dependentsKey, job.ID)
	}
	pipe.SAdd(ctx, deferredKey, job.ID)

	if _, err := pipe.Exec(ctx); err != nil {
		// Clean up orphaned key from HSetNX if pipeline fails.
		if job.unique {
			c.rc.rdb.Del(ctx, jobKey)
		}
		return nil, fmt.Errorf("enqueuing deferred job %s: %w", job.ID, err)
	}

	return job, nil
}

// EnqueueAt creates a new job scheduled for execution at the given time.
// The job is placed in the scheduled sorted set and will be moved to the
// ready queue by the scheduler when the time arrives.
func (c *Client) EnqueueAt(ctx context.Context, at time.Time, jobType string, payload Payload, opts ...EnqueueOption) (*Job, error) {
	if jobType == "" {
		return nil, ErrInvalidJobType
	}

	job := NewJob(jobType, payload)
	for _, opt := range opts {
		opt(job)
	}

	// Validate queue name and custom job ID.
	if err := validateJobInputs(job); err != nil {
		return nil, err
	}

	// DependsOn is not supported for scheduled jobs.
	if len(job.DependsOn) > 0 {
		return nil, fmt.Errorf("gqm: DependsOn is not supported with EnqueueAt/EnqueueIn")
	}

	job.Status = StatusScheduled
	job.ScheduledAt = at.Unix()

	jobMap, err := job.ToMap()
	if err != nil {
		return nil, fmt.Errorf("converting job to map: %w", err)
	}

	jobKey := c.rc.Key("job", job.ID)

	// Unique check: atomically claim the job key if it doesn't exist.
	if job.unique {
		ok, err := c.rc.rdb.HSetNX(ctx, jobKey, "id", job.ID).Result()
		if err != nil {
			return nil, fmt.Errorf("checking uniqueness for scheduled job %s: %w", job.ID, err)
		}
		if !ok {
			return nil, ErrDuplicateJobID
		}
	}

	scheduledKey := c.rc.Key("scheduled")
	queuesKey := c.rc.Key("queues")

	pipe := c.rc.rdb.Pipeline()
	pipe.HSet(ctx, jobKey, jobMap)
	pipe.ZAdd(ctx, scheduledKey, redis.Z{Score: float64(at.Unix()), Member: job.ID})
	pipe.SAdd(ctx, queuesKey, job.Queue)
	if _, err := pipe.Exec(ctx); err != nil {
		if job.unique {
			c.rc.rdb.Del(ctx, jobKey)
		}
		return nil, fmt.Errorf("scheduling job %s: %w", job.ID, err)
	}

	return job, nil
}

// EnqueueIn creates a new job scheduled for execution after the given delay.
func (c *Client) EnqueueIn(ctx context.Context, delay time.Duration, jobType string, payload Payload, opts ...EnqueueOption) (*Job, error) {
	return c.EnqueueAt(ctx, time.Now().Add(delay), jobType, payload, opts...)
}

// BatchItem represents a single job to be enqueued in a batch.
type BatchItem struct {
	JobType string
	Payload Payload
	Options []EnqueueOption
}

// maxBatchEnqueueSize is the maximum number of items allowed in a single
// EnqueueBatch call. Each item generates 2-3 Redis commands, so this
// limits the pipeline to a manageable size.
const maxBatchEnqueueSize = 1000

// EnqueueBatch creates multiple jobs in a single Redis pipeline for
// efficiency. All jobs are validated upfront (including serialization);
// if any item fails validation, no jobs are enqueued and the error
// identifies the failing item.
//
// The pipeline is NOT a Redis transaction — on network errors, some jobs
// may have been created while others were not. Callers should treat a
// pipeline error as "unknown state" and verify job existence if needed.
//
// Maximum batch size is 1000 items. Returns ErrBatchTooLarge if exceeded.
//
// Limitations compared to single Enqueue:
//   - Unique() is not supported (returns error if set on any item).
//   - DependsOn() is not supported (returns error if set on any item).
//   - EnqueueAtFront() is not supported (returns error if set on any item).
//
// Use individual Enqueue calls if you need uniqueness, DAG dependencies,
// or front-of-queue insertion.
func (c *Client) EnqueueBatch(ctx context.Context, items []BatchItem) ([]*Job, error) {
	if len(items) == 0 {
		return nil, nil
	}
	if len(items) > maxBatchEnqueueSize {
		return nil, fmt.Errorf("%w: got %d", ErrBatchTooLarge, len(items))
	}

	// Phase 1: validate, build all jobs, and serialize upfront.
	jobs := make([]*Job, len(items))
	jobMaps := make([]map[string]any, len(items))
	seenIDs := make(map[string]int, len(items))
	uniqueQueues := make(map[string]struct{})

	for i, item := range items {
		if item.JobType == "" {
			return nil, fmt.Errorf("batch item %d: %w", i, ErrInvalidJobType)
		}

		job := NewJob(item.JobType, item.Payload)
		for _, opt := range item.Options {
			opt(job)
		}

		if err := validateJobInputs(job); err != nil {
			return nil, fmt.Errorf("batch item %d: %w", i, err)
		}
		if len(job.DependsOn) > 0 {
			return nil, fmt.Errorf("batch item %d: %w", i, ErrBatchDependsOn)
		}
		if job.unique {
			return nil, fmt.Errorf("batch item %d: %w", i, ErrBatchUnique)
		}
		if job.EnqueueAtFront {
			return nil, fmt.Errorf("batch item %d: %w", i, ErrBatchEnqueueAtFront)
		}
		if job.Status != StatusReady {
			return nil, fmt.Errorf("batch item %d: job status must be ready, got %q", i, job.Status)
		}
		if job.ScheduledAt != 0 {
			return nil, fmt.Errorf("batch item %d: ScheduledAt is not supported in EnqueueBatch (use EnqueueAt)", i)
		}

		// Duplicate job ID detection within the batch.
		if prev, ok := seenIDs[job.ID]; ok {
			return nil, fmt.Errorf("batch item %d: duplicate job ID %q (same as item %d)", i, job.ID, prev)
		}
		seenIDs[job.ID] = i

		// Serialize now so ToMap errors fail the whole batch before any
		// pipeline commands are queued.
		jobMap, err := job.ToMap()
		if err != nil {
			return nil, fmt.Errorf("batch item %d: converting to map: %w", i, err)
		}

		jobs[i] = job
		jobMaps[i] = jobMap
		uniqueQueues[job.Queue] = struct{}{}
	}

	// Check context before building pipeline.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}

	// Phase 2: build a single pipeline from pre-validated data.
	queuesKey := c.rc.Key("queues")
	pipe := c.rc.rdb.Pipeline()

	for i, job := range jobs {
		jobKey := c.rc.Key("job", job.ID)
		queueKey := c.rc.Key("queue", job.Queue, "ready")

		pipe.HSet(ctx, jobKey, jobMaps[i])
		pipe.LPush(ctx, queueKey, job.ID)
	}

	// Single SAdd for queues not yet cached.
	newQueues := make([]any, 0, len(uniqueQueues))
	for q := range uniqueQueues {
		if _, loaded := c.knownQueues.LoadOrStore(q, struct{}{}); !loaded {
			newQueues = append(newQueues, q)
		}
	}
	if len(newQueues) > 0 {
		pipe.SAdd(ctx, queuesKey, newQueues...)
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("executing batch enqueue pipeline: %w", err)
	}

	return jobs, nil
}

// GetJob retrieves a job by ID from Redis.
func (c *Client) GetJob(ctx context.Context, jobID string) (*Job, error) {
	jobKey := c.rc.Key("job", jobID)
	result, err := c.rc.rdb.HGetAll(ctx, jobKey).Result()
	if err != nil {
		return nil, fmt.Errorf("getting job %s: %w", jobID, err)
	}
	if len(result) == 0 {
		return nil, ErrJobNotFound
	}

	job, err := JobFromMap(result)
	if err != nil {
		return nil, fmt.Errorf("parsing job %s: %w", jobID, err)
	}
	return job, nil
}

// RedisClient returns the underlying RedisClient for testing/advanced use.
func (c *Client) RedisClient() *redis.Client {
	return c.rc.rdb
}
