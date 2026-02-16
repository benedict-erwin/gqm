package gqm

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client is used to enqueue jobs into the queue system.
type Client struct {
	rc *RedisClient
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
// Returns the created job or an error.
func (c *Client) Enqueue(ctx context.Context, jobType string, payload Payload, opts ...EnqueueOption) (*Job, error) {
	if jobType == "" {
		return nil, ErrInvalidJobType
	}

	job := NewJob(jobType, payload)
	for _, opt := range opts {
		opt(job)
	}

	jobMap, err := job.ToMap()
	if err != nil {
		return nil, fmt.Errorf("converting job to map: %w", err)
	}

	jobKey := c.rc.Key("job", job.ID)
	queueKey := c.rc.Key("queue", job.Queue, "ready")
	queuesKey := c.rc.Key("queues")

	pipe := c.rc.rdb.Pipeline()
	pipe.HSet(ctx, jobKey, jobMap)
	pipe.LPush(ctx, queueKey, job.ID)
	pipe.SAdd(ctx, queuesKey, job.Queue)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("enqueuing job %s: %w", job.ID, err)
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
	job.Status = StatusScheduled
	job.ScheduledAt = at.UnixNano()

	jobMap, err := job.ToMap()
	if err != nil {
		return nil, fmt.Errorf("converting job to map: %w", err)
	}

	jobKey := c.rc.Key("job", job.ID)
	scheduledKey := c.rc.Key("scheduled")
	queuesKey := c.rc.Key("queues")

	pipe := c.rc.rdb.Pipeline()
	pipe.HSet(ctx, jobKey, jobMap)
	pipe.ZAdd(ctx, scheduledKey, redis.Z{Score: float64(at.Unix()), Member: job.ID})
	pipe.SAdd(ctx, queuesKey, job.Queue)
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, fmt.Errorf("scheduling job %s: %w", job.ID, err)
	}

	return job, nil
}

// EnqueueIn creates a new job scheduled for execution after the given delay.
func (c *Client) EnqueueIn(ctx context.Context, delay time.Duration, jobType string, payload Payload, opts ...EnqueueOption) (*Job, error) {
	return c.EnqueueAt(ctx, time.Now().Add(delay), jobType, payload, opts...)
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
