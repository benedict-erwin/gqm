package gqm

import (
	"context"
	"fmt"

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
