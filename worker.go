package gqm

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"strconv"
	"sync"
	"time"
)

const (
	// dequeueTimeout is the BLMOVE/poll timeout before looping.
	dequeueTimeout = 5 * time.Second
)

// pool manages a set of worker goroutines for processing jobs.
type pool struct {
	cfg     *poolConfig
	server  *Server
	logger  *slog.Logger

	// activeJobs tracks which worker is processing which job.
	// Protected by mu. Worker goroutine Lock() on update, heartbeat RLock() on read.
	mu         sync.RWMutex
	activeJobs map[int]string // workerIdx -> jobID
}

func newPool(cfg *poolConfig, server *Server) *pool {
	return &pool{
		cfg:        cfg,
		server:     server,
		logger:     server.logger.With("pool", cfg.name),
		activeJobs: make(map[int]string, cfg.concurrency),
	}
}

// run starts the pool's worker goroutines and heartbeat, blocking until ctx is done.
// When ctx is cancelled (shutdown signal), workers stop dequeuing new jobs but
// in-flight jobs are allowed to complete within the pool's shutdown timeout.
func (p *pool) run(ctx context.Context) {
	p.logger.Info("pool starting",
		"concurrency", p.cfg.concurrency,
		"queue", p.cfg.queue,
	)

	var wg sync.WaitGroup

	// Start heartbeat goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		p.heartbeatLoop(ctx)
	}()

	// Start worker goroutines
	for i := 0; i < p.cfg.concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			p.workerLoop(ctx, idx)
		}(i)
	}

	wg.Wait()
	p.logger.Info("pool stopped")
}

// workerLoop is the main loop for a single worker goroutine.
func (p *pool) workerLoop(ctx context.Context, workerIdx int) {
	p.logger.Debug("worker started", "worker", workerIdx)
	defer p.logger.Debug("worker stopped", "worker", workerIdx)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		jobID, err := p.dequeue(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Dequeue timeout or transient error — loop again
			continue
		}
		if jobID == "" {
			continue
		}

		// processJob uses context.Background() for job execution so that
		// in-flight jobs can complete during graceful shutdown.
		// The pool context (ctx) only controls the dequeue loop.
		p.processJob(context.Background(), workerIdx, jobID)
	}
}

// dequeue attempts to atomically dequeue a job using the Lua script.
func (p *pool) dequeue(ctx context.Context) (string, error) {
	rc := p.server.rc

	readyKey := rc.Key("queue", p.cfg.queue, "ready")
	processingKey := rc.Key("queue", p.cfg.queue, "processing")
	jobPrefix := rc.Key("job") + ":"

	// Use BRPOP with timeout, then process via Lua for atomicity
	// First try: use the dequeue Lua script with RPOP (non-blocking)
	now := time.Now().Unix()
	timeout := int64(p.server.cfg.globalTimeout.Seconds())

	result := p.server.scripts.run(ctx, rc.rdb, "dequeue",
		[]string{readyKey, processingKey, jobPrefix},
		now, timeout, p.cfg.name,
	)

	if result.Err() != nil {
		// No job available
		if result.Err().Error() == "redis: nil" {
			// Wait a bit before retrying (simulates BLMOVE timeout)
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(dequeueTimeout):
				return "", nil
			}
		}
		return "", result.Err()
	}

	jobID, err := result.Text()
	if err != nil {
		return "", fmt.Errorf("reading dequeue result: %w", err)
	}

	return jobID, nil
}

// processJob fetches a job, executes the handler, and updates the result.
func (p *pool) processJob(ctx context.Context, workerIdx int, jobID string) {
	rc := p.server.rc

	// Track active job
	p.mu.Lock()
	p.activeJobs[workerIdx] = jobID
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		p.activeJobs[workerIdx] = ""
		p.mu.Unlock()
	}()

	// Fetch job data
	jobKey := rc.Key("job", jobID)
	result, err := rc.rdb.HGetAll(ctx, jobKey).Result()
	if err != nil || len(result) == 0 {
		p.logger.Error("failed to fetch job", "job_id", jobID, "error", err)
		return
	}

	job, err := JobFromMap(result)
	if err != nil {
		p.logger.Error("failed to parse job", "job_id", jobID, "error", err)
		return
	}

	// Find handler
	handler, ok := p.server.handlers[job.Type]
	if !ok {
		p.logger.Error("no handler for job type", "job_id", jobID, "type", job.Type)
		p.failJob(ctx, job, "no handler registered for job type: "+job.Type)
		return
	}

	// Resolve timeout and create context
	timeout := p.server.resolveTimeout(job, p.cfg)
	jobCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Execute handler in a separate goroutine (Opsi B model)
	startTime := time.Now()
	resultCh := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				stack := string(debug.Stack())
				p.logger.Error("handler panic recovered",
					"job_id", jobID,
					"type", job.Type,
					"panic", r,
					"stack", stack,
				)
				resultCh <- fmt.Errorf("panic: %v", r)
			}
		}()
		resultCh <- handler(jobCtx, job)
	}()

	// Wait for handler result, timeout, or grace period
	select {
	case handlerErr := <-resultCh:
		elapsed := time.Since(startTime)
		if handlerErr != nil {
			p.logger.Info("job failed",
				"job_id", jobID,
				"type", job.Type,
				"error", handlerErr,
				"duration", elapsed,
			)
			p.handleFailure(ctx, job, handlerErr.Error(), elapsed)
		} else {
			p.logger.Info("job completed",
				"job_id", jobID,
				"type", job.Type,
				"duration", elapsed,
			)
			p.completeJob(ctx, job, elapsed)
		}

	case <-jobCtx.Done():
		// Timeout reached — cancel context, wait grace period
		cancel()
		elapsed := time.Since(startTime)
		p.logger.Warn("job timeout, waiting grace period",
			"job_id", jobID,
			"type", job.Type,
			"timeout", timeout,
			"grace_period", p.cfg.gracePeriod,
		)

		gracePeriod := p.cfg.gracePeriod
		if gracePeriod == 0 {
			gracePeriod = defaultGracePeriod
		}

		select {
		case handlerErr := <-resultCh:
			elapsed = time.Since(startTime)
			if handlerErr != nil {
				p.handleFailure(ctx, job, "timeout exceeded, handler stopped: "+handlerErr.Error(), elapsed)
			} else {
				// Handler completed during grace period
				p.completeJob(ctx, job, elapsed)
			}
		case <-time.After(gracePeriod):
			// Handler still stuck — abandon
			elapsed = time.Since(startTime)
			p.logger.Warn("handler abandoned after grace period",
				"job_id", jobID,
				"type", job.Type,
			)
			p.handleFailure(ctx, job, "timeout exceeded, handler abandoned", elapsed)
		}
	}
}

// completeJob marks a job as completed in Redis using the Lua script.
func (p *pool) completeJob(ctx context.Context, job *Job, elapsed time.Duration) {
	rc := p.server.rc
	now := time.Now().Unix()
	durationMS := elapsed.Milliseconds()

	processingKey := rc.Key("queue", job.Queue, "processing")
	completedKey := rc.Key("queue", job.Queue, "completed")
	jobKey := rc.Key("job", job.ID)

	result := p.server.scripts.run(ctx, rc.rdb, "complete",
		[]string{processingKey, completedKey, jobKey},
		job.ID, now, "", strconv.FormatInt(durationMS, 10),
	)
	if result.Err() != nil {
		p.logger.Error("failed to complete job in Redis",
			"job_id", job.ID,
			"error", result.Err(),
		)
	}
}

// failJob marks a job as failed without retry evaluation.
func (p *pool) failJob(ctx context.Context, job *Job, errMsg string) {
	rc := p.server.rc
	now := time.Now().Unix()

	processingKey := rc.Key("queue", job.Queue, "processing")
	failedKey := rc.Key("queue", job.Queue, "failed")
	jobKey := rc.Key("job", job.ID)

	result := p.server.scripts.run(ctx, rc.rdb, "fail",
		[]string{processingKey, failedKey, jobKey},
		job.ID, now, errMsg, "0",
	)
	if result.Err() != nil {
		p.logger.Error("failed to mark job as failed in Redis",
			"job_id", job.ID,
			"error", result.Err(),
		)
	}
}

// handleFailure evaluates retry policy and either retries or moves to DLQ.
func (p *pool) handleFailure(ctx context.Context, job *Job, errMsg string, elapsed time.Duration) {
	newRetryCount := job.RetryCount + 1

	if newRetryCount <= job.MaxRetry {
		p.retryJob(ctx, job, errMsg, newRetryCount)
	} else {
		p.deadLetterJob(ctx, job, errMsg)
	}
}

// retryJob schedules a job for retry.
func (p *pool) retryJob(ctx context.Context, job *Job, errMsg string, newRetryCount int) {
	rc := p.server.rc

	// Calculate retry delay
	delay := p.retryDelay(job, newRetryCount)
	retryAt := time.Now().Add(delay).Unix()

	processingKey := rc.Key("queue", job.Queue, "processing")
	scheduledKey := rc.Key("scheduled")
	jobKey := rc.Key("job", job.ID)

	result := p.server.scripts.run(ctx, rc.rdb, "retry",
		[]string{processingKey, scheduledKey, jobKey},
		job.ID, retryAt, errMsg, newRetryCount,
	)
	if result.Err() != nil {
		p.logger.Error("failed to retry job",
			"job_id", job.ID,
			"error", result.Err(),
		)
	} else {
		p.logger.Info("job scheduled for retry",
			"job_id", job.ID,
			"retry_count", newRetryCount,
			"retry_at", time.Unix(retryAt, 0),
		)
	}
}

// deadLetterJob moves a job to the dead letter queue.
func (p *pool) deadLetterJob(ctx context.Context, job *Job, errMsg string) {
	rc := p.server.rc
	now := time.Now().Unix()

	processingKey := rc.Key("queue", job.Queue, "processing")
	dlqKey := rc.Key("queue", job.Queue, "dead_letter")
	jobKey := rc.Key("job", job.ID)

	result := p.server.scripts.run(ctx, rc.rdb, "deadletter",
		[]string{processingKey, dlqKey, jobKey},
		job.ID, now, errMsg,
	)
	if result.Err() != nil {
		p.logger.Error("failed to move job to DLQ",
			"job_id", job.ID,
			"error", result.Err(),
		)
	} else {
		p.logger.Warn("job moved to dead letter queue",
			"job_id", job.ID,
			"type", job.Type,
			"retry_count", job.RetryCount,
			"max_retry", job.MaxRetry,
		)
	}
}

// retryDelay calculates the delay before the next retry attempt.
// Uses fixed intervals from job config, falling back to a default.
func (p *pool) retryDelay(job *Job, retryCount int) time.Duration {
	if len(job.RetryIntervals) > 0 {
		idx := retryCount - 1
		if idx >= len(job.RetryIntervals) {
			idx = len(job.RetryIntervals) - 1
		}
		return time.Duration(job.RetryIntervals[idx]) * time.Second
	}
	// Default: 10 seconds fixed interval
	return 10 * time.Second
}

// heartbeatLoop runs the heartbeat goroutine for this pool.
func (p *pool) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(defaultHeartbeatInterval)
	defer ticker.Stop()

	p.logger.Debug("heartbeat started")
	defer p.logger.Debug("heartbeat stopped")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.sendHeartbeat(ctx)
		}
	}
}

// sendHeartbeat updates pool and per-job heartbeat in Redis.
func (p *pool) sendHeartbeat(ctx context.Context) {
	rc := p.server.rc
	now := time.Now().UnixNano()

	// Update pool heartbeat
	workerKey := rc.Key("worker", p.cfg.name)
	pipe := rc.rdb.Pipeline()
	pipe.HSet(ctx, workerKey,
		"id", p.cfg.name,
		"pool", p.cfg.name,
		"queue", p.cfg.queue,
		"status", "active",
		"last_heartbeat", now,
		"concurrency", p.cfg.concurrency,
	)
	pipe.SAdd(ctx, rc.Key("workers"), p.cfg.name)

	// Update per-job heartbeat
	p.mu.RLock()
	for _, jobID := range p.activeJobs {
		if jobID != "" {
			jobKey := rc.Key("job", jobID)
			pipe.HSet(ctx, jobKey, "last_heartbeat", now)
		}
	}
	p.mu.RUnlock()

	if _, err := pipe.Exec(ctx); err != nil {
		if ctx.Err() == nil {
			p.logger.Error("heartbeat update failed", "error", err)
		}
	}
}
