package gqm

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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

	// rrCounter is the round-robin rotation counter, incremented atomically
	// by each dequeue call to rotate the starting queue index.
	rrCounter atomic.Uint64
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
		"queues", p.cfg.queues,
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

// dequeue attempts to dequeue a job from any of the pool's queues using the
// configured dequeue strategy. If the selected queue is empty, remaining queues
// are tried as fallback. If all queues are empty, sleeps for dequeueTimeout.
func (p *pool) dequeue(ctx context.Context) (string, error) {
	queues := p.cfg.queues

	// Build queue visit order based on strategy
	order := p.dequeueOrder()

	for _, idx := range order {
		jobID, err := p.dequeueFromQueue(ctx, queues[idx])
		if err != nil {
			return "", err
		}
		if jobID != "" {
			return jobID, nil
		}
	}

	// No job in any queue — wait before retrying
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(dequeueTimeout):
		return "", nil
	}
}

// dequeueOrder returns the queue index visit order based on the pool's strategy.
func (p *pool) dequeueOrder() []int {
	n := len(p.cfg.queues)
	if n <= 1 {
		return []int{0}
	}

	switch p.cfg.dequeueStrategy {
	case StrategyRoundRobin:
		return p.roundRobinOrder(n)
	case StrategyWeighted:
		return p.weightedOrder(n)
	default: // StrategyStrict
		return p.strictOrder(n)
	}
}

// strictOrder returns queues in their original priority order: [0, 1, 2, ...].
// Higher-priority queues are always checked first; lower queues may starve.
func (p *pool) strictOrder(n int) []int {
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	return order
}

// roundRobinOrder rotates the starting queue each call, wrapping around.
// All queues get equal opportunity regardless of position.
func (p *pool) roundRobinOrder(n int) []int {
	start := int(p.rrCounter.Add(1)-1) % n
	order := make([]int, n)
	for i := range order {
		order[i] = (start + i) % n
	}
	return order
}

// weightedOrder selects a starting queue probabilistically based on position
// weight (first queue = highest weight), then falls back to remaining queues
// in strict priority order. This provides priority without starvation.
//
// Weights: queue[0]=N, queue[1]=N-1, ..., queue[N-1]=1.
func (p *pool) weightedOrder(n int) []int {
	// Total weight = n*(n+1)/2
	totalWeight := n * (n + 1) / 2
	r := rand.IntN(totalWeight)

	// Find which queue the random value maps to
	startIdx := 0
	cumulative := 0
	for i := 0; i < n; i++ {
		cumulative += n - i // weight of queue[i]
		if r < cumulative {
			startIdx = i
			break
		}
	}

	// Build order: selected queue first, then remaining in strict order
	order := make([]int, 0, n)
	order = append(order, startIdx)
	for i := 0; i < n; i++ {
		if i != startIdx {
			order = append(order, i)
		}
	}
	return order
}

// dequeueFromQueue attempts to atomically dequeue a single job from the given queue.
// Returns ("", nil) if the queue is empty.
func (p *pool) dequeueFromQueue(ctx context.Context, queue string) (string, error) {
	rc := p.server.rc

	readyKey := rc.Key("queue", queue, "ready")
	processingKey := rc.Key("queue", queue, "processing")
	jobPrefix := rc.Key("job") + ":"

	now := time.Now().Unix()
	timeout := int64(p.server.cfg.globalTimeout.Seconds())

	result := p.server.scripts.run(ctx, rc.rdb, "dequeue",
		[]string{readyKey, processingKey, jobPrefix},
		now, timeout, p.cfg.name,
	)

	if result.Err() != nil {
		if result.Err().Error() == "redis: nil" {
			return "", nil // Queue empty
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
// Max retry is resolved: job-level > pool-level > 0 (no retry).
func (p *pool) handleFailure(ctx context.Context, job *Job, errMsg string, _ time.Duration) {
	newRetryCount := job.RetryCount + 1
	maxRetry := p.resolveMaxRetry(job)

	if maxRetry > 0 && newRetryCount <= maxRetry {
		p.retryJob(ctx, job, errMsg, newRetryCount)
	} else {
		p.deadLetterJob(ctx, job, errMsg)
	}
}

// resolveMaxRetry returns the effective max retry for a job.
// Hierarchy: job-level (if > 0) → pool-level → 0 (no retry).
func (p *pool) resolveMaxRetry(job *Job) int {
	if job.MaxRetry > 0 {
		return job.MaxRetry
	}
	if p.cfg.retryPolicy != nil && p.cfg.retryPolicy.MaxRetry > 0 {
		return p.cfg.retryPolicy.MaxRetry
	}
	return 0
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
// Hierarchy: job-level intervals → pool-level backoff policy → default 10s fixed.
func (p *pool) retryDelay(job *Job, retryCount int) time.Duration {
	// Job-level intervals (highest priority)
	if len(job.RetryIntervals) > 0 {
		idx := retryCount - 1
		if idx >= len(job.RetryIntervals) {
			idx = len(job.RetryIntervals) - 1
		}
		return time.Duration(job.RetryIntervals[idx]) * time.Second
	}

	// Pool-level retry policy
	if rp := p.cfg.retryPolicy; rp != nil {
		return p.poolRetryDelay(rp, retryCount)
	}

	// Global default
	return defaultRetryDelay
}

// poolRetryDelay calculates delay based on the pool's retry policy.
func (p *pool) poolRetryDelay(rp *RetryPolicy, retryCount int) time.Duration {
	switch rp.Backoff {
	case BackoffExponential:
		base := rp.BackoffBase
		if base <= 0 {
			base = defaultRetryDelay
		}
		// base * 2^(attempt-1)
		delay := base
		for i := 1; i < retryCount; i++ {
			delay *= 2
			if rp.BackoffMax > 0 && delay > rp.BackoffMax {
				delay = rp.BackoffMax
				break
			}
		}
		return delay

	case BackoffCustom:
		if len(rp.Intervals) > 0 {
			idx := retryCount - 1
			if idx >= len(rp.Intervals) {
				idx = len(rp.Intervals) - 1
			}
			return time.Duration(rp.Intervals[idx]) * time.Second
		}
		if rp.BackoffBase > 0 {
			return rp.BackoffBase
		}
		return defaultRetryDelay

	default: // BackoffFixed or empty
		if rp.BackoffBase > 0 {
			return rp.BackoffBase
		}
		return defaultRetryDelay
	}
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
		"queues", strings.Join(p.cfg.queues, ","),
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
