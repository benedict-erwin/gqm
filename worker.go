package gqm

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	// dequeueTimeout is the BLMOVE/poll timeout before looping.
	dequeueTimeout = 1 * time.Second

	// maxAbandonedHandlers is the per-pool limit for leaked handler goroutines.
	// When reached, the pool logs a critical warning on every new abandon.
	maxAbandonedHandlers = 100
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

	// abandonedHandlers counts handler goroutines that were abandoned after
	// grace period timeout. These goroutines may still be running.
	abandonedHandlers atomic.Int64

	// lastDequeueErrLog is the unix timestamp (seconds) of the last dequeue
	// error log. Used for rate-limiting to avoid log spam during Redis outages.
	lastDequeueErrLog atomic.Int64
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

		jobID, queue, data, err := p.dequeue(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			// Rate-limited error logging: at most once per 3 seconds per pool.
			now := time.Now().Unix()
			if last := p.lastDequeueErrLog.Load(); now-last >= 3 {
				if p.lastDequeueErrLog.CompareAndSwap(last, now) {
					p.logger.Error("dequeue error", "worker", workerIdx, "error", err)
				}
			}
			continue
		}
		if jobID == "" {
			continue
		}

		// processJob uses a dedicated context with shutdownTimeout so that
		// in-flight jobs can complete during graceful shutdown but won't
		// block forever if Redis becomes unreachable.
		jobCtx, jobCancel := context.WithTimeout(context.Background(), p.server.cfg.shutdownTimeout)
		p.processJob(jobCtx, workerIdx, jobID, queue, data)
		jobCancel()
	}
}

// dequeue attempts to dequeue a job from any of the pool's queues using the
// configured dequeue strategy. If the selected queue is empty, remaining queues
// are tried as fallback. If all queues are empty, sleeps for dequeueTimeout.
func (p *pool) dequeue(ctx context.Context) (string, string, map[string]string, error) {
	queues := p.cfg.queues

	// Build queue visit order based on strategy
	order := p.dequeueOrder()

	for _, idx := range order {
		jobID, data, err := p.dequeueFromQueue(ctx, queues[idx])
		if err != nil {
			return "", "", nil, err
		}
		if jobID != "" {
			return jobID, queues[idx], data, nil
		}
	}

	// No job in any queue — wait before retrying
	timer := time.NewTimer(dequeueTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return "", "", nil, ctx.Err()
	case <-timer.C:
		return "", "", nil, nil
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
// Returns ("", nil) if the queue is empty or the queue is paused.
func (p *pool) dequeueFromQueue(ctx context.Context, queue string) (string, map[string]string, error) {
	rc := p.server.rc

	readyKey := rc.Key("queue", queue, "ready")
	processingKey := rc.Key("queue", queue, "processing")
	jobPrefix := rc.Key("job") + ":"
	pausedKey := rc.Key("paused")

	now := time.Now().Unix()
	globalTimeout := int64(p.server.cfg.globalTimeout.Seconds())
	poolTimeout := int64(p.cfg.jobTimeout.Seconds())

	result := p.server.scripts.run(ctx, rc.rdb, "dequeue",
		[]string{readyKey, processingKey, jobPrefix, pausedKey},
		now, globalTimeout, p.cfg.name, queue, poolTimeout,
	)

	if result.Err() != nil {
		if errors.Is(result.Err(), redis.Nil) {
			return "", nil, nil // Queue empty or paused
		}
		return "", nil, result.Err()
	}

	// Lua returns [job_id, field1, val1, field2, val2, ...].
	arr, err := result.StringSlice()
	if err != nil {
		return "", nil, fmt.Errorf("reading dequeue result: %w", err)
	}
	if len(arr) < 3 {
		return "", nil, nil
	}

	jobID := arr[0]
	data := make(map[string]string, (len(arr)-1)/2)
	for i := 1; i+1 < len(arr); i += 2 {
		data[arr[i]] = arr[i+1]
	}

	return jobID, data, nil
}

// processJob fetches a job, executes the handler, and updates the result.
// The queue parameter is the queue name from which the job was dequeued,
// used as fallback when the job hash cannot be fetched.
func (p *pool) processJob(ctx context.Context, workerIdx int, jobID string, queue string, data map[string]string) {
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

	// Use job data returned from dequeue Lua script.
	// Fall back to HGetAll if data is missing (shouldn't happen in normal flow).
	if len(data) == 0 {
		jobKey := rc.Key("job", jobID)
		var err error
		data, err = rc.rdb.HGetAll(ctx, jobKey).Result()
		if err != nil || len(data) == 0 {
			p.logger.Error("failed to fetch job", "job_id", jobID, "error", err)
			failedJob := &Job{ID: jobID, Queue: queue}
			p.handleFailure(ctx, failedJob, "failed to fetch job data", nil)
			return
		}
	}

	job, err := JobFromMap(data)
	if err != nil {
		p.logger.Error("failed to parse job", "job_id", jobID, "error", err)
		failedJob := &Job{ID: jobID, Queue: queue}
		if q, ok := data["queue"]; ok && q != "" {
			failedJob.Queue = q
		}
		p.handleFailure(ctx, failedJob, "failed to parse job data: "+err.Error(), nil)
		return
	}

	// Find handler
	handler, ok := p.server.handlers[job.Type]
	if !ok {
		p.logger.Error("no handler for job type", "job_id", jobID, "type", job.Type)
		p.handleFailure(ctx, job, "no handler registered for job type: "+job.Type, nil)
		return
	}

	// Resolve timeout for context deadline. The processing set score was already
	// set correctly by dequeue.lua using the same timeout hierarchy.
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
			p.fireCallbacks(ctx, job, handlerErr)
			p.handleFailure(ctx, job, handlerErr.Error(), handlerErr)
		} else {
			p.logger.Info("job completed",
				"job_id", jobID,
				"type", job.Type,
				"duration", elapsed,
			)
			p.fireCallbacks(ctx, job, nil)
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

		graceTimer := time.NewTimer(gracePeriod)
		select {
		case handlerErr := <-resultCh:
			graceTimer.Stop()
			elapsed = time.Since(startTime)
			if handlerErr != nil {
				p.fireCallbacks(ctx, job, handlerErr)
				p.handleFailure(ctx, job, "timeout exceeded, handler stopped: "+handlerErr.Error(), handlerErr)
			} else {
				// Handler completed during grace period
				p.fireCallbacks(ctx, job, nil)
				p.completeJob(ctx, job, elapsed)
			}
		case <-graceTimer.C:
			// Handler still stuck — abandon
			elapsed = time.Since(startTime)
			total := p.abandonedHandlers.Add(1)
			timeoutErr := fmt.Errorf("timeout exceeded, handler abandoned after %v", elapsed)
			if total >= maxAbandonedHandlers {
				p.logger.Error("CRITICAL: abandoned handler limit reached, pool may be leaking goroutines",
					"job_id", jobID,
					"type", job.Type,
					"abandoned_total", total,
					"limit", maxAbandonedHandlers,
				)
			} else {
				p.logger.Warn("handler abandoned after grace period",
					"job_id", jobID,
					"type", job.Type,
					"abandoned_total", total,
				)
			}
			p.fireCallbacks(ctx, job, timeoutErr)
			p.handleFailure(ctx, job, "timeout exceeded, handler abandoned", nil)
			// Drain resultCh in background to decrement counter when handler finishes.
			go func() {
				<-resultCh
				p.abandonedHandlers.Add(-1)
			}()
		}
	}
}

// fireCallbacks invokes the registered OnSuccess/OnFailure/OnComplete
// callbacks for the given job type. Each callback is wrapped in a
// deferred panic recovery so a misbehaving callback never crashes
// the worker.
func (p *pool) fireCallbacks(ctx context.Context, job *Job, handlerErr error) {
	hcfg, ok := p.server.handlerConfigs[job.Type]
	if !ok {
		return
	}

	if handlerErr == nil && hcfg.onSuccess != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					p.logger.Error("OnSuccess callback panic",
						"job_id", job.ID, "type", job.Type, "panic", r,
						"stack", string(debug.Stack()))
				}
			}()
			hcfg.onSuccess(ctx, job)
		}()
	}

	if handlerErr != nil && hcfg.onFailure != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					p.logger.Error("OnFailure callback panic",
						"job_id", job.ID, "type", job.Type, "panic", r,
						"stack", string(debug.Stack()))
				}
			}()
			hcfg.onFailure(ctx, job, handlerErr)
		}()
	}

	if hcfg.onComplete != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					p.logger.Error("OnComplete callback panic",
						"job_id", job.ID, "type", job.Type, "panic", r,
						"stack", string(debug.Stack()))
				}
			}()
			hcfg.onComplete(ctx, job, handlerErr)
		}()
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
	dependentsKey := rc.Key("job", job.ID, "dependents")
	date := time.Now().UTC().Format("2006-01-02")
	dailyStatsKey := rc.Key("stats", job.Queue, "processed", date)
	totalStatsKey := rc.Key("stats", job.Queue, "processed_total")
	const statsTTL = 90 * 24 * 3600 // 90 days in seconds

	result := p.server.scripts.run(ctx, rc.rdb, "complete",
		[]string{processingKey, completedKey, jobKey, dependentsKey, dailyStatsKey, totalStatsKey},
		job.ID, now, "", strconv.FormatInt(durationMS, 10), statsTTL,
	)
	if result.Err() != nil {
		p.logger.Error("failed to complete job in Redis",
			"job_id", job.ID,
			"error", result.Err(),
		)
		return
	}

	// Check Lua return value: 0 = not in processing set, 1 = done, 2 = done + has dependents.
	val, err := result.Int64()
	if err != nil || val == 0 {
		p.logger.Warn("complete script returned 0; job was not in processing set",
			"job_id", job.ID,
		)
		return
	}

	// DAG: resolve dependents only if Lua detected the dependents set exists.
	if val == 2 {
		promoted, err := resolveDependents(ctx, rc, p.server.scripts, job.ID)
		if err != nil {
			p.logger.Error("failed to resolve dependents",
				"job_id", job.ID,
				"error", err,
			)
		} else if len(promoted) > 0 {
			p.logger.Info("DAG: promoted dependent jobs to ready",
				"parent_job_id", job.ID,
				"promoted", promoted,
			)
		}
	}
}

// handleFailure evaluates retry policy and either retries or moves to DLQ.
// Max retry is resolved: job-level > pool-level > 0 (no retry).
//
// handlerErr is the original error returned by the handler (nil for internal
// errors like fetch/parse failures). It is used to check ErrSkipRetry and
// the IsFailure predicate.
func (p *pool) handleFailure(ctx context.Context, job *Job, errMsg string, handlerErr error) {
	// ErrSkipRetry: bypass all retry logic, go straight to DLQ.
	if handlerErr != nil && errors.Is(handlerErr, ErrSkipRetry) {
		p.logger.Info("job skip retry requested",
			"job_id", job.ID,
			"type", job.Type,
			"error", errMsg,
		)
		p.deadLetterJob(ctx, job, errMsg)
		return
	}

	// IsFailure predicate: if set and returns false, retry without
	// incrementing the retry counter (transient/non-failure error).
	countAsFailure := true
	if handlerErr != nil {
		if hcfg, ok := p.server.handlerConfigs[job.Type]; ok && hcfg.isFailure != nil {
			func() {
				defer func() {
					if r := recover(); r != nil {
						p.logger.Error("IsFailure predicate panic, treating as failure",
							"job_id", job.ID, "type", job.Type, "panic", r,
							"stack", string(debug.Stack()),
						)
						countAsFailure = true
					}
				}()
				countAsFailure = hcfg.isFailure(handlerErr)
			}()
		}
	}

	newRetryCount := job.RetryCount + 1
	maxRetry := p.resolveMaxRetry(job)

	if countAsFailure {
		if maxRetry > 0 && newRetryCount <= maxRetry {
			p.retryJob(ctx, job, errMsg, newRetryCount)
		} else {
			p.deadLetterJob(ctx, job, errMsg)
		}
	} else {
		// Non-failure: retry without incrementing counter.
		// Still requires retry to be configured (maxRetry > 0).
		if maxRetry > 0 {
			p.retryJob(ctx, job, errMsg, job.RetryCount)
		} else {
			p.logger.Warn("non-failure error sent to DLQ because maxRetry=0",
				"job_id", job.ID, "type", job.Type, "error", errMsg,
			)
			p.deadLetterJob(ctx, job, errMsg)
		}
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
		return
	}

	val, err := result.Int64()
	if err != nil || val == 0 {
		p.logger.Warn("retry script returned 0; job was not in processing set",
			"job_id", job.ID,
		)
		return
	}

	p.logger.Info("job scheduled for retry",
		"job_id", job.ID,
		"retry_count", newRetryCount,
		"retry_at", time.Unix(retryAt, 0),
	)
}

// deadLetterJob moves a job to the dead letter queue.
func (p *pool) deadLetterJob(ctx context.Context, job *Job, errMsg string) {
	rc := p.server.rc
	now := time.Now().Unix()

	processingKey := rc.Key("queue", job.Queue, "processing")
	dlqKey := rc.Key("queue", job.Queue, "dead_letter")
	jobKey := rc.Key("job", job.ID)
	dependentsKey := rc.Key("job", job.ID, "dependents")
	date := time.Now().UTC().Format("2006-01-02")
	dailyStatsKey := rc.Key("stats", job.Queue, "failed", date)
	totalStatsKey := rc.Key("stats", job.Queue, "failed_total")
	const statsTTL = 90 * 24 * 3600 // 90 days in seconds

	result := p.server.scripts.run(ctx, rc.rdb, "deadletter",
		[]string{processingKey, dlqKey, jobKey, dependentsKey, dailyStatsKey, totalStatsKey},
		job.ID, now, errMsg, statsTTL,
	)
	if result.Err() != nil {
		p.logger.Error("failed to move job to DLQ",
			"job_id", job.ID,
			"error", result.Err(),
		)
		return
	}

	// Check Lua return value: 0 = not in processing set, 1 = done, 2 = done + has dependents.
	val, err := result.Int64()
	if err != nil || val == 0 {
		p.logger.Warn("deadletter script returned 0; job was not in processing set",
			"job_id", job.ID,
		)
		return
	}

	p.logger.Warn("job moved to dead letter queue",
		"job_id", job.ID,
		"type", job.Type,
		"retry_count", job.RetryCount,
		"max_retry", job.MaxRetry,
		"error", errMsg,
	)

	// DAG: propagate failure to dependents only if Lua detected the dependents set exists.
	if val == 2 {
		if err := propagateFailure(ctx, rc, p.server.scripts, job.ID); err != nil {
			p.logger.Error("failed to propagate failure to dependents",
				"job_id", job.ID,
				"error", err,
			)
		}
	}
}

// retryDelay calculates the delay before the next retry attempt.
// Hierarchy: job-level intervals → pool-level backoff policy → default 10s fixed.
func (p *pool) retryDelay(job *Job, retryCount int) time.Duration {
	// Job-level intervals (highest priority)
	if len(job.RetryIntervals) > 0 {
		idx := retryCount - 1
		if idx < 0 {
			idx = 0
		} else if idx >= len(job.RetryIntervals) {
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
			if idx < 0 {
				idx = 0
			} else if idx >= len(rp.Intervals) {
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
		"server_id", p.server.serverID,
		"queues", strings.Join(p.cfg.queues, ","),
		"status", "active",
		"last_heartbeat", now,
		"concurrency", p.cfg.concurrency,
		"started_at", p.server.startedAt.Unix(),
	)
	pipe.SAdd(ctx, rc.Key("workers"), p.cfg.name)
	// Set TTL so stale worker keys auto-expire on crash (3x heartbeat interval).
	pipe.Expire(ctx, workerKey, 3*defaultHeartbeatInterval)

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

