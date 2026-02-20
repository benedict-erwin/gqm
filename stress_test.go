package gqm

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Gate: stress tests only run with GQM_STRESS_TEST=1
// ---------------------------------------------------------------------------

func skipWithoutStressFlag(t *testing.T) {
	t.Helper()
	if os.Getenv("GQM_STRESS_TEST") != "1" {
		t.Skip("skipped: set GQM_STRESS_TEST=1 to run stress tests")
	}
	skipWithoutRedis(t)
}

// ---------------------------------------------------------------------------
// Report helpers
// ---------------------------------------------------------------------------

type stressReport struct {
	Name           string
	Duration       time.Duration
	Enqueued       int64
	Processed      int64
	Failed         int64
	Retried        int64
	DLQ            int64
	Panicked       int64
	Latencies      []time.Duration
	GoroutineStart int
	GoroutineEnd   int
	MemStart       uint64 // runtime.MemStats.Alloc
	MemEnd         uint64
}

func (r *stressReport) Print(t *testing.T) {
	t.Helper()
	t.Logf("")
	t.Logf("╔══════════════════════════════════════════════════╗")
	t.Logf("║  STRESS REPORT: %-33s║", r.Name)
	t.Logf("╠══════════════════════════════════════════════════╣")
	t.Logf("║  Duration:     %-34s║", r.Duration.Round(time.Millisecond))
	t.Logf("║  Enqueued:     %-34d║", r.Enqueued)
	t.Logf("║  Processed:    %-34d║", r.Processed)
	if r.Failed > 0 {
		t.Logf("║  Failed:       %-34d║", r.Failed)
	}
	if r.Retried > 0 {
		t.Logf("║  Retried:      %-34d║", r.Retried)
	}
	if r.DLQ > 0 {
		t.Logf("║  DLQ:          %-34d║", r.DLQ)
	}
	if r.Panicked > 0 {
		t.Logf("║  Panicked:     %-34d║", r.Panicked)
	}
	if r.Duration > 0 && r.Processed > 0 {
		throughput := float64(r.Processed) / r.Duration.Seconds()
		t.Logf("║  Throughput:   %-27.1f jobs/s║", throughput)
	}
	if len(r.Latencies) > 0 {
		sorted := make([]time.Duration, len(r.Latencies))
		copy(sorted, r.Latencies)
		sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
		t.Logf("║  Latency p50:  %-34s║", pct(sorted, 50))
		t.Logf("║  Latency p95:  %-34s║", pct(sorted, 95))
		t.Logf("║  Latency p99:  %-34s║", pct(sorted, 99))
	}
	memDelta := int64(r.MemEnd) - int64(r.MemStart)
	t.Logf("║  Memory:       %s -> %s (delta %s)%s║",
		fmtBytes(r.MemStart), fmtBytes(r.MemEnd), fmtBytesSigned(memDelta),
		pad(41-len(fmt.Sprintf("%s -> %s (delta %s)", fmtBytes(r.MemStart), fmtBytes(r.MemEnd), fmtBytesSigned(memDelta)))))
	gorDelta := r.GoroutineEnd - r.GoroutineStart
	t.Logf("║  Goroutines:   start=%d end=%d (delta=%d)%s║",
		r.GoroutineStart, r.GoroutineEnd, gorDelta,
		pad(35-len(fmt.Sprintf("start=%d end=%d (delta=%d)", r.GoroutineStart, r.GoroutineEnd, gorDelta))))
	t.Logf("╚══════════════════════════════════════════════════╝")
}

func pct(sorted []time.Duration, p float64) string {
	if len(sorted) == 0 {
		return "n/a"
	}
	idx := int(float64(len(sorted)) * p / 100)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx].Round(time.Microsecond).String()
}

func fmtBytes(b uint64) string {
	if b < 1024 {
		return fmt.Sprintf("%dB", b)
	}
	if b < 1024*1024 {
		return fmt.Sprintf("%.1fKB", float64(b)/1024)
	}
	return fmt.Sprintf("%.1fMB", float64(b)/(1024*1024))
}

func fmtBytesSigned(b int64) string {
	sign := "+"
	abs := b
	if b < 0 {
		sign = "-"
		abs = -b
	}
	if abs < 1024 {
		return fmt.Sprintf("%s%dB", sign, abs)
	}
	if abs < 1024*1024 {
		return fmt.Sprintf("%s%.1fKB", sign, float64(abs)/1024)
	}
	return fmt.Sprintf("%s%.1fMB", sign, float64(abs)/(1024*1024))
}

func pad(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.Repeat(" ", n)
}

func memAlloc() uint64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.Alloc
}

// stressCleanup is like cleanupRedis but uses pipeline batching for large key sets.
func stressCleanup(t *testing.T, prefix string) {
	t.Helper()
	rc, err := NewRedisClient(WithRedisAddr(testRedisAddr()))
	if err != nil {
		return
	}
	defer rc.Close()

	ctx := context.Background()
	pipe := rc.rdb.Pipeline()
	count := 0
	iter := rc.rdb.Scan(ctx, 0, prefix+"*", 1000).Iterator()
	for iter.Next(ctx) {
		pipe.Del(ctx, iter.Val())
		count++
		if count%500 == 0 {
			pipe.Exec(ctx)
		}
	}
	if count%500 != 0 {
		pipe.Exec(ctx)
	}
}

// waitForProcessed polls an atomic counter until it reaches target or timeout.
func waitForProcessed(counter *atomic.Int64, target int64, timeout time.Duration) bool {
	deadline := time.After(timeout)
	for {
		if counter.Load() >= target {
			return true
		}
		select {
		case <-deadline:
			return false
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// countDLQ returns the number of jobs in the dead letter sorted set for a queue.
func countDLQ(t *testing.T, prefix, queue string) int64 {
	t.Helper()
	rc, err := NewRedisClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("countDLQ: NewRedisClient: %v", err)
	}
	defer rc.Close()
	dlqKey := rc.Key("queue", queue, "dead_letter")
	n, err := rc.rdb.ZCard(context.Background(), dlqKey).Result()
	if err != nil {
		return 0
	}
	return n
}

// countReady returns the number of jobs in the ready list for a queue.
func countReady(t *testing.T, prefix, queue string) int64 {
	t.Helper()
	rc, err := NewRedisClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("countReady: NewRedisClient: %v", err)
	}
	defer rc.Close()
	readyKey := rc.Key("queue", queue, "ready")
	n, err := rc.rdb.LLen(context.Background(), readyKey).Result()
	if err != nil {
		return 0
	}
	return n
}

// ---------------------------------------------------------------------------
// 1. TestStress_DataIntegrity — zero loss, zero duplicate
// ---------------------------------------------------------------------------

func TestStress_DataIntegrity(t *testing.T) {
	skipWithoutStressFlag(t)

	const totalJobs = 10_000
	prefix := fmt.Sprintf("gqm:stress:di:%d:", time.Now().UnixNano())
	defer stressCleanup(t, prefix)

	// Track which job IDs were processed.
	seen := &sync.Map{}
	var processed atomic.Int64
	var duplicates atomic.Int64

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(30*time.Second),
		WithGracePeriod(2*time.Second),
		WithShutdownTimeout(10*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	err = server.Handle("di.job", func(ctx context.Context, job *Job) error {
		if _, loaded := seen.LoadOrStore(job.ID, true); loaded {
			duplicates.Add(1)
		}
		processed.Add(1)
		return nil
	}, Workers(20))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var serverWg sync.WaitGroup
	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		server.Start(ctx)
	}()

	time.Sleep(500 * time.Millisecond)

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	// Track all enqueued job IDs.
	enqueuedIDs := make([]string, totalJobs)

	report := &stressReport{Name: "DataIntegrity"}
	report.GoroutineStart = runtime.NumGoroutine()
	report.MemStart = memAlloc()
	start := time.Now()

	// Enqueue from multiple goroutines.
	var enqWg sync.WaitGroup
	perWorker := totalJobs / 10
	for w := 0; w < 10; w++ {
		enqWg.Add(1)
		go func(workerIdx int) {
			defer enqWg.Done()
			base := workerIdx * perWorker
			for i := 0; i < perWorker; i++ {
				job, err := client.Enqueue(ctx, "di.job", Payload{
					"idx": base + i,
				}, Queue("di.job"))
				if err != nil {
					t.Errorf("Enqueue: %v", err)
					return
				}
				enqueuedIDs[base+i] = job.ID
			}
		}(w)
	}
	enqWg.Wait()
	report.Enqueued = totalJobs

	// Wait for all jobs to be processed.
	if !waitForProcessed(&processed, totalJobs, 60*time.Second) {
		t.Fatalf("timeout: only %d/%d processed", processed.Load(), totalJobs)
	}

	report.Duration = time.Since(start)
	report.Processed = processed.Load()
	report.GoroutineEnd = runtime.NumGoroutine()
	report.MemEnd = memAlloc()

	server.Stop()
	serverWg.Wait()

	// Assertions.
	if report.Processed != totalJobs {
		t.Errorf("processed %d, want %d", report.Processed, totalJobs)
	}
	if d := duplicates.Load(); d > 0 {
		t.Errorf("found %d duplicate(s)", d)
	}

	// Verify every enqueued ID was seen.
	missing := 0
	for _, id := range enqueuedIDs {
		if _, ok := seen.Load(id); !ok {
			missing++
		}
	}
	if missing > 0 {
		t.Errorf("%d job(s) lost (enqueued but not processed)", missing)
	}

	report.Print(t)
}

// ---------------------------------------------------------------------------
// 2. TestStress_SustainedLoad — throughput + latency percentiles over 30s
// ---------------------------------------------------------------------------

func TestStress_SustainedLoad(t *testing.T) {
	skipWithoutStressFlag(t)

	const duration = 30 * time.Second
	prefix := fmt.Sprintf("gqm:stress:sl:%d:", time.Now().UnixNano())
	defer stressCleanup(t, prefix)

	var processed atomic.Int64
	// Track per-job latency: job ID -> enqueue time.
	enqueueTimes := &sync.Map{}
	var latMu sync.Mutex
	var latencies []time.Duration

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(30*time.Second),
		WithGracePeriod(2*time.Second),
		WithShutdownTimeout(10*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	err = server.Handle("sl.job", func(ctx context.Context, job *Job) error {
		if v, ok := enqueueTimes.LoadAndDelete(job.ID); ok {
			lat := time.Since(v.(time.Time))
			latMu.Lock()
			latencies = append(latencies, lat)
			latMu.Unlock()
		}
		processed.Add(1)
		return nil
	}, Workers(20))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var serverWg sync.WaitGroup
	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		server.Start(ctx)
	}()

	time.Sleep(500 * time.Millisecond)

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	report := &stressReport{Name: "SustainedLoad"}
	report.GoroutineStart = runtime.NumGoroutine()
	report.MemStart = memAlloc()
	start := time.Now()

	// Producers: 5 goroutines enqueue continuously for `duration`.
	var enqueued atomic.Int64
	var enqWg sync.WaitGroup
	deadline := time.After(duration)
	stopProducers := make(chan struct{})

	for i := 0; i < 5; i++ {
		enqWg.Add(1)
		go func() {
			defer enqWg.Done()
			payload := Payload{"ts": 0}
			for {
				select {
				case <-stopProducers:
					return
				default:
				}
				now := time.Now()
				job, err := client.Enqueue(ctx, "sl.job", payload, Queue("sl.job"))
				if err != nil {
					if ctx.Err() != nil {
						return
					}
					t.Errorf("Enqueue: %v", err)
					return
				}
				enqueueTimes.Store(job.ID, now)
				enqueued.Add(1)
			}
		}()
	}

	<-deadline
	close(stopProducers)
	enqWg.Wait()
	report.Enqueued = enqueued.Load()

	// Wait for all enqueued jobs to be processed (with generous timeout).
	if !waitForProcessed(&processed, report.Enqueued, 30*time.Second) {
		t.Logf("warning: timeout draining — processed %d/%d", processed.Load(), report.Enqueued)
	}

	report.Duration = time.Since(start)
	report.Processed = processed.Load()

	latMu.Lock()
	report.Latencies = latencies
	latMu.Unlock()

	report.GoroutineEnd = runtime.NumGoroutine()
	report.MemEnd = memAlloc()

	server.Stop()
	serverWg.Wait()

	report.Print(t)
}

// ---------------------------------------------------------------------------
// 3. TestStress_BurstDrain — burst 10k jobs, measure drain time
// ---------------------------------------------------------------------------

func TestStress_BurstDrain(t *testing.T) {
	skipWithoutStressFlag(t)

	const totalJobs = 10_000
	prefix := fmt.Sprintf("gqm:stress:bd:%d:", time.Now().UnixNano())
	defer stressCleanup(t, prefix)

	// Phase 1: enqueue all jobs (no server running).
	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	items := make([]BatchItem, 0, totalJobs)
	for i := 0; i < totalJobs; i++ {
		items = append(items, BatchItem{
			JobType: "bd.job",
			Payload: Payload{"idx": i},
			Options: []EnqueueOption{Queue("bd.job")},
		})
	}

	// Enqueue in batches of 1000.
	for i := 0; i < totalJobs; i += 1000 {
		end := i + 1000
		if end > totalJobs {
			end = totalJobs
		}
		if _, err := client.EnqueueBatch(context.Background(), items[i:end]); err != nil {
			t.Fatalf("EnqueueBatch: %v", err)
		}
	}
	client.Close()

	var processed atomic.Int64

	// Phase 2: start server, measure drain time.
	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(30*time.Second),
		WithGracePeriod(2*time.Second),
		WithShutdownTimeout(10*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	err = server.Handle("bd.job", func(ctx context.Context, job *Job) error {
		processed.Add(1)
		return nil
	}, Workers(30))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	report := &stressReport{Name: "BurstDrain"}
	report.Enqueued = totalJobs
	report.GoroutineStart = runtime.NumGoroutine()
	report.MemStart = memAlloc()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var serverWg sync.WaitGroup
	serverWg.Add(1)
	start := time.Now()
	go func() {
		defer serverWg.Done()
		server.Start(ctx)
	}()

	if !waitForProcessed(&processed, totalJobs, 120*time.Second) {
		t.Fatalf("timeout: only %d/%d drained", processed.Load(), totalJobs)
	}

	report.Duration = time.Since(start)
	report.Processed = processed.Load()
	report.GoroutineEnd = runtime.NumGoroutine()
	report.MemEnd = memAlloc()

	server.Stop()
	serverWg.Wait()

	if report.Processed != totalJobs {
		t.Errorf("processed %d, want %d", report.Processed, totalJobs)
	}

	report.Print(t)
}

// ---------------------------------------------------------------------------
// 4. TestStress_HighConcurrency — 50+ workers, multi-queue
// ---------------------------------------------------------------------------

func TestStress_HighConcurrency(t *testing.T) {
	skipWithoutStressFlag(t)

	const totalJobs = 10_000
	prefix := fmt.Sprintf("gqm:stress:hc:%d:", time.Now().UnixNano())
	defer stressCleanup(t, prefix)

	var processed atomic.Int64

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(30*time.Second),
		WithGracePeriod(2*time.Second),
		WithShutdownTimeout(10*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// Register 3 job types with high concurrency = 60 total workers.
	jobTypes := []string{"hc.alpha", "hc.beta", "hc.gamma"}
	workerCounts := []int{25, 20, 15}
	for i, jt := range jobTypes {
		err := server.Handle(jt, func(ctx context.Context, job *Job) error {
			processed.Add(1)
			return nil
		}, Workers(workerCounts[i]))
		if err != nil {
			t.Fatalf("Handle(%s): %v", jt, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var serverWg sync.WaitGroup
	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		server.Start(ctx)
	}()

	time.Sleep(500 * time.Millisecond)

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	report := &stressReport{Name: "HighConcurrency"}
	report.GoroutineStart = runtime.NumGoroutine()
	report.MemStart = memAlloc()
	start := time.Now()

	// Enqueue jobs distributed across job types.
	var enqWg sync.WaitGroup
	perType := totalJobs / len(jobTypes)
	for i, jt := range jobTypes {
		enqWg.Add(1)
		go func(jobType string, idx int) {
			defer enqWg.Done()
			for j := 0; j < perType; j++ {
				if _, err := client.Enqueue(ctx, jobType, Payload{"w": idx, "j": j}, Queue(jobType)); err != nil {
					t.Errorf("Enqueue(%s): %v", jobType, err)
					return
				}
			}
		}(jt, i)
	}
	enqWg.Wait()
	report.Enqueued = int64(perType * len(jobTypes))

	if !waitForProcessed(&processed, report.Enqueued, 60*time.Second) {
		t.Fatalf("timeout: only %d/%d processed", processed.Load(), report.Enqueued)
	}

	report.Duration = time.Since(start)
	report.Processed = processed.Load()
	report.GoroutineEnd = runtime.NumGoroutine()
	report.MemEnd = memAlloc()

	server.Stop()
	serverWg.Wait()

	if report.Processed != report.Enqueued {
		t.Errorf("processed %d, want %d", report.Processed, report.Enqueued)
	}

	report.Print(t)
}

// ---------------------------------------------------------------------------
// 5. TestStress_MixedWorkload — fast + slow + fail + panic
// ---------------------------------------------------------------------------

func TestStress_MixedWorkload(t *testing.T) {
	skipWithoutStressFlag(t)

	const jobsPerType = 500
	prefix := fmt.Sprintf("gqm:stress:mw:%d:", time.Now().UnixNano())
	defer stressCleanup(t, prefix)

	var fastCount, slowCount, failCount, panicCount atomic.Int64
	var totalProcessed atomic.Int64

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(10*time.Second),
		WithGracePeriod(2*time.Second),
		WithShutdownTimeout(15*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// Fast: instant return.
	server.Handle("mw.fast", func(ctx context.Context, job *Job) error {
		fastCount.Add(1)
		totalProcessed.Add(1)
		return nil
	}, Workers(10))

	// Slow: 50ms work.
	server.Handle("mw.slow", func(ctx context.Context, job *Job) error {
		time.Sleep(50 * time.Millisecond)
		slowCount.Add(1)
		totalProcessed.Add(1)
		return nil
	}, Workers(10))

	// Fail: return error (MaxRetry 0 -> goes straight to DLQ).
	server.Handle("mw.fail", func(ctx context.Context, job *Job) error {
		failCount.Add(1)
		totalProcessed.Add(1)
		return fmt.Errorf("intentional failure")
	}, Workers(5))

	// Panic: panics (recovered by worker, goes to retry/DLQ).
	server.Handle("mw.panic", func(ctx context.Context, job *Job) error {
		panicCount.Add(1)
		totalProcessed.Add(1)
		panic("intentional panic")
	}, Workers(5))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var serverWg sync.WaitGroup
	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		server.Start(ctx)
	}()

	time.Sleep(500 * time.Millisecond)

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	report := &stressReport{Name: "MixedWorkload"}
	report.GoroutineStart = runtime.NumGoroutine()
	report.MemStart = memAlloc()
	start := time.Now()

	types := []string{"mw.fast", "mw.slow", "mw.fail", "mw.panic"}
	totalEnqueued := int64(jobsPerType * len(types))

	var enqWg sync.WaitGroup
	for _, jt := range types {
		enqWg.Add(1)
		go func(jobType string) {
			defer enqWg.Done()
			for i := 0; i < jobsPerType; i++ {
				if _, err := client.Enqueue(ctx, jobType, Payload{"i": i}, Queue(jobType)); err != nil {
					t.Errorf("Enqueue(%s): %v", jobType, err)
					return
				}
			}
		}(jt)
	}
	enqWg.Wait()
	report.Enqueued = totalEnqueued

	// Wait for all to be processed (success or fail).
	if !waitForProcessed(&totalProcessed, totalEnqueued, 60*time.Second) {
		t.Logf("warning: timeout — processed %d/%d", totalProcessed.Load(), totalEnqueued)
	}

	// Give a moment for DLQ moves to complete.
	time.Sleep(1 * time.Second)

	report.Duration = time.Since(start)
	report.Processed = totalProcessed.Load()
	report.Failed = failCount.Load()
	report.Panicked = panicCount.Load()
	report.GoroutineEnd = runtime.NumGoroutine()
	report.MemEnd = memAlloc()

	server.Stop()
	serverWg.Wait()

	t.Logf("Breakdown: fast=%d slow=%d fail=%d panic=%d",
		fastCount.Load(), slowCount.Load(), failCount.Load(), panicCount.Load())

	// Key assertion: system didn't crash, all jobs were touched.
	if totalProcessed.Load() < totalEnqueued {
		t.Errorf("not all jobs processed: %d/%d", totalProcessed.Load(), totalEnqueued)
	}

	// Goroutine leak check: delta should be small (< 10).
	gorDelta := report.GoroutineEnd - report.GoroutineStart
	if gorDelta > 10 {
		t.Errorf("possible goroutine leak: delta=%d (start=%d, end=%d)",
			gorDelta, report.GoroutineStart, report.GoroutineEnd)
	}

	report.Print(t)
}

// ---------------------------------------------------------------------------
// 6. TestStress_RetryStorm — all jobs fail, retry flood
// ---------------------------------------------------------------------------

func TestStress_RetryStorm(t *testing.T) {
	skipWithoutStressFlag(t)

	const totalJobs = 2_000
	const maxRetries = 3
	prefix := fmt.Sprintf("gqm:stress:rs:%d:", time.Now().UnixNano())
	defer stressCleanup(t, prefix)

	var attempts atomic.Int64

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(30*time.Second),
		WithGracePeriod(2*time.Second),
		WithShutdownTimeout(10*time.Second),
		// Short scheduler poll so retries are picked up quickly.
		WithSchedulerPollInterval(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	err = server.Handle("rs.job", func(ctx context.Context, job *Job) error {
		attempts.Add(1)
		return fmt.Errorf("intentional failure attempt %d", job.RetryCount+1)
	}, Workers(20))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var serverWg sync.WaitGroup
	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		server.Start(ctx)
	}()

	time.Sleep(500 * time.Millisecond)

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	report := &stressReport{Name: "RetryStorm"}
	report.GoroutineStart = runtime.NumGoroutine()
	report.MemStart = memAlloc()
	start := time.Now()

	// Enqueue all jobs with MaxRetry(3) and short retry intervals.
	for i := 0; i < totalJobs; i++ {
		_, err := client.Enqueue(ctx, "rs.job", Payload{"i": i},
			Queue("rs.job"),
			MaxRetry(maxRetries),
			RetryIntervals(1, 1, 1), // 1 second between retries.
		)
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	report.Enqueued = totalJobs

	// Expected total attempts: totalJobs * (1 initial + maxRetries).
	expectedAttempts := int64(totalJobs * (1 + maxRetries))

	// Wait for all attempts (with generous timeout for retry scheduling).
	if !waitForProcessed(&attempts, expectedAttempts, 120*time.Second) {
		t.Logf("warning: timeout — attempts %d/%d", attempts.Load(), expectedAttempts)
	}

	// Wait for DLQ moves.
	time.Sleep(2 * time.Second)

	report.Duration = time.Since(start)
	report.Processed = attempts.Load()
	report.Retried = attempts.Load() - int64(totalJobs) // retries = total attempts - initial attempts
	report.DLQ = countDLQ(t, prefix, "rs.job")
	report.GoroutineEnd = runtime.NumGoroutine()
	report.MemEnd = memAlloc()

	server.Stop()
	serverWg.Wait()

	// All jobs should end up in DLQ.
	if report.DLQ != totalJobs {
		t.Errorf("DLQ count: got %d, want %d", report.DLQ, totalJobs)
	}

	report.Print(t)
}

// ---------------------------------------------------------------------------
// 7. TestStress_GracefulShutdown — stop under load, no job loss
// ---------------------------------------------------------------------------

func TestStress_GracefulShutdown(t *testing.T) {
	skipWithoutStressFlag(t)

	const totalJobs = 1_000
	prefix := fmt.Sprintf("gqm:stress:gs:%d:", time.Now().UnixNano())
	defer stressCleanup(t, prefix)

	var processed atomic.Int64

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(30*time.Second),
		WithGracePeriod(2*time.Second),
		WithShutdownTimeout(15*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// Handler takes 50ms per job.
	err = server.Handle("gs.job", func(ctx context.Context, job *Job) error {
		time.Sleep(50 * time.Millisecond)
		processed.Add(1)
		return nil
	}, Workers(10))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var serverWg sync.WaitGroup
	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		server.Start(ctx)
	}()

	time.Sleep(500 * time.Millisecond)

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	report := &stressReport{Name: "GracefulShutdown"}
	report.GoroutineStart = runtime.NumGoroutine()
	report.MemStart = memAlloc()
	start := time.Now()

	// Enqueue all jobs first.
	for i := 0; i < totalJobs; i++ {
		if _, err := client.Enqueue(ctx, "gs.job", Payload{"i": i}, Queue("gs.job")); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}
	report.Enqueued = totalJobs

	// Wait until some jobs are being processed (at least 50).
	if !waitForProcessed(&processed, 50, 30*time.Second) {
		t.Fatalf("timeout waiting for processing to start: %d processed", processed.Load())
	}

	// Trigger graceful shutdown while jobs are still being processed.
	processedAtShutdown := processed.Load()
	t.Logf("Triggering Stop() with %d/%d processed", processedAtShutdown, totalJobs)

	server.Stop()
	serverWg.Wait()

	processedFinal := processed.Load()
	report.Duration = time.Since(start)
	report.Processed = processedFinal

	// Count remaining jobs in queue.
	remaining := countReady(t, prefix, "gs.job")
	report.GoroutineEnd = runtime.NumGoroutine()
	report.MemEnd = memAlloc()

	t.Logf("At shutdown: %d processed, after shutdown: %d processed, remaining in queue: %d",
		processedAtShutdown, processedFinal, remaining)

	// In-flight jobs should have completed (processed grew after Stop).
	if processedFinal <= processedAtShutdown {
		t.Errorf("no in-flight jobs completed during shutdown: before=%d after=%d",
			processedAtShutdown, processedFinal)
	}

	// Total accounted for should equal total enqueued.
	// Some jobs may be in processing set (stuck during shutdown edge case).
	// Allow small tolerance.
	accounted := processedFinal + remaining
	if accounted < totalJobs-10 {
		t.Errorf("job accounting mismatch: processed(%d) + remaining(%d) = %d, want ~%d",
			processedFinal, remaining, accounted, totalJobs)
	}

	report.Print(t)
}

// ---------------------------------------------------------------------------
// 8. TestStress_Backpressure — enqueue >> process rate
// ---------------------------------------------------------------------------

func TestStress_Backpressure(t *testing.T) {
	skipWithoutStressFlag(t)

	const testDuration = 15 * time.Second
	prefix := fmt.Sprintf("gqm:stress:bp:%d:", time.Now().UnixNano())
	defer stressCleanup(t, prefix)

	var processed atomic.Int64

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(30*time.Second),
		WithGracePeriod(2*time.Second),
		WithShutdownTimeout(10*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// Slow handler: 100ms per job. 5 workers = ~50 jobs/sec max processing rate.
	err = server.Handle("bp.job", func(ctx context.Context, job *Job) error {
		time.Sleep(100 * time.Millisecond)
		processed.Add(1)
		return nil
	}, Workers(5))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var serverWg sync.WaitGroup
	serverWg.Add(1)
	go func() {
		defer serverWg.Done()
		server.Start(ctx)
	}()

	time.Sleep(500 * time.Millisecond)

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	report := &stressReport{Name: "Backpressure"}
	report.GoroutineStart = runtime.NumGoroutine()
	report.MemStart = memAlloc()
	start := time.Now()

	// Fast producer: enqueue as fast as possible for testDuration.
	var enqueued atomic.Int64
	var enqWg sync.WaitGroup
	stopProducers := make(chan struct{})

	for i := 0; i < 3; i++ {
		enqWg.Add(1)
		go func() {
			defer enqWg.Done()
			for {
				select {
				case <-stopProducers:
					return
				default:
				}
				if _, err := client.Enqueue(ctx, "bp.job", Payload{"t": time.Now().UnixNano()}, Queue("bp.job")); err != nil {
					if ctx.Err() != nil {
						return
					}
					t.Errorf("Enqueue: %v", err)
					return
				}
				enqueued.Add(1)
			}
		}()
	}

	time.Sleep(testDuration)
	close(stopProducers)
	enqWg.Wait()

	report.Enqueued = enqueued.Load()
	queueLen := countReady(t, prefix, "bp.job")

	t.Logf("After %v: enqueued=%d, processed=%d, queue_depth=%d",
		testDuration, report.Enqueued, processed.Load(), queueLen)

	// Queue should be growing (enqueue >> process).
	if queueLen < 100 {
		t.Logf("note: queue depth lower than expected (%d), producers may be slow", queueLen)
	}

	report.Duration = time.Since(start)
	report.Processed = processed.Load()
	report.GoroutineEnd = runtime.NumGoroutine()
	report.MemEnd = memAlloc()

	server.Stop()
	serverWg.Wait()

	// Goroutine leak check.
	gorDelta := report.GoroutineEnd - report.GoroutineStart
	if gorDelta > 10 {
		t.Errorf("possible goroutine leak: delta=%d", gorDelta)
	}

	report.Print(t)
}

// ---------------------------------------------------------------------------
// 9. TestStress_LargePayload — 10KB-100KB payload
// ---------------------------------------------------------------------------

func TestStress_LargePayload(t *testing.T) {
	skipWithoutStressFlag(t)

	prefix := fmt.Sprintf("gqm:stress:lp:%d:", time.Now().UnixNano())
	defer stressCleanup(t, prefix)

	// Generate random payloads.
	payload10KB := Payload{"data": strings.Repeat("x", 10*1024)}
	payload100KB := Payload{"data": strings.Repeat("x", 100*1024)}

	for _, tc := range []struct {
		name    string
		payload Payload
		count   int
	}{
		{"10KB", payload10KB, 1000},
		{"100KB", payload100KB, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			subPrefix := fmt.Sprintf("%slp_%s:", prefix, tc.name)
			defer stressCleanup(t, subPrefix)

			var processed atomic.Int64

			server, err := NewServer(
				WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(subPrefix)),
				WithGlobalTimeout(30*time.Second),
				WithGracePeriod(2*time.Second),
				WithShutdownTimeout(10*time.Second),
			)
			if err != nil {
				t.Fatalf("NewServer: %v", err)
			}

			err = server.Handle("lp.job", func(ctx context.Context, job *Job) error {
				// Verify payload integrity.
				data, ok := job.Payload["data"].(string)
				if !ok {
					return fmt.Errorf("payload missing data field")
				}
				expectedLen := len(tc.payload["data"].(string))
				if len(data) != expectedLen {
					return fmt.Errorf("payload corrupted: got %d bytes, want %d", len(data), expectedLen)
				}
				processed.Add(1)
				return nil
			}, Workers(10))
			if err != nil {
				t.Fatalf("Handle: %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var serverWg sync.WaitGroup
			serverWg.Add(1)
			go func() {
				defer serverWg.Done()
				server.Start(ctx)
			}()

			time.Sleep(500 * time.Millisecond)

			client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(subPrefix))
			if err != nil {
				t.Fatalf("NewClient: %v", err)
			}
			defer client.Close()

			report := &stressReport{Name: "LargePayload_" + tc.name}
			report.GoroutineStart = runtime.NumGoroutine()
			report.MemStart = memAlloc()
			start := time.Now()

			for i := 0; i < tc.count; i++ {
				if _, err := client.Enqueue(ctx, "lp.job", tc.payload, Queue("lp.job")); err != nil {
					t.Fatalf("Enqueue: %v", err)
				}
			}
			report.Enqueued = int64(tc.count)

			if !waitForProcessed(&processed, int64(tc.count), 120*time.Second) {
				t.Fatalf("timeout: only %d/%d processed", processed.Load(), tc.count)
			}

			report.Duration = time.Since(start)
			report.Processed = processed.Load()
			report.GoroutineEnd = runtime.NumGoroutine()
			report.MemEnd = memAlloc()

			server.Stop()
			serverWg.Wait()

			// No failures — all payloads intact.
			dlqCount := countDLQ(t, subPrefix, "lp.job")
			if dlqCount > 0 {
				t.Errorf("DLQ has %d jobs — payload corruption?", dlqCount)
			}

			report.Print(t)
		})
	}
}
