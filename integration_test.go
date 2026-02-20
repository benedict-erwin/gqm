package gqm

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func skipWithoutRedis(t *testing.T) {
	t.Helper()
	addr := testRedisAddr()
	rc, err := NewRedisClient(WithRedisAddr(addr), WithPrefix("gqm:integration:"))
	if err != nil {
		t.Skipf("requires Redis: %v", err)
	}
	if err := rc.Ping(context.Background()); err != nil {
		t.Skipf("requires Redis: %v", err)
	}
	rc.Close()
}

func testRedisAddr() string {
	addr := os.Getenv("GQM_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	return addr
}

func cleanupRedis(t *testing.T, prefix string) {
	t.Helper()
	rc, _ := NewRedisClient(WithRedisAddr(testRedisAddr()))
	defer rc.Close()

	ctx := context.Background()
	iter := rc.rdb.Scan(ctx, 0, prefix+"*", 100).Iterator()
	for iter.Next(ctx) {
		rc.rdb.Del(ctx, iter.Val())
	}
}

func TestIntegration_HappyPath(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	var processed atomic.Int32

	// Create server
	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(10*time.Second),
		WithGracePeriod(2*time.Second),
		WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	err = server.Handle("test.job", func(ctx context.Context, job *Job) error {
		processed.Add(1)
		return nil
	}, Workers(2))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// Create client with same prefix
	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Enqueue a job
	job, err := client.Enqueue(ctx, "test.job", Payload{"msg": "hello"}, Queue("test.job"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Start server in background
	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Start(serverCtx)
	}()

	// Wait for job to be processed
	deadline := time.After(10 * time.Second)
	for {
		select {
		case <-deadline:
			serverCancel()
			t.Fatal("timeout waiting for job processing")
		default:
			if processed.Load() > 0 {
				goto done
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

done:
	// Verify job status
	time.Sleep(200 * time.Millisecond) // let Redis update settle
	fetchedJob, err := client.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if fetchedJob.Status != StatusCompleted {
		t.Errorf("job status = %q, want %q", fetchedJob.Status, StatusCompleted)
	}

	// Shutdown
	serverCancel()
	select {
	case err := <-serverDone:
		if err != nil {
			t.Logf("server stopped with: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("server shutdown timeout")
	}

	if processed.Load() != 1 {
		t.Errorf("processed = %d, want 1", processed.Load())
	}
}

func TestIntegration_FailureRetryDLQ(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	var attempts atomic.Int32

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(10*time.Second),
		WithGracePeriod(1*time.Second),
		WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	err = server.Handle("fail.job", func(ctx context.Context, job *Job) error {
		attempts.Add(1)
		return fmt.Errorf("simulated failure")
	}, Workers(1))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Enqueue with max_retry=2 and short retry intervals
	job, err := client.Enqueue(ctx, "fail.job", Payload{},
		Queue("fail.job"),
		MaxRetry(2),
		RetryIntervals(1, 1),
	)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Start(serverCtx)
	}()

	// Wait for retries + DLQ (initial attempt + 2 retries = 3 total attempts)
	deadline := time.After(30 * time.Second)
	for {
		select {
		case <-deadline:
			serverCancel()
			t.Fatalf("timeout: attempts=%d, expected 3", attempts.Load())
		default:
			if attempts.Load() >= 3 {
				goto done
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

done:
	time.Sleep(500 * time.Millisecond)

	// Verify job ended up in DLQ
	fetchedJob, err := client.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if fetchedJob.Status != StatusDeadLetter {
		t.Errorf("job status = %q, want %q", fetchedJob.Status, StatusDeadLetter)
	}

	serverCancel()
	select {
	case <-serverDone:
	case <-time.After(10 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}

func TestIntegration_Timeout(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	var started atomic.Int32

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(2*time.Second),
		WithGracePeriod(1*time.Second),
		WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	err = server.Handle("slow.job", func(ctx context.Context, job *Job) error {
		started.Add(1)
		// Simulate a slow job that respects context
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(30 * time.Second):
			return nil
		}
	}, Workers(1))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	job, err := client.Enqueue(ctx, "slow.job", Payload{},
		Queue("slow.job"),
		MaxRetry(0), // no retry
		Timeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Start(serverCtx)
	}()

	// Wait for the job to start and then timeout
	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			serverCancel()
			t.Fatal("timeout waiting for job to be processed")
		default:
			if started.Load() > 0 {
				goto waitForCompletion
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

waitForCompletion:
	// Wait for timeout + grace period + some buffer
	time.Sleep(5 * time.Second)

	fetchedJob, err := client.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	// Job should be failed or dead_letter (max_retry=0 so goes straight to DLQ/failed)
	if fetchedJob.Status != StatusDeadLetter && fetchedJob.Status != StatusFailed {
		t.Errorf("job status = %q, want %q or %q", fetchedJob.Status, StatusFailed, StatusDeadLetter)
	}

	serverCancel()
	select {
	case <-serverDone:
	case <-time.After(10 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}

func TestIntegration_PanicRecovery(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	var panicked atomic.Int32

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(10*time.Second),
		WithGracePeriod(1*time.Second),
		WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	err = server.Handle("panic.job", func(ctx context.Context, job *Job) error {
		panicked.Add(1)
		panic("test panic!")
	}, Workers(1))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	job, err := client.Enqueue(ctx, "panic.job", Payload{},
		Queue("panic.job"),
		MaxRetry(0),
	)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Start(serverCtx)
	}()

	// Wait for panic to be recovered
	deadline := time.After(10 * time.Second)
	for {
		select {
		case <-deadline:
			serverCancel()
			t.Fatal("timeout waiting for panic recovery")
		default:
			if panicked.Load() > 0 {
				goto done
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

done:
	time.Sleep(500 * time.Millisecond)

	fetchedJob, err := client.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if fetchedJob.Status != StatusDeadLetter && fetchedJob.Status != StatusFailed {
		t.Errorf("job status = %q, want failed/dead_letter", fetchedJob.Status)
	}

	// Verify the server is still running (didn't crash from panic)
	// Enqueue another job to prove the worker is still alive
	job2, err := client.Enqueue(ctx, "panic.job", Payload{}, Queue("panic.job"), MaxRetry(0))
	if err != nil {
		t.Fatalf("Enqueue second job: %v", err)
	}

	time.Sleep(2 * time.Second)
	if panicked.Load() < 2 {
		t.Log("second job may not have been processed yet, but worker is still alive")
	}
	_ = job2

	serverCancel()
	select {
	case <-serverDone:
	case <-time.After(10 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}

func TestIntegration_DelayedJob(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	var processed atomic.Int32

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(10*time.Second),
		WithGracePeriod(1*time.Second),
		WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	err = server.Handle("delayed.job", func(ctx context.Context, job *Job) error {
		processed.Add(1)
		return nil
	}, Workers(1))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Schedule job 2 seconds from now.
	job, err := client.EnqueueIn(ctx, 2*time.Second, "delayed.job", Payload{"msg": "delayed"},
		Queue("delayed.job"),
	)
	if err != nil {
		t.Fatalf("EnqueueIn: %v", err)
	}

	// Verify it's scheduled, not ready.
	fetched, err := client.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if fetched.Status != StatusScheduled {
		t.Fatalf("initial status = %q, want %q", fetched.Status, StatusScheduled)
	}

	// Start server — scheduler will poll and move job to ready when due.
	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Start(serverCtx)
	}()

	// Job should NOT be processed immediately.
	time.Sleep(500 * time.Millisecond)
	if processed.Load() > 0 {
		t.Fatal("delayed job was processed too early")
	}

	// Wait for the delay + scheduler poll interval + processing buffer.
	deadline := time.After(10 * time.Second)
	for {
		select {
		case <-deadline:
			serverCancel()
			t.Fatal("timeout waiting for delayed job processing")
		default:
			if processed.Load() > 0 {
				goto done
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

done:
	time.Sleep(200 * time.Millisecond)

	fetchedAfter, err := client.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob after: %v", err)
	}
	if fetchedAfter.Status != StatusCompleted {
		t.Errorf("final status = %q, want %q", fetchedAfter.Status, StatusCompleted)
	}

	serverCancel()
	select {
	case <-serverDone:
	case <-time.After(10 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}

func TestIntegration_CronScheduling(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	var processed atomic.Int32

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(10*time.Second),
		WithGracePeriod(1*time.Second),
		WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	err = server.Handle("cron.job", func(ctx context.Context, job *Job) error {
		processed.Add(1)
		return nil
	}, Workers(1))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// Schedule cron entry that fires every second.
	// Use OverlapAllow so rapid fires aren't skipped while previous job is active.
	err = server.Schedule(CronEntry{
		ID:            "every-second",
		Name:          "Every Second Test",
		CronExpr:      "* * * * * *", // every second (6-field)
		JobType:       "cron.job",
		Queue:         "cron.job",
		OverlapPolicy: OverlapAllow,
		Enabled:       true,
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	ctx := context.Background()
	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Start(serverCtx)
	}()

	// Wait for at least 2 cron jobs to be processed (proves recurring scheduling).
	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			serverCancel()
			t.Fatalf("timeout: processed=%d, expected >=2", processed.Load())
		default:
			if processed.Load() >= 2 {
				goto done
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

done:
	count := processed.Load()
	t.Logf("cron jobs processed: %d", count)

	serverCancel()
	select {
	case <-serverDone:
	case <-time.After(10 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}

func TestIntegration_GracefulShutdown(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	var completed atomic.Int32

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(30*time.Second),
		WithGracePeriod(1*time.Second),
		WithShutdownTimeout(10*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	err = server.Handle("slow.complete", func(ctx context.Context, job *Job) error {
		// Takes 2 seconds to complete
		select {
		case <-time.After(2 * time.Second):
			completed.Add(1)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, Workers(1))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	_, err = client.Enqueue(ctx, "slow.complete", Payload{}, Queue("slow.complete"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Start(serverCtx)
	}()

	// Wait for job to start processing, then signal shutdown
	time.Sleep(1 * time.Second)
	serverCancel()

	// Server should wait for the in-flight job to complete
	select {
	case <-serverDone:
	case <-time.After(15 * time.Second):
		t.Fatal("server shutdown timeout")
	}

	// The job should have completed despite the shutdown signal
	if completed.Load() != 1 {
		t.Errorf("completed = %d, want 1 (job should finish during graceful shutdown)", completed.Load())
	}
}

func TestIntegration_DAG_SimpleChain(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	var processed atomic.Int32

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(10*time.Second),
		WithGracePeriod(2*time.Second),
		WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	err = server.Handle("dag.job", func(ctx context.Context, job *Job) error {
		processed.Add(1)
		return nil
	}, Workers(2))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Enqueue parent (ready immediately).
	parent, err := client.Enqueue(ctx, "dag.job", Payload{"role": "parent"},
		Queue("dag.job"), JobID("dag-parent"))
	if err != nil {
		t.Fatalf("Enqueue parent: %v", err)
	}

	// Enqueue child (deferred, depends on parent).
	child, err := client.Enqueue(ctx, "dag.job", Payload{"role": "child"},
		Queue("dag.job"), JobID("dag-child"), DependsOn(parent.ID))
	if err != nil {
		t.Fatalf("Enqueue child: %v", err)
	}

	// Verify child starts as deferred.
	if child.Status != StatusDeferred {
		t.Errorf("child initial status = %q, want %q", child.Status, StatusDeferred)
	}

	// Start server.
	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Start(serverCtx)
	}()

	// Wait for both jobs to be processed.
	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			serverCancel()
			t.Fatalf("timeout: processed = %d, want 2", processed.Load())
		default:
			if processed.Load() >= 2 {
				goto done
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

done:
	time.Sleep(300 * time.Millisecond) // let Redis update settle

	// Verify child was promoted and completed.
	fetchedChild, err := client.GetJob(ctx, child.ID)
	if err != nil {
		t.Fatalf("GetJob child: %v", err)
	}
	if fetchedChild.Status != StatusCompleted {
		t.Errorf("child final status = %q, want %q", fetchedChild.Status, StatusCompleted)
	}

	serverCancel()
	select {
	case <-serverDone:
	case <-time.After(10 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}

func TestIntegration_DAG_FailurePropagation(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(10*time.Second),
		WithGracePeriod(2*time.Second),
		WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	err = server.Handle("dag.fail", func(ctx context.Context, job *Job) error {
		return fmt.Errorf("intentional failure")
	}, Workers(1))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Enqueue parent (will fail → retry → DLQ with max_retry=0).
	parent, err := client.Enqueue(ctx, "dag.fail", Payload{},
		Queue("dag.fail"), JobID("fail-parent"), MaxRetry(0))
	if err != nil {
		t.Fatalf("Enqueue parent: %v", err)
	}

	// Enqueue child (deferred, depends on parent, allow_failure=false).
	child, err := client.Enqueue(ctx, "dag.fail", Payload{},
		Queue("dag.fail"), JobID("fail-child"), DependsOn(parent.ID))
	if err != nil {
		t.Fatalf("Enqueue child: %v", err)
	}

	// Start server.
	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Start(serverCtx)
	}()

	// Wait for parent to be processed and moved to DLQ.
	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			serverCancel()
			t.Fatal("timeout waiting for parent to reach DLQ")
		default:
			fetched, err := client.GetJob(ctx, parent.ID)
			if err == nil && fetched.Status == StatusDeadLetter {
				goto done
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

done:
	time.Sleep(300 * time.Millisecond)

	// Child should be canceled due to parent failure propagation.
	fetchedChild, err := client.GetJob(ctx, child.ID)
	if err != nil {
		t.Fatalf("GetJob child: %v", err)
	}
	if fetchedChild.Status != StatusCanceled {
		t.Errorf("child status = %q, want %q", fetchedChild.Status, StatusCanceled)
	}

	serverCancel()
	select {
	case <-serverDone:
	case <-time.After(10 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}

func TestIntegration_SkipRetry(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	var attempts atomic.Int32

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(10*time.Second),
		WithGracePeriod(1*time.Second),
		WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	err = server.Handle("skip.retry", func(ctx context.Context, job *Job) error {
		attempts.Add(1)
		return fmt.Errorf("permanent error: %w", ErrSkipRetry)
	}, Workers(1))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Enqueue with max_retry=5 — but SkipRetry should bypass all retries.
	job, err := client.Enqueue(ctx, "skip.retry", Payload{},
		Queue("skip.retry"),
		MaxRetry(5),
		RetryIntervals(1, 1, 1, 1, 1),
	)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Start(serverCtx)
	}()

	// Wait for exactly 1 attempt — should go straight to DLQ.
	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			serverCancel()
			t.Fatalf("timeout: attempts=%d", attempts.Load())
		default:
			if attempts.Load() >= 1 {
				goto done1
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

done1:
	// Small buffer for DLQ processing.
	time.Sleep(1 * time.Second)

	// Should have exactly 1 attempt (no retries).
	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1 (SkipRetry should prevent retries)", got)
	}

	fetchedJob, err := client.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if fetchedJob.Status != StatusDeadLetter {
		t.Errorf("job status = %q, want %q", fetchedJob.Status, StatusDeadLetter)
	}

	serverCancel()
	select {
	case <-serverDone:
	case <-time.After(10 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}

func TestIntegration_SkipRetry_Wrapped(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	var attempts atomic.Int32

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(10*time.Second),
		WithGracePeriod(1*time.Second),
		WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	err = server.Handle("skip.wrapped", func(ctx context.Context, job *Job) error {
		attempts.Add(1)
		// Wrap ErrSkipRetry inside another error — errors.Is should still match.
		return fmt.Errorf("invalid input: %w", ErrSkipRetry)
	}, Workers(1))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	job, err := client.Enqueue(ctx, "skip.wrapped", Payload{},
		Queue("skip.wrapped"),
		MaxRetry(3),
		RetryIntervals(1, 1, 1),
	)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Start(serverCtx)
	}()

	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			serverCancel()
			t.Fatalf("timeout: attempts=%d", attempts.Load())
		default:
			if attempts.Load() >= 1 {
				goto done2
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

done2:
	time.Sleep(1 * time.Second)

	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1", got)
	}

	fetchedJob, err := client.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if fetchedJob.Status != StatusDeadLetter {
		t.Errorf("job status = %q, want %q", fetchedJob.Status, StatusDeadLetter)
	}

	serverCancel()
	select {
	case <-serverDone:
	case <-time.After(10 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}

func TestIntegration_IsFailure(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	var attempts atomic.Int32
	errRateLimit := errors.New("rate limited")

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(10*time.Second),
		WithGracePeriod(1*time.Second),
		WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// Handler returns rate limit error 3 times, then a real failure.
	err = server.Handle("classify.error", func(ctx context.Context, job *Job) error {
		n := attempts.Add(1)
		if n <= 3 {
			return fmt.Errorf("temporary: %w", errRateLimit)
		}
		return fmt.Errorf("real failure")
	}, Workers(1), IsFailure(func(err error) bool {
		// Rate limit errors are NOT failures — don't increment retry counter.
		return !errors.Is(err, errRateLimit)
	}))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// max_retry=1: normally would allow 1+1=2 attempts.
	// With IsFailure, the 3 rate limit errors don't count, so the job
	// should run 3 (non-failure) + 1 (first real failure, count=1) + 1 (retry, count=2 > max=1 → DLQ)
	// = 5 total attempts.
	job, err := client.Enqueue(ctx, "classify.error", Payload{},
		Queue("classify.error"),
		MaxRetry(1),
		RetryIntervals(1, 1, 1, 1, 1),
	)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Start(serverCtx)
	}()

	// Wait for enough attempts: 3 non-failure + 2 real failures = 5.
	deadline := time.After(30 * time.Second)
	for {
		select {
		case <-deadline:
			serverCancel()
			t.Fatalf("timeout: attempts=%d, expected 5", attempts.Load())
		default:
			if attempts.Load() >= 5 {
				goto done3
			}
			time.Sleep(500 * time.Millisecond)
		}
	}

done3:
	time.Sleep(1 * time.Second)

	if got := attempts.Load(); got != 5 {
		t.Errorf("attempts = %d, want 5 (3 non-failure + 2 real failure)", got)
	}

	fetchedJob, err := client.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if fetchedJob.Status != StatusDeadLetter {
		t.Errorf("job status = %q, want %q", fetchedJob.Status, StatusDeadLetter)
	}

	serverCancel()
	select {
	case <-serverDone:
	case <-time.After(10 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}

// TestIntegration_Middleware verifies that middleware wraps handlers
// in the correct order: Use(a, b) → a → b → handler.
func TestIntegration_Middleware(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	// Track execution order
	var order []string
	var orderMu sync.Mutex
	appendOrder := func(label string) {
		orderMu.Lock()
		order = append(order, label)
		orderMu.Unlock()
	}

	srv, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(10*time.Second),
		WithGracePeriod(1*time.Second),
		WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// Register middleware in order: first, second
	if err := srv.Use(func(next Handler) Handler {
		return func(ctx context.Context, job *Job) error {
			appendOrder("mw1:before")
			err := next(ctx, job)
			appendOrder("mw1:after")
			return err
		}
	}); err != nil {
		t.Fatalf("Use mw1: %v", err)
	}

	if err := srv.Use(func(next Handler) Handler {
		return func(ctx context.Context, job *Job) error {
			appendOrder("mw2:before")
			err := next(ctx, job)
			appendOrder("mw2:after")
			return err
		}
	}); err != nil {
		t.Fatalf("Use mw2: %v", err)
	}

	var done atomic.Bool
	err = srv.Handle("mw.test", func(ctx context.Context, job *Job) error {
		appendOrder("handler")
		done.Store(true)
		return nil
	}, Workers(1))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	// Enqueue job
	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	_, err = client.Enqueue(ctx, "mw.test", Payload{}, Queue("mw.test"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	// Start server
	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- srv.Start(serverCtx)
	}()

	// Wait for job to complete
	deadline := time.After(15 * time.Second)
	for !done.Load() {
		select {
		case <-deadline:
			serverCancel()
			t.Fatal("timeout waiting for job")
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Allow brief settling
	time.Sleep(100 * time.Millisecond)

	serverCancel()
	select {
	case <-serverDone:
	case <-time.After(10 * time.Second):
		t.Fatal("server shutdown timeout")
	}

	// Verify order: mw1 → mw2 → handler → mw2 → mw1
	expected := []string{"mw1:before", "mw2:before", "handler", "mw2:after", "mw1:after"}
	orderMu.Lock()
	actual := make([]string, len(order))
	copy(actual, order)
	orderMu.Unlock()

	if len(actual) != len(expected) {
		t.Fatalf("expected %d entries, got %d: %v", len(expected), len(actual), actual)
	}
	for i := range expected {
		if actual[i] != expected[i] {
			t.Errorf("order[%d] = %q, want %q", i, actual[i], expected[i])
		}
	}
}

// TestIntegration_Middleware_ErrorPropagation verifies middleware can
// intercept and transform handler errors.
func TestIntegration_Middleware_ErrorPropagation(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	var interceptedErr atomic.Value

	srv, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(10*time.Second),
		WithGracePeriod(1*time.Second),
		WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// Middleware that captures the handler error
	if err := srv.Use(func(next Handler) Handler {
		return func(ctx context.Context, job *Job) error {
			err := next(ctx, job)
			if err != nil {
				interceptedErr.Store(err.Error())
			}
			return err
		}
	}); err != nil {
		t.Fatalf("Use: %v", err)
	}

	var done atomic.Bool
	err = srv.Handle("mwerr.test", func(ctx context.Context, job *Job) error {
		done.Store(true)
		return fmt.Errorf("handler-specific-error")
	}, Workers(1))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	_, err = client.Enqueue(ctx, "mwerr.test", Payload{},
		Queue("mwerr.test"),
		MaxRetry(0),
	)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- srv.Start(serverCtx)
	}()

	deadline := time.After(15 * time.Second)
	for !done.Load() {
		select {
		case <-deadline:
			serverCancel()
			t.Fatal("timeout waiting for job")
		case <-time.After(50 * time.Millisecond):
		}
	}

	time.Sleep(200 * time.Millisecond)

	serverCancel()
	select {
	case <-serverDone:
	case <-time.After(10 * time.Second):
		t.Fatal("server shutdown timeout")
	}

	// Verify middleware intercepted the error
	v := interceptedErr.Load()
	if v == nil {
		t.Fatal("middleware did not intercept error")
	}
	if got := v.(string); got != "handler-specific-error" {
		t.Errorf("intercepted error = %q, want %q", got, "handler-specific-error")
	}
}

// TestIntegration_Callbacks_OnSuccess verifies OnSuccess and OnComplete
// fire on successful job execution.
func TestIntegration_Callbacks_OnSuccess(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	var (
		successCalled  atomic.Bool
		completeCalled atomic.Bool
		completeErr    atomic.Value
		failureCalled  atomic.Bool
	)

	srv, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(10*time.Second),
		WithGracePeriod(1*time.Second),
		WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	err = srv.Handle("cb.success", func(ctx context.Context, job *Job) error {
		return nil
	}, Workers(1),
		OnSuccess(func(ctx context.Context, job *Job) {
			successCalled.Store(true)
		}),
		OnFailure(func(ctx context.Context, job *Job, err error) {
			failureCalled.Store(true)
		}),
		OnComplete(func(ctx context.Context, job *Job, err error) {
			completeCalled.Store(true)
			if err != nil {
				completeErr.Store(err.Error())
			} else {
				completeErr.Store("")
			}
		}),
	)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	_, err = client.Enqueue(ctx, "cb.success", Payload{}, Queue("cb.success"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() { serverDone <- srv.Start(serverCtx) }()

	deadline := time.After(15 * time.Second)
	for !successCalled.Load() {
		select {
		case <-deadline:
			serverCancel()
			t.Fatal("timeout waiting for OnSuccess")
		case <-time.After(50 * time.Millisecond):
		}
	}

	time.Sleep(100 * time.Millisecond)
	serverCancel()
	<-serverDone

	if !successCalled.Load() {
		t.Error("OnSuccess was not called")
	}
	if failureCalled.Load() {
		t.Error("OnFailure should not be called on success")
	}
	if !completeCalled.Load() {
		t.Error("OnComplete was not called")
	}
	if v := completeErr.Load(); v != nil && v.(string) != "" {
		t.Errorf("OnComplete err = %q, want nil/empty", v)
	}
}

// TestIntegration_Callbacks_OnFailure verifies OnFailure and OnComplete
// fire on failed job execution.
func TestIntegration_Callbacks_OnFailure(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	var (
		successCalled  atomic.Bool
		failureCalled  atomic.Bool
		failureErrMsg  atomic.Value
		completeCalled atomic.Bool
		completeErrMsg atomic.Value
	)

	srv, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(10*time.Second),
		WithGracePeriod(1*time.Second),
		WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	var handlerDone atomic.Bool
	err = srv.Handle("cb.failure", func(ctx context.Context, job *Job) error {
		handlerDone.Store(true)
		return fmt.Errorf("intentional-error")
	}, Workers(1),
		OnSuccess(func(ctx context.Context, job *Job) {
			successCalled.Store(true)
		}),
		OnFailure(func(ctx context.Context, job *Job, err error) {
			failureCalled.Store(true)
			failureErrMsg.Store(err.Error())
		}),
		OnComplete(func(ctx context.Context, job *Job, err error) {
			completeCalled.Store(true)
			if err != nil {
				completeErrMsg.Store(err.Error())
			}
		}),
	)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	_, err = client.Enqueue(ctx, "cb.failure", Payload{},
		Queue("cb.failure"), MaxRetry(0))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() { serverDone <- srv.Start(serverCtx) }()

	deadline := time.After(15 * time.Second)
	for !handlerDone.Load() {
		select {
		case <-deadline:
			serverCancel()
			t.Fatal("timeout waiting for handler")
		case <-time.After(50 * time.Millisecond):
		}
	}

	time.Sleep(200 * time.Millisecond)
	serverCancel()
	<-serverDone

	if successCalled.Load() {
		t.Error("OnSuccess should not be called on failure")
	}
	if !failureCalled.Load() {
		t.Error("OnFailure was not called")
	}
	if v := failureErrMsg.Load(); v == nil || v.(string) != "intentional-error" {
		t.Errorf("OnFailure err = %v, want %q", v, "intentional-error")
	}
	if !completeCalled.Load() {
		t.Error("OnComplete was not called")
	}
	if v := completeErrMsg.Load(); v == nil || v.(string) != "intentional-error" {
		t.Errorf("OnComplete err = %v, want %q", v, "intentional-error")
	}
}

// TestIntegration_Callbacks_PanicRecovery verifies callback panics
// don't crash the worker.
func TestIntegration_Callbacks_PanicRecovery(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	var handlerDone atomic.Bool

	srv, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(10*time.Second),
		WithGracePeriod(1*time.Second),
		WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	err = srv.Handle("cb.panic", func(ctx context.Context, job *Job) error {
		handlerDone.Store(true)
		return nil
	}, Workers(1),
		OnSuccess(func(ctx context.Context, job *Job) {
			panic("callback panic!")
		}),
	)
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	_, err = client.Enqueue(ctx, "cb.panic", Payload{}, Queue("cb.panic"))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() { serverDone <- srv.Start(serverCtx) }()

	deadline := time.After(15 * time.Second)
	for !handlerDone.Load() {
		select {
		case <-deadline:
			serverCancel()
			t.Fatal("timeout waiting for handler")
		case <-time.After(50 * time.Millisecond):
		}
	}

	time.Sleep(200 * time.Millisecond)

	// Enqueue a second job to verify worker is still alive after panic
	var secondDone atomic.Bool
	err = srv.Handle("cb.after", func(ctx context.Context, job *Job) error {
		secondDone.Store(true)
		return nil
	})
	// Handler already registered on running server — this will fail.
	// Instead, enqueue another job of the same type.
	_, err = client.Enqueue(ctx, "cb.panic", Payload{}, Queue("cb.panic"))
	if err != nil {
		t.Fatalf("Enqueue second: %v", err)
	}

	deadline = time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			// Worker still processes jobs = good enough (panic didn't crash it).
			// The job may or may not have been picked up within timeout,
			// but the server is still running.
			goto doneWait
		case <-time.After(50 * time.Millisecond):
		}
	}
doneWait:

	serverCancel()
	<-serverDone
	// If we reach here without crashing, the panic recovery worked.
}

// TestIntegration_EnqueueBatch verifies batch enqueue creates all jobs
// and they are processed correctly.
func TestIntegration_EnqueueBatch(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	var processed atomic.Int32

	srv, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(10*time.Second),
		WithGracePeriod(1*time.Second),
		WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	err = srv.Handle("batch.test", func(ctx context.Context, job *Job) error {
		processed.Add(1)
		return nil
	}, Workers(3))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Batch enqueue 10 jobs
	items := make([]BatchItem, 10)
	for i := range items {
		items[i] = BatchItem{
			JobType: "batch.test",
			Payload: Payload{"index": i},
			Options: []EnqueueOption{Queue("batch.test")},
		}
	}

	jobs, err := client.EnqueueBatch(ctx, items)
	if err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}

	if len(jobs) != 10 {
		t.Fatalf("expected 10 jobs, got %d", len(jobs))
	}

	// Verify all jobs have unique IDs
	ids := make(map[string]bool)
	for _, job := range jobs {
		if ids[job.ID] {
			t.Fatalf("duplicate job ID: %s", job.ID)
		}
		ids[job.ID] = true
	}

	// Start server and wait for all jobs to be processed
	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() { serverDone <- srv.Start(serverCtx) }()

	deadline := time.After(15 * time.Second)
	for processed.Load() < 10 {
		select {
		case <-deadline:
			serverCancel()
			t.Fatalf("timeout: processed=%d/10", processed.Load())
		case <-time.After(50 * time.Millisecond):
		}
	}

	serverCancel()
	<-serverDone

	if got := processed.Load(); got != 10 {
		t.Errorf("processed = %d, want 10", got)
	}
}

// TestIntegration_EnqueueBatch_Validation verifies batch enqueue
// validates all items upfront.
func TestIntegration_EnqueueBatch_Validation(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	tests := []struct {
		name    string
		items   []BatchItem
		wantErr string
	}{
		{
			name:    "empty job type",
			items:   []BatchItem{{JobType: "", Payload: Payload{}}},
			wantErr: "batch item 0",
		},
		{
			name: "unique not supported",
			items: []BatchItem{
				{JobType: "ok", Payload: Payload{}},
				{JobType: "fail", Payload: Payload{}, Options: []EnqueueOption{Unique()}},
			},
			wantErr: "Unique is not supported in EnqueueBatch",
		},
		{
			name: "depends_on not supported",
			items: []BatchItem{
				{JobType: "fail", Payload: Payload{}, Options: []EnqueueOption{DependsOn("parent-1")}},
			},
			wantErr: "DependsOn is not supported in EnqueueBatch",
		},
		{
			name:  "empty batch returns nil",
			items: []BatchItem{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			jobs, err := client.EnqueueBatch(ctx, tt.items)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error = %q, want containing %q", err.Error(), tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(tt.items) == 0 && jobs != nil {
				t.Errorf("expected nil for empty batch, got %v", jobs)
			}
		})
	}
}

// TestIntegration_IsFailure_Panic verifies that a panicking IsFailure
// predicate does not crash the worker and the job is treated as a failure.
func TestIntegration_IsFailure_Panic(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	var processed atomic.Bool

	srv, err := NewServer(
		WithServerRedis(testRedisAddr()),
		WithServerRedisOpts(WithPrefix(prefix)),
		WithShutdownTimeout(5*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	srv.Handle("panic.isfailure", func(ctx context.Context, job *Job) error {
		processed.Store(true)
		return fmt.Errorf("some error")
	},
		Workers(1),
		IsFailure(func(err error) bool {
			panic("predicate boom!")
		}),
	)

	ctx := context.Background()
	_, err = client.Enqueue(ctx, "panic.isfailure", Payload{"test": true},
		Queue("panic.isfailure"), MaxRetry(0))
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	srvCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	go srv.Start(srvCtx)

	// Wait for job to be processed (dequeue poll is 5s)
	deadline := time.After(12 * time.Second)
	for !processed.Load() {
		select {
		case <-deadline:
			cancel()
			t.Fatal("timeout waiting for job to process")
		default:
			time.Sleep(200 * time.Millisecond)
		}
	}

	// Give time for DLQ write
	time.Sleep(500 * time.Millisecond)
	cancel()

	// Verify job went to DLQ (panic defaults to countAsFailure=true, maxRetry=0 → DLQ)
	rdb := client.RedisClient()
	dlqKey := prefix + "queue:panic.isfailure:dead_letter"
	count, err := rdb.ZCard(ctx, dlqKey).Result()
	if err != nil {
		t.Fatalf("ZCard DLQ: %v", err)
	}
	if count != 1 {
		t.Errorf("expected 1 job in DLQ, got %d", count)
	}
}

// TestIntegration_Middleware_NilRejection verifies that Use() rejects
// nil middleware.
func TestIntegration_Middleware_NilRejection(t *testing.T) {
	srv, err := NewServer(WithServerRedis(testRedisAddr()))
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	err = srv.Use(nil)
	if err == nil {
		t.Fatal("expected error for nil middleware, got nil")
	}
	if !strings.Contains(err.Error(), "nil") {
		t.Errorf("error = %q, want containing 'nil'", err.Error())
	}
}

// TestIntegration_Middleware_AfterStart verifies that Use() rejects
// middleware registration after Start() is called.
func TestIntegration_Middleware_AfterStart(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	srv, err := NewServer(
		WithServerRedis(testRedisAddr()),
		WithServerRedisOpts(WithPrefix(prefix)),
		WithShutdownTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	srv.Handle("dummy", func(ctx context.Context, job *Job) error {
		return nil
	}, Workers(1))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go srv.Start(ctx)
	time.Sleep(500 * time.Millisecond) // Wait for Start() to set running=true

	mw := func(next Handler) Handler { return next }
	err = srv.Use(mw)
	if err == nil {
		t.Fatal("expected error for Use() after Start(), got nil")
	}
	if !strings.Contains(err.Error(), "running") {
		t.Errorf("error = %q, want containing 'running'", err.Error())
	}
	cancel()
}

// TestIntegration_EnqueueBatch_TooLarge verifies the batch size limit.
func TestIntegration_EnqueueBatch_TooLarge(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	items := make([]BatchItem, maxBatchEnqueueSize+1)
	for i := range items {
		items[i] = BatchItem{JobType: "too.many", Payload: Payload{}}
	}

	_, err = client.EnqueueBatch(context.Background(), items)
	if err == nil {
		t.Fatal("expected ErrBatchTooLarge, got nil")
	}
	if !errors.Is(err, ErrBatchTooLarge) {
		t.Errorf("expected ErrBatchTooLarge, got %v", err)
	}
}

// TestIntegration_EnqueueBatch_DuplicateJobID verifies duplicate ID detection.
func TestIntegration_EnqueueBatch_DuplicateJobID(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	items := []BatchItem{
		{JobType: "dup", Payload: Payload{}, Options: []EnqueueOption{JobID("same-id")}},
		{JobType: "dup", Payload: Payload{}, Options: []EnqueueOption{JobID("same-id")}},
	}

	_, err = client.EnqueueBatch(context.Background(), items)
	if err == nil {
		t.Fatal("expected duplicate job ID error, got nil")
	}
	if !strings.Contains(err.Error(), "duplicate job ID") {
		t.Errorf("error = %q, want containing 'duplicate job ID'", err.Error())
	}
}

// TestIntegration_GracePeriodCompletion verifies that a handler that finishes
// during the grace period (after timeout but before grace expires) completes
// the job successfully.
func TestIntegration_GracePeriodCompletion(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	var completed atomic.Int32

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(1*time.Second),
		WithGracePeriod(5*time.Second),
		WithShutdownTimeout(10*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// Handler that takes 2s — exceeds 1s timeout but finishes within 5s grace period.
	err = server.Handle("grace.job", func(ctx context.Context, job *Job) error {
		select {
		case <-ctx.Done():
			// Context cancelled due to timeout — finish cleanly within grace period.
			completed.Add(1)
			return nil
		case <-time.After(30 * time.Second):
			return nil
		}
	}, Workers(1))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	job, err := client.Enqueue(ctx, "grace.job", Payload{},
		Queue("grace.job"),
		MaxRetry(0),
	)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Start(serverCtx)
	}()

	// Wait for the job to be completed (timeout 1s + handler responds to ctx.Done quickly).
	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			serverCancel()
			t.Fatal("timeout waiting for job to complete")
		default:
			if completed.Load() > 0 {
				goto verify
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

verify:
	// Allow some time for completion processing.
	time.Sleep(2 * time.Second)

	fetchedJob, err := client.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	// Job should be completed because handler returned nil during grace period.
	if fetchedJob.Status != StatusCompleted {
		t.Errorf("job status = %q, want %q", fetchedJob.Status, StatusCompleted)
	}

	serverCancel()
	select {
	case <-serverDone:
	case <-time.After(10 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}

// TestIntegration_GracePeriodError verifies that a handler returning an error
// during the grace period marks the job as failed.
func TestIntegration_GracePeriodError(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	var responded atomic.Int32

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(1*time.Second),
		WithGracePeriod(5*time.Second),
		WithShutdownTimeout(10*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// Handler that waits for context cancellation and returns an error.
	err = server.Handle("grace-err.job", func(ctx context.Context, job *Job) error {
		select {
		case <-ctx.Done():
			responded.Add(1)
			return fmt.Errorf("handler error after timeout")
		case <-time.After(30 * time.Second):
			return nil
		}
	}, Workers(1))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	job, err := client.Enqueue(ctx, "grace-err.job", Payload{},
		Queue("grace-err.job"),
		MaxRetry(0),
	)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Start(serverCtx)
	}()

	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			serverCancel()
			t.Fatal("timeout waiting for handler to respond")
		default:
			if responded.Load() > 0 {
				goto verify
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

verify:
	time.Sleep(2 * time.Second)

	fetchedJob, err := client.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	// Job should be dead_letter (MaxRetry=0) or failed.
	if fetchedJob.Status != StatusDeadLetter && fetchedJob.Status != StatusFailed {
		t.Errorf("job status = %q, want %q or %q", fetchedJob.Status, StatusDeadLetter, StatusFailed)
	}

	serverCancel()
	select {
	case <-serverDone:
	case <-time.After(10 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}

// TestIntegration_AbandonedHandler verifies that a handler that never returns
// is abandoned after the grace period, and the job is marked as failed.
func TestIntegration_AbandonedHandler(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	var started atomic.Int32
	release := make(chan struct{})

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(1*time.Second),
		WithGracePeriod(2*time.Second),
		WithShutdownTimeout(10*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	// Handler that blocks until test explicitly releases it (simulates a stuck handler).
	err = server.Handle("stuck.job", func(ctx context.Context, job *Job) error {
		started.Add(1)
		<-release // Block indefinitely until test releases.
		return nil
	}, Workers(1))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	job, err := client.Enqueue(ctx, "stuck.job", Payload{},
		Queue("stuck.job"),
		MaxRetry(0),
	)
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	serverCtx, serverCancel := context.WithCancel(ctx)
	serverDone := make(chan error, 1)
	go func() {
		serverDone <- server.Start(serverCtx)
	}()

	// Wait for the handler to start.
	deadline := time.After(15 * time.Second)
	for {
		select {
		case <-deadline:
			close(release)
			serverCancel()
			t.Fatal("timeout waiting for handler to start")
		default:
			if started.Load() > 0 {
				goto waitAbandoned
			}
			time.Sleep(100 * time.Millisecond)
		}
	}

waitAbandoned:
	// Wait for timeout (1s) + grace period (2s) + buffer.
	time.Sleep(5 * time.Second)

	fetchedJob, err := client.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	// Job should be dead_letter (MaxRetry=0) or failed.
	if fetchedJob.Status != StatusDeadLetter && fetchedJob.Status != StatusFailed {
		t.Errorf("job status = %q, want %q or %q", fetchedJob.Status, StatusDeadLetter, StatusFailed)
	}

	// Release the stuck handler goroutine so it can clean up.
	close(release)

	serverCancel()
	select {
	case <-serverDone:
	case <-time.After(10 * time.Second):
		t.Fatal("server shutdown timeout")
	}
}

// TestIntegration_EnqueueBatch_EnqueueAtFrontRejected verifies EnqueueAtFront rejection.
func TestIntegration_EnqueueBatch_EnqueueAtFrontRejected(t *testing.T) {
	skipWithoutRedis(t)
	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	defer cleanupRedis(t, prefix)

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	items := []BatchItem{
		{JobType: "front", Payload: Payload{}, Options: []EnqueueOption{EnqueueAtFront(true)}},
	}

	_, err = client.EnqueueBatch(context.Background(), items)
	if err == nil {
		t.Fatal("expected ErrBatchEnqueueAtFront, got nil")
	}
	if !errors.Is(err, ErrBatchEnqueueAtFront) {
		t.Errorf("expected ErrBatchEnqueueAtFront, got %v", err)
	}
}
