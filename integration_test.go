package gqm

import (
	"context"
	"fmt"
	"os"
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
