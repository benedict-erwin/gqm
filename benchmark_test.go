package gqm

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// --- helpers ----------------------------------------------------------------

func skipBenchWithoutRedis(b *testing.B) {
	b.Helper()
	rc, err := NewRedisClient(WithRedisAddr(testRedisAddr()), WithPrefix("gqm:benchcheck:"))
	if err != nil {
		b.Skipf("requires Redis: %v", err)
	}
	if err := rc.Ping(context.Background()); err != nil {
		b.Skipf("requires Redis: %v", err)
	}
	rc.Close()
}

func benchCleanup(b *testing.B, prefix string) {
	b.Helper()
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

// --- BenchmarkEnqueue -------------------------------------------------------

func BenchmarkEnqueue(b *testing.B) {
	skipBenchWithoutRedis(b)
	prefix := fmt.Sprintf("gqm:bench:enq:%d:", time.Now().UnixNano())
	defer benchCleanup(b, prefix)

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		b.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	payload := Payload{"key": "value", "num": 42}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := client.Enqueue(ctx, "bench.enqueue", payload); err != nil {
			b.Fatalf("Enqueue: %v", err)
		}
	}
}

// --- BenchmarkEnqueueBatch --------------------------------------------------

func BenchmarkEnqueueBatch(b *testing.B) {
	skipBenchWithoutRedis(b)

	for _, batchSize := range []int{100, 500, 1000} {
		b.Run(fmt.Sprintf("size_%d", batchSize), func(b *testing.B) {
			prefix := fmt.Sprintf("gqm:bench:batch:%d:%d:", batchSize, time.Now().UnixNano())
			defer benchCleanup(b, prefix)

			client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
			if err != nil {
				b.Fatalf("NewClient: %v", err)
			}
			defer client.Close()

			ctx := context.Background()
			items := make([]BatchItem, batchSize)
			for i := range items {
				items[i] = BatchItem{
					JobType: "bench.batch",
					Payload: Payload{"idx": i},
				}
			}

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := client.EnqueueBatch(ctx, items); err != nil {
					b.Fatalf("EnqueueBatch: %v", err)
				}
			}
			b.ReportMetric(float64(batchSize), "jobs/op")
		})
	}
}

// --- BenchmarkEndToEnd ------------------------------------------------------
// Measures aggregate enqueue→process→complete throughput.
// Uses 20 workers to saturate dequeue polling. The reported ns/op is the
// average end-to-end time per job including enqueue, dequeue poll, and handler.

func BenchmarkEndToEnd(b *testing.B) {
	skipBenchWithoutRedis(b)
	prefix := fmt.Sprintf("gqm:bench:e2e:%d:", time.Now().UnixNano())
	defer benchCleanup(b, prefix)

	var processed atomic.Int64

	server, err := NewServer(
		WithServerRedisOpts(WithRedisAddr(testRedisAddr()), WithPrefix(prefix)),
		WithGlobalTimeout(30*time.Second),
		WithGracePeriod(2*time.Second),
		WithShutdownTimeout(10*time.Second),
	)
	if err != nil {
		b.Fatalf("NewServer: %v", err)
	}

	err = server.Handle("bench.e2e", func(ctx context.Context, job *Job) error {
		processed.Add(1)
		return nil
	}, Workers(20))
	if err != nil {
		b.Fatalf("Handle: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		server.Start(ctx)
	}()

	time.Sleep(500 * time.Millisecond)

	client, err := NewClient(WithRedisAddr(testRedisAddr()), WithPrefix(prefix))
	if err != nil {
		b.Fatalf("NewClient: %v", err)
	}
	defer client.Close()

	payload := Payload{"key": "value"}

	b.ResetTimer()

	// Enqueue all b.N jobs. Queue must match the implicit pool's queue.
	for i := 0; i < b.N; i++ {
		if _, err := client.Enqueue(ctx, "bench.e2e", payload, Queue("bench.e2e")); err != nil {
			b.Fatalf("Enqueue: %v", err)
		}
	}

	// Wait for all to be processed.
	deadline := time.After(60 * time.Second)
	for processed.Load() < int64(b.N) {
		select {
		case <-deadline:
			b.Fatalf("timeout: processed %d/%d", processed.Load(), b.N)
		default:
			runtime.Gosched()
		}
	}

	b.StopTimer()

	server.Stop()
	wg.Wait()
}
