package gqm

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The retention tests next door prove that GQM sets the right TTL on a terminal
// job. That is not the same as proving the data goes away. Every one of them
// reads the TTL and stops there, because a seven-day expiry cannot be waited
// out in a test.
//
// The gap that matters is not the waiting, though — Redis can be trusted to
// honour an EXPIRE. It is that TTL only ever touches the **job hash**. A job
// with dependencies also writes three sets — `:deps`, `:pending_deps` and
// `:dependents` — and none of them is given an expiry. They are removed by
// dag.go when a dependency resolves, which means cleanup rests on two unrelated
// mechanisms: retention for the hash, and the resolution path for everything
// else.
//
// So a terminal state reached by a route that skips resolution would leave its
// sets behind permanently. Retention would never collect them, because
// retention does not know they exist. Nothing would report it: the job is gone
// from every listing, the accounts balance, and the keys simply accumulate.
//
// This runs the three terminal routes with a TTL short enough to actually
// elapse, then requires that nothing under `<prefix>job:*` survives — one
// assertion that covers the hash and all three sets at once.

const reclaimTTL = 2 * time.Second

func TestReclaim_NothingSurvivesExpiry(t *testing.T) {
	f := newRetentionFixture(t,
		WithResultTTL(reclaimTTL),
		WithFailureTTL(reclaimTTL),
		WithLogLevel("error"),
	)

	const n = 20

	var completed, failed atomic.Int64
	if err := f.server.Handle("reclaim.ok", func(ctx context.Context, job *Job) error {
		completed.Add(1)
		return nil
	}, Workers(4)); err != nil {
		t.Fatalf("Handle(reclaim.ok): %v", err)
	}
	if err := f.server.Handle("reclaim.fail", func(ctx context.Context, job *Job) error {
		failed.Add(1)
		return fmt.Errorf("deliberate terminal failure: %w", ErrSkipRetry)
	}, Workers(4)); err != nil {
		t.Fatalf("Handle(reclaim.fail): %v", err)
	}

	bg := context.Background()

	// Route 1 — plain jobs that complete. No dependency sets involved.
	for i := 0; i < n; i++ {
		if _, err := f.client.Enqueue(bg, "reclaim.ok", Payload{"i": i}, Queue("reclaim.ok")); err != nil {
			t.Fatalf("enqueue plain %d: %v", i, err)
		}
	}

	// Route 2 — jobs that dead-letter. Also no dependency sets.
	for i := 0; i < n; i++ {
		if _, err := f.client.Enqueue(bg, "reclaim.fail", Payload{"i": i},
			Queue("reclaim.fail"), MaxRetry(0)); err != nil {
			t.Fatalf("enqueue failing %d: %v", i, err)
		}
	}

	// Route 3 — chains that succeed end to end. Each link writes all three sets,
	// and the resolution path is expected to remove them.
	for i := 0; i < n; i++ {
		parent, err := f.client.Enqueue(bg, "reclaim.ok", Payload{"chain": i, "stage": 0}, Queue("reclaim.ok"))
		if err != nil {
			t.Fatalf("enqueue chain %d parent: %v", i, err)
		}
		if _, err := f.client.Enqueue(bg, "reclaim.ok", Payload{"chain": i, "stage": 1},
			Queue("reclaim.ok"), DependsOn(parent.ID)); err != nil {
			t.Fatalf("enqueue chain %d child: %v", i, err)
		}
	}

	// Route 4 — the one that skips resolution. The parent dead-letters, so the
	// dependent is cancelled rather than promoted, and never runs at all. If any
	// route leaves bookkeeping behind, this is the one.
	for i := 0; i < n; i++ {
		parent, err := f.client.Enqueue(bg, "reclaim.fail", Payload{"cascade": i},
			Queue("reclaim.fail"), MaxRetry(0))
		if err != nil {
			t.Fatalf("enqueue cascade %d parent: %v", i, err)
		}
		if _, err := f.client.Enqueue(bg, "reclaim.ok", Payload{"cascade": i, "child": true},
			Queue("reclaim.ok"), DependsOn(parent.ID)); err != nil {
			t.Fatalf("enqueue cascade %d child: %v", i, err)
		}
	}

	// 2n plain-ish successes (route 1 + route 3 parents) plus n chain children.
	wantCompleted := int64(n + n*2)
	wantFailed := int64(n * 2)
	f.runUntil(t, 60*time.Second, func() bool {
		return completed.Load() >= wantCompleted && failed.Load() >= wantFailed
	})

	// Cascade cancellation happens after the parent's terminal write, and the
	// TTL on each record starts when that record reaches its own terminal state.
	// Waiting a comfortable multiple of the TTL keeps this from turning into a
	// race the test loses on a slow machine.
	time.Sleep(reclaimTTL + 5*time.Second)

	if left := scanJobKeys(t, f.rc); len(left) > 0 {
		t.Errorf("%d job key(s) survived expiry — retention does not reach them and nothing else will:\n%s",
			len(left), summariseLeftovers(t, f.rc, left))
	}
}

// scanJobKeys returns every key under the prefix that belongs to a job, hash and
// dependency sets alike.
func scanJobKeys(t *testing.T, rc *RedisClient) []string {
	t.Helper()

	ctx := context.Background()
	pattern := rc.Key("job", "*")
	var out []string
	var cursor uint64
	for {
		keys, next, err := rc.rdb.Scan(ctx, cursor, pattern, 500).Result()
		if err != nil {
			t.Fatalf("scanning %s: %v", pattern, err)
		}
		out = append(out, keys...)
		cursor = next
		if cursor == 0 {
			break
		}
	}
	sort.Strings(out)
	return out
}

// summariseLeftovers groups surviving keys by kind, and for a job hash also by
// the status and TTL it was left with. "40 canceled records with TTL -1" names
// the mechanism that failed; a list of ids does not.
func summariseLeftovers(t *testing.T, rc *RedisClient, keys []string) string {
	t.Helper()

	ctx := context.Background()
	counts := map[string]int{}
	example := map[string]string{}

	for _, k := range keys {
		kind := "job hash"
		for _, suffix := range []string{":deps", ":pending_deps", ":dependents"} {
			if strings.HasSuffix(k, suffix) {
				kind = "set " + suffix
				break
			}
		}
		if kind == "job hash" {
			status, err := rc.rdb.HGet(ctx, k, "status").Result()
			if err != nil {
				status = "<unreadable>"
			}
			ttl, err := rc.rdb.Do(ctx, "TTL", k).Int64()
			if err != nil {
				ttl = -99
			}
			kind = fmt.Sprintf("%s, TTL %d", status, ttl)
		}
		counts[kind]++
		if _, seen := example[kind]; !seen {
			example[kind] = k
		}
	}

	kinds := make([]string, 0, len(counts))
	for kind := range counts {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)

	var b strings.Builder
	for _, kind := range kinds {
		fmt.Fprintf(&b, "  %-24s %4d   e.g. %s\n", kind, counts[kind], example[kind])
	}
	return strings.TrimRight(b.String(), "\n")
}
