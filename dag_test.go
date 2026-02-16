package gqm

import (
	"context"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
)

func testScripts(t *testing.T) *scriptRegistry {
	t.Helper()
	sr := &scriptRegistry{scripts: make(map[string]*redis.Script)}
	if err := sr.load(); err != nil {
		t.Fatalf("loading scripts: %v", err)
	}
	return sr
}

func TestDetectCycle_NoCycle(t *testing.T) {
	rc := testRedisClient(t)
	ctx := context.Background()

	// Setup: A depends on B, B depends on C (no cycle).
	rc.rdb.SAdd(ctx, rc.Key("job", "B", "deps"), "C")
	rc.rdb.SAdd(ctx, rc.Key("job", "C", "deps"))
	t.Cleanup(func() {
		rc.rdb.Del(ctx, rc.Key("job", "B", "deps"), rc.Key("job", "C", "deps"))
	})

	err := detectCycle(ctx, rc, "A", []string{"B"})
	if err != nil {
		t.Errorf("detectCycle returned error for acyclic graph: %v", err)
	}
}

func TestDetectCycle_DirectCycle(t *testing.T) {
	rc := testRedisClient(t)
	ctx := context.Background()

	// Setup: B already depends on A. Adding A depends on B creates a cycle.
	rc.rdb.SAdd(ctx, rc.Key("job", "B", "deps"), "A")
	t.Cleanup(func() {
		rc.rdb.Del(ctx, rc.Key("job", "B", "deps"))
	})

	err := detectCycle(ctx, rc, "A", []string{"B"})
	if err == nil {
		t.Fatal("detectCycle should return error for direct cycle")
	}
	if err != ErrCyclicDependency {
		t.Errorf("error = %v, want ErrCyclicDependency", err)
	}
}

func TestDetectCycle_IndirectCycle(t *testing.T) {
	rc := testRedisClient(t)
	ctx := context.Background()

	// Setup: B→C→A. Adding A→B creates cycle A→B→C→A.
	rc.rdb.SAdd(ctx, rc.Key("job", "B", "deps"), "C")
	rc.rdb.SAdd(ctx, rc.Key("job", "C", "deps"), "A")
	t.Cleanup(func() {
		rc.rdb.Del(ctx, rc.Key("job", "B", "deps"), rc.Key("job", "C", "deps"))
	})

	err := detectCycle(ctx, rc, "A", []string{"B"})
	if err == nil {
		t.Fatal("detectCycle should return error for indirect cycle")
	}
}

func TestDetectCycle_DepthLimit(t *testing.T) {
	rc := testRedisClient(t)
	ctx := context.Background()

	// Build chain deeper than maxDependencyDepth.
	var keys []string
	for i := 0; i <= maxDependencyDepth; i++ {
		node := fmt.Sprintf("node-%d", i)
		next := fmt.Sprintf("node-%d", i+1)
		key := rc.Key("job", node, "deps")
		rc.rdb.SAdd(ctx, key, next)
		keys = append(keys, key)
	}
	t.Cleanup(func() {
		rc.rdb.Del(ctx, keys...)
	})

	err := detectCycle(ctx, rc, "target", []string{"node-0"})
	if err == nil {
		t.Fatal("detectCycle should return error when depth exceeds limit")
	}
}

func TestResolveDependents_SingleParent(t *testing.T) {
	rc := testRedisClient(t)
	scripts := testScripts(t)
	ctx := context.Background()

	// Setup: child depends on parent-1 only.
	// child is deferred, parent-1 has child as dependent.
	rc.rdb.HSet(ctx, rc.Key("job", "child"), "status", "deferred", "queue", "default")
	rc.rdb.SAdd(ctx, rc.Key("job", "child", "pending_deps"), "parent-1")
	rc.rdb.SAdd(ctx, rc.Key("job", "parent-1", "dependents"), "child")
	rc.rdb.SAdd(ctx, rc.Key("deferred"), "child")

	t.Cleanup(func() {
		rc.rdb.Del(ctx,
			rc.Key("job", "child"),
			rc.Key("job", "child", "pending_deps"),
			rc.Key("job", "parent-1", "dependents"),
			rc.Key("deferred"),
			rc.Key("queue", "default", "ready"),
		)
	})

	promoted, err := resolveDependents(ctx, rc, scripts, "parent-1")
	if err != nil {
		t.Fatalf("resolveDependents: %v", err)
	}
	if len(promoted) != 1 || promoted[0] != "child" {
		t.Errorf("promoted = %v, want [child]", promoted)
	}

	// Verify child is now ready.
	status, _ := rc.rdb.HGet(ctx, rc.Key("job", "child"), "status").Result()
	if status != "ready" {
		t.Errorf("child status = %q, want %q", status, "ready")
	}

	// Verify child is in the ready queue.
	members, _ := rc.rdb.LRange(ctx, rc.Key("queue", "default", "ready"), 0, -1).Result()
	found := false
	for _, m := range members {
		if m == "child" {
			found = true
		}
	}
	if !found {
		t.Error("child should be in the ready queue")
	}

	// Verify child removed from deferred set.
	deferred, _ := rc.rdb.SIsMember(ctx, rc.Key("deferred"), "child").Result()
	if deferred {
		t.Error("child should be removed from deferred set")
	}
}

func TestResolveDependents_MultipleParents(t *testing.T) {
	rc := testRedisClient(t)
	scripts := testScripts(t)
	ctx := context.Background()

	// Setup: child depends on parent-1 and parent-2.
	rc.rdb.HSet(ctx, rc.Key("job", "child"), "status", "deferred", "queue", "default")
	rc.rdb.SAdd(ctx, rc.Key("job", "child", "pending_deps"), "parent-1", "parent-2")
	rc.rdb.SAdd(ctx, rc.Key("job", "parent-1", "dependents"), "child")
	rc.rdb.SAdd(ctx, rc.Key("job", "parent-2", "dependents"), "child")
	rc.rdb.SAdd(ctx, rc.Key("deferred"), "child")

	t.Cleanup(func() {
		rc.rdb.Del(ctx,
			rc.Key("job", "child"),
			rc.Key("job", "child", "pending_deps"),
			rc.Key("job", "parent-1", "dependents"),
			rc.Key("job", "parent-2", "dependents"),
			rc.Key("deferred"),
			rc.Key("queue", "default", "ready"),
		)
	})

	// Resolve parent-1: child still has parent-2 pending.
	promoted, err := resolveDependents(ctx, rc, scripts, "parent-1")
	if err != nil {
		t.Fatalf("resolveDependents(parent-1): %v", err)
	}
	if len(promoted) != 0 {
		t.Errorf("after parent-1: promoted = %v, want []", promoted)
	}

	status, _ := rc.rdb.HGet(ctx, rc.Key("job", "child"), "status").Result()
	if status != "deferred" {
		t.Errorf("after parent-1: child status = %q, want %q", status, "deferred")
	}

	// Resolve parent-2: now all deps met, child should be promoted.
	promoted, err = resolveDependents(ctx, rc, scripts, "parent-2")
	if err != nil {
		t.Fatalf("resolveDependents(parent-2): %v", err)
	}
	if len(promoted) != 1 || promoted[0] != "child" {
		t.Errorf("after parent-2: promoted = %v, want [child]", promoted)
	}

	status, _ = rc.rdb.HGet(ctx, rc.Key("job", "child"), "status").Result()
	if status != "ready" {
		t.Errorf("after parent-2: child status = %q, want %q", status, "ready")
	}
}

func TestResolveDependents_EnqueueAtFront(t *testing.T) {
	rc := testRedisClient(t)
	scripts := testScripts(t)
	ctx := context.Background()

	// Pre-populate queue with an existing job.
	rc.rdb.RPush(ctx, rc.Key("queue", "default", "ready"), "existing-job")

	// Setup: child with enqueue_at_front=1.
	rc.rdb.HSet(ctx, rc.Key("job", "child"), "status", "deferred", "queue", "default", "enqueue_at_front", "1")
	rc.rdb.SAdd(ctx, rc.Key("job", "child", "pending_deps"), "parent-1")
	rc.rdb.SAdd(ctx, rc.Key("job", "parent-1", "dependents"), "child")
	rc.rdb.SAdd(ctx, rc.Key("deferred"), "child")

	t.Cleanup(func() {
		rc.rdb.Del(ctx,
			rc.Key("job", "child"),
			rc.Key("job", "child", "pending_deps"),
			rc.Key("job", "parent-1", "dependents"),
			rc.Key("deferred"),
			rc.Key("queue", "default", "ready"),
		)
	})

	_, err := resolveDependents(ctx, rc, scripts, "parent-1")
	if err != nil {
		t.Fatalf("resolveDependents: %v", err)
	}

	// Child should be at the front for RPOP (rightmost position via RPUSH).
	front, _ := rc.rdb.LIndex(ctx, rc.Key("queue", "default", "ready"), -1).Result()
	if front != "child" {
		t.Errorf("front of queue (RPOP side) = %q, want %q (enqueue_at_front)", front, "child")
	}
}

func TestPropagateFailure_AllowFailure(t *testing.T) {
	rc := testRedisClient(t)
	scripts := testScripts(t)
	ctx := context.Background()

	// Setup: child depends on parent, allow_failure=1.
	rc.rdb.HSet(ctx, rc.Key("job", "child"), "status", "deferred", "queue", "default", "allow_failure", "1")
	rc.rdb.SAdd(ctx, rc.Key("job", "child", "pending_deps"), "parent")
	rc.rdb.SAdd(ctx, rc.Key("job", "parent", "dependents"), "child")
	rc.rdb.SAdd(ctx, rc.Key("deferred"), "child")

	t.Cleanup(func() {
		rc.rdb.Del(ctx,
			rc.Key("job", "child"),
			rc.Key("job", "child", "pending_deps"),
			rc.Key("job", "parent", "dependents"),
			rc.Key("deferred"),
			rc.Key("queue", "default", "ready"),
		)
	})

	err := propagateFailure(ctx, rc, scripts, "parent")
	if err != nil {
		t.Fatalf("propagateFailure: %v", err)
	}

	// Child should be promoted (allow_failure treats failed parent as resolved).
	status, _ := rc.rdb.HGet(ctx, rc.Key("job", "child"), "status").Result()
	if status != "ready" {
		t.Errorf("child status = %q, want %q", status, "ready")
	}
}

func TestPropagateFailure_CascadeCancel(t *testing.T) {
	rc := testRedisClient(t)
	scripts := testScripts(t)
	ctx := context.Background()

	// Setup: parent → child (allow_failure=0) → grandchild (allow_failure=0).
	rc.rdb.HSet(ctx, rc.Key("job", "child"), "status", "deferred", "queue", "default")
	rc.rdb.HSet(ctx, rc.Key("job", "grandchild"), "status", "deferred", "queue", "default")
	rc.rdb.SAdd(ctx, rc.Key("job", "child", "pending_deps"), "parent")
	rc.rdb.SAdd(ctx, rc.Key("job", "grandchild", "pending_deps"), "child")
	rc.rdb.SAdd(ctx, rc.Key("job", "parent", "dependents"), "child")
	rc.rdb.SAdd(ctx, rc.Key("job", "child", "dependents"), "grandchild")
	rc.rdb.SAdd(ctx, rc.Key("deferred"), "child", "grandchild")

	t.Cleanup(func() {
		rc.rdb.Del(ctx,
			rc.Key("job", "child"),
			rc.Key("job", "grandchild"),
			rc.Key("job", "child", "pending_deps"),
			rc.Key("job", "grandchild", "pending_deps"),
			rc.Key("job", "parent", "dependents"),
			rc.Key("job", "child", "dependents"),
			rc.Key("deferred"),
		)
	})

	err := propagateFailure(ctx, rc, scripts, "parent")
	if err != nil {
		t.Fatalf("propagateFailure: %v", err)
	}

	// Both child and grandchild should be canceled.
	childStatus, _ := rc.rdb.HGet(ctx, rc.Key("job", "child"), "status").Result()
	if childStatus != StatusCanceled {
		t.Errorf("child status = %q, want %q", childStatus, StatusCanceled)
	}
	grandchildStatus, _ := rc.rdb.HGet(ctx, rc.Key("job", "grandchild"), "status").Result()
	if grandchildStatus != StatusCanceled {
		t.Errorf("grandchild status = %q, want %q", grandchildStatus, StatusCanceled)
	}

	// Both should be removed from deferred set.
	childDeferred, _ := rc.rdb.SIsMember(ctx, rc.Key("deferred"), "child").Result()
	if childDeferred {
		t.Error("child should be removed from deferred set")
	}
	grandchildDeferred, _ := rc.rdb.SIsMember(ctx, rc.Key("deferred"), "grandchild").Result()
	if grandchildDeferred {
		t.Error("grandchild should be removed from deferred set")
	}
}

func TestCancelDependents_AlreadyPromoted(t *testing.T) {
	rc := testRedisClient(t)
	scripts := testScripts(t)
	ctx := context.Background()

	// Setup: job already promoted to "ready" (not "deferred").
	rc.rdb.HSet(ctx, rc.Key("job", "promoted-job"), "status", "ready", "queue", "default")
	rc.rdb.SAdd(ctx, rc.Key("deferred"), "promoted-job")

	t.Cleanup(func() {
		rc.rdb.Del(ctx,
			rc.Key("job", "promoted-job"),
			rc.Key("deferred"),
			rc.Key("job", "promoted-job", "pending_deps"),
		)
	})

	err := cancelDependents(ctx, rc, scripts, "promoted-job")
	if err != nil {
		t.Fatalf("cancelDependents: %v", err)
	}

	// Status should remain "ready" (cancel is no-op).
	status, _ := rc.rdb.HGet(ctx, rc.Key("job", "promoted-job"), "status").Result()
	if status != "ready" {
		t.Errorf("status = %q, want %q (should not overwrite)", status, "ready")
	}
}

func TestCancelDependents_MixedAllowFailure(t *testing.T) {
	rc := testRedisClient(t)
	scripts := testScripts(t)
	ctx := context.Background()

	// Setup: parent → child-allow (allow_failure=1) + child-deny (allow_failure=0).
	// parent is deferred and will be canceled.
	rc.rdb.HSet(ctx, rc.Key("job", "parent"), "status", "deferred", "queue", "default")
	rc.rdb.HSet(ctx, rc.Key("job", "child-allow"), "status", "deferred", "queue", "default", "allow_failure", "1")
	rc.rdb.HSet(ctx, rc.Key("job", "child-deny"), "status", "deferred", "queue", "default")
	rc.rdb.SAdd(ctx, rc.Key("job", "child-allow", "pending_deps"), "parent")
	rc.rdb.SAdd(ctx, rc.Key("job", "child-deny", "pending_deps"), "parent")
	rc.rdb.SAdd(ctx, rc.Key("job", "parent", "dependents"), "child-allow", "child-deny")
	rc.rdb.SAdd(ctx, rc.Key("deferred"), "parent", "child-allow", "child-deny")

	t.Cleanup(func() {
		rc.rdb.Del(ctx,
			rc.Key("job", "parent"),
			rc.Key("job", "child-allow"),
			rc.Key("job", "child-deny"),
			rc.Key("job", "child-allow", "pending_deps"),
			rc.Key("job", "child-deny", "pending_deps"),
			rc.Key("job", "parent", "dependents"),
			rc.Key("deferred"),
			rc.Key("queue", "default", "ready"),
		)
	})

	err := cancelDependents(ctx, rc, scripts, "parent")
	if err != nil {
		t.Fatalf("cancelDependents: %v", err)
	}

	// child-allow: allow_failure=1 → resolved (promoted to ready).
	allowStatus, _ := rc.rdb.HGet(ctx, rc.Key("job", "child-allow"), "status").Result()
	if allowStatus != "ready" {
		t.Errorf("child-allow status = %q, want %q", allowStatus, "ready")
	}

	// child-deny: allow_failure=0 → canceled.
	denyStatus, _ := rc.rdb.HGet(ctx, rc.Key("job", "child-deny"), "status").Result()
	if denyStatus != StatusCanceled {
		t.Errorf("child-deny status = %q, want %q", denyStatus, StatusCanceled)
	}
}

func TestClient_Enqueue_Unique(t *testing.T) {
	rc := testRedisClient(t)
	ctx := context.Background()
	client := &Client{rc: rc}

	job, err := client.Enqueue(ctx, "test.job", Payload{"x": 1},
		JobID("unique-job-1"),
		Unique(),
	)
	if err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	if job.ID != "unique-job-1" {
		t.Errorf("job ID = %q, want %q", job.ID, "unique-job-1")
	}

	t.Cleanup(func() {
		rc.rdb.Del(ctx,
			rc.Key("job", "unique-job-1"),
			rc.Key("queue", "default", "ready"),
			rc.Key("queues"),
		)
	})

	// Second enqueue with same ID should fail.
	_, err = client.Enqueue(ctx, "test.job", Payload{"x": 2},
		JobID("unique-job-1"),
		Unique(),
	)
	if err != ErrDuplicateJobID {
		t.Errorf("second Enqueue error = %v, want ErrDuplicateJobID", err)
	}
}

func TestClient_Enqueue_WithoutUnique_AllowsDuplicate(t *testing.T) {
	rc := testRedisClient(t)
	ctx := context.Background()
	client := &Client{rc: rc}

	t.Cleanup(func() {
		rc.rdb.Del(ctx,
			rc.Key("job", "dup-job-1"),
			rc.Key("queue", "default", "ready"),
			rc.Key("queues"),
		)
	})

	// Without Unique(), same ID is allowed (overwrites).
	_, err := client.Enqueue(ctx, "test.job", Payload{"x": 1}, JobID("dup-job-1"))
	if err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	_, err = client.Enqueue(ctx, "test.job", Payload{"x": 2}, JobID("dup-job-1"))
	if err != nil {
		t.Fatalf("second Enqueue (no Unique): %v", err)
	}
}

func TestClient_EnqueueDeferred_Unique_Duplicate(t *testing.T) {
	rc := testRedisClient(t)
	ctx := context.Background()
	client := &Client{rc: rc}

	// Create a parent job first.
	parentJob, err := client.Enqueue(ctx, "parent.job", Payload{}, JobID("parent-for-unique"))
	if err != nil {
		t.Fatalf("Enqueue parent: %v", err)
	}

	t.Cleanup(func() {
		rc.rdb.Del(ctx,
			rc.Key("job", parentJob.ID),
			rc.Key("job", "deferred-unique-1"),
			rc.Key("job", "deferred-unique-1", "deps"),
			rc.Key("job", "deferred-unique-1", "pending_deps"),
			rc.Key("job", parentJob.ID, "dependents"),
			rc.Key("deferred"),
			rc.Key("queue", "default", "ready"),
			rc.Key("queues"),
		)
	})

	// First deferred enqueue.
	_, err = client.Enqueue(ctx, "child.job", Payload{},
		JobID("deferred-unique-1"),
		DependsOn(parentJob.ID),
		Unique(),
	)
	if err != nil {
		t.Fatalf("first deferred Enqueue: %v", err)
	}

	// Second deferred enqueue with same ID should fail.
	_, err = client.Enqueue(ctx, "child.job", Payload{},
		JobID("deferred-unique-1"),
		DependsOn(parentJob.ID),
		Unique(),
	)
	if err != ErrDuplicateJobID {
		t.Errorf("second deferred Enqueue error = %v, want ErrDuplicateJobID", err)
	}
}

func TestDequeue_StatusGuard_NonReadySkipped(t *testing.T) {
	rc := testRedisClient(t)
	scripts := testScripts(t)
	ctx := context.Background()

	queue := "guard-test"
	readyKey := rc.Key("queue", queue, "ready")
	processingKey := rc.Key("queue", queue, "processing")
	jobPrefix := rc.Key("job") + ":"

	// Put a canceled job ID in the ready queue.
	rc.rdb.HSet(ctx, rc.Key("job", "canceled-job"), "status", StatusCanceled, "queue", queue)
	rc.rdb.LPush(ctx, readyKey, "canceled-job")

	t.Cleanup(func() {
		rc.rdb.Del(ctx,
			rc.Key("job", "canceled-job"),
			readyKey, processingKey,
		)
	})

	// Dequeue should skip the canceled job and return nil (empty).
	result := scripts.run(ctx, rc.rdb, "dequeue",
		[]string{readyKey, processingKey, jobPrefix},
		fmt.Sprintf("%d", 9999999999), "3600", "test-worker",
	)

	jobID, _ := result.Text()
	if jobID != "" {
		t.Errorf("expected empty dequeue (canceled job skipped), got %q", jobID)
	}

	// Verify the canceled job was NOT added to the processing set.
	score, err := rc.rdb.ZScore(ctx, processingKey, "canceled-job").Result()
	if err == nil {
		t.Errorf("canceled job should not be in processing set, score = %v", score)
	}
}

func TestDequeue_StatusGuard_MissingHashSkipped(t *testing.T) {
	rc := testRedisClient(t)
	scripts := testScripts(t)
	ctx := context.Background()

	queue := "guard-test-missing"
	readyKey := rc.Key("queue", queue, "ready")
	processingKey := rc.Key("queue", queue, "processing")
	jobPrefix := rc.Key("job") + ":"

	// Put a job ID in the ready queue WITHOUT creating the hash.
	rc.rdb.LPush(ctx, readyKey, "phantom-job")

	t.Cleanup(func() {
		rc.rdb.Del(ctx, readyKey, processingKey)
	})

	// Dequeue should skip the phantom job (no hash = status nil != 'ready').
	result := scripts.run(ctx, rc.rdb, "dequeue",
		[]string{readyKey, processingKey, jobPrefix},
		fmt.Sprintf("%d", 9999999999), "3600", "test-worker",
	)

	jobID, _ := result.Text()
	if jobID != "" {
		t.Errorf("expected empty dequeue (phantom job skipped), got %q", jobID)
	}

	// Verify the phantom job was NOT added to the processing set.
	count, _ := rc.rdb.ZCard(ctx, processingKey).Result()
	if count != 0 {
		t.Errorf("processing set should be empty, got %d entries", count)
	}
}
