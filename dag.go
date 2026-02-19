package gqm

import (
	"context"
	"fmt"
	"log/slog"
)

// maxDependencyDepth is the maximum depth for cycle detection traversal.
const maxDependencyDepth = 100

// detectCycle performs DFS traversal on the dependency graph to check if adding
// parentIDs as dependencies of jobID would create a cycle.
//
// It traverses each parent's ancestors via the Redis deps sets. If jobID is
// found anywhere in the ancestor chain, a cycle is detected.
//
// Limitation: if two clients concurrently enqueue jobs that form a cycle
// (e.g., A→B and B→A), both detectCycle calls may pass before either writes
// their deps sets, creating a stuck cycle. This requires a distributed lock
// or Lua script for the entire enqueue-deferred flow to fully prevent.
func detectCycle(ctx context.Context, rc *RedisClient, jobID string, parentIDs []string) error {
	visited := make(map[string]bool)
	visited[jobID] = true

	for _, pid := range parentIDs {
		if err := dfsCheckCycle(ctx, rc, pid, visited, 0); err != nil {
			return err
		}
	}
	return nil
}

// dfsCheckCycle recursively checks whether the given nodeID or any of its
// ancestors are already in the visited set (which includes the target jobID).
func dfsCheckCycle(ctx context.Context, rc *RedisClient, nodeID string, visited map[string]bool, depth int) error {
	if depth >= maxDependencyDepth {
		return fmt.Errorf("%w: dependency depth exceeds %d", ErrCyclicDependency, maxDependencyDepth)
	}
	if visited[nodeID] {
		return ErrCyclicDependency
	}

	visited[nodeID] = true
	defer func() { visited[nodeID] = false }()

	depsKey := rc.Key("job", nodeID, "deps")
	deps, err := rc.rdb.SMembers(ctx, depsKey).Result()
	if err != nil {
		return fmt.Errorf("reading deps for job %s: %w", nodeID, err)
	}

	for _, dep := range deps {
		if err := dfsCheckCycle(ctx, rc, dep, visited, depth+1); err != nil {
			return err
		}
	}

	return nil
}

// resolveDependents checks all dependents of a completed parent job and
// atomically promotes any whose dependencies are fully met to the ready queue.
// It uses the dag_resolve Lua script for atomicity to prevent race conditions
// when multiple parents complete concurrently.
//
// Returns the list of job IDs that were promoted to ready.
func resolveDependents(ctx context.Context, rc *RedisClient, scripts *scriptRegistry, parentJobID string) ([]string, error) {
	dependentsKey := rc.Key("job", parentJobID, "dependents")
	dependentIDs, err := rc.rdb.SMembers(ctx, dependentsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("reading dependents for job %s: %w", parentJobID, err)
	}

	if len(dependentIDs) == 0 {
		return nil, nil
	}

	var promoted []string
	for _, depID := range dependentIDs {
		jobKey := rc.Key("job", depID)

		// Read dependent job's queue and enqueue_at_front flag.
		vals, err := rc.rdb.HMGet(ctx, jobKey, "queue", "enqueue_at_front").Result()
		if err != nil {
			return promoted, fmt.Errorf("reading dependent job %s: %w", depID, err)
		}
		if vals[0] == nil {
			// Job hash missing — skip silently.
			continue
		}

		queue, _ := vals[0].(string)
		if queue == "" {
			queue = "default"
		}
		atFront := "0"
		if v, _ := vals[1].(string); v == "1" {
			atFront = "1"
		}

		pendingDepsKey := rc.Key("job", depID, "pending_deps")
		queueKey := rc.Key("queue", queue, "ready")
		deferredKey := rc.Key("deferred")

		result := scripts.run(ctx, rc.rdb, "dag_resolve",
			[]string{pendingDepsKey, queueKey, jobKey, deferredKey},
			parentJobID, depID, atFront,
		)
		if result.Err() != nil {
			return promoted, fmt.Errorf("resolving dependency for job %s: %w", depID, result.Err())
		}

		val, err := result.Int64()
		if err != nil {
			return promoted, fmt.Errorf("reading dag_resolve result for job %s: %w", depID, err)
		}
		if val == 1 {
			promoted = append(promoted, depID)
			// Clean up DAG metadata keys now that the job is promoted.
			depsKey := rc.Key("job", depID, "deps")
			if err := rc.rdb.Del(ctx, depsKey, pendingDepsKey).Err(); err != nil && ctx.Err() == nil {
				slog.Warn("failed to clean up DAG metadata for promoted job",
					"job_id", depID, "error", err)
			}
		}
	}

	// Clean up parent's dependents set after all resolutions.
	if err := rc.rdb.Del(ctx, dependentsKey).Err(); err != nil && ctx.Err() == nil {
		slog.Warn("failed to clean up parent dependents set",
			"parent_job_id", parentJobID, "error", err)
	}

	return promoted, nil
}

// propagateFailure handles dependents of a parent job that has permanently
// failed (moved to DLQ). For each dependent:
//   - allow_failure=true: resolve normally (remove parent from pending_deps, promote if ready)
//   - allow_failure=false: cancel the dependent and cascade to its own dependents
func propagateFailure(ctx context.Context, rc *RedisClient, scripts *scriptRegistry, parentJobID string) error {
	dependentsKey := rc.Key("job", parentJobID, "dependents")
	dependentIDs, err := rc.rdb.SMembers(ctx, dependentsKey).Result()
	if err != nil {
		return fmt.Errorf("reading dependents for failed job %s: %w", parentJobID, err)
	}

	for _, depID := range dependentIDs {
		jobKey := rc.Key("job", depID)

		// Read allow_failure, queue, and enqueue_at_front for this dependent.
		vals, err := rc.rdb.HMGet(ctx, jobKey, "allow_failure", "queue", "enqueue_at_front").Result()
		if err != nil {
			return fmt.Errorf("reading dependent job %s: %w", depID, err)
		}
		if vals[1] == nil {
			// Job hash missing — skip.
			continue
		}

		allowFailure, _ := vals[0].(string)
		if allowFailure == "1" {
			// Treat failed parent as resolved — remove from pending_deps.
			queue, _ := vals[1].(string)
			if queue == "" {
				queue = "default"
			}
			atFront := "0"
			if v, _ := vals[2].(string); v == "1" {
				atFront = "1"
			}

			pendingDepsKey := rc.Key("job", depID, "pending_deps")
			queueKey := rc.Key("queue", queue, "ready")
			deferredKey := rc.Key("deferred")

			result := scripts.run(ctx, rc.rdb, "dag_resolve",
				[]string{pendingDepsKey, queueKey, jobKey, deferredKey},
				parentJobID, depID, atFront,
			)
			if result.Err() != nil {
				return fmt.Errorf("resolving dependency for job %s after parent failure: %w", depID, result.Err())
			}
		} else {
			// Cancel dependent and cascade.
			if err := cancelDependents(ctx, rc, scripts, depID); err != nil {
				return err
			}
		}
	}

	return nil
}

// cancelDependents atomically cancels a deferred job and recursively cascades
// cancellation to all of its own dependents that have allow_failure=false.
func cancelDependents(ctx context.Context, rc *RedisClient, scripts *scriptRegistry, jobID string) error {
	return cancelDependentsRecursive(ctx, rc, scripts, jobID, 0)
}

// cancelDependentsRecursive is the depth-limited implementation of cancelDependents.
func cancelDependentsRecursive(ctx context.Context, rc *RedisClient, scripts *scriptRegistry, jobID string, depth int) error {
	if depth >= maxDependencyDepth {
		return fmt.Errorf("cancel cascade depth exceeds %d for job %s", maxDependencyDepth, jobID)
	}

	jobKey := rc.Key("job", jobID)
	deferredKey := rc.Key("deferred")
	pendingDepsKey := rc.Key("job", jobID, "pending_deps")

	// Atomically cancel this job (only if still deferred).
	result := scripts.run(ctx, rc.rdb, "dag_cancel",
		[]string{jobKey, deferredKey, pendingDepsKey},
		jobID,
	)
	if result.Err() != nil {
		return fmt.Errorf("canceling dependent job %s: %w", jobID, result.Err())
	}

	val, err := result.Int64()
	if err != nil {
		return fmt.Errorf("reading dag_cancel result for job %s: %w", jobID, err)
	}
	if val == 0 {
		// Job was not deferred (already promoted or canceled) — stop cascade.
		return nil
	}

	// Cascade: cancel this job's own dependents.
	dependentsKey := rc.Key("job", jobID, "dependents")
	dependentIDs, err := rc.rdb.SMembers(ctx, dependentsKey).Result()
	if err != nil {
		return fmt.Errorf("reading dependents for canceled job %s: %w", jobID, err)
	}

	for _, depID := range dependentIDs {
		depJobKey := rc.Key("job", depID)
		// Missing allow_failure field (redis.Nil) is treated as false.
		allowFailure, _ := rc.rdb.HGet(ctx, depJobKey, "allow_failure").Result()

		if allowFailure == "1" {
			// This dependent tolerates parent failure — resolve instead of cancel.
			vals, err := rc.rdb.HMGet(ctx, depJobKey, "queue", "enqueue_at_front").Result()
			if err != nil {
				return fmt.Errorf("reading allow_failure dependent job %s: %w", depID, err)
			}
			queue, _ := vals[0].(string)
			if queue == "" {
				queue = "default"
			}
			atFront := "0"
			if v, _ := vals[1].(string); v == "1" {
				atFront = "1"
			}

			depPendingKey := rc.Key("job", depID, "pending_deps")
			queueKey := rc.Key("queue", queue, "ready")

			result := scripts.run(ctx, rc.rdb, "dag_resolve",
				[]string{depPendingKey, queueKey, depJobKey, deferredKey},
				jobID, depID, atFront,
			)
			if result.Err() != nil {
				return fmt.Errorf("resolving allow_failure dependent %s: %w", depID, result.Err())
			}
			// Clean up DAG metadata for the resolved dependent.
			depDepsKey := rc.Key("job", depID, "deps")
			if err := rc.rdb.Del(ctx, depDepsKey, depPendingKey).Err(); err != nil && ctx.Err() == nil {
				slog.Warn("failed to clean up DAG metadata for allow_failure dependent",
					"job_id", depID, "error", err)
			}
		} else {
			// Recursive cascade cancel.
			if err := cancelDependentsRecursive(ctx, rc, scripts, depID, depth+1); err != nil {
				return err
			}
		}
	}

	// Clean up this job's DAG metadata keys.
	depsKey := rc.Key("job", jobID, "deps")
	if err := rc.rdb.Del(ctx, depsKey, dependentsKey).Err(); err != nil && ctx.Err() == nil {
		slog.Warn("failed to clean up DAG metadata for canceled job",
			"job_id", jobID, "error", err)
	}

	return nil
}
