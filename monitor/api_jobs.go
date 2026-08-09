package monitor

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

// handleGetJob returns details for a single job.
func (m *Monitor) handleGetJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	if !validateJobIDParam(w, "id", id) {
		return
	}

	jobKey := m.key("job", id)
	data, err := m.rdb.HGetAll(ctx, jobKey).Result()
	if err != nil || len(data) == 0 {
		writeError(w, http.StatusNotFound, "job not found", "NOT_FOUND")
		return
	}

	job := mapToJobResponse(data)
	m.annotateStale(ctx, []map[string]any{job})
	writeJSON(w, http.StatusOK, response{Data: job})
}

// staleMarginSeconds is how long past its processing deadline a claim may sit
// before it is reported as stale.
//
// A live worker leaves the processing set by its deadline plus the pool grace
// period; the server's reaper waits that long plus a slack window before
// reclaiming the job. This constant mirrors the sum of the two defaults so the
// dashboard and the reaper agree on which claims are abandoned. Pools
// configured with a longer grace period than the default may see the flag
// appear slightly before the reaper acts.
const staleMarginSeconds int64 = 25 // default grace period (10s) + reaper slack (15s)

// annotateStale adds the processing deadline and a stale flag to every
// processing job in jobs. A job whose deadline has passed by more than
// staleMarginSeconds is one no live worker could still be holding, so rendering
// it as a healthy PROCESSING job would misreport a dead process as work in
// flight.
//
// Jobs in any other status, and processing jobs with no entry in their queue's
// processing set, are left untouched: there is no deadline to judge them by.
func (m *Monitor) annotateStale(ctx context.Context, jobs []map[string]any) {
	type claim struct {
		job map[string]any
		cmd *redis.FloatCmd
	}

	pipe := m.rdb.Pipeline()
	claims := make([]claim, 0, len(jobs))
	for _, job := range jobs {
		if status, _ := job["status"].(string); status != "processing" {
			continue
		}
		id, _ := job["id"].(string)
		queue, _ := job["queue"].(string)
		if id == "" || queue == "" {
			continue
		}
		claims = append(claims, claim{
			job: job,
			cmd: pipe.ZScore(ctx, m.key("queue", queue, "processing"), id),
		})
	}
	if len(claims) == 0 {
		return
	}
	pipe.Exec(ctx)

	now := time.Now().Unix()
	for _, c := range claims {
		deadline, err := c.cmd.Result()
		if err != nil {
			continue
		}
		c.job["processing_deadline"] = int64(deadline)
		c.job["stale"] = now > int64(deadline)+staleMarginSeconds
	}
}

// handleListDLQ returns paginated dead letter queue jobs.
func (m *Monitor) handleListDLQ(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if !validatePathParam(w, "name", name) {
		return
	}
	page, limit := pagination(r)

	// Same listing as ?status=dead_letter on the queue jobs endpoint, down to
	// the pagination and the orphan repair — sharing it keeps the two from
	// drifting into reporting different counts for the same sorted set.
	m.listSortedSetJobs(w, r, m.key("queue", name, "dead_letter"), page, limit)
}

// fetchJobSummaries fetches job data for a list of job IDs using pipelining.
func (m *Monitor) fetchJobSummaries(ctx context.Context, jobIDs []string) []map[string]any {
	jobs, _ := m.fetchJobSummariesWithMissing(ctx, jobIDs)
	return jobs
}

// fetchJobSummariesWithMissing is fetchJobSummaries plus the ids that no longer
// have a job hash, so a caller listing from an index can tell an index entry
// that outlived its job from one that is simply not there yet.
//
// Only an empty hash counts as missing: a Redis error says nothing about
// whether the job exists, and treating it as absence would delete live entries
// during an outage.
func (m *Monitor) fetchJobSummariesWithMissing(ctx context.Context, jobIDs []string) ([]map[string]any, []string) {
	if len(jobIDs) == 0 {
		return []map[string]any{}, nil
	}

	pipe := m.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(jobIDs))
	for i, id := range jobIDs {
		cmds[i] = pipe.HGetAll(ctx, m.key("job", id))
	}
	pipe.Exec(ctx)

	jobs := make([]map[string]any, 0, len(jobIDs))
	var missing []string
	for i, cmd := range cmds {
		data, err := cmd.Result()
		if err != nil {
			continue
		}
		if len(data) == 0 {
			missing = append(missing, jobIDs[i])
			continue
		}
		jobs = append(jobs, mapToJobResponse(data))
	}
	m.annotateStale(ctx, jobs)
	return jobs, missing
}

// jobAllowedFields is the set of job hash fields that are safe to expose in
// API responses. Fields not in this set are filtered out to prevent accidental
// leakage of internal-only data.
var jobAllowedFields = map[string]bool{
	"id":                 true,
	"type":               true,
	"queue":              true,
	"payload":            true,
	"status":             true,
	"result":             true,
	"error":              true,
	"retry_count":        true,
	"max_retry":          true,
	"retry_intervals":    true,
	"timeout":            true,
	"created_at":         true,
	"scheduled_at":       true,
	"started_at":         true,
	"completed_at":       true,
	"worker_id":          true,
	"last_heartbeat":     true,
	"execution_duration": true,
	"enqueued_by":        true,
	"meta":               true,
	"depends_on":         true,
	"allow_failure":      true,
	"enqueue_at_front":   true,
	"result_ttl":         true,
	"failure_ttl":        true,
}

// mapToJobResponse converts a Redis hash to a job response map.
// Only includes fields in the jobAllowedFields allowlist.
// Parses JSON fields (payload, meta, depends_on, result) into proper types.
func mapToJobResponse(data map[string]string) map[string]any {
	job := make(map[string]any, len(data))
	for k, v := range data {
		if !jobAllowedFields[k] {
			continue
		}
		switch k {
		case "payload", "meta":
			var parsed any
			if err := json.Unmarshal([]byte(v), &parsed); err != nil {
				slog.Warn("job: failed to parse JSON field", "field", k, "error", err)
				job[k] = v
			} else {
				job[k] = parsed
			}
		case "depends_on", "retry_intervals":
			var parsed any
			if err := json.Unmarshal([]byte(v), &parsed); err != nil {
				slog.Warn("job: failed to parse JSON field", "field", k, "error", err)
				job[k] = v
			} else {
				job[k] = parsed
			}
		case "result":
			var parsed any
			if v != "" {
				if err := json.Unmarshal([]byte(v), &parsed); err != nil {
					slog.Warn("job: failed to parse result field", "error", err)
					job[k] = v
				} else {
					job[k] = parsed
				}
			}
		default:
			job[k] = v
		}
	}
	return job
}
