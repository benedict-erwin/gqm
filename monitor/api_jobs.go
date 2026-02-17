package monitor

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/redis/go-redis/v9"
)

// handleGetJob returns details for a single job.
func (m *Monitor) handleGetJob(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	if !validatePathParam(w, "id", id) {
		return
	}

	jobKey := m.key("job", id)
	data, err := m.rdb.HGetAll(ctx, jobKey).Result()
	if err != nil || len(data) == 0 {
		writeError(w, http.StatusNotFound, "job not found", "NOT_FOUND")
		return
	}

	job := mapToJobResponse(data)
	writeJSON(w, http.StatusOK, response{Data: job})
}

// handleListDLQ returns paginated dead letter queue jobs.
func (m *Monitor) handleListDLQ(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	name := r.PathValue("name")
	if !validatePathParam(w, "name", name) {
		return
	}
	page, limit := pagination(r)

	dlqKey := m.key("queue", name, "dead_letter")

	total, err := m.rdb.ZCard(ctx, dlqKey).Result()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to count DLQ", "INTERNAL")
		return
	}

	start := int64((page - 1) * limit)
	stop := start + int64(limit) - 1

	jobIDs, err := m.rdb.ZRange(ctx, dlqKey, start, stop).Result()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list DLQ jobs", "INTERNAL")
		return
	}

	jobs := m.fetchJobSummaries(ctx, jobIDs)
	writeJSON(w, http.StatusOK, response{
		Data: jobs,
		Meta: &meta{Page: page, Limit: limit, Total: int(total)},
	})
}

// fetchJobSummaries fetches job data for a list of job IDs using pipelining.
func (m *Monitor) fetchJobSummaries(ctx context.Context, jobIDs []string) []map[string]any {
	if len(jobIDs) == 0 {
		return []map[string]any{}
	}

	pipe := m.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(jobIDs))
	for i, id := range jobIDs {
		cmds[i] = pipe.HGetAll(ctx, m.key("job", id))
	}
	pipe.Exec(ctx)

	jobs := make([]map[string]any, 0, len(jobIDs))
	for _, cmd := range cmds {
		data, err := cmd.Result()
		if err != nil || len(data) == 0 {
			continue
		}
		jobs = append(jobs, mapToJobResponse(data))
	}
	return jobs
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
			if json.Unmarshal([]byte(v), &parsed) == nil {
				job[k] = parsed
			} else {
				job[k] = v
			}
		case "depends_on", "retry_intervals":
			var parsed any
			if json.Unmarshal([]byte(v), &parsed) == nil {
				job[k] = parsed
			} else {
				job[k] = v
			}
		case "result":
			var parsed any
			if v != "" && json.Unmarshal([]byte(v), &parsed) == nil {
				job[k] = parsed
			} else if v != "" {
				job[k] = v
			}
		default:
			job[k] = v
		}
	}
	return job
}
