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
	page, limit := pagination(r)

	dlqKey := m.key("queue", name, "dead_letter")

	total, err := m.rdb.LLen(ctx, dlqKey).Result()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to count DLQ", "INTERNAL")
		return
	}

	start := int64((page - 1) * limit)
	stop := start + int64(limit) - 1

	jobIDs, err := m.rdb.LRange(ctx, dlqKey, start, stop).Result()
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

// mapToJobResponse converts a Redis hash to a job response map.
// Parses JSON fields (payload, meta, depends_on, result) into proper types.
func mapToJobResponse(data map[string]string) map[string]any {
	job := make(map[string]any, len(data))
	for k, v := range data {
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
