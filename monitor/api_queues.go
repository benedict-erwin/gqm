package monitor

import (
	"net/http"
	"sort"

	"github.com/redis/go-redis/v9"
)

// handleListQueues returns all queues with their stats.
func (m *Monitor) handleListQueues(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get all registered queues
	queues, err := m.rdb.SMembers(ctx, m.key("queues")).Result()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list queues", "INTERNAL")
		return
	}
	sort.Strings(queues)

	type queueInfo struct {
		Name           string `json:"name"`
		Ready          int64  `json:"ready"`
		Processing     int64  `json:"processing"`
		Completed      int64  `json:"completed"`
		DeadLetter     int64  `json:"dead_letter"`
		ProcessedTotal int64  `json:"processed_total"`
		FailedTotal    int64  `json:"failed_total"`
		Paused         bool   `json:"paused"`
	}

	results := make([]queueInfo, 0, len(queues))
	pipe := m.rdb.Pipeline()
	type pipeResults struct {
		ready, completed, dlq *redis.IntCmd
		processing            *redis.IntCmd
		processedTotal        *redis.StringCmd
		failedTotal           *redis.StringCmd
		paused                *redis.BoolCmd
	}

	pausedKey := m.key("paused")
	pipes := make([]pipeResults, len(queues))
	for i, q := range queues {
		pipes[i] = pipeResults{
			ready:          pipe.LLen(ctx, m.key("queue", q, "ready")),
			processing:     pipe.ZCard(ctx, m.key("queue", q, "processing")),
			completed:      pipe.ZCard(ctx, m.key("queue", q, "completed")),
			dlq:            pipe.ZCard(ctx, m.key("queue", q, "dead_letter")),
			processedTotal: pipe.Get(ctx, m.key("stats", q, "processed_total")),
			failedTotal:    pipe.Get(ctx, m.key("stats", q, "failed_total")),
			paused:         pipe.SIsMember(ctx, pausedKey, q),
		}
	}
	pipe.Exec(ctx)

	for i, q := range queues {
		pr := pipes[i]
		qi := queueInfo{
			Name:       q,
			Ready:      pr.ready.Val(),
			Processing: pr.processing.Val(),
			Completed:  pr.completed.Val(),
			DeadLetter: pr.dlq.Val(),
			Paused:     pr.paused.Val(),
		}
		if v, err := pr.processedTotal.Int64(); err == nil {
			qi.ProcessedTotal = v
		}
		if v, err := pr.failedTotal.Int64(); err == nil {
			qi.FailedTotal = v
		}
		results = append(results, qi)
	}

	writeJSON(w, http.StatusOK, response{Data: results})
}

// handleGetQueue returns details for a single queue.
func (m *Monitor) handleGetQueue(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	name := r.PathValue("name")
	if !validatePathParam(w, "name", name) {
		return
	}

	// Check queue exists
	exists, err := m.rdb.SIsMember(ctx, m.key("queues"), name).Result()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to check queue", "INTERNAL")
		return
	}
	if !exists {
		writeError(w, http.StatusNotFound, "queue not found", "NOT_FOUND")
		return
	}

	pipe := m.rdb.Pipeline()
	ready := pipe.LLen(ctx, m.key("queue", name, "ready"))
	processing := pipe.ZCard(ctx, m.key("queue", name, "processing"))
	completed := pipe.ZCard(ctx, m.key("queue", name, "completed"))
	dlq := pipe.ZCard(ctx, m.key("queue", name, "dead_letter"))
	processedTotal := pipe.Get(ctx, m.key("stats", name, "processed_total"))
	failedTotal := pipe.Get(ctx, m.key("stats", name, "failed_total"))
	paused := pipe.SIsMember(ctx, m.key("paused"), name)
	pipe.Exec(ctx)

	qi := map[string]any{
		"name":            name,
		"ready":           ready.Val(),
		"processing":      processing.Val(),
		"completed":       completed.Val(),
		"dead_letter":     dlq.Val(),
		"processed_total": int64(0),
		"failed_total":    int64(0),
		"paused":          paused.Val(),
	}
	if v, err := processedTotal.Int64(); err == nil {
		qi["processed_total"] = v
	}
	if v, err := failedTotal.Int64(); err == nil {
		qi["failed_total"] = v
	}

	writeJSON(w, http.StatusOK, response{Data: qi})
}

// handleListQueueJobs returns paginated jobs for a queue.
func (m *Monitor) handleListQueueJobs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	name := r.PathValue("name")
	if !validatePathParam(w, "name", name) {
		return
	}
	status := r.URL.Query().Get("status")
	page, limit := pagination(r)

	// Validate status parameter against known values.
	switch status {
	case "", "ready":
		// fall through to ready queue below
	case "processing":
		m.listSortedSetJobs(w, r, m.key("queue", name, "processing"), page, limit)
		return
	case "completed":
		m.listSortedSetJobs(w, r, m.key("queue", name, "completed"), page, limit)
		return
	case "dead_letter":
		m.listSortedSetJobs(w, r, m.key("queue", name, "dead_letter"), page, limit)
		return
	default:
		writeError(w, http.StatusBadRequest, "invalid status parameter; must be one of: ready, processing, completed, dead_letter", "BAD_REQUEST")
		return
	}

	// Ready queue (list)
	listKey := m.key("queue", name, "ready")

	total, err := m.rdb.LLen(ctx, listKey).Result()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to count jobs", "INTERNAL")
		return
	}

	start := int64((page - 1) * limit)
	stop := start + int64(limit) - 1

	jobIDs, err := m.rdb.LRange(ctx, listKey, start, stop).Result()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list jobs", "INTERNAL")
		return
	}

	jobs := m.fetchJobSummaries(ctx, jobIDs)
	writeJSON(w, http.StatusOK, response{
		Data: jobs,
		Meta: &meta{Page: page, Limit: limit, Total: int(total)},
	})
}

// listSortedSetJobs handles paginated listing from a sorted set (processing, completed, dead_letter).
func (m *Monitor) listSortedSetJobs(w http.ResponseWriter, r *http.Request, key string, page, limit int) {
	ctx := r.Context()

	total, err := m.rdb.ZCard(ctx, key).Result()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to count jobs", "INTERNAL")
		return
	}

	start := int64((page - 1) * limit)
	stop := start + int64(limit) - 1

	jobIDs, err := m.rdb.ZRange(ctx, key, start, stop).Result()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list jobs", "INTERNAL")
		return
	}

	jobs := m.fetchJobSummaries(ctx, jobIDs)
	writeJSON(w, http.StatusOK, response{
		Data: jobs,
		Meta: &meta{Page: page, Limit: limit, Total: int(total)},
	})
}
