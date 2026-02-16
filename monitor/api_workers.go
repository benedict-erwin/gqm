package monitor

import (
	"net/http"
	"sort"
	"strings"

	"github.com/redis/go-redis/v9"
)

// handleListWorkers returns all registered workers/pools with status.
func (m *Monitor) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	workerIDs, err := m.rdb.SMembers(ctx, m.key("workers")).Result()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list workers", "INTERNAL")
		return
	}
	sort.Strings(workerIDs)

	if len(workerIDs) == 0 {
		writeJSON(w, http.StatusOK, response{Data: []any{}})
		return
	}

	pipe := m.rdb.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, len(workerIDs))
	for i, id := range workerIDs {
		cmds[i] = pipe.HGetAll(ctx, m.key("worker", id))
	}
	pipe.Exec(ctx)

	workers := make([]map[string]any, 0, len(workerIDs))
	for i, cmd := range cmds {
		data, err := cmd.Result()
		if err != nil || len(data) == 0 {
			// Worker key expired — clean up stale set entry
			m.rdb.SRem(ctx, m.key("workers"), workerIDs[i])
			continue
		}
		w := make(map[string]any, len(data)+1)
		for k, v := range data {
			if k == "queues" {
				w[k] = strings.Split(v, ",")
			} else {
				w[k] = v
			}
		}
		workers = append(workers, w)
	}

	writeJSON(w, http.StatusOK, response{Data: workers})
}

// handleGetWorker returns details for a single worker/pool.
func (m *Monitor) handleGetWorker(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")

	workerKey := m.key("worker", id)
	data, err := m.rdb.HGetAll(ctx, workerKey).Result()
	if err != nil || len(data) == 0 {
		writeError(w, http.StatusNotFound, "worker not found", "NOT_FOUND")
		return
	}

	worker := make(map[string]any, len(data))
	for k, v := range data {
		if k == "queues" {
			worker[k] = strings.Split(v, ",")
		} else {
			worker[k] = v
		}
	}

	writeJSON(w, http.StatusOK, response{Data: worker})
}
