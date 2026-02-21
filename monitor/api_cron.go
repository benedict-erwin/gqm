package monitor

import (
	"encoding/json"
	"net/http"
	"sort"

	"github.com/redis/go-redis/v9"
)

// handleListCron returns all registered cron entries.
func (m *Monitor) handleListCron(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	entriesKey := m.key("cron", "entries")
	data, err := m.rdb.HGetAll(ctx, entriesKey).Result()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list cron entries", "INTERNAL")
		return
	}

	entries := make([]map[string]any, 0, len(data))
	for id, raw := range data {
		var entry map[string]any
		if err := json.Unmarshal([]byte(raw), &entry); err != nil {
			m.logger.Warn("cron: failed to parse entry", "entry_id", id, "error", err)
			continue
		}
		entries = append(entries, entry)
	}

	// Sort by ID for deterministic output
	sort.Slice(entries, func(i, j int) bool {
		a, _ := entries[i]["id"].(string)
		b, _ := entries[j]["id"].(string)
		return a < b
	})

	writeJSON(w, http.StatusOK, response{Data: entries})
}

// handleGetCron returns a single cron entry with next/last run info.
func (m *Monitor) handleGetCron(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	if !validatePathParam(w, "id", id) {
		return
	}

	entriesKey := m.key("cron", "entries")
	raw, err := m.rdb.HGet(ctx, entriesKey, id).Result()
	if err == redis.Nil {
		writeError(w, http.StatusNotFound, "cron entry not found", "NOT_FOUND")
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get cron entry", "INTERNAL")
		return
	}

	var entry map[string]any
	if err := json.Unmarshal([]byte(raw), &entry); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to parse cron entry", "INTERNAL")
		return
	}

	// Get last run from history (sorted set: member=jobID, score=timestamp)
	historyKey := m.key("cron", "history", id)
	lastRuns, err := m.rdb.ZRevRangeWithScores(ctx, historyKey, 0, 0).Result()
	if err == nil && len(lastRuns) > 0 {
		entry["last_run"] = map[string]any{
			"job_id":       lastRuns[0].Member,
			"triggered_at": int64(lastRuns[0].Score),
		}
	}

	writeJSON(w, http.StatusOK, response{Data: entry})
}

// handleCronHistory returns the execution history for a cron entry.
func (m *Monitor) handleCronHistory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	id := r.PathValue("id")
	if !validatePathParam(w, "id", id) {
		return
	}
	limit := queryInt(r, "limit", 20)
	if limit < 1 {
		limit = 1
	}
	if limit > 100 {
		limit = 100
	}

	historyKey := m.key("cron", "history", id)
	records, err := m.rdb.ZRevRangeWithScores(ctx, historyKey, 0, int64(limit-1)).Result()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get cron history", "INTERNAL")
		return
	}

	entries := make([]map[string]any, 0, len(records))
	for _, z := range records {
		entries = append(entries, map[string]any{
			"job_id":       z.Member,
			"triggered_at": int64(z.Score),
		})
	}

	writeJSON(w, http.StatusOK, response{Data: entries})
}
