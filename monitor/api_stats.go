package monitor

import (
	"net/http"
	"runtime"
	"sort"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// handleStats returns an overview of system stats.
func (m *Monitor) handleStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get all queues
	queues, _ := m.rdb.SMembers(ctx, m.key("queues")).Result()

	var totalReady, totalProcessing, totalCompleted, totalDLQ int64
	var totalProcessed, totalFailed int64

	if len(queues) > 0 {
		pipe := m.rdb.Pipeline()

		type qPipe struct {
			ready, completed, dlq *redis.IntCmd
			processing            *redis.IntCmd
			processedTotal        *redis.StringCmd
			failedTotal           *redis.StringCmd
		}
		qps := make([]qPipe, len(queues))
		for i, q := range queues {
			qps[i] = qPipe{
				ready:          pipe.LLen(ctx, m.key("queue", q, "ready")),
				processing:     pipe.ZCard(ctx, m.key("queue", q, "processing")),
				completed:      pipe.ZCard(ctx, m.key("queue", q, "completed")),
				dlq:            pipe.ZCard(ctx, m.key("queue", q, "dead_letter")),
				processedTotal: pipe.Get(ctx, m.key("stats", q, "processed_total")),
				failedTotal:    pipe.Get(ctx, m.key("stats", q, "failed_total")),
			}
		}
		pipe.Exec(ctx)

		for _, qp := range qps {
			totalReady += qp.ready.Val()
			totalProcessing += qp.processing.Val()
			totalCompleted += qp.completed.Val()
			totalDLQ += qp.dlq.Val()
			if v, err := qp.processedTotal.Int64(); err == nil {
				totalProcessed += v
			}
			if v, err := qp.failedTotal.Int64(); err == nil {
				totalFailed += v
			}
		}
	}

	// Get worker count
	workerCount, _ := m.rdb.SCard(ctx, m.key("workers")).Result()

	// Get scheduled jobs count
	scheduledCount, _ := m.rdb.ZCard(ctx, m.key("scheduled")).Result()

	uptime := time.Since(m.startedAt).Truncate(time.Second).String()

	writeJSON(w, http.StatusOK, response{Data: map[string]any{
		"queues":          len(queues),
		"workers":         workerCount,
		"ready":           totalReady,
		"processing":      totalProcessing,
		"completed":       totalCompleted,
		"dead_letter":     totalDLQ,
		"scheduled":       scheduledCount,
		"processed_total": totalProcessed,
		"failed_total":    totalFailed,
		"uptime":          uptime,
	}})
}

// handleStatsDaily returns daily processed/failed counts per queue.
func (m *Monitor) handleStatsDaily(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	queueFilter := r.URL.Query().Get("queue")
	days := queryInt(r, "days", 30)
	if days < 1 {
		days = 1
	}
	if days > 90 {
		days = 90
	}

	// Get queues to report on
	var queues []string
	if queueFilter != "" {
		queues = []string{queueFilter}
	} else {
		var err error
		queues, err = m.rdb.SMembers(ctx, m.key("queues")).Result()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to list queues", "INTERNAL")
			return
		}
	}
	sort.Strings(queues)

	// Generate date list
	now := time.Now().UTC()
	dates := make([]string, days)
	for i := 0; i < days; i++ {
		dates[i] = now.AddDate(0, 0, -i).Format("2006-01-02")
	}

	type dailyEntry struct {
		Date      string `json:"date"`
		Processed int64  `json:"processed"`
		Failed    int64  `json:"failed"`
	}

	type queueDaily struct {
		Queue string       `json:"queue"`
		Days  []dailyEntry `json:"days"`
	}

	results := make([]queueDaily, 0, len(queues))

	for _, q := range queues {
		pipe := m.rdb.Pipeline()
		pCmds := make([]*redis.StringCmd, days)
		fCmds := make([]*redis.StringCmd, days)
		for i, d := range dates {
			pCmds[i] = pipe.Get(ctx, m.key("stats", q, "processed", d))
			fCmds[i] = pipe.Get(ctx, m.key("stats", q, "failed", d))
		}
		pipe.Exec(ctx)

		entries := make([]dailyEntry, days)
		for i, d := range dates {
			entries[i].Date = d
			if v, err := pCmds[i].Int64(); err == nil {
				entries[i].Processed = v
			}
			if v, err := fCmds[i].Int64(); err == nil {
				entries[i].Failed = v
			}
		}
		results = append(results, queueDaily{Queue: q, Days: entries})
	}

	writeJSON(w, http.StatusOK, response{Data: results})
}

// handleStatsRuntime returns Go runtime statistics.
func (m *Monitor) handleStatsRuntime(w http.ResponseWriter, r *http.Request) {
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	uptime := time.Since(m.startedAt).Truncate(time.Second).String()

	writeJSON(w, http.StatusOK, response{Data: map[string]any{
		"goroutines":     runtime.NumGoroutine(),
		"go_version":     runtime.Version(),
		"num_cpu":        runtime.NumCPU(),
		"uptime":         uptime,
		"alloc_mb":       roundFloat(float64(mem.Alloc) / 1024 / 1024),
		"total_alloc_mb": roundFloat(float64(mem.TotalAlloc) / 1024 / 1024),
		"sys_mb":         roundFloat(float64(mem.Sys) / 1024 / 1024),
		"num_gc":         mem.NumGC,
		"gc_pause_ns":    mem.PauseTotalNs,
	}})
}

// roundFloat rounds a float to 2 decimal places.
func roundFloat(f float64) float64 {
	v, _ := strconv.ParseFloat(strconv.FormatFloat(f, 'f', 2, 64), 64)
	return v
}
