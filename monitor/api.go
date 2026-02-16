package monitor

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"
)

// setupRoutes registers all HTTP routes on the monitor's mux.
func (m *Monitor) setupRoutes() {
	// Health — no auth required
	m.mux.HandleFunc("GET /health", m.handleHealth)

	// Auth endpoints
	m.mux.HandleFunc("POST /auth/login", m.handleLogin)
	m.mux.HandleFunc("POST /auth/logout", m.requireAuth(m.handleLogout))
	m.mux.HandleFunc("GET /auth/me", m.requireAuth(m.handleMe))

	// API v1 — all require auth
	m.mux.HandleFunc("GET /api/v1/queues", m.requireAuth(m.handleListQueues))
	m.mux.HandleFunc("GET /api/v1/queues/{name}", m.requireAuth(m.handleGetQueue))
	m.mux.HandleFunc("GET /api/v1/queues/{name}/jobs", m.requireAuth(m.handleListQueueJobs))
	m.mux.HandleFunc("GET /api/v1/queues/{name}/dead-letter", m.requireAuth(m.handleListDLQ))

	m.mux.HandleFunc("GET /api/v1/jobs/{id}", m.requireAuth(m.handleGetJob))

	m.mux.HandleFunc("GET /api/v1/workers", m.requireAuth(m.handleListWorkers))
	m.mux.HandleFunc("GET /api/v1/workers/{id}", m.requireAuth(m.handleGetWorker))

	m.mux.HandleFunc("GET /api/v1/cron", m.requireAuth(m.handleListCron))
	m.mux.HandleFunc("GET /api/v1/cron/{id}", m.requireAuth(m.handleGetCron))
	m.mux.HandleFunc("GET /api/v1/cron/{id}/history", m.requireAuth(m.handleCronHistory))

	m.mux.HandleFunc("GET /api/v1/stats", m.requireAuth(m.handleStats))
	m.mux.HandleFunc("GET /api/v1/stats/daily", m.requireAuth(m.handleStatsDaily))
	m.mux.HandleFunc("GET /api/v1/stats/runtime", m.requireAuth(m.handleStatsRuntime))

	// Write endpoints — Jobs
	m.mux.HandleFunc("POST /api/v1/jobs/{id}/retry", m.requireAuth(m.handleRetryJob))
	m.mux.HandleFunc("POST /api/v1/jobs/{id}/cancel", m.requireAuth(m.handleCancelJob))
	m.mux.HandleFunc("DELETE /api/v1/jobs/{id}", m.requireAuth(m.handleDeleteJob))
	m.mux.HandleFunc("POST /api/v1/jobs/batch/retry", m.requireAuth(m.handleBatchRetry))
	m.mux.HandleFunc("POST /api/v1/jobs/batch/delete", m.requireAuth(m.handleBatchDelete))

	// Write endpoints — Queues + DLQ
	m.mux.HandleFunc("POST /api/v1/queues/{name}/pause", m.requireAuth(m.handlePauseQueue))
	m.mux.HandleFunc("POST /api/v1/queues/{name}/resume", m.requireAuth(m.handleResumeQueue))
	m.mux.HandleFunc("DELETE /api/v1/queues/{name}/empty", m.requireAuth(m.handleEmptyQueue))
	m.mux.HandleFunc("POST /api/v1/queues/{name}/dead-letter/retry-all", m.requireAuth(m.handleRetryAllDLQ))
	m.mux.HandleFunc("DELETE /api/v1/queues/{name}/dead-letter/clear", m.requireAuth(m.handleClearDLQ))

	// Write endpoints — Cron
	m.mux.HandleFunc("POST /api/v1/cron/{id}/trigger", m.requireAuth(m.handleTriggerCron))
	m.mux.HandleFunc("POST /api/v1/cron/{id}/enable", m.requireAuth(m.handleEnableCron))
	m.mux.HandleFunc("POST /api/v1/cron/{id}/disable", m.requireAuth(m.handleDisableCron))

	// Dashboard placeholder
	if m.cfg.DashEnabled {
		prefix := m.cfg.DashPathPrefix
		if prefix == "" {
			prefix = "/dashboard"
		}
		m.mux.HandleFunc("GET "+prefix, m.handleDashboard)
		m.mux.HandleFunc("GET "+prefix+"/", m.handleDashboard)
	}
}

// response is the standard JSON envelope for successful responses.
type response struct {
	Data any  `json:"data"`
	Meta *meta `json:"meta,omitempty"`
}

// meta holds pagination metadata.
type meta struct {
	Page  int `json:"page"`
	Limit int `json:"limit"`
	Total int `json:"total"`
}

// errorResponse is the standard JSON envelope for errors.
type errorResponse struct {
	Error string `json:"error"`
	Code  string `json:"code"`
}

// writeJSON writes a JSON response with the given status code.
func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// writeError writes a JSON error response.
func writeError(w http.ResponseWriter, status int, msg, code string) {
	writeJSON(w, status, errorResponse{Error: msg, Code: code})
}

// pagination extracts page and limit from query parameters with defaults and bounds.
func pagination(r *http.Request) (page, limit int) {
	page = queryInt(r, "page", 1)
	if page < 1 {
		page = 1
	}
	limit = queryInt(r, "limit", 20)
	if limit < 1 {
		limit = 1
	}
	if limit > 100 {
		limit = 100
	}
	return
}

// queryInt returns the integer value of a query parameter, or a default.
func queryInt(r *http.Request, key string, def int) int {
	s := r.URL.Query().Get(key)
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return v
}

// handleHealth is the health check endpoint (no auth required).
func (m *Monitor) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	status := "ok"
	redisOK := true
	if err := m.rdb.Ping(ctx).Err(); err != nil {
		status = "degraded"
		redisOK = false
	}

	uptime := time.Since(m.startedAt).Truncate(time.Second).String()

	writeJSON(w, http.StatusOK, map[string]any{
		"status": status,
		"redis":  redisOK,
		"uptime": uptime,
	})
}
