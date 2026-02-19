package monitor

import (
	"encoding/json"
	"net/http"
	"regexp"
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

	m.mux.HandleFunc("GET /api/v1/servers", m.requireAuth(m.handleListServers))
	m.mux.HandleFunc("GET /api/v1/servers/{id}", m.requireAuth(m.handleGetServer))

	m.mux.HandleFunc("GET /api/v1/workers", m.requireAuth(m.handleListWorkers))
	m.mux.HandleFunc("GET /api/v1/workers/{id}", m.requireAuth(m.handleGetWorker))

	m.mux.HandleFunc("GET /api/v1/cron", m.requireAuth(m.handleListCron))
	m.mux.HandleFunc("GET /api/v1/cron/{id}", m.requireAuth(m.handleGetCron))
	m.mux.HandleFunc("GET /api/v1/cron/{id}/history", m.requireAuth(m.handleCronHistory))

	m.mux.HandleFunc("GET /api/v1/stats", m.requireAuth(m.handleStats))
	m.mux.HandleFunc("GET /api/v1/stats/daily", m.requireAuth(m.handleStatsDaily))
	m.mux.HandleFunc("GET /api/v1/stats/runtime", m.requireAuth(m.handleStatsRuntime))

	// Write endpoints — Jobs (require admin role)
	m.mux.HandleFunc("POST /api/v1/jobs/{id}/retry", m.requireAuth(m.requireAdmin(m.handleRetryJob)))
	m.mux.HandleFunc("POST /api/v1/jobs/{id}/cancel", m.requireAuth(m.requireAdmin(m.handleCancelJob)))
	m.mux.HandleFunc("DELETE /api/v1/jobs/{id}", m.requireAuth(m.requireAdmin(m.handleDeleteJob)))
	m.mux.HandleFunc("POST /api/v1/jobs/batch/retry", m.requireAuth(m.requireAdmin(m.handleBatchRetry)))
	m.mux.HandleFunc("POST /api/v1/jobs/batch/delete", m.requireAuth(m.requireAdmin(m.handleBatchDelete)))

	// Write endpoints — Queues + DLQ (require admin role)
	m.mux.HandleFunc("POST /api/v1/queues/{name}/pause", m.requireAuth(m.requireAdmin(m.handlePauseQueue)))
	m.mux.HandleFunc("POST /api/v1/queues/{name}/resume", m.requireAuth(m.requireAdmin(m.handleResumeQueue)))
	m.mux.HandleFunc("DELETE /api/v1/queues/{name}/empty", m.requireAuth(m.requireAdmin(m.handleEmptyQueue)))
	m.mux.HandleFunc("POST /api/v1/queues/{name}/dead-letter/retry-all", m.requireAuth(m.requireAdmin(m.handleRetryAllDLQ)))
	m.mux.HandleFunc("DELETE /api/v1/queues/{name}/dead-letter/clear", m.requireAuth(m.requireAdmin(m.handleClearDLQ)))

	// DAG endpoints — read-only, require auth
	m.mux.HandleFunc("GET /api/v1/dag/roots", m.requireAuth(m.handleListDAGRoots))
	m.mux.HandleFunc("GET /api/v1/dag/deferred", m.requireAuth(m.handleListDeferred))
	m.mux.HandleFunc("GET /api/v1/dag/graph/{id}", m.requireAuth(m.handleDAGGraph))

	// Write endpoints — Cron (require admin role)
	m.mux.HandleFunc("POST /api/v1/cron/{id}/trigger", m.requireAuth(m.requireAdmin(m.handleTriggerCron)))
	m.mux.HandleFunc("POST /api/v1/cron/{id}/enable", m.requireAuth(m.requireAdmin(m.handleEnableCron)))
	m.mux.HandleFunc("POST /api/v1/cron/{id}/disable", m.requireAuth(m.requireAdmin(m.handleDisableCron)))

	// Dashboard — static files served without auth. This is intentional:
	// the dashboard is a SPA that calls /auth/me on init to check auth status.
	// All sensitive data is fetched via API endpoints which require auth.
	// Static HTML/CSS/JS assets contain no user data.
	if m.cfg.DashEnabled {
		prefix := m.cfg.DashPathPrefix
		if prefix == "" {
			prefix = "/dashboard"
		}
		fs := m.dashboardFileServer()
		m.mux.Handle("GET "+prefix+"/", http.StripPrefix(prefix, fs))
		// Redirect /dashboard to /dashboard/
		m.mux.HandleFunc("GET "+prefix, func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, prefix+"/", http.StatusMovedPermanently)
		})
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
	if page > 10000 {
		page = 10000
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

// maxRequestBody is the maximum allowed request body size (1 MB).
const maxRequestBody = 1 << 20

// validPathParam matches safe path parameter values for use in Redis key lookups.
// Allows alphanumeric, hyphens, underscores, dots, @ signs, and colons.
// Colons are required because job types use "namespace:action" convention (e.g., "email:send")
// and implicit pools derive queue names from the job type.
// Max 256 chars. Rejects slashes, spaces, and other special characters.
var validPathParam = regexp.MustCompile(`^[a-zA-Z0-9._@:\-]{1,256}$`)

// validatePathParam checks a path parameter value is safe to use in Redis keys.
// Returns an empty string and writes a 400 error if invalid.
func validatePathParam(w http.ResponseWriter, name, value string) bool {
	if !validPathParam.MatchString(value) {
		writeError(w, http.StatusBadRequest, name+" contains invalid characters", "BAD_REQUEST")
		return false
	}
	return true
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
