package monitor

import "net/http"

// handlePauseQueue pauses a queue.
func (m *Monitor) handlePauseQueue(w http.ResponseWriter, r *http.Request) {
	if m.admin == nil {
		writeError(w, http.StatusNotImplemented, "admin operations not available", "NOT_IMPLEMENTED")
		return
	}

	name := r.PathValue("name")
	if !validatePathParam(w, "name", name) {
		return
	}

	// Verify queue exists before allowing pause (prevents orphaned pause entries).
	exists, err := m.rdb.SIsMember(r.Context(), m.key("queues"), name).Result()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error", "INTERNAL")
		return
	}
	if !exists {
		writeError(w, http.StatusNotFound, "queue not found", "NOT_FOUND")
		return
	}

	if err := m.admin.PauseQueue(r.Context(), name); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error", "INTERNAL")
		return
	}

	m.logger.Info("admin: pause queue", "queue", name, "user", r.Header.Get("X-GQM-User"))
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":     true,
		"queue":  name,
		"status": "paused",
	})
}

// handleResumeQueue resumes a paused queue.
func (m *Monitor) handleResumeQueue(w http.ResponseWriter, r *http.Request) {
	if m.admin == nil {
		writeError(w, http.StatusNotImplemented, "admin operations not available", "NOT_IMPLEMENTED")
		return
	}

	name := r.PathValue("name")
	if !validatePathParam(w, "name", name) {
		return
	}

	// Verify queue exists before allowing resume.
	exists, err := m.rdb.SIsMember(r.Context(), m.key("queues"), name).Result()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error", "INTERNAL")
		return
	}
	if !exists {
		writeError(w, http.StatusNotFound, "queue not found", "NOT_FOUND")
		return
	}

	if err := m.admin.ResumeQueue(r.Context(), name); err != nil {
		writeError(w, http.StatusInternalServerError, "internal error", "INTERNAL")
		return
	}

	m.logger.Info("admin: resume queue", "queue", name, "user", r.Header.Get("X-GQM-User"))
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":     true,
		"queue":  name,
		"status": "active",
	})
}

// handleEmptyQueue removes all pending jobs from a queue.
func (m *Monitor) handleEmptyQueue(w http.ResponseWriter, r *http.Request) {
	if m.admin == nil {
		writeError(w, http.StatusNotImplemented, "admin operations not available", "NOT_IMPLEMENTED")
		return
	}

	name := r.PathValue("name")
	if !validatePathParam(w, "name", name) {
		return
	}
	removed, err := m.admin.EmptyQueue(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error", "INTERNAL")
		return
	}

	m.logger.Info("admin: empty queue", "queue", name, "removed", removed, "user", r.Header.Get("X-GQM-User"))
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"removed": removed,
	})
}

// handleRetryAllDLQ retries all jobs in the dead letter queue.
func (m *Monitor) handleRetryAllDLQ(w http.ResponseWriter, r *http.Request) {
	if m.admin == nil {
		writeError(w, http.StatusNotImplemented, "admin operations not available", "NOT_IMPLEMENTED")
		return
	}

	name := r.PathValue("name")
	if !validatePathParam(w, "name", name) {
		return
	}
	retried, err := m.admin.RetryAllDLQ(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error", "INTERNAL")
		return
	}

	m.logger.Info("admin: retry all DLQ", "queue", name, "retried", retried, "user", r.Header.Get("X-GQM-User"))
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"retried": retried,
	})
}

// handleClearDLQ deletes all jobs from the dead letter queue.
func (m *Monitor) handleClearDLQ(w http.ResponseWriter, r *http.Request) {
	if m.admin == nil {
		writeError(w, http.StatusNotImplemented, "admin operations not available", "NOT_IMPLEMENTED")
		return
	}

	name := r.PathValue("name")
	if !validatePathParam(w, "name", name) {
		return
	}

	// Verify queue exists before clearing DLQ.
	exists, err := m.rdb.SIsMember(r.Context(), m.key("queues"), name).Result()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error", "INTERNAL")
		return
	}
	if !exists {
		writeError(w, http.StatusNotFound, "queue not found", "NOT_FOUND")
		return
	}

	cleared, err := m.admin.ClearDLQ(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error", "INTERNAL")
		return
	}

	m.logger.Info("admin: clear DLQ", "queue", name, "cleared", cleared, "user", r.Header.Get("X-GQM-User"))
	writeJSON(w, http.StatusOK, map[string]any{
		"data": map[string]any{
			"cleared": cleared,
		},
	})
}
