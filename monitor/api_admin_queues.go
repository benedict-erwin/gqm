package monitor

import "net/http"

// handlePauseQueue pauses a queue.
func (m *Monitor) handlePauseQueue(w http.ResponseWriter, r *http.Request) {
	if m.admin == nil {
		writeError(w, http.StatusNotImplemented, "admin operations not available", "NOT_IMPLEMENTED")
		return
	}

	name := r.PathValue("name")
	if err := m.admin.PauseQueue(r.Context(), name); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error(), "INTERNAL")
		return
	}

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
	if err := m.admin.ResumeQueue(r.Context(), name); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error(), "INTERNAL")
		return
	}

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
	removed, err := m.admin.EmptyQueue(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error(), "INTERNAL")
		return
	}

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
	retried, err := m.admin.RetryAllDLQ(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error(), "INTERNAL")
		return
	}

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
	_, err := m.admin.ClearDLQ(r.Context(), name)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error(), "INTERNAL")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
