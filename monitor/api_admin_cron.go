package monitor

import "net/http"

// handleTriggerCron manually triggers a cron entry.
func (m *Monitor) handleTriggerCron(w http.ResponseWriter, r *http.Request) {
	if m.admin == nil {
		writeError(w, http.StatusNotImplemented, "admin operations not available", "NOT_IMPLEMENTED")
		return
	}

	id := r.PathValue("id")
	if !validatePathParam(w, "id", id) {
		return
	}
	jobID, err := m.admin.TriggerCron(r.Context(), id)
	if err != nil {
		code := errorToHTTPStatus(err)
		writeError(w, code, sanitizeError(err), errorToCode(err))
		return
	}

	m.logger.Info("admin: trigger cron", "cron_id", id, "job_id", jobID, "user", r.Header.Get("X-GQM-User"))
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"job_id":  jobID,
		"cron_id": id,
	})
}

// handleEnableCron enables a cron entry.
func (m *Monitor) handleEnableCron(w http.ResponseWriter, r *http.Request) {
	if m.admin == nil {
		writeError(w, http.StatusNotImplemented, "admin operations not available", "NOT_IMPLEMENTED")
		return
	}

	id := r.PathValue("id")
	if !validatePathParam(w, "id", id) {
		return
	}
	if err := m.admin.EnableCron(r.Context(), id); err != nil {
		code := errorToHTTPStatus(err)
		writeError(w, code, sanitizeError(err), errorToCode(err))
		return
	}

	m.logger.Info("admin: enable cron", "cron_id", id, "user", r.Header.Get("X-GQM-User"))
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"cron_id": id,
		"enabled": true,
	})
}

// handleDisableCron disables a cron entry.
func (m *Monitor) handleDisableCron(w http.ResponseWriter, r *http.Request) {
	if m.admin == nil {
		writeError(w, http.StatusNotImplemented, "admin operations not available", "NOT_IMPLEMENTED")
		return
	}

	id := r.PathValue("id")
	if !validatePathParam(w, "id", id) {
		return
	}
	if err := m.admin.DisableCron(r.Context(), id); err != nil {
		code := errorToHTTPStatus(err)
		writeError(w, code, sanitizeError(err), errorToCode(err))
		return
	}

	m.logger.Info("admin: disable cron", "cron_id", id, "user", r.Header.Get("X-GQM-User"))
	writeJSON(w, http.StatusOK, map[string]any{
		"ok":      true,
		"cron_id": id,
		"enabled": false,
	})
}
