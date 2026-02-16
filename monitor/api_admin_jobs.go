package monitor

import (
	"encoding/json"
	"net/http"
	"strings"
)

const maxBatchSize = 100

// handleRetryJob retries a dead-letter job.
func (m *Monitor) handleRetryJob(w http.ResponseWriter, r *http.Request) {
	if m.admin == nil {
		writeError(w, http.StatusNotImplemented, "admin operations not available", "NOT_IMPLEMENTED")
		return
	}

	id := r.PathValue("id")
	if err := m.admin.RetryJob(r.Context(), id); err != nil {
		code := errorToHTTPStatus(err)
		writeError(w, code, err.Error(), errorToCode(err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":     true,
		"job_id": id,
		"status": "ready",
	})
}

// handleCancelJob cancels a pending/scheduled/deferred job.
func (m *Monitor) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	if m.admin == nil {
		writeError(w, http.StatusNotImplemented, "admin operations not available", "NOT_IMPLEMENTED")
		return
	}

	id := r.PathValue("id")
	if err := m.admin.CancelJob(r.Context(), id); err != nil {
		code := errorToHTTPStatus(err)
		writeError(w, code, err.Error(), errorToCode(err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"ok":     true,
		"job_id": id,
		"status": "canceled",
	})
}

// handleDeleteJob deletes a job.
func (m *Monitor) handleDeleteJob(w http.ResponseWriter, r *http.Request) {
	if m.admin == nil {
		writeError(w, http.StatusNotImplemented, "admin operations not available", "NOT_IMPLEMENTED")
		return
	}

	id := r.PathValue("id")
	if err := m.admin.DeleteJob(r.Context(), id); err != nil {
		code := errorToHTTPStatus(err)
		writeError(w, code, err.Error(), errorToCode(err))
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// batchRequest is the JSON body for batch operations.
type batchRequest struct {
	JobIDs []string `json:"job_ids"`
}

// batchResult is a per-item result in a batch response.
type batchResult struct {
	JobID string `json:"job_id"`
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// handleBatchRetry retries multiple dead-letter jobs.
func (m *Monitor) handleBatchRetry(w http.ResponseWriter, r *http.Request) {
	if m.admin == nil {
		writeError(w, http.StatusNotImplemented, "admin operations not available", "NOT_IMPLEMENTED")
		return
	}

	var req batchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
		return
	}

	if len(req.JobIDs) == 0 {
		writeError(w, http.StatusBadRequest, "job_ids must not be empty", "BAD_REQUEST")
		return
	}
	if len(req.JobIDs) > maxBatchSize {
		writeError(w, http.StatusBadRequest, "max 100 job IDs per request", "BAD_REQUEST")
		return
	}

	ctx := r.Context()
	results := make([]batchResult, 0, len(req.JobIDs))
	var succeeded, failed int

	for _, id := range req.JobIDs {
		if err := m.admin.RetryJob(ctx, id); err != nil {
			results = append(results, batchResult{JobID: id, OK: false, Error: err.Error()})
			failed++
		} else {
			results = append(results, batchResult{JobID: id, OK: true})
			succeeded++
		}
	}

	writeJSON(w, http.StatusOK, response{Data: map[string]any{
		"succeeded": succeeded,
		"failed":    failed,
		"results":   results,
	}})
}

// handleBatchDelete deletes multiple jobs.
func (m *Monitor) handleBatchDelete(w http.ResponseWriter, r *http.Request) {
	if m.admin == nil {
		writeError(w, http.StatusNotImplemented, "admin operations not available", "NOT_IMPLEMENTED")
		return
	}

	var req batchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body", "BAD_REQUEST")
		return
	}

	if len(req.JobIDs) == 0 {
		writeError(w, http.StatusBadRequest, "job_ids must not be empty", "BAD_REQUEST")
		return
	}
	if len(req.JobIDs) > maxBatchSize {
		writeError(w, http.StatusBadRequest, "max 100 job IDs per request", "BAD_REQUEST")
		return
	}

	ctx := r.Context()
	results := make([]batchResult, 0, len(req.JobIDs))
	var succeeded, failed int

	for _, id := range req.JobIDs {
		if err := m.admin.DeleteJob(ctx, id); err != nil {
			results = append(results, batchResult{JobID: id, OK: false, Error: err.Error()})
			failed++
		} else {
			results = append(results, batchResult{JobID: id, OK: true})
			succeeded++
		}
	}

	writeJSON(w, http.StatusOK, response{Data: map[string]any{
		"succeeded": succeeded,
		"failed":    failed,
		"results":   results,
	}})
}

// errorToHTTPStatus maps common error patterns to HTTP status codes.
func errorToHTTPStatus(err error) int {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "not found"):
		return http.StatusNotFound
	case strings.Contains(msg, "cannot"):
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

// errorToCode maps common error patterns to API error codes.
func errorToCode(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "not found"):
		return "NOT_FOUND"
	case strings.Contains(msg, "cannot"):
		return "CONFLICT"
	default:
		return "INTERNAL"
	}
}
