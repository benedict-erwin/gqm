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
	if !validatePathParam(w, "id", id) {
		return
	}
	if err := m.admin.RetryJob(r.Context(), id); err != nil {
		code := errorToHTTPStatus(err)
		writeError(w, code, sanitizeError(err), errorToCode(err))
		return
	}

	m.logger.Info("admin: retry job", "job_id", id, "user", r.Header.Get("X-GQM-User"))
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
	if !validatePathParam(w, "id", id) {
		return
	}
	if err := m.admin.CancelJob(r.Context(), id); err != nil {
		code := errorToHTTPStatus(err)
		writeError(w, code, sanitizeError(err), errorToCode(err))
		return
	}

	m.logger.Info("admin: cancel job", "job_id", id, "user", r.Header.Get("X-GQM-User"))
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
	if !validatePathParam(w, "id", id) {
		return
	}
	if err := m.admin.DeleteJob(r.Context(), id); err != nil {
		code := errorToHTTPStatus(err)
		writeError(w, code, sanitizeError(err), errorToCode(err))
		return
	}

	m.logger.Info("admin: delete job", "job_id", id, "user", r.Header.Get("X-GQM-User"))
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
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
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

	// Validate and deduplicate job IDs before processing.
	ids := deduplicateIDs(req.JobIDs)
	for _, id := range ids {
		if !validPathParam.MatchString(id) {
			writeError(w, http.StatusBadRequest, "job_ids contains invalid characters", "BAD_REQUEST")
			return
		}
	}

	ctx := r.Context()
	results := make([]batchResult, 0, len(ids))
	var succeeded, failed int

	for _, id := range ids {
		if err := m.admin.RetryJob(ctx, id); err != nil {
			results = append(results, batchResult{JobID: id, OK: false, Error: sanitizeError(err)})
			failed++
		} else {
			results = append(results, batchResult{JobID: id, OK: true})
			succeeded++
		}
	}

	m.logger.Info("admin: batch retry", "succeeded", succeeded, "failed", failed, "total", len(ids), "user", r.Header.Get("X-GQM-User"))
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
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
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

	// Validate and deduplicate job IDs before processing.
	ids := deduplicateIDs(req.JobIDs)
	for _, id := range ids {
		if !validPathParam.MatchString(id) {
			writeError(w, http.StatusBadRequest, "job_ids contains invalid characters", "BAD_REQUEST")
			return
		}
	}

	ctx := r.Context()
	results := make([]batchResult, 0, len(ids))
	var succeeded, failed int

	for _, id := range ids {
		if err := m.admin.DeleteJob(ctx, id); err != nil {
			results = append(results, batchResult{JobID: id, OK: false, Error: sanitizeError(err)})
			failed++
		} else {
			results = append(results, batchResult{JobID: id, OK: true})
			succeeded++
		}
	}

	m.logger.Info("admin: batch delete", "succeeded", succeeded, "failed", failed, "total", len(ids), "user", r.Header.Get("X-GQM-User"))
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

// sanitizeError returns a safe error message for the client. Errors prefixed
// with "gqm:" are user-facing business logic errors and are passed through.
// All other errors (Redis failures, marshaling, etc.) are replaced with a
// generic message to avoid leaking internal details.
func sanitizeError(err error) string {
	msg := err.Error()
	if strings.HasPrefix(msg, "gqm:") {
		return msg
	}
	return "internal error"
}

// deduplicateIDs returns a new slice with duplicate IDs removed, preserving order.
func deduplicateIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}
