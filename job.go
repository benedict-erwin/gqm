package gqm

import (
	"encoding/json"
	"fmt"
	"time"
)

// Job status constants.
const (
	StatusReady      = "ready"
	StatusProcessing = "processing"
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
	StatusRetry      = "retry"
	StatusDeadLetter = "dead_letter"
	StatusStopped    = "stopped"
)

// Payload is a type alias for job payload data.
type Payload map[string]any

// Job represents a unit of work in the queue.
type Job struct {
	ID                string        `json:"id"`
	Type              string        `json:"type"`
	Queue             string        `json:"queue"`
	Payload           Payload       `json:"payload"`
	Status            string        `json:"status"`
	Result            json.RawMessage `json:"result,omitempty"`
	Error             string        `json:"error,omitempty"`
	RetryCount        int           `json:"retry_count"`
	MaxRetry          int           `json:"max_retry"`
	RetryIntervals    []int         `json:"retry_intervals,omitempty"`
	Timeout           int           `json:"timeout,omitempty"`
	CreatedAt         int64         `json:"created_at"`
	StartedAt         int64         `json:"started_at,omitempty"`
	CompletedAt       int64         `json:"completed_at,omitempty"`
	WorkerID          string        `json:"worker_id,omitempty"`
	LastHeartbeat     int64         `json:"last_heartbeat,omitempty"`
	ExecutionDuration int64         `json:"execution_duration,omitempty"`
	EnqueuedBy        string        `json:"enqueued_by,omitempty"`
	Meta              Payload       `json:"meta,omitempty"`
}

// NewJob creates a new Job with a generated UUID v7 and the given type.
func NewJob(jobType string, payload Payload) *Job {
	return &Job{
		ID:        NewUUID(),
		Type:      jobType,
		Queue:     "default",
		Payload:   payload,
		Status:    StatusReady,
		MaxRetry:  3,
		CreatedAt: time.Now().UnixNano(),
	}
}

// Decode unmarshals the job payload into the given target.
func (j *Job) Decode(target any) error {
	data, err := json.Marshal(j.Payload)
	if err != nil {
		return fmt.Errorf("encoding payload for decode: %w", err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		return fmt.Errorf("decoding payload: %w", err)
	}
	return nil
}

// Encode serializes the job to JSON bytes.
func (j *Job) Encode() ([]byte, error) {
	data, err := json.Marshal(j)
	if err != nil {
		return nil, fmt.Errorf("encoding job: %w", err)
	}
	return data, nil
}

// DecodeJob deserializes a Job from JSON bytes.
func DecodeJob(data []byte) (*Job, error) {
	var j Job
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, fmt.Errorf("decoding job: %w", err)
	}
	return &j, nil
}

// ToMap converts a Job to a map suitable for Redis HSET.
func (j *Job) ToMap() (map[string]any, error) {
	payloadJSON, err := json.Marshal(j.Payload)
	if err != nil {
		return nil, fmt.Errorf("encoding payload: %w", err)
	}

	m := map[string]any{
		"id":           j.ID,
		"type":         j.Type,
		"queue":        j.Queue,
		"payload":      string(payloadJSON),
		"status":       j.Status,
		"retry_count":  j.RetryCount,
		"max_retry":    j.MaxRetry,
		"timeout":      j.Timeout,
		"created_at":   j.CreatedAt,
		"started_at":   j.StartedAt,
		"completed_at": j.CompletedAt,
	}

	if j.Result != nil {
		m["result"] = string(j.Result)
	}
	if j.Error != "" {
		m["error"] = j.Error
	}
	if len(j.RetryIntervals) > 0 {
		ri, _ := json.Marshal(j.RetryIntervals)
		m["retry_intervals"] = string(ri)
	}
	if j.WorkerID != "" {
		m["worker_id"] = j.WorkerID
	}
	if j.LastHeartbeat != 0 {
		m["last_heartbeat"] = j.LastHeartbeat
	}
	if j.ExecutionDuration != 0 {
		m["execution_duration"] = j.ExecutionDuration
	}
	if j.EnqueuedBy != "" {
		m["enqueued_by"] = j.EnqueuedBy
	}
	if j.Meta != nil {
		metaJSON, _ := json.Marshal(j.Meta)
		m["meta"] = string(metaJSON)
	}

	return m, nil
}

// JobFromMap creates a Job from a Redis HGETALL result.
func JobFromMap(m map[string]string) (*Job, error) {
	j := &Job{
		ID:     m["id"],
		Type:   m["type"],
		Queue:  m["queue"],
		Status: m["status"],
		Error:  m["error"],
	}

	if v, ok := m["worker_id"]; ok {
		j.WorkerID = v
	}
	if v, ok := m["enqueued_by"]; ok {
		j.EnqueuedBy = v
	}

	if v, ok := m["payload"]; ok && v != "" {
		if err := json.Unmarshal([]byte(v), &j.Payload); err != nil {
			return nil, fmt.Errorf("decoding payload from map: %w", err)
		}
	}
	if v, ok := m["result"]; ok && v != "" {
		j.Result = json.RawMessage(v)
	}
	if v, ok := m["retry_intervals"]; ok && v != "" {
		if err := json.Unmarshal([]byte(v), &j.RetryIntervals); err != nil {
			return nil, fmt.Errorf("decoding retry_intervals from map: %w", err)
		}
	}
	if v, ok := m["meta"]; ok && v != "" {
		if err := json.Unmarshal([]byte(v), &j.Meta); err != nil {
			return nil, fmt.Errorf("decoding meta from map: %w", err)
		}
	}

	j.RetryCount = parseInt(m["retry_count"])
	j.MaxRetry = parseInt(m["max_retry"])
	j.Timeout = parseInt(m["timeout"])
	j.CreatedAt = parseInt64(m["created_at"])
	j.StartedAt = parseInt64(m["started_at"])
	j.CompletedAt = parseInt64(m["completed_at"])
	j.LastHeartbeat = parseInt64(m["last_heartbeat"])
	j.ExecutionDuration = parseInt64(m["execution_duration"])

	return j, nil
}
