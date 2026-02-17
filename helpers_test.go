package gqm

import (
	"strings"
	"testing"
)

func TestValidateJobInputs_Valid(t *testing.T) {
	tests := []struct {
		name  string
		queue string
		jobID string
	}{
		{"default values", "default", "abc-123"},
		{"dots and underscores", "email.queue", "job_123.v2"},
		{"alphanumeric", "payments", "PAY001"},
		{"colon namespace", "email:send", "job-001"},
		{"nested namespace", "app:email:send", "job-002"},
		{"max queue length", strings.Repeat("a", 128), "ok"},
		{"max job ID length", "ok", strings.Repeat("b", 256)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := &Job{ID: tt.jobID, Queue: tt.queue}
			if err := validateJobInputs(job); err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestValidateJobInputs_InvalidQueue(t *testing.T) {
	tests := []struct {
		name  string
		queue string
	}{
		{"slash", "queue/path"},
		{"space", "queue name"},
		{"empty after validation skip", ""},
		{"exceeds 128 chars", strings.Repeat("x", 129)},
		{"special chars", "queue@#$"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := &Job{ID: "valid-id", Queue: tt.queue}
			err := validateJobInputs(job)
			// Empty queue is allowed (defaults to "default" at enqueue time).
			if tt.queue == "" {
				if err != nil {
					t.Errorf("empty queue should be allowed, got: %v", err)
				}
				return
			}
			if err != ErrInvalidQueueName {
				t.Errorf("expected ErrInvalidQueueName, got: %v", err)
			}
		})
	}
}

func TestValidateJobInputs_InvalidJobID(t *testing.T) {
	tests := []struct {
		name  string
		jobID string
	}{
		{"slash", "../../etc/passwd"},
		{"exceeds 256 chars", strings.Repeat("y", 257)},
		{"empty string", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := &Job{ID: tt.jobID, Queue: "default"}
			err := validateJobInputs(job)
			if err != ErrInvalidJobID {
				t.Errorf("expected ErrInvalidJobID, got: %v", err)
			}
		})
	}
}
