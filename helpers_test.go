package gqm

import (
	"context"
	"log/slog"
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

func TestValidateJobInputs_InvalidDependsOn(t *testing.T) {
	tests := []struct {
		name    string
		deps    []string
	}{
		{"empty dep ID", []string{""}},
		{"invalid chars in dep", []string{"dep/bad"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := &Job{ID: "valid-id", Queue: "default", DependsOn: tt.deps}
			if err := validateJobInputs(job); err != ErrInvalidJobID {
				t.Errorf("expected ErrInvalidJobID, got: %v", err)
			}
		})
	}
}

func TestNewLoggerFromLevel(t *testing.T) {
	tests := []struct {
		level    string
		wantNil  bool
	}{
		{"", false},       // default logger
		{"debug", false},
		{"info", false},
		{"warn", false},
		{"error", false},
		{"DEBUG", false},  // case insensitive
		{"unknown", false}, // falls back to default
	}
	for _, tt := range tests {
		t.Run(tt.level, func(t *testing.T) {
			logger := newLoggerFromLevel(tt.level)
			if logger == nil {
				t.Error("logger should not be nil")
			}
		})
	}
}

func TestNewLoggerFromLevel_LevelCheck(t *testing.T) {
	// Debug level should enable debug messages.
	logger := newLoggerFromLevel("debug")
	if !logger.Enabled(context.Background(), slog.LevelDebug) {
		t.Error("debug logger should enable debug level")
	}

	// Error level should not enable debug messages.
	logger = newLoggerFromLevel("error")
	if logger.Enabled(context.Background(), slog.LevelDebug) {
		t.Error("error logger should not enable debug level")
	}
}

func TestParseInt_Valid(t *testing.T) {
	if v := parseInt("42"); v != 42 {
		t.Errorf("parseInt(42) = %d, want 42", v)
	}
}

func TestParseInt_Empty(t *testing.T) {
	if v := parseInt(""); v != 0 {
		t.Errorf("parseInt('') = %d, want 0", v)
	}
}

func TestParseInt_Invalid(t *testing.T) {
	// Should return 0 and log warning (not panic).
	if v := parseInt("not-a-number"); v != 0 {
		t.Errorf("parseInt(not-a-number) = %d, want 0", v)
	}
}

func TestParseInt64_Valid(t *testing.T) {
	if v := parseInt64("1234567890"); v != 1234567890 {
		t.Errorf("parseInt64(1234567890) = %d, want 1234567890", v)
	}
}

func TestParseInt64_Empty(t *testing.T) {
	if v := parseInt64(""); v != 0 {
		t.Errorf("parseInt64('') = %d, want 0", v)
	}
}

func TestParseInt64_Invalid(t *testing.T) {
	if v := parseInt64("bad"); v != 0 {
		t.Errorf("parseInt64(bad) = %d, want 0", v)
	}
}
