package gqm

import (
	"testing"
	"time"
)

func TestResultTTL_Semantics(t *testing.T) {
	tests := []struct {
		name string
		in   time.Duration
		want int
	}{
		{"whole seconds", 7 * 24 * time.Hour, 604800},
		{"zero deletes immediately", 0, 0},
		{"permanent sentinel", TTLPermanent, -1},
		{"any negative collapses to permanent", -5 * time.Hour, -1},
		{"sub-second rounds up", 500 * time.Millisecond, 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := NewJob("test", nil)
			ResultTTL(tt.in)(j)
			if j.ResultTTL == nil {
				t.Fatal("ResultTTL should be set, got nil")
			}
			if *j.ResultTTL != tt.want {
				t.Errorf("ResultTTL = %d, want %d", *j.ResultTTL, tt.want)
			}
		})
	}
}

func TestFailureTTL_Semantics(t *testing.T) {
	tests := []struct {
		name string
		in   time.Duration
		want int
	}{
		{"whole seconds", 30 * 24 * time.Hour, 2592000},
		{"zero deletes immediately", 0, 0},
		{"permanent sentinel", TTLPermanent, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := NewJob("test", nil)
			FailureTTL(tt.in)(j)
			if j.FailureTTL == nil {
				t.Fatal("FailureTTL should be set, got nil")
			}
			if *j.FailureTTL != tt.want {
				t.Errorf("FailureTTL = %d, want %d", *j.FailureTTL, tt.want)
			}
		})
	}
}

func TestRetentionTTL_Unset(t *testing.T) {
	j := NewJob("test", nil)
	if j.ResultTTL != nil {
		t.Errorf("ResultTTL should default to nil (server default), got %d", *j.ResultTTL)
	}
	if j.FailureTTL != nil {
		t.Errorf("FailureTTL should default to nil (server default), got %d", *j.FailureTTL)
	}
}

func TestRetentionTTL_OverrideWinsOverDefault(t *testing.T) {
	tests := []struct {
		name          string
		override      *int
		serverDefault int
		want          int
	}{
		{"nil falls back to server default", nil, 604800, 604800},
		{"override wins", intPtr(3600), 604800, 3600},
		{"override of zero wins over non-zero default", intPtr(0), 604800, 0},
		{"override of permanent wins", intPtr(-1), 604800, -1},
		{"nil falls back to a permanent default", nil, -1, -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := retentionTTL(tt.override, tt.serverDefault); got != tt.want {
				t.Errorf("retentionTTL = %d, want %d", got, tt.want)
			}
		})
	}
}

func intPtr(v int) *int { return &v }

func TestAllowFailure_True(t *testing.T) {
	j := NewJob("test", nil)
	AllowFailure(true)(j)
	if !j.AllowFailure {
		t.Error("AllowFailure should be true")
	}
}

func TestAllowFailure_False(t *testing.T) {
	j := NewJob("test", nil)
	j.AllowFailure = true
	AllowFailure(false)(j)
	if j.AllowFailure {
		t.Error("AllowFailure should be false")
	}
}

func TestMaxRetry_Positive(t *testing.T) {
	j := NewJob("test", nil)
	MaxRetry(5)(j)
	if j.MaxRetry != 5 {
		t.Errorf("MaxRetry = %d, want 5", j.MaxRetry)
	}
}

func TestMaxRetry_NegativeClamped(t *testing.T) {
	j := NewJob("test", nil)
	MaxRetry(-1)(j)
	if j.MaxRetry != 0 {
		t.Errorf("MaxRetry = %d, want 0 (clamped from -1)", j.MaxRetry)
	}
}

func TestMaxRetry_Zero(t *testing.T) {
	j := NewJob("test", nil)
	j.MaxRetry = 5
	MaxRetry(0)(j)
	if j.MaxRetry != 0 {
		t.Errorf("MaxRetry = %d, want 0", j.MaxRetry)
	}
}

func TestQueue_Option(t *testing.T) {
	j := NewJob("test", nil)
	Queue("high-priority")(j)
	if j.Queue != "high-priority" {
		t.Errorf("Queue = %q, want %q", j.Queue, "high-priority")
	}
}

func TestTimeout_Option(t *testing.T) {
	j := NewJob("test", nil)
	Timeout(30e9)(j) // 30 seconds in nanoseconds
	if j.Timeout != 30 {
		t.Errorf("Timeout = %d, want 30", j.Timeout)
	}
}

func TestRetryIntervals_Option(t *testing.T) {
	j := NewJob("test", nil)
	RetryIntervals(10, 30, 60)(j)
	if len(j.RetryIntervals) != 3 {
		t.Errorf("RetryIntervals len = %d, want 3", len(j.RetryIntervals))
	}
}

func TestJobID_Option(t *testing.T) {
	j := NewJob("test", nil)
	JobID("custom-id")(j)
	if j.ID != "custom-id" {
		t.Errorf("ID = %q, want %q", j.ID, "custom-id")
	}
}

func TestMeta_Option(t *testing.T) {
	j := NewJob("test", nil)
	Meta(Payload{"key": "value"})(j)
	if j.Meta["key"] != "value" {
		t.Errorf("Meta[key] = %v, want %q", j.Meta["key"], "value")
	}
}

func TestEnqueuedBy_Option(t *testing.T) {
	j := NewJob("test", nil)
	EnqueuedBy("api-server")(j)
	if j.EnqueuedBy != "api-server" {
		t.Errorf("EnqueuedBy = %q, want %q", j.EnqueuedBy, "api-server")
	}
}

func TestDependsOn_Option(t *testing.T) {
	j := NewJob("test", nil)
	DependsOn("parent-a", "parent-b")(j)
	if len(j.DependsOn) != 2 {
		t.Errorf("DependsOn len = %d, want 2", len(j.DependsOn))
	}
}

func TestEnqueueAtFront_Option(t *testing.T) {
	j := NewJob("test", nil)
	EnqueueAtFront(true)(j)
	if !j.EnqueueAtFront {
		t.Error("EnqueueAtFront should be true")
	}
}

func TestUnique_Option(t *testing.T) {
	j := NewJob("test", nil)
	Unique()(j)
	if !j.unique {
		t.Error("unique should be true")
	}
}
