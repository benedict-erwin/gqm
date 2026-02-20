package gqm

import "testing"

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
