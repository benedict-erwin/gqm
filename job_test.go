package gqm

import (
	"encoding/json"
	"strconv"
	"testing"
)

func TestNewJob(t *testing.T) {
	j := NewJob("email.send", Payload{"to": "user@example.com"})

	if j.ID == "" {
		t.Error("expected non-empty ID")
	}
	if j.Type != "email.send" {
		t.Errorf("Type = %q, want %q", j.Type, "email.send")
	}
	if j.Queue != "default" {
		t.Errorf("Queue = %q, want %q", j.Queue, "default")
	}
	if j.Status != StatusReady {
		t.Errorf("Status = %q, want %q", j.Status, StatusReady)
	}
	if j.MaxRetry != 3 {
		t.Errorf("MaxRetry = %d, want 3", j.MaxRetry)
	}
	if j.CreatedAt == 0 {
		t.Error("expected non-zero CreatedAt")
	}
}

func TestJob_EncodeDecode(t *testing.T) {
	original := NewJob("payment.process", Payload{
		"amount":   100.50,
		"currency": "USD",
	})
	original.RetryIntervals = []int{10, 30, 60}
	original.Meta = Payload{"source": "api"}

	data, err := original.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	decoded, err := DecodeJob(data)
	if err != nil {
		t.Fatalf("DecodeJob: %v", err)
	}

	if decoded.ID != original.ID {
		t.Errorf("ID = %q, want %q", decoded.ID, original.ID)
	}
	if decoded.Type != original.Type {
		t.Errorf("Type = %q, want %q", decoded.Type, original.Type)
	}
	if decoded.Status != original.Status {
		t.Errorf("Status = %q, want %q", decoded.Status, original.Status)
	}
	if decoded.Queue != original.Queue {
		t.Errorf("Queue = %q, want %q", decoded.Queue, original.Queue)
	}
	if decoded.MaxRetry != original.MaxRetry {
		t.Errorf("MaxRetry = %d, want %d", decoded.MaxRetry, original.MaxRetry)
	}
	if decoded.CreatedAt != original.CreatedAt {
		t.Errorf("CreatedAt = %d, want %d", decoded.CreatedAt, original.CreatedAt)
	}
	if len(decoded.RetryIntervals) != len(original.RetryIntervals) {
		t.Errorf("RetryIntervals len = %d, want %d", len(decoded.RetryIntervals), len(original.RetryIntervals))
	}
	for i, v := range decoded.RetryIntervals {
		if v != original.RetryIntervals[i] {
			t.Errorf("RetryIntervals[%d] = %d, want %d", i, v, original.RetryIntervals[i])
		}
	}
	if len(decoded.Meta) == 0 {
		t.Error("Meta should not be empty after round-trip")
	}
}

func TestJob_Decode(t *testing.T) {
	j := NewJob("email.send", Payload{
		"to":      "user@example.com",
		"subject": "Hello",
	})

	var p struct {
		To      string `json:"to"`
		Subject string `json:"subject"`
	}

	if err := j.Decode(&p); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if p.To != "user@example.com" {
		t.Errorf("To = %q, want %q", p.To, "user@example.com")
	}
	if p.Subject != "Hello" {
		t.Errorf("Subject = %q, want %q", p.Subject, "Hello")
	}
}

func TestJob_ToMap_FromMap_RoundTrip(t *testing.T) {
	original := NewJob("report.generate", Payload{
		"format": "pdf",
		"pages":  float64(10),
	})
	original.RetryIntervals = []int{5, 15, 45}
	original.Meta = Payload{"priority": "high"}
	original.EnqueuedBy = "api-server"
	original.DependsOn = []string{"parent-a", "parent-b"}
	original.AllowFailure = true
	original.EnqueueAtFront = true

	m, err := original.ToMap()
	if err != nil {
		t.Fatalf("ToMap: %v", err)
	}

	strMap := toStringMap(m)

	restored, err := JobFromMap(strMap)
	if err != nil {
		t.Fatalf("JobFromMap: %v", err)
	}

	if restored.ID != original.ID {
		t.Errorf("ID = %q, want %q", restored.ID, original.ID)
	}
	if restored.Type != original.Type {
		t.Errorf("Type = %q, want %q", restored.Type, original.Type)
	}
	if restored.EnqueuedBy != original.EnqueuedBy {
		t.Errorf("EnqueuedBy = %q, want %q", restored.EnqueuedBy, original.EnqueuedBy)
	}
	if len(restored.RetryIntervals) != len(original.RetryIntervals) {
		t.Errorf("RetryIntervals len = %d, want %d", len(restored.RetryIntervals), len(original.RetryIntervals))
	}
	if len(restored.Meta) == 0 {
		t.Error("Meta should not be empty after round-trip")
	}
	if len(restored.DependsOn) != 2 {
		t.Errorf("DependsOn len = %d, want 2", len(restored.DependsOn))
	}
	if !restored.AllowFailure {
		t.Error("AllowFailure should be true after round-trip")
	}
	if !restored.EnqueueAtFront {
		t.Error("EnqueueAtFront should be true after round-trip")
	}
}

func TestStatusConstants(t *testing.T) {
	statuses := []string{
		StatusReady, StatusScheduled, StatusDeferred,
		StatusProcessing, StatusCompleted, StatusFailed,
		StatusRetry, StatusDeadLetter, StatusStopped, StatusCanceled,
	}

	seen := make(map[string]bool)
	for _, s := range statuses {
		if s == "" {
			t.Error("empty status constant")
		}
		if seen[s] {
			t.Errorf("duplicate status: %q", s)
		}
		seen[s] = true
	}
}

func toStringMap(m map[string]any) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		switch val := v.(type) {
		case string:
			out[k] = val
		case int:
			out[k] = strconv.Itoa(val)
		case int64:
			out[k] = strconv.FormatInt(val, 10)
		default:
			data, _ := json.Marshal(val)
			out[k] = string(data)
		}
	}
	return out
}
