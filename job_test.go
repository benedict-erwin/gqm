package gqm

import (
	"encoding/json"
	"math"
	"strconv"
	"strings"
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

func TestJob_Decode_MarshalError(t *testing.T) {
	// Payload with unmarshallable channel value triggers json.Marshal error.
	j := &Job{Payload: Payload{"bad": make(chan int)}}
	if err := j.Decode(&struct{}{}); err == nil {
		t.Error("expected error for unmarshallable payload")
	}
}

func TestJob_Decode_UnmarshalError(t *testing.T) {
	// Payload that marshals to valid JSON but can't unmarshal into target.
	j := &Job{Payload: Payload{"count": "not-a-number"}}
	var target struct {
		Count int `json:"count"`
	}
	// string->int unmarshal fails
	if err := j.Decode(&target); err == nil {
		t.Error("expected error for type mismatch")
	}
}

func TestJob_Encode_Error(t *testing.T) {
	// Job with unmarshallable field.
	j := &Job{Payload: Payload{"bad": math.Inf(1)}}
	if _, err := j.Encode(); err == nil {
		t.Error("expected error for unencodable job")
	}
}

func TestDecodeJob_InvalidJSON(t *testing.T) {
	_, err := DecodeJob([]byte("{invalid"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestDecodeJob_EmptyJSON(t *testing.T) {
	j, err := DecodeJob([]byte("{}"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if j.ID != "" {
		t.Errorf("ID = %q, want empty", j.ID)
	}
}

func TestJob_ToMap_PayloadMarshalError(t *testing.T) {
	j := &Job{ID: "test", Payload: Payload{"bad": make(chan int)}}
	if _, err := j.ToMap(); err == nil {
		t.Error("expected error for unmarshallable payload")
	}
}

func TestJob_ToMap_MetaMarshalError(t *testing.T) {
	j := NewJob("test", Payload{"ok": true})
	j.Meta = Payload{"bad": make(chan int)}
	if _, err := j.ToMap(); err == nil {
		t.Error("expected error for unmarshallable meta")
	}
}

func TestJob_ToMap_WithResult(t *testing.T) {
	j := NewJob("test", Payload{"ok": true})
	j.Result = json.RawMessage(`{"status":"done"}`)
	j.LastHeartbeat = 123
	j.ExecutionDuration = 456
	m, err := j.ToMap()
	if err != nil {
		t.Fatal(err)
	}
	if m["result"] != `{"status":"done"}` {
		t.Errorf("result = %v", m["result"])
	}
	if m["last_heartbeat"] != int64(123) {
		t.Errorf("last_heartbeat = %v", m["last_heartbeat"])
	}
	if m["execution_duration"] != int64(456) {
		t.Errorf("execution_duration = %v", m["execution_duration"])
	}
}

func TestJobFromMap_InvalidPayload(t *testing.T) {
	m := map[string]string{
		"id": "test", "type": "t", "queue": "q", "status": "ready",
		"payload": "{invalid",
	}
	if _, err := JobFromMap(m); err == nil {
		t.Error("expected error for invalid payload JSON")
	}
}

func TestJobFromMap_InvalidRetryIntervals(t *testing.T) {
	m := map[string]string{
		"id": "test", "type": "t", "queue": "q", "status": "ready",
		"retry_intervals": "{bad}",
	}
	if _, err := JobFromMap(m); err == nil {
		t.Error("expected error for invalid retry_intervals JSON")
	}
}

func TestJobFromMap_InvalidMeta(t *testing.T) {
	m := map[string]string{
		"id": "test", "type": "t", "queue": "q", "status": "ready",
		"meta": "{bad}",
	}
	if _, err := JobFromMap(m); err == nil {
		t.Error("expected error for invalid meta JSON")
	}
}

func TestJobFromMap_InvalidDependsOn(t *testing.T) {
	m := map[string]string{
		"id": "test", "type": "t", "queue": "q", "status": "ready",
		"depends_on": "{bad}",
	}
	if _, err := JobFromMap(m); err == nil {
		t.Error("expected error for invalid depends_on JSON")
	}
}

func TestJobFromMap_ResultAndWorkerID(t *testing.T) {
	m := map[string]string{
		"id": "test", "type": "t", "queue": "q", "status": "ready",
		"result":    `{"ok":true}`,
		"worker_id": "w-1",
	}
	j, err := JobFromMap(m)
	if err != nil {
		t.Fatal(err)
	}
	if string(j.Result) != `{"ok":true}` {
		t.Errorf("Result = %s", j.Result)
	}
	if j.WorkerID != "w-1" {
		t.Errorf("WorkerID = %q", j.WorkerID)
	}
}

func TestJobFromMap_AllIntFields(t *testing.T) {
	m := map[string]string{
		"id": "test", "type": "t", "queue": "q", "status": "ready",
		"retry_count":        "3",
		"max_retry":          "5",
		"timeout":            "30",
		"created_at":         "1000",
		"scheduled_at":       "2000",
		"started_at":         "3000",
		"completed_at":       "4000",
		"last_heartbeat":     "5000",
		"execution_duration": "6000",
		"allow_failure":      "1",
		"enqueue_at_front":   "1",
		"enqueued_by":        "test-user",
	}
	j, err := JobFromMap(m)
	if err != nil {
		t.Fatal(err)
	}
	if j.RetryCount != 3 {
		t.Errorf("RetryCount = %d", j.RetryCount)
	}
	if j.MaxRetry != 5 {
		t.Errorf("MaxRetry = %d", j.MaxRetry)
	}
	if j.Timeout != 30 {
		t.Errorf("Timeout = %d", j.Timeout)
	}
	if j.CreatedAt != 1000 {
		t.Errorf("CreatedAt = %d", j.CreatedAt)
	}
	if j.ScheduledAt != 2000 {
		t.Errorf("ScheduledAt = %d", j.ScheduledAt)
	}
	if j.StartedAt != 3000 {
		t.Errorf("StartedAt = %d", j.StartedAt)
	}
	if j.CompletedAt != 4000 {
		t.Errorf("CompletedAt = %d", j.CompletedAt)
	}
	if j.LastHeartbeat != 5000 {
		t.Errorf("LastHeartbeat = %d", j.LastHeartbeat)
	}
	if j.ExecutionDuration != 6000 {
		t.Errorf("ExecutionDuration = %d", j.ExecutionDuration)
	}
	if !j.AllowFailure {
		t.Error("AllowFailure should be true")
	}
	if !j.EnqueueAtFront {
		t.Error("EnqueueAtFront should be true")
	}
	if j.EnqueuedBy != "test-user" {
		t.Errorf("EnqueuedBy = %q", j.EnqueuedBy)
	}
}

func TestJobFromMap_EmptyPayload(t *testing.T) {
	m := map[string]string{
		"id": "test", "type": "t", "queue": "q", "status": "ready",
		"payload": "",
	}
	j, err := JobFromMap(m)
	if err != nil {
		t.Fatal(err)
	}
	if j.Payload != nil {
		t.Errorf("Payload = %v, want nil for empty", j.Payload)
	}
}

func TestJob_Decode_NilPayload(t *testing.T) {
	j := &Job{}
	var target struct{}
	// nil payload marshals to "null".
	if err := j.Decode(&target); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestJob_ToMap_DependsOnMarshalError(t *testing.T) {
	// DependsOn is []string which always marshals fine, but we can test the branch
	// by ensuring it appears in the map.
	j := NewJob("test", Payload{"ok": true})
	j.DependsOn = []string{"dep-1", "dep-2"}
	m, err := j.ToMap()
	if err != nil {
		t.Fatal(err)
	}
	v, ok := m["depends_on"]
	if !ok {
		t.Error("depends_on not in map")
	}
	if !strings.Contains(v.(string), "dep-1") {
		t.Errorf("depends_on = %v, missing dep-1", v)
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
