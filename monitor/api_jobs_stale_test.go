package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// seedProcessingJob writes a processing job and its claim in the queue's
// processing set, scored with the given deadline.
func seedProcessingJob(t *testing.T, m *Monitor, rdb *redis.Client, queue, id string, deadline int64) {
	t.Helper()
	ctx := context.Background()
	rdb.HSet(ctx, m.key("job", id),
		"id", id,
		"type", "email.send",
		"queue", queue,
		"status", "processing",
		"payload", `{}`,
	)
	rdb.ZAdd(ctx, m.key("queue", queue, "processing"),
		redis.Z{Score: float64(deadline), Member: id})
}

func TestJobs_GetProcessingStaleFlag(t *testing.T) {
	tests := []struct {
		name       string
		deadlineIn time.Duration
		wantStale  bool
	}{
		{name: "deadline still ahead", deadlineIn: time.Hour, wantStale: false},
		{name: "just past the deadline", deadlineIn: -1 * time.Second, wantStale: false},
		{name: "inside the stale margin", deadlineIn: -time.Duration(staleMarginSeconds-5) * time.Second, wantStale: false},
		{name: "worker presumed dead", deadlineIn: -time.Hour, wantStale: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, rdb := testMonitor(t, Config{})
			m.startedAt = time.Now()

			deadline := time.Now().Add(tt.deadlineIn).Unix()
			seedProcessingJob(t, m, rdb, "default", "job-stale", deadline)

			w := doRequest(m, "GET", "/api/v1/jobs/job-stale", "")
			if w.Code != http.StatusOK {
				t.Fatalf("status = %d", w.Code)
			}

			var resp response
			json.NewDecoder(w.Body).Decode(&resp)
			data, ok := resp.Data.(map[string]any)
			if !ok {
				t.Fatalf("data type = %T", resp.Data)
			}
			if got, _ := data["stale"].(bool); got != tt.wantStale {
				t.Errorf("stale = %v, want %v", data["stale"], tt.wantStale)
			}
			if got, _ := data["processing_deadline"].(float64); int64(got) != deadline {
				t.Errorf("processing_deadline = %v, want %d", data["processing_deadline"], deadline)
			}
		})
	}
}

func TestJobs_GetNonProcessingHasNoStaleFlag(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.HSet(ctx, m.key("job", "job-done"),
		"id", "job-done", "type", "email.send", "queue", "default", "status", "completed")

	w := doRequest(m, "GET", "/api/v1/jobs/job-done", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, _ := resp.Data.(map[string]any)
	if _, ok := data["stale"]; ok {
		t.Errorf("stale flag present on a %v job", data["status"])
	}
	if _, ok := data["processing_deadline"]; ok {
		t.Error("processing_deadline present on a job that is not processing")
	}
}

func TestJobs_GetProcessingWithoutClaimHasNoStaleFlag(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	// Hash says processing but no entry in the processing set: there is no
	// deadline to judge, so the API must not invent one.
	rdb.HSet(ctx, m.key("job", "job-noclaim"),
		"id", "job-noclaim", "type", "email.send", "queue", "default", "status", "processing")

	w := doRequest(m, "GET", "/api/v1/jobs/job-noclaim", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	data, _ := resp.Data.(map[string]any)
	if _, ok := data["stale"]; ok {
		t.Error("stale flag present for a job with no claim in the processing set")
	}
}

func TestQueueJobs_ListProcessingStaleFlag(t *testing.T) {
	m, rdb := testMonitor(t, Config{})
	m.startedAt = time.Now()
	ctx := context.Background()

	rdb.SAdd(ctx, m.key("queues"), "default")
	seedProcessingJob(t, m, rdb, "default", "job-live", time.Now().Add(time.Hour).Unix())
	seedProcessingJob(t, m, rdb, "default", "job-zombie", time.Now().Add(-time.Hour).Unix())

	w := doRequest(m, "GET", "/api/v1/queues/default/jobs?status=processing", "")
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d", w.Code)
	}

	var resp response
	json.NewDecoder(w.Body).Decode(&resp)
	list, ok := resp.Data.([]any)
	if !ok {
		t.Fatalf("data type = %T", resp.Data)
	}
	if len(list) != 2 {
		t.Fatalf("jobs = %d, want 2", len(list))
	}

	got := map[string]bool{}
	for _, item := range list {
		job, _ := item.(map[string]any)
		id, _ := job["id"].(string)
		stale, _ := job["stale"].(bool)
		got[id] = stale
	}
	if got["job-live"] {
		t.Error("job-live flagged stale")
	}
	if !got["job-zombie"] {
		t.Error("job-zombie not flagged stale")
	}
}
