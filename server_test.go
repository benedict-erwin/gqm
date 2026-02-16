package gqm

import (
	"context"
	"errors"
	"testing"
)

func testServer(t *testing.T) *Server {
	t.Helper()
	skipWithoutRedis(t)
	s, err := NewServer(
		WithServerRedis(testRedisAddr()),
	)
	if err != nil {
		t.Fatalf("creating server: %v", err)
	}
	t.Cleanup(func() { s.rc.Close() })
	return s
}

func dummyHandler(_ context.Context, _ *Job) error { return nil }

func TestServer_Pool_Valid(t *testing.T) {
	s := testServer(t)

	err := s.Pool(PoolConfig{
		Name:        "email-pool",
		JobTypes:    []string{"email.send", "email.bulk"},
		Queues:      []string{"email"},
		Concurrency: 10,
	})
	if err != nil {
		t.Fatalf("Pool() error = %v", err)
	}

	if len(s.pools) != 1 {
		t.Fatalf("pools count = %d, want 1", len(s.pools))
	}
	if !s.poolNames["email-pool"] {
		t.Error("pool name not tracked")
	}
	if s.jobTypePool["email.send"] != "email-pool" {
		t.Errorf("jobTypePool[email.send] = %q, want %q", s.jobTypePool["email.send"], "email-pool")
	}
	if s.jobTypePool["email.bulk"] != "email-pool" {
		t.Errorf("jobTypePool[email.bulk] = %q, want %q", s.jobTypePool["email.bulk"], "email-pool")
	}
}

func TestServer_Pool_EmptyName(t *testing.T) {
	s := testServer(t)

	err := s.Pool(PoolConfig{Name: ""})
	if err == nil {
		t.Fatal("expected error for empty pool name")
	}
}

func TestServer_Pool_DuplicateName(t *testing.T) {
	s := testServer(t)

	if err := s.Pool(PoolConfig{Name: "mypool"}); err != nil {
		t.Fatalf("first Pool() error = %v", err)
	}

	err := s.Pool(PoolConfig{Name: "mypool"})
	if !errors.Is(err, ErrDuplicatePool) {
		t.Errorf("expected ErrDuplicatePool, got %v", err)
	}
}

func TestServer_ConflictDetection_ImplicitThenExplicit(t *testing.T) {
	s := testServer(t)

	// Register handler with Workers() — creates implicit pool
	if err := s.Handle("email.send", dummyHandler, Workers(5)); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Try to assign same job type to explicit pool — should conflict
	err := s.Pool(PoolConfig{
		Name:     "email-pool",
		JobTypes: []string{"email.send"},
	})
	if !errors.Is(err, ErrJobTypeConflict) {
		t.Errorf("expected ErrJobTypeConflict, got %v", err)
	}
}

func TestServer_ConflictDetection_ExplicitThenImplicit(t *testing.T) {
	s := testServer(t)

	// Register explicit pool with job type
	if err := s.Pool(PoolConfig{
		Name:     "email-pool",
		JobTypes: []string{"email.send"},
	}); err != nil {
		t.Fatalf("Pool() error = %v", err)
	}

	// Try to register handler with Workers() for same job type — should conflict
	err := s.Handle("email.send", dummyHandler, Workers(5))
	if !errors.Is(err, ErrJobTypeConflict) {
		t.Errorf("expected ErrJobTypeConflict, got %v", err)
	}
}

func TestServer_ConflictDetection_ExplicitThenExplicit(t *testing.T) {
	s := testServer(t)

	if err := s.Pool(PoolConfig{
		Name:     "pool-a",
		JobTypes: []string{"email.send"},
	}); err != nil {
		t.Fatalf("first Pool() error = %v", err)
	}

	err := s.Pool(PoolConfig{
		Name:     "pool-b",
		JobTypes: []string{"email.send"},
	})
	if !errors.Is(err, ErrJobTypeConflict) {
		t.Errorf("expected ErrJobTypeConflict, got %v", err)
	}
}

func TestServer_ConflictDetection_NoConflict(t *testing.T) {
	s := testServer(t)

	// Different job types in different pools — no conflict
	if err := s.Pool(PoolConfig{
		Name:     "pool-a",
		JobTypes: []string{"email.send"},
	}); err != nil {
		t.Fatalf("Pool() error = %v", err)
	}

	if err := s.Handle("payment.process", dummyHandler, Workers(3)); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	// Handler without Workers() — goes to default pool, no conflict
	if err := s.Handle("report.generate", dummyHandler); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
}

func TestServer_EnsureDefaultPool(t *testing.T) {
	s := testServer(t)

	// Register explicit pool for email
	if err := s.Pool(PoolConfig{
		Name:        "email-pool",
		JobTypes:    []string{"email.send"},
		Queues:      []string{"email"},
		Concurrency: 5,
	}); err != nil {
		t.Fatalf("Pool() error = %v", err)
	}

	// Register handlers
	if err := s.Handle("email.send", dummyHandler); err != nil {
		t.Fatalf("Handle(email.send) error = %v", err)
	}
	if err := s.Handle("report.generate", dummyHandler); err != nil {
		t.Fatalf("Handle(report.generate) error = %v", err)
	}

	// Before ensureDefaultPool: 1 explicit pool
	if len(s.pools) != 1 {
		t.Fatalf("pools before ensure = %d, want 1", len(s.pools))
	}

	s.ensureDefaultPool()

	// After: 1 explicit + 1 default pool (for report.generate)
	if len(s.pools) != 2 {
		t.Fatalf("pools after ensure = %d, want 2", len(s.pools))
	}

	// The default pool should have queue "default"
	defaultPool := s.pools[1]
	if defaultPool.cfg.name != "default" {
		t.Errorf("default pool name = %q, want %q", defaultPool.cfg.name, "default")
	}
}

func TestServer_EnsureDefaultPool_AllAssigned(t *testing.T) {
	s := testServer(t)

	// All handlers have explicit Workers() — no default pool needed
	if err := s.Handle("email.send", dummyHandler, Workers(5)); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if err := s.Handle("payment.process", dummyHandler, Workers(3)); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	poolsBefore := len(s.pools)
	s.ensureDefaultPool()

	if len(s.pools) != poolsBefore {
		t.Errorf("pools changed from %d to %d, expected no change", poolsBefore, len(s.pools))
	}
}

func TestServer_Schedule_Valid(t *testing.T) {
	s := testServer(t)

	err := s.Schedule(CronEntry{
		ID:       "daily-backup",
		Name:     "Daily Backup",
		CronExpr: "0 0 3 * * *",
		Timezone: "Asia/Jakarta",
		JobType:  "backup.database",
		Queue:    "critical",
		Enabled:  true,
	})
	if err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}

	if len(s.cronEntries) != 1 {
		t.Fatalf("cronEntries count = %d, want 1", len(s.cronEntries))
	}
	entry := s.cronEntries["daily-backup"]
	if entry.Queue != "critical" {
		t.Errorf("queue = %q, want %q", entry.Queue, "critical")
	}
	if entry.OverlapPolicy != OverlapSkip {
		t.Errorf("overlap_policy = %q, want %q", entry.OverlapPolicy, OverlapSkip)
	}
	if entry.expr == nil {
		t.Error("parsed cron expression is nil")
	}
}

func TestServer_Schedule_Defaults(t *testing.T) {
	s := testServer(t)

	err := s.Schedule(CronEntry{
		ID:       "test-entry",
		CronExpr: "* * * * *",
		JobType:  "test.job",
	})
	if err != nil {
		t.Fatalf("Schedule() error = %v", err)
	}

	entry := s.cronEntries["test-entry"]
	if entry.Queue != "default" {
		t.Errorf("queue = %q, want %q", entry.Queue, "default")
	}
	if entry.OverlapPolicy != OverlapSkip {
		t.Errorf("overlap_policy = %q, want %q", entry.OverlapPolicy, OverlapSkip)
	}
}

func TestServer_Schedule_DuplicateID(t *testing.T) {
	s := testServer(t)

	if err := s.Schedule(CronEntry{
		ID: "dup", CronExpr: "* * * * *", JobType: "a",
	}); err != nil {
		t.Fatal(err)
	}

	err := s.Schedule(CronEntry{
		ID: "dup", CronExpr: "* * * * *", JobType: "b",
	})
	if !errors.Is(err, ErrDuplicateCronEntry) {
		t.Errorf("expected ErrDuplicateCronEntry, got %v", err)
	}
}

func TestServer_Schedule_Invalid(t *testing.T) {
	s := testServer(t)

	tests := []struct {
		name  string
		entry CronEntry
	}{
		{"empty ID", CronEntry{CronExpr: "* * * * *", JobType: "a"}},
		{"empty cron expr", CronEntry{ID: "x", JobType: "a"}},
		{"empty job type", CronEntry{ID: "x", CronExpr: "* * * * *"}},
		{"bad cron expr", CronEntry{ID: "x", CronExpr: "bad", JobType: "a"}},
		{"bad timezone", CronEntry{ID: "x", CronExpr: "* * * * *", JobType: "a", Timezone: "Invalid/Zone"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := s.Schedule(tt.entry)
			if err == nil {
				t.Error("expected error")
			}
		})
	}
}
