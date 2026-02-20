package gqm

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// --- LoadConfig / validation tests ---

func TestLoadConfig_ValidFull(t *testing.T) {
	yaml := `
redis:
  addr: "localhost:6380"
  password: "secret"
  db: 2
  prefix: "myapp:"

app:
  timezone: "Asia/Jakarta"
  log_level: "debug"
  shutdown_timeout: 60
  global_job_timeout: 1800
  grace_period: 15

queues:
  - name: critical
    priority: 10
  - name: default
    priority: 1

pools:
  - name: email-pool
    job_types: ["email.send", "email.bulk"]
    queues: ["critical", "default"]
    concurrency: 10
    job_timeout: 300
    grace_period: 20
    shutdown_timeout: 45
    dequeue_strategy: weighted
    retry:
      max_retry: 5
      backoff: exponential
      backoff_base: 2
      backoff_max: 600

  - name: catch-all
    job_types: ["*"]
    concurrency: 4

scheduler:
  enabled: true
  poll_interval: 2
  cron_entries:
    - id: daily-report
      name: "Daily Report"
      cron_expr: "0 0 3 * * *"
      timezone: "Asia/Jakarta"
      job_type: report.daily
      queue: critical
      payload: '{"format": "pdf"}'
      timeout: 120
      max_retry: 3
      overlap_policy: skip
`
	cfg, err := LoadConfig([]byte(yaml))
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Redis
	if cfg.Redis.Addr != "localhost:6380" {
		t.Errorf("redis.addr = %q, want %q", cfg.Redis.Addr, "localhost:6380")
	}
	if cfg.Redis.Password != "secret" {
		t.Errorf("redis.password = %q, want %q", cfg.Redis.Password, "secret")
	}
	if cfg.Redis.DB != 2 {
		t.Errorf("redis.db = %d, want 2", cfg.Redis.DB)
	}
	if cfg.Redis.Prefix != "myapp:" {
		t.Errorf("redis.prefix = %q, want %q", cfg.Redis.Prefix, "myapp:")
	}

	// App
	if cfg.App.Timezone != "Asia/Jakarta" {
		t.Errorf("app.timezone = %q", cfg.App.Timezone)
	}
	if cfg.App.LogLevel != "debug" {
		t.Errorf("app.log_level = %q", cfg.App.LogLevel)
	}
	if cfg.App.ShutdownTimeout != 60 {
		t.Errorf("app.shutdown_timeout = %d", cfg.App.ShutdownTimeout)
	}
	if cfg.App.GlobalJobTimeout != 1800 {
		t.Errorf("app.global_job_timeout = %d", cfg.App.GlobalJobTimeout)
	}
	if cfg.App.GracePeriod != 15 {
		t.Errorf("app.grace_period = %d", cfg.App.GracePeriod)
	}

	// Queues
	if len(cfg.Queues) != 2 {
		t.Fatalf("queues count = %d, want 2", len(cfg.Queues))
	}
	if cfg.Queues[0].Name != "critical" || cfg.Queues[0].Priority != 10 {
		t.Errorf("queues[0] = %+v", cfg.Queues[0])
	}

	// Pools
	if len(cfg.Pools) != 2 {
		t.Fatalf("pools count = %d, want 2", len(cfg.Pools))
	}
	ep := cfg.Pools[0]
	if ep.Name != "email-pool" {
		t.Errorf("pools[0].name = %q", ep.Name)
	}
	if ep.Concurrency != 10 {
		t.Errorf("pools[0].concurrency = %d", ep.Concurrency)
	}
	if ep.DequeueStrategy != "weighted" {
		t.Errorf("pools[0].dequeue_strategy = %q", ep.DequeueStrategy)
	}
	if ep.Retry == nil {
		t.Fatal("pools[0].retry is nil")
	}
	if ep.Retry.Backoff != "exponential" {
		t.Errorf("pools[0].retry.backoff = %q", ep.Retry.Backoff)
	}

	// Catch-all
	if cfg.Pools[1].Name != "catch-all" {
		t.Errorf("pools[1].name = %q", cfg.Pools[1].Name)
	}
	if len(cfg.Pools[1].JobTypes) != 1 || cfg.Pools[1].JobTypes[0] != "*" {
		t.Errorf("pools[1].job_types = %v", cfg.Pools[1].JobTypes)
	}

	// Scheduler
	if cfg.Scheduler.Enabled == nil || !*cfg.Scheduler.Enabled {
		t.Error("scheduler.enabled should be true")
	}
	if cfg.Scheduler.PollInterval != 2 {
		t.Errorf("scheduler.poll_interval = %d", cfg.Scheduler.PollInterval)
	}
	if len(cfg.Scheduler.CronEntries) != 1 {
		t.Fatalf("cron_entries count = %d", len(cfg.Scheduler.CronEntries))
	}
	ce := cfg.Scheduler.CronEntries[0]
	if ce.ID != "daily-report" {
		t.Errorf("cron_entries[0].id = %q", ce.ID)
	}
	if ce.Payload != `{"format": "pdf"}` {
		t.Errorf("cron_entries[0].payload = %q", ce.Payload)
	}
}

func TestLoadConfig_MinimalValid(t *testing.T) {
	yaml := `
pools:
  - name: worker
    job_types: ["email.send"]
`
	_, err := LoadConfig([]byte(yaml))
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
}

func TestLoadConfig_EmptyConfig(t *testing.T) {
	// Empty YAML is valid (all fields have zero values)
	_, err := LoadConfig([]byte(""))
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
}

func TestLoadConfig_MalformedYAML(t *testing.T) {
	// Use indentation that is structurally invalid for the target type
	malformed := "redis:\n  addr:\n  - list\n  - where\n  - string\n  - expected"
	_, err := LoadConfig([]byte(malformed))
	if err == nil {
		t.Fatal("expected error for malformed YAML")
	}
}

// --- Validation error tests ---

func TestValidate_RedisDBNegative(t *testing.T) {
	_, err := LoadConfig([]byte("redis:\n  db: -1"))
	if err == nil || !strings.Contains(err.Error(), "redis.db") {
		t.Errorf("expected redis.db error, got %v", err)
	}
}

func TestValidate_AppTimezoneInvalid(t *testing.T) {
	_, err := LoadConfig([]byte("app:\n  timezone: Invalid/Zone"))
	if err == nil || !strings.Contains(err.Error(), "app.timezone") {
		t.Errorf("expected app.timezone error, got %v", err)
	}
}

func TestValidate_AppLogLevelInvalid(t *testing.T) {
	_, err := LoadConfig([]byte("app:\n  log_level: verbose"))
	if err == nil || !strings.Contains(err.Error(), "app.log_level") {
		t.Errorf("expected app.log_level error, got %v", err)
	}
}

func TestValidate_QueueNameEmpty(t *testing.T) {
	yaml := `
queues:
  - name: ""
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "queues[0].name") {
		t.Errorf("expected queue name error, got %v", err)
	}
}

func TestValidate_QueueNameDuplicate(t *testing.T) {
	yaml := `
queues:
  - name: critical
  - name: critical
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("expected duplicate queue error, got %v", err)
	}
}

func TestValidate_QueueNameInvalidChars(t *testing.T) {
	yaml := `
queues:
  - name: "my queue!"
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Errorf("expected invalid chars error, got %v", err)
	}
}

func TestValidate_PoolNameEmpty(t *testing.T) {
	yaml := `
pools:
  - name: ""
    job_types: ["a"]
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "pools[0].name") {
		t.Errorf("expected pool name error, got %v", err)
	}
}

func TestValidate_PoolNameDuplicate(t *testing.T) {
	yaml := `
pools:
  - name: worker
    job_types: ["a"]
  - name: worker
    job_types: ["b"]
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("expected duplicate pool error, got %v", err)
	}
}

func TestValidate_PoolJobTypesEmpty(t *testing.T) {
	yaml := `
pools:
  - name: worker
    job_types: []
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "job_types") {
		t.Errorf("expected job_types error, got %v", err)
	}
}

func TestValidate_PoolJobTypeConflict(t *testing.T) {
	yaml := `
pools:
  - name: pool-a
    job_types: ["email.send"]
  - name: pool-b
    job_types: ["email.send"]
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "already assigned") {
		t.Errorf("expected job type conflict error, got %v", err)
	}
}

func TestValidate_PoolDequeueStrategyInvalid(t *testing.T) {
	yaml := `
pools:
  - name: worker
    job_types: ["a"]
    dequeue_strategy: random
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "dequeue_strategy") {
		t.Errorf("expected dequeue_strategy error, got %v", err)
	}
}

func TestValidate_PoolRetryBackoffInvalid(t *testing.T) {
	yaml := `
pools:
  - name: worker
    job_types: ["a"]
    retry:
      backoff: linear
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "retry.backoff") {
		t.Errorf("expected retry.backoff error, got %v", err)
	}
}

func TestValidate_PoolRetryCustomNoIntervals(t *testing.T) {
	yaml := `
pools:
  - name: worker
    job_types: ["a"]
    retry:
      backoff: custom
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "intervals") {
		t.Errorf("expected intervals error, got %v", err)
	}
}

func TestValidate_MultipleCatchAllPools(t *testing.T) {
	yaml := `
pools:
  - name: catch-a
    job_types: ["*"]
  - name: catch-b
    job_types: ["*"]
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "only one pool") {
		t.Errorf("expected catch-all error, got %v", err)
	}
}

func TestValidate_CatchAllWithOtherTypes(t *testing.T) {
	yaml := `
pools:
  - name: mixed
    job_types: ["*", "email.send"]
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "sole entry") {
		t.Errorf("expected sole entry error, got %v", err)
	}
}

func TestValidate_CronEntryIDEmpty(t *testing.T) {
	yaml := `
scheduler:
  cron_entries:
    - id: ""
      cron_expr: "* * * * *"
      job_type: a
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "cron_entries[0].id") {
		t.Errorf("expected cron ID error, got %v", err)
	}
}

func TestValidate_CronEntryIDDuplicate(t *testing.T) {
	yaml := `
scheduler:
  cron_entries:
    - id: daily
      cron_expr: "* * * * *"
      job_type: a
    - id: daily
      cron_expr: "0 * * * *"
      job_type: b
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("expected duplicate cron ID error, got %v", err)
	}
}

func TestValidate_CronExprInvalid(t *testing.T) {
	yaml := `
scheduler:
  cron_entries:
    - id: test
      cron_expr: "bad expr"
      job_type: a
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "invalid cron_expr") {
		t.Errorf("expected cron_expr error, got %v", err)
	}
}

func TestValidate_CronJobTypeEmpty(t *testing.T) {
	yaml := `
scheduler:
  cron_entries:
    - id: test
      cron_expr: "* * * * *"
      job_type: ""
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "job_type") {
		t.Errorf("expected job_type error, got %v", err)
	}
}

func TestValidate_CronTimezoneInvalid(t *testing.T) {
	yaml := `
scheduler:
  cron_entries:
    - id: test
      cron_expr: "* * * * *"
      job_type: a
      timezone: NotA/Timezone
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "timezone") {
		t.Errorf("expected timezone error, got %v", err)
	}
}

func TestValidate_CronOverlapPolicyInvalid(t *testing.T) {
	yaml := `
scheduler:
  cron_entries:
    - id: test
      cron_expr: "* * * * *"
      job_type: a
      overlap_policy: cancel
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "overlap_policy") {
		t.Errorf("expected overlap_policy error, got %v", err)
	}
}

func TestValidate_CronPayloadInvalidJSON(t *testing.T) {
	yaml := `
scheduler:
  cron_entries:
    - id: test
      cron_expr: "* * * * *"
      job_type: a
      payload: "not json"
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "payload") {
		t.Errorf("expected payload JSON error, got %v", err)
	}
}

// --- LoadConfigFile ---

func TestLoadConfigFile_Valid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gqm.yaml")
	content := `
pools:
  - name: worker
    job_types: ["email.send"]
`
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := LoadConfigFile(path)
	if err != nil {
		t.Fatalf("LoadConfigFile() error = %v", err)
	}
	if len(cfg.Pools) != 1 {
		t.Errorf("pools count = %d, want 1", len(cfg.Pools))
	}
}

func TestLoadConfigFile_NotFound(t *testing.T) {
	_, err := LoadConfigFile("/nonexistent/path/gqm.yaml")
	if err == nil {
		t.Fatal("expected error for missing file")
	}
}

// --- Conversion tests ---

func TestPoolYAML_ToPoolConfig(t *testing.T) {
	py := &PoolYAML{
		Name:            "email",
		JobTypes:        []string{"email.send", "email.bulk"},
		Queues:          []string{"critical", "default"},
		Concurrency:     8,
		JobTimeout:      300,
		GracePeriod:     15,
		ShutdownTimeout: 45,
		DequeueStrategy: "round_robin",
		Retry: &RetryYAML{
			MaxRetry:    5,
			Backoff:     "exponential",
			BackoffBase: 2,
			BackoffMax:  600,
		},
	}

	pc := py.toPoolConfig()

	if pc.Name != "email" {
		t.Errorf("name = %q", pc.Name)
	}
	if len(pc.JobTypes) != 2 {
		t.Errorf("job_types len = %d", len(pc.JobTypes))
	}
	if len(pc.Queues) != 2 {
		t.Errorf("queues len = %d", len(pc.Queues))
	}
	if pc.Concurrency != 8 {
		t.Errorf("concurrency = %d", pc.Concurrency)
	}
	if pc.JobTimeout != 300*time.Second {
		t.Errorf("job_timeout = %v", pc.JobTimeout)
	}
	if pc.GracePeriod != 15*time.Second {
		t.Errorf("grace_period = %v", pc.GracePeriod)
	}
	if pc.ShutdownTimeout != 45*time.Second {
		t.Errorf("shutdown_timeout = %v", pc.ShutdownTimeout)
	}
	if pc.DequeueStrategy != StrategyRoundRobin {
		t.Errorf("dequeue_strategy = %q", pc.DequeueStrategy)
	}
	if pc.RetryPolicy == nil {
		t.Fatal("retry_policy is nil")
	}
	if pc.RetryPolicy.Backoff != BackoffExponential {
		t.Errorf("retry.backoff = %q", pc.RetryPolicy.Backoff)
	}
	if pc.RetryPolicy.BackoffBase != 2*time.Second {
		t.Errorf("retry.backoff_base = %v", pc.RetryPolicy.BackoffBase)
	}
	if pc.RetryPolicy.BackoffMax != 600*time.Second {
		t.Errorf("retry.backoff_max = %v", pc.RetryPolicy.BackoffMax)
	}
}

func TestPoolYAML_ToPoolConfig_Minimal(t *testing.T) {
	py := &PoolYAML{
		Name:     "worker",
		JobTypes: []string{"a"},
	}

	pc := py.toPoolConfig()

	if pc.Concurrency != 0 {
		t.Errorf("concurrency = %d, want 0 (let Pool() default)", pc.Concurrency)
	}
	if pc.JobTimeout != 0 {
		t.Errorf("job_timeout = %v, want 0", pc.JobTimeout)
	}
	if pc.DequeueStrategy != "" {
		t.Errorf("dequeue_strategy = %q, want empty", pc.DequeueStrategy)
	}
	if pc.RetryPolicy != nil {
		t.Errorf("retry_policy should be nil")
	}
}

func TestRetryYAML_ToRetryPolicy(t *testing.T) {
	r := &RetryYAML{
		MaxRetry:    3,
		Intervals:   []int{5, 10, 30},
		Backoff:     "custom",
		BackoffBase: 1,
		BackoffMax:  60,
	}

	rp := r.toRetryPolicy()

	if rp.MaxRetry != 3 {
		t.Errorf("max_retry = %d", rp.MaxRetry)
	}
	if len(rp.Intervals) != 3 {
		t.Errorf("intervals len = %d", len(rp.Intervals))
	}
	if rp.Backoff != BackoffCustom {
		t.Errorf("backoff = %q", rp.Backoff)
	}
	if rp.BackoffBase != 1*time.Second {
		t.Errorf("backoff_base = %v", rp.BackoffBase)
	}
	if rp.BackoffMax != 60*time.Second {
		t.Errorf("backoff_max = %v", rp.BackoffMax)
	}
}

func TestCronEntryYAML_ToCronEntry(t *testing.T) {
	enabled := true
	ce := &CronEntryYAML{
		ID:            "daily-report",
		Name:          "Daily Report",
		CronExpr:      "0 0 3 * * *",
		Timezone:      "Asia/Jakarta",
		JobType:       "report.daily",
		Queue:         "critical",
		Payload:       `{"format": "pdf"}`,
		Timeout:       120,
		MaxRetry:      3,
		OverlapPolicy: "skip",
		Enabled:       &enabled,
	}

	entry := ce.toCronEntry()

	if entry.ID != "daily-report" {
		t.Errorf("id = %q", entry.ID)
	}
	if entry.Name != "Daily Report" {
		t.Errorf("name = %q", entry.Name)
	}
	if entry.CronExpr != "0 0 3 * * *" {
		t.Errorf("cron_expr = %q", entry.CronExpr)
	}
	if entry.Timezone != "Asia/Jakarta" {
		t.Errorf("timezone = %q", entry.Timezone)
	}
	if entry.JobType != "report.daily" {
		t.Errorf("job_type = %q", entry.JobType)
	}
	if entry.Queue != "critical" {
		t.Errorf("queue = %q", entry.Queue)
	}
	if entry.Payload == nil {
		t.Fatal("payload is nil")
	}
	if entry.Payload["format"] != "pdf" {
		t.Errorf("payload[format] = %v", entry.Payload["format"])
	}
	if entry.Timeout != 120 {
		t.Errorf("timeout = %d", entry.Timeout)
	}
	if entry.MaxRetry != 3 {
		t.Errorf("max_retry = %d", entry.MaxRetry)
	}
	if entry.OverlapPolicy != OverlapSkip {
		t.Errorf("overlap_policy = %q", entry.OverlapPolicy)
	}
	if !entry.Enabled {
		t.Error("expected enabled=true")
	}
}

func TestCronEntryYAML_ToCronEntry_DefaultEnabled(t *testing.T) {
	ce := &CronEntryYAML{
		ID:       "test",
		CronExpr: "* * * * *",
		JobType:  "a",
	}

	entry := ce.toCronEntry()
	if !entry.Enabled {
		t.Error("nil enabled should default to true")
	}
}

func TestCronEntryYAML_ToCronEntry_Disabled(t *testing.T) {
	disabled := false
	ce := &CronEntryYAML{
		ID:       "test",
		CronExpr: "* * * * *",
		JobType:  "a",
		Enabled:  &disabled,
	}

	entry := ce.toCronEntry()
	if entry.Enabled {
		t.Error("expected enabled=false")
	}
}

// --- NewServerFromConfig tests ---

func TestNewServerFromConfig_FullConfig(t *testing.T) {
	skipWithoutRedis(t)

	enabled := true
	cfg := &Config{
		Redis: RedisYAML{
			Addr: testRedisAddr(),
		},
		App: AppConfig{
			Timezone:         "Asia/Jakarta",
			LogLevel:         "warn",
			ShutdownTimeout:  60,
			GlobalJobTimeout: 1800,
			GracePeriod:      15,
		},
		Pools: []PoolYAML{
			{
				Name:        "email",
				JobTypes:    []string{"email.send"},
				Queues:      []string{"email"},
				Concurrency: 5,
			},
		},
		Scheduler: SchedulerConfig{
			Enabled:      &enabled,
			PollInterval: 3,
			CronEntries: []CronEntryYAML{
				{
					ID:       "test-cron",
					CronExpr: "* * * * *",
					JobType:  "test.job",
				},
			},
		},
	}

	server, err := NewServerFromConfig(cfg)
	if err != nil {
		t.Fatalf("NewServerFromConfig() error = %v", err)
	}
	defer server.rc.Close()

	// Check serverConfig fields
	if server.cfg.defaultTimezone != "Asia/Jakarta" {
		t.Errorf("defaultTimezone = %q, want %q", server.cfg.defaultTimezone, "Asia/Jakarta")
	}
	if !server.cfg.schedulerEnabled {
		t.Error("schedulerEnabled = false, want true")
	}
	if server.cfg.schedulerPollInterval != 3*time.Second {
		t.Errorf("schedulerPollInterval = %v, want 3s", server.cfg.schedulerPollInterval)
	}
	if server.cfg.globalTimeout != 1800*time.Second {
		t.Errorf("globalTimeout = %v, want 1800s", server.cfg.globalTimeout)
	}
	if server.cfg.gracePeriod != 15*time.Second {
		t.Errorf("gracePeriod = %v, want 15s", server.cfg.gracePeriod)
	}
	if server.cfg.shutdownTimeout != 60*time.Second {
		t.Errorf("shutdownTimeout = %v, want 60s", server.cfg.shutdownTimeout)
	}

	// Check pool registered
	if len(server.pools) != 1 {
		t.Errorf("pools count = %d, want 1", len(server.pools))
	}
	if !server.poolNames["email"] {
		t.Error("email pool not registered")
	}
	if server.jobTypePool["email.send"] != "email" {
		t.Errorf("jobTypePool[email.send] = %q", server.jobTypePool["email.send"])
	}

	// Check cron entry registered
	if len(server.cronEntries) != 1 {
		t.Errorf("cronEntries count = %d, want 1", len(server.cronEntries))
	}
	if _, ok := server.cronEntries["test-cron"]; !ok {
		t.Error("test-cron entry not registered")
	}
}

func TestNewServerFromConfig_CodeOverrides(t *testing.T) {
	skipWithoutRedis(t)

	cfg := &Config{
		Redis: RedisYAML{Addr: testRedisAddr()},
		App: AppConfig{
			GlobalJobTimeout: 100,
			GracePeriod:      5,
		},
	}

	// Code options override config
	server, err := NewServerFromConfig(cfg,
		WithGlobalTimeout(999*time.Second),
		WithGracePeriod(77*time.Second),
	)
	if err != nil {
		t.Fatalf("NewServerFromConfig() error = %v", err)
	}
	defer server.rc.Close()

	if server.cfg.globalTimeout != 999*time.Second {
		t.Errorf("globalTimeout = %v, want 999s (code override)", server.cfg.globalTimeout)
	}
	if server.cfg.gracePeriod != 77*time.Second {
		t.Errorf("gracePeriod = %v, want 77s (code override)", server.cfg.gracePeriod)
	}
}

func TestNewServerFromConfig_SchedulerDisabled(t *testing.T) {
	skipWithoutRedis(t)

	disabled := false
	cfg := &Config{
		Redis:     RedisYAML{Addr: testRedisAddr()},
		Scheduler: SchedulerConfig{Enabled: &disabled},
	}

	server, err := NewServerFromConfig(cfg)
	if err != nil {
		t.Fatalf("NewServerFromConfig() error = %v", err)
	}
	defer server.rc.Close()

	if server.cfg.schedulerEnabled {
		t.Error("schedulerEnabled = true, want false")
	}
}

func TestNewServerFromConfig_CatchAllPool(t *testing.T) {
	skipWithoutRedis(t)

	cfg := &Config{
		Redis: RedisYAML{Addr: testRedisAddr()},
		Pools: []PoolYAML{
			{
				Name:        "specific",
				JobTypes:    []string{"email.send"},
				Queues:      []string{"email"},
				Concurrency: 5,
			},
			{
				Name:        "catch-all",
				JobTypes:    []string{"*"},
				Concurrency: 2,
			},
		},
	}

	server, err := NewServerFromConfig(cfg)
	if err != nil {
		t.Fatalf("NewServerFromConfig() error = %v", err)
	}
	defer server.rc.Close()

	if server.cfg.catchAllPool != "catch-all" {
		t.Errorf("catchAllPool = %q, want %q", server.cfg.catchAllPool, "catch-all")
	}

	// Register handlers
	if err := server.Handle("email.send", dummyHandler); err != nil {
		t.Fatal(err)
	}
	if err := server.Handle("report.generate", dummyHandler); err != nil {
		t.Fatal(err)
	}

	// ensureDefaultPool should assign unassigned handlers to catch-all
	server.ensureDefaultPool()

	// report.generate should be routed to catch-all
	if server.jobTypePool["report.generate"] != "catch-all" {
		t.Errorf("jobTypePool[report.generate] = %q, want %q", server.jobTypePool["report.generate"], "catch-all")
	}

	// No new pool created (existing 2 pools: specific + catch-all)
	if len(server.pools) != 2 {
		t.Errorf("pools count = %d, want 2 (no new default pool)", len(server.pools))
	}
}

func TestNewServerFromConfig_MonitoringFull(t *testing.T) {
	skipWithoutRedis(t)

	cfg := &Config{
		Redis: RedisYAML{
			Addr:     testRedisAddr(),
			Password: "testpass",
			DB:       2,
			Prefix:   "test:",
		},
		Monitoring: MonitoringConfig{
			API: APIConfig{
				Enabled:   true,
				Addr:      "127.0.0.1:0",
				RateLimit: 50,
				APIKeys: []APIKeyYAML{
					{Name: "key1", Key: "gqm_ak_test1234567890abcdef", Role: "admin"},
				},
			},
			Dashboard: DashboardConfig{
				Enabled:    true,
				PathPrefix: "/dash",
				CustomDir:  "/tmp/test-dash",
			},
			Auth: AuthConfig{
				Enabled:    true,
				SessionTTL: 3600,
				Users: []UserYAML{
					{Username: "admin", PasswordHash: "$2a$10$hash", Role: "admin"},
				},
			},
		},
	}

	server, err := NewServerFromConfig(cfg)
	if err != nil {
		t.Fatalf("NewServerFromConfig: %v", err)
	}
	defer server.rc.Close()

	if !server.cfg.dashEnabled {
		t.Error("dashEnabled should be true")
	}
	if server.cfg.dashPathPrefix != "/dash" {
		t.Errorf("dashPathPrefix = %q", server.cfg.dashPathPrefix)
	}
	if server.cfg.dashCustomDir != "/tmp/test-dash" {
		t.Errorf("dashCustomDir = %q", server.cfg.dashCustomDir)
	}
	if !server.cfg.authEnabled {
		t.Error("authEnabled should be true")
	}
	if len(server.cfg.authUsers) != 1 {
		t.Errorf("authUsers len = %d", len(server.cfg.authUsers))
	}
	if len(server.cfg.apiKeys) != 1 {
		t.Errorf("apiKeys len = %d", len(server.cfg.apiKeys))
	}
	if server.cfg.apiRateLimit != 50 {
		t.Errorf("apiRateLimit = %d", server.cfg.apiRateLimit)
	}
	if server.cfg.authSessionTTL != 3600 {
		t.Errorf("authSessionTTL = %d", server.cfg.authSessionTTL)
	}
	if server.mon == nil {
		t.Error("monitor should be initialized")
	}
}

func TestNewServerFromConfig_PoolRegistrationError(t *testing.T) {
	skipWithoutRedis(t)

	cfg := &Config{
		Redis: RedisYAML{Addr: testRedisAddr()},
		Pools: []PoolYAML{
			{Name: "dup", JobTypes: []string{"a"}, Concurrency: 1},
			{Name: "dup", JobTypes: []string{"b"}, Concurrency: 1},
		},
	}

	_, err := NewServerFromConfig(cfg)
	if err == nil {
		t.Error("expected error for duplicate pool names")
	}
}

// --- ServerOption tests ---

func TestWithDefaultTimezone(t *testing.T) {
	skipWithoutRedis(t)

	s, err := NewServer(
		WithServerRedis(testRedisAddr()),
		WithDefaultTimezone("Europe/London"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer s.rc.Close()

	if s.cfg.defaultTimezone != "Europe/London" {
		t.Errorf("defaultTimezone = %q", s.cfg.defaultTimezone)
	}
}

func TestWithSchedulerEnabled(t *testing.T) {
	skipWithoutRedis(t)

	s, err := NewServer(
		WithServerRedis(testRedisAddr()),
		WithSchedulerEnabled(false),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer s.rc.Close()

	if s.cfg.schedulerEnabled {
		t.Error("schedulerEnabled = true, want false")
	}
}

func TestWithSchedulerPollInterval(t *testing.T) {
	skipWithoutRedis(t)

	s, err := NewServer(
		WithServerRedis(testRedisAddr()),
		WithSchedulerPollInterval(5*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer s.rc.Close()

	if s.cfg.schedulerPollInterval != 5*time.Second {
		t.Errorf("schedulerPollInterval = %v, want 5s", s.cfg.schedulerPollInterval)
	}
}

func TestWithSchedulerPollInterval_IgnoresZero(t *testing.T) {
	skipWithoutRedis(t)

	s, err := NewServer(
		WithServerRedis(testRedisAddr()),
		WithSchedulerPollInterval(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer s.rc.Close()

	if s.cfg.schedulerPollInterval != defaultSchedulerPollInterval {
		t.Errorf("schedulerPollInterval = %v, want default %v", s.cfg.schedulerPollInterval, defaultSchedulerPollInterval)
	}
}

func TestWithLogLevel(t *testing.T) {
	skipWithoutRedis(t)

	s, err := NewServer(
		WithServerRedis(testRedisAddr()),
		WithLogLevel("debug"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer s.rc.Close()

	if s.cfg.logLevel != "debug" {
		t.Errorf("logLevel = %q, want %q", s.cfg.logLevel, "debug")
	}
	// Logger should be non-nil (auto-created from logLevel)
	if s.logger == nil {
		t.Error("logger is nil after WithLogLevel")
	}
}

func TestWithLogLevel_LoggerTakesPrecedence(t *testing.T) {
	skipWithoutRedis(t)

	// When WithLogger is also provided, it should take precedence
	s, err := NewServer(
		WithServerRedis(testRedisAddr()),
		WithLogLevel("debug"),
		WithLogger(nil), // Explicitly set nil, then NewServer falls back to logLevel
	)
	if err != nil {
		t.Fatal(err)
	}
	defer s.rc.Close()

	// Logger should still be created from logLevel since WithLogger(nil) sets nil
	if s.logger == nil {
		t.Error("logger is nil")
	}
}

// --- Scheduler timezone fallback ---

func TestResolveLocation_DefaultTimezone(t *testing.T) {
	skipWithoutRedis(t)

	s, err := NewServer(
		WithServerRedis(testRedisAddr()),
		WithDefaultTimezone("Asia/Jakarta"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer s.rc.Close()

	se := newSchedulerEngine(s)

	// Entry without timezone → should fall back to defaultTimezone
	entry := &CronEntry{ID: "test"}
	loc := se.resolveLocation(entry)

	jkt, _ := time.LoadLocation("Asia/Jakarta")
	if loc.String() != jkt.String() {
		t.Errorf("resolveLocation = %q, want %q", loc, jkt)
	}
}

func TestResolveLocation_EntryTimezoneWins(t *testing.T) {
	skipWithoutRedis(t)

	s, err := NewServer(
		WithServerRedis(testRedisAddr()),
		WithDefaultTimezone("Asia/Jakarta"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer s.rc.Close()

	se := newSchedulerEngine(s)

	// Entry with its own timezone → should use entry timezone
	entry := &CronEntry{ID: "test", Timezone: "Europe/London"}
	loc := se.resolveLocation(entry)

	london, _ := time.LoadLocation("Europe/London")
	if loc.String() != london.String() {
		t.Errorf("resolveLocation = %q, want %q", loc, london)
	}
}

func TestResolveLocation_FallbackUTC(t *testing.T) {
	skipWithoutRedis(t)

	s, err := NewServer(WithServerRedis(testRedisAddr()))
	if err != nil {
		t.Fatal(err)
	}
	defer s.rc.Close()

	se := newSchedulerEngine(s)

	// No entry timezone, no defaultTimezone → UTC
	entry := &CronEntry{ID: "test"}
	loc := se.resolveLocation(entry)

	if loc != time.UTC {
		t.Errorf("resolveLocation = %q, want UTC", loc)
	}
}

// --- Monitoring config validation tests ---

func TestValidate_MonitoringAuthEnabledNoUsers(t *testing.T) {
	yaml := `
monitoring:
  auth:
    enabled: true
  api:
    enabled: true
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "no users configured") {
		t.Errorf("expected no users error, got %v", err)
	}
}

func TestValidate_MonitoringAuthUserEmptyUsername(t *testing.T) {
	yaml := `
monitoring:
  auth:
    enabled: true
    users:
      - username: ""
        password_hash: "$2a$10$hash"
  api:
    enabled: true
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "username must not be empty") {
		t.Errorf("expected username error, got %v", err)
	}
}

func TestValidate_MonitoringAuthUserEmptyHash(t *testing.T) {
	yaml := `
monitoring:
  auth:
    enabled: true
    users:
      - username: admin
        password_hash: ""
  api:
    enabled: true
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "password_hash must not be empty") {
		t.Errorf("expected password_hash error, got %v", err)
	}
}

func TestValidate_MonitoringAPIKeyEmptyName(t *testing.T) {
	yaml := `
monitoring:
  api:
    enabled: true
    api_keys:
      - name: ""
        key: "gqm_ak_test_0123456789abcdefghijkl"
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "name must not be empty") {
		t.Errorf("expected API key name error, got %v", err)
	}
}

func TestValidate_MonitoringAPIKeyEmptyKey(t *testing.T) {
	yaml := `
monitoring:
  api:
    enabled: true
    api_keys:
      - name: "test-key"
        key: ""
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "key must not be empty") {
		t.Errorf("expected API key error, got %v", err)
	}
}

func TestValidate_MonitoringUserInvalidRole(t *testing.T) {
	yaml := `
monitoring:
  auth:
    enabled: true
    users:
      - username: admin
        password_hash: "$2a$10$hashhashhashhashhashhashhashhashhashhashhashhashha"
        role: "superadmin"
  api:
    enabled: true
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "role must be") {
		t.Errorf("expected role validation error, got %v", err)
	}
}

func TestValidate_MonitoringAPIKeyInvalidRole(t *testing.T) {
	yaml := `
monitoring:
  api:
    enabled: true
    api_keys:
      - name: "test-key"
        key: "gqm_ak_test_0123456789abcdefghijkl"
        role: "root"
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "role must be") {
		t.Errorf("expected role validation error, got %v", err)
	}
}

func TestValidate_MonitoringValidRoles(t *testing.T) {
	yaml := `
monitoring:
  auth:
    enabled: true
    users:
      - username: admin
        password_hash: "$2a$10$hashhashhashhashhashhashhashhashhashhashhashhashha"
        role: "admin"
      - username: viewer
        password_hash: "$2a$10$hashhashhashhashhashhashhashhashhashhashhashhashha"
        role: "viewer"
      - username: default
        password_hash: "$2a$10$hashhashhashhashhashhashhashhashhashhashhashhashha"
  api:
    enabled: true
    api_keys:
      - name: admin-key
        key: gqm_ak_admin_0123456789abcdefghij
        role: admin
      - name: viewer-key
        key: gqm_ak_viewer_0123456789abcdefghi
        role: viewer
      - name: default-key
        key: gqm_ak_default_0123456789abcdefgh
`
	_, err := LoadConfig([]byte(yaml))
	if err != nil {
		t.Errorf("valid roles should not error: %v", err)
	}
}

func TestValidate_MonitoringDashboardImpliesAPI(t *testing.T) {
	yaml := `
monitoring:
  dashboard:
    enabled: true
`
	cfg, err := LoadConfig([]byte(yaml))
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if !cfg.Monitoring.API.Enabled {
		t.Error("dashboard.enabled should auto-enable API")
	}
}

func TestValidate_MonitoringSessionTTLDefault(t *testing.T) {
	yaml := `
monitoring:
  api:
    enabled: true
`
	cfg, err := LoadConfig([]byte(yaml))
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.Monitoring.Auth.SessionTTL != 86400 {
		t.Errorf("session_ttl = %d, want 86400", cfg.Monitoring.Auth.SessionTTL)
	}
}

func TestValidate_MonitoringAddrDefault(t *testing.T) {
	yaml := `
monitoring:
  api:
    enabled: true
`
	cfg, err := LoadConfig([]byte(yaml))
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.Monitoring.API.Addr != ":8080" {
		t.Errorf("api.addr = %q, want %q", cfg.Monitoring.API.Addr, ":8080")
	}
}

func TestValidate_MonitoringDashboardPathPrefixDefault(t *testing.T) {
	yaml := `
monitoring:
  api:
    enabled: true
  dashboard:
    enabled: true
`
	cfg, err := LoadConfig([]byte(yaml))
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if cfg.Monitoring.Dashboard.PathPrefix != "/dashboard" {
		t.Errorf("dashboard.path_prefix = %q, want %q", cfg.Monitoring.Dashboard.PathPrefix, "/dashboard")
	}
}

func TestValidate_MonitoringSessionTTLNegative(t *testing.T) {
	yaml := `
monitoring:
  auth:
    session_ttl: -1
  api:
    enabled: true
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "session_ttl") {
		t.Errorf("expected session_ttl error, got %v", err)
	}
}

func TestValidate_MonitoringPasswordHashNotBcrypt(t *testing.T) {
	yaml := `
monitoring:
  auth:
    enabled: true
    users:
      - username: admin
        password_hash: "plaintext_password"
  api:
    enabled: true
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "bcrypt hash") {
		t.Errorf("expected bcrypt hash error, got %v", err)
	}
}

func TestValidate_MonitoringAPIKeyTooShort(t *testing.T) {
	yaml := `
monitoring:
  api:
    enabled: true
    api_keys:
      - name: "short-key"
        key: "too_short"
`
	_, err := LoadConfig([]byte(yaml))
	if err == nil || !strings.Contains(err.Error(), "at least") {
		t.Errorf("expected min length error, got %v", err)
	}
}

func TestLoadConfig_ValidFullWithMonitoring(t *testing.T) {
	yaml := `
monitoring:
  auth:
    enabled: true
    session_ttl: 3600
    users:
      - username: admin
        password_hash: "$2a$10$hashhashhashhashhashhashhashhashhashhashhashhashha"
  api:
    enabled: true
    addr: ":9090"
    api_keys:
      - name: tui-key
        key: gqm_ak_testkey123_0123456789abcdef
  dashboard:
    enabled: true
    path_prefix: "/ui"
    custom_dir: "/opt/gqm/dashboard"
`
	cfg, err := LoadConfig([]byte(yaml))
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	if !cfg.Monitoring.Auth.Enabled {
		t.Error("auth.enabled = false")
	}
	if cfg.Monitoring.Auth.SessionTTL != 3600 {
		t.Errorf("auth.session_ttl = %d, want 3600", cfg.Monitoring.Auth.SessionTTL)
	}
	if len(cfg.Monitoring.Auth.Users) != 1 {
		t.Fatalf("auth.users len = %d", len(cfg.Monitoring.Auth.Users))
	}
	if cfg.Monitoring.Auth.Users[0].Username != "admin" {
		t.Errorf("auth.users[0].username = %q", cfg.Monitoring.Auth.Users[0].Username)
	}

	if !cfg.Monitoring.API.Enabled {
		t.Error("api.enabled = false")
	}
	if cfg.Monitoring.API.Addr != ":9090" {
		t.Errorf("api.addr = %q", cfg.Monitoring.API.Addr)
	}
	if len(cfg.Monitoring.API.APIKeys) != 1 {
		t.Fatalf("api.api_keys len = %d", len(cfg.Monitoring.API.APIKeys))
	}
	if cfg.Monitoring.API.APIKeys[0].Key != "gqm_ak_testkey123_0123456789abcdef" {
		t.Errorf("api.api_keys[0].key = %q", cfg.Monitoring.API.APIKeys[0].Key)
	}

	if !cfg.Monitoring.Dashboard.Enabled {
		t.Error("dashboard.enabled = false")
	}
	if cfg.Monitoring.Dashboard.PathPrefix != "/ui" {
		t.Errorf("dashboard.path_prefix = %q", cfg.Monitoring.Dashboard.PathPrefix)
	}
	if cfg.Monitoring.Dashboard.CustomDir != "/opt/gqm/dashboard" {
		t.Errorf("dashboard.custom_dir = %q", cfg.Monitoring.Dashboard.CustomDir)
	}
}

// --- NewServerFromConfig with monitoring ---

func TestNewServerFromConfig_WithMonitoring(t *testing.T) {
	skipWithoutRedis(t)

	cfg := &Config{
		Redis: RedisYAML{Addr: testRedisAddr()},
		Monitoring: MonitoringConfig{
			API: APIConfig{
				Enabled: true,
				Addr:    ":0", // port 0 = random
			},
		},
	}

	server, err := NewServerFromConfig(cfg)
	if err != nil {
		t.Fatalf("NewServerFromConfig() error = %v", err)
	}
	defer server.rc.Close()

	if !server.cfg.apiEnabled {
		t.Error("apiEnabled = false, want true")
	}
	if server.mon == nil {
		t.Error("monitor should be initialized when API is enabled")
	}
}
