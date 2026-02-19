# GQM — Go Queue Manager

Redis-based task queue library for Go. Built from scratch with minimal dependencies, progressive disclosure API, and production-grade features including worker isolation, DAG dependencies, cron scheduling, and an embedded monitoring dashboard.

## Features

- **Worker pool isolation** — Dedicated goroutine pools per job type with independent concurrency, timeout, and retry policies
- **DAG job dependencies** — Linear chains or full DAG with cycle detection, cascade cancellation, and per-dependency failure tolerance
- **Cron scheduler** — 6-field cron expressions (incl. seconds), overlap policies (skip/allow/replace), timezone support, distributed locking
- **Delayed jobs** — Schedule jobs for future execution with `EnqueueAt()` / `EnqueueIn()`
- **Retry & dead letter queue** — Configurable retry with fixed/exponential/custom backoff, automatic DLQ after max retries
- **Unique jobs** — Idempotent enqueue via `Unique()` option (backed by atomic `HSETNX`)
- **Dequeue strategies** — Strict priority, round-robin, or weighted (default) across multi-queue pools
- **Timeout hierarchy** — Job-level → pool-level → global default (always enforced, never disabled)
- **Panic recovery** — Handler panics are caught per-goroutine; worker pools remain operational
- **Graceful shutdown** — In-flight jobs complete before exit, with configurable grace period
- **YAML config** — Full config-file-driven deployment with 20+ structural validation rules
- **Progressive disclosure** — Zero-config to start, full control when needed (4 layers)
- **HTTP monitoring API** — 32 REST endpoints for queue stats, job management, worker status, cron control
- **Web dashboard** — Embedded vanilla HTML/CSS/JS dashboard with auth, RBAC (admin/viewer), CSRF protection
- **CLI tool** — Config management, password hashing, API key generation, dashboard export
- **TUI monitor** — Terminal UI with live queue/worker/cron monitoring (separate Go module)
- **Atomic operations** — 12 Lua scripts for race-free Redis state transitions
- **Minimal dependencies** — Only 3 production deps: go-redis, yaml.v3, x/crypto

## Requirements

- Go 1.22+
- Redis 6.2+ (for `BLMOVE`)

## Installation

```bash
# Core library
go get github.com/benedict-erwin/gqm

# TUI (optional, separate module)
go get github.com/benedict-erwin/gqm/tui

# CLI binary
go install github.com/benedict-erwin/gqm/cmd/gqm@latest
```

## Quick Start

### Layer 1 — Zero Config

```go
// Producer: enqueue jobs
client, _ := gqm.NewClient(gqm.WithRedisAddr("localhost:6379"))
defer client.Close()

client.Enqueue("email.send", gqm.Payload{
    "to":      "user@example.com",
    "subject": "Welcome",
})

// Consumer: process jobs (shared default pool)
server, _ := gqm.NewServer(gqm.WithServerRedis("localhost:6379"))
server.Handle("email.send", func(ctx context.Context, job *gqm.Job) error {
    var p EmailPayload
    job.Decode(&p)
    return sendEmail(ctx, p.To, p.Subject)
})
server.Start(context.Background())
```

### Layer 2 — Per-Handler Concurrency

```go
server, _ := gqm.NewServer(gqm.WithServerRedis("localhost:6379"))

// Each handler gets a dedicated pool with N workers
server.Handle("email.send", emailHandler, gqm.Workers(5))
server.Handle("payment.process", paymentHandler, gqm.Workers(3))

server.Start(context.Background())
```

### Layer 3 — Explicit Pools

```go
server, _ := gqm.NewServer(
    gqm.WithServerRedis("localhost:6379"),
    gqm.WithAPI(true, ":8080"),
    gqm.WithDashboard(true),
)

server.Pool(gqm.PoolConfig{
    Name:        "email-pool",
    JobTypes:    []string{"email.send", "email.bulk"},
    Queues:      []string{"critical", "email"},  // priority order
    Concurrency: 10,
    JobTimeout:  30 * time.Second,
    DequeueStrategy: gqm.StrategyWeighted,
    RetryPolicy: &gqm.RetryPolicy{
        MaxRetry:    5,
        Backoff:     gqm.BackoffExponential,
        BackoffBase: 10 * time.Second,
        BackoffMax:  10 * time.Minute,
    },
})

server.Handle("email.send", sendHandler)
server.Handle("email.bulk", bulkHandler)
server.Start(context.Background())
```

### Layer 4 — Config File

Define everything in YAML — pools, queues, cron, auth, dashboard. See [YAML Configuration](#yaml-configuration) below.

```go
cfg, _ := gqm.LoadConfigFile("gqm.yaml")
server, _ := gqm.NewServerFromConfig(cfg)
server.Handle("email.send", emailHandler)
server.Start(context.Background())
```

## YAML Configuration

Generate a template with `gqm init`, then customize:

```yaml
# gqm.yaml
redis:
  addr: "localhost:6379"
  password: ""
  db: 0
  prefix: "gqm"

app:
  timezone: "Asia/Jakarta"
  log_level: "info"               # debug, info, warn, error
  shutdown_timeout: 30            # seconds
  global_job_timeout: 1800        # seconds (30 min default, cannot be disabled)
  grace_period: 10                # seconds

queues:
  - name: "critical"
    priority: 10
  - name: "default"
    priority: 1
  - name: "low"
    priority: 0

pools:
  - name: "fast"
    job_types: ["email.send", "notification.push"]
    queues: ["critical", "default"]
    concurrency: 10
    job_timeout: 60
    dequeue_strategy: "weighted"  # strict, round_robin, weighted
    retry:
      max_retry: 5
      backoff: "exponential"      # fixed, exponential, custom
      backoff_base: 10            # seconds
      backoff_max: 3600           # seconds
  - name: "background"
    job_types: ["*"]              # catch-all for unassigned job types
    queues: ["default", "low"]
    concurrency: 3

scheduler:
  enabled: true
  poll_interval: 5                # seconds — how often to check for due jobs
                                  # lower = faster promotion, higher Redis load
  cron_entries:
    - id: "cleanup-daily"
      name: "Daily cleanup"
      cron_expr: "0 0 2 * * *"   # 6-field: sec min hour dom month dow
      timezone: "UTC"
      job_type: "cleanup"
      queue: "default"
      overlap_policy: "skip"      # skip, allow, replace

monitoring:
  auth:
    enabled: true
    session_ttl: 86400
    users:
      - username: "admin"
        password_hash: ""         # gqm set-password admin
        role: "admin"             # admin or viewer
  api:
    enabled: true
    addr: ":8080"
    api_keys:
      - name: "grafana"
        key: ""                   # gqm add-api-key grafana
        role: "viewer"
  dashboard:
    enabled: true
    path_prefix: "/dashboard"
    # custom_dir: "./my-dashboard"  # override embedded dashboard
```

Code options always override config values:

```go
cfg, _ := gqm.LoadConfigFile("gqm.yaml")
server, _ := gqm.NewServerFromConfig(cfg,
    gqm.WithGlobalTimeout(10 * time.Minute), // overrides app.global_job_timeout
    gqm.WithSchedulerEnabled(false),         // worker-only instance
)
```

## Enqueue Options

```go
client.Enqueue("report.generate", payload,
    gqm.Queue("reports"),                      // target queue (default: "default")
    gqm.MaxRetry(5),                           // max retry attempts
    gqm.Timeout(2 * time.Minute),              // job-level timeout
    gqm.RetryIntervals(10, 30, 60, 300),       // custom backoff (seconds)
    gqm.JobID("report-2026-02"),               // custom job ID (default: UUID v7)
    gqm.Meta(map[string]string{"user": "42"}), // arbitrary metadata
    gqm.EnqueuedBy("api-gateway"),             // audit trail
    gqm.EnqueueAtFront(true),                  // push to front of queue
    gqm.Unique(),                              // idempotent (requires custom JobID)
    gqm.DependsOn(parentID),                   // DAG dependency
    gqm.AllowFailure(true),                    // run even if parent fails
)
```

## Delayed & Scheduled Jobs

```go
// Run at a specific time
client.EnqueueAt("report.generate", payload, time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC))

// Run after a delay
client.EnqueueIn("reminder.send", payload, 24 * time.Hour)
```

Jobs are held in a Redis sorted set (scored by timestamp) and promoted to the ready queue by the scheduler engine.

## Job Dependencies (DAG)

```go
jobA, _ := client.Enqueue("step.one", payloadA)
jobB, _ := client.Enqueue("step.two", payloadB)

// jobC runs only after both A and B complete successfully
jobC, _ := client.Enqueue("step.three", payloadC,
    gqm.DependsOn(jobA.ID, jobB.ID),
)
```

**Failure behavior:**

- **Default** — If a parent job fails (exhausts retries → DLQ), all dependent children are **cascade-canceled** recursively. The entire downstream chain is canceled.
- **`AllowFailure(true)`** — Opt-in per dependency. The child treats a failed parent as "resolved" and runs anyway once all dependencies are satisfied (completed or failed).

```go
//   A (fail)
//   ├── B                    → canceled (default)
//   │   └── D                → canceled (cascade from B)
//   └── C [AllowFailure]     → still runs (tolerates A's failure)
//       └── E                → runs after C completes

jobB, _ := client.Enqueue("step.b", p, gqm.DependsOn(jobA.ID))
jobC, _ := client.Enqueue("step.c", p, gqm.DependsOn(jobA.ID), gqm.AllowFailure(true))
jobD, _ := client.Enqueue("step.d", p, gqm.DependsOn(jobB.ID))
jobE, _ := client.Enqueue("step.e", p, gqm.DependsOn(jobC.ID))
```

Cycle detection (DFS, depth limit 100) runs at enqueue time — circular dependencies are rejected before any job is queued.

## Cron Scheduling

Cron works by automatically enqueuing jobs on a schedule. You define **what** to run (job type) and **when** (cron expression) — the scheduler handles the rest.

**Step 1: Register the handler** — this is the code that runs when the cron fires:

```go
// The handler is a regular job handler — same as any other job.
// The scheduler enqueues a job with this type on each cron tick.
server.Handle("cleanup", func(ctx context.Context, job *gqm.Job) error {
    deleted, err := db.DeleteExpiredSessions(ctx)
    if err != nil {
        return err // will retry based on retry policy
    }
    slog.Info("cleanup complete", "deleted", deleted)
    return nil
}, gqm.Workers(1))
```

**Step 2: Define the schedule** — either in code or YAML config:

```go
// Option A: in code
server.Schedule(gqm.CronEntry{
    ID:            "cleanup-daily",
    Name:          "Daily Session Cleanup",
    CronExpr:      "0 0 2 * * *",  // 6-field: sec min hour dom month dow
    Timezone:      "Asia/Jakarta",
    JobType:       "cleanup",       // must match the handler registered above
    Queue:         "default",
    OverlapPolicy: gqm.OverlapSkip, // skip | allow | replace
})
```

```yaml
# Option B: in gqm.yaml (same effect)
scheduler:
  cron_entries:
    - id: "cleanup-daily"
      name: "Daily Session Cleanup"
      cron_expr: "0 0 2 * * *"
      timezone: "Asia/Jakarta"
      job_type: "cleanup"
      queue: "default"
      overlap_policy: "skip"
```

**How it works:** The scheduler goroutine checks cron entries every `poll_interval` seconds. When an entry is due, it enqueues a new job with the specified `job_type` into the target `queue`. The job is then picked up by a worker pool that handles that job type — exactly like a manually enqueued job. Overlap policy controls what happens if the previous cron job is still running when the next tick fires.

## Monitoring

### Web Dashboard

Embedded vanilla HTML/CSS/JS dashboard — no build step, no npm. Served directly from the Go binary via `embed.FS`.

**Enable programmatically:**

```go
server, _ := gqm.NewServer(
    gqm.WithServerRedis("localhost:6379"),
    gqm.WithAPI(true, ":8080"),
    gqm.WithDashboard(true),
    gqm.WithAuthEnabled(true),
    gqm.WithAuthUsers([]gqm.AuthUser{
        {Username: "admin", PasswordHash: "$2a$10$...", Role: "admin"},
        {Username: "viewer", PasswordHash: "$2a$10$...", Role: "viewer"},
    }),
    gqm.WithAPIKeys([]gqm.AuthAPIKey{
        {Name: "grafana", Key: "gqm_ak_...", Role: "viewer"},
    }),
)
// Dashboard: http://localhost:8080/dashboard/
// Health:    http://localhost:8080/health (no auth)
```

**Or via YAML config:**

```yaml
monitoring:
  api:
    enabled: true
    addr: ":8080"
  dashboard:
    enabled: true
    # path_prefix: "/dashboard"     # default
    # custom_dir: "./my-dashboard"  # override embedded assets
  auth:
    enabled: true
    users:
      - username: admin
        password_hash: ""  # generate with: gqm hash-password
        role: admin
```

**Dashboard pages:**

| Page | Description |
|------|-------------|
| Overview | Job stats with Chart.js graphs, stat cards per status |
| Servers | Live server heartbeats, uptime, active jobs |
| Queues | Queue sizes, pause/resume, empty queue, DLQ retry |
| Workers | Per-pool worker status, active job tracking |
| Failed / DLQ | Failed job browser, retry/delete individual or batch |
| Scheduler | Cron entries, next/last run, trigger/enable/disable |
| DAG | Dependency graph visualization with Cytoscape.js |

**Auth & security:** Session cookies (bcrypt + HttpOnly/Secure/SameSite), API keys with constant-time comparison, RBAC (admin/viewer), CSRF protection, login rate limiting. Health check at `GET /health` requires no auth.

### TUI

Terminal UI for quick monitoring without a browser. Connects to a running GQM server via the HTTP API.

```bash
# Requires a server with monitoring enabled (WithAPI or monitoring.enabled in YAML)
gqm tui --api-url http://localhost:8080 --api-key gqm_ak_xxx

# Or via environment variables
export GQM_API_URL=http://localhost:8080
export GQM_API_KEY=gqm_ak_xxx
gqm tui
```

4 tabs: Queues, Workers, Failed, Cron. Auto-refreshes every second.

**Keyboard shortcuts:**

| Key | Action |
|-----|--------|
| `1-4` | Switch tab directly |
| `Tab` / `Shift+Tab` | Cycle tabs |
| `j/k` or `Up/Down` | Navigate list |
| `h/l` or `Left/Right` | Switch queue (Failed tab) |
| `p` | Pause/resume queue (Queues tab) |
| `r` | Retry failed job (Failed tab) |
| `t` | Trigger cron entry (Cron tab) |
| `e` | Enable/disable cron entry (Cron tab) |
| `F5` | Force refresh |
| `q` / `Ctrl+C` | Quit |

### CLI

```
gqm init                    Generate template gqm.yaml
gqm set-password <user>     Set/update dashboard password
gqm add-api-key <name>      Add API key to config
gqm revoke-api-key <name>   Remove API key from config
gqm hash-password           Generate bcrypt hash
gqm generate-api-key        Generate random API key
gqm dashboard export <dir>  Export embedded dashboard for customization
gqm tui [--api-url <url>] [--api-key <key>]  Launch terminal monitor
gqm version                 Show version
```

## Architecture

```
Producer App                  Redis                     Worker Binary
─────────────                 ─────                     ────────────
gqm.Client                                              gqm.Server
  .Enqueue() ──────────────►  Queues (Lists)  ◄───────  Pool "email" (5 workers)
  .EnqueueAt()                Jobs (Hashes)             Pool "payment" (3 workers)
  .EnqueueIn()                Scheduled (ZSet)          Scheduler (delayed + cron)
                              Cron (Hash)               Heartbeat (1/pool)
                              Sessions (Strings)        HTTP API + Dashboard
```

## Dependencies

| Dependency | Purpose |
|---|---|
| `github.com/redis/go-redis/v9` | Redis client |
| `gopkg.in/yaml.v3` | YAML config parsing |
| `golang.org/x/crypto/bcrypt` | Password hashing (dashboard auth) |

Everything else is stdlib or implemented from scratch (UUID v7, cron parser, HTTP router via Go 1.22+, logging via `log/slog`).

TUI module (`gqm/tui`) additionally depends on `bubbletea` and charmbracelet ecosystem, but is a separate Go module — importing the core library does not pull TUI dependencies.

## License

MIT
