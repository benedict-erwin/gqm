# Changelog

All notable changes to this project will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.2] — 2026-02-24

### Fixed
- **Job timeout capped by shutdownTimeout** — `workerLoop` outer context used `shutdownTimeout` (30s) as hard cap on all job execution, ignoring `globalTimeout`. Now uses `globalTimeout + gracePeriod + shutdownTimeout` as safety net
- **Retry/DLQ fails after job timeout** — post-timeout cleanup operations (retry, dead-letter) used expired context, causing all Redis operations to fail. Now uses a fresh background context
- **Zombie jobs on Lua script failure** — `retryJob`, `deadLetterJob`, `completeJob` now fall back to ZREM + status update when Lua script fails, preventing jobs from being permanently stuck in processing set
- **Heartbeat stale data during grace period** — active job tracking is cleared immediately on timeout, before grace period starts

### Added
- Integration tests for timeout context cascade scenarios (4 new tests)

### Changed
- **Dashboard queue table** — add grouped column headers (Current vs Cumulative) with visual separator and tooltips

## [0.1.1] — 2026-02-21

Security hardening and bug fix release from full codebase audit.

### Security
- **XSS fix** — replace all inline `onclick`/`onchange` handlers with `data-action` attributes + event delegation; fix `escapeHTML` to escape all 5 special characters (`&`, `<`, `>`, `"`, `'`)
- **CSP hardened** — remove `'unsafe-inline'` from `script-src` directive
- **HSTS header** — add `Strict-Transport-Security` to security headers middleware

### Fixed
- **Clear DLQ response** — return JSON `{ data: { cleared: N } }` instead of empty 204 body; fix API client to handle 204 No Content gracefully
- **JSON unmarshal logging** — add warning logs for silent JSON parse failures in DAG traversal (`depends_on`), cron entry listing, and job response fields (`payload`, `meta`, `result`, `depends_on`, `retry_intervals`)
- **Exponential backoff overflow** — guard against `int64` overflow in `poolRetryDelay`; hard cap at 24h when no `BackoffMax` is set
- **Abandoned handler drain timeout** — drain goroutine now has 1h hard timeout to prevent permanent goroutine leak
- **Job payload size limit** — reject payloads exceeding 1 MB (`ErrJobDataTooLarge`) in `ToMap()`; protects all enqueue paths
- **Cron empty set guard** — `cronNextInSet` now panics on empty value set instead of index-out-of-bounds
- **CLI input validation** — trim whitespace on username, API key name in `set-password`, `add-api-key`, `revoke-api-key`
- **Zero concurrency in config** — YAML `concurrency: 0` now resolves to `runtime.NumCPU()` at config validation time

## [0.1.0] — 2026-02-21

Initial feature-complete release. All planned phases (1–7) implemented.

### Added

**Core Engine**
- Redis-based job queue with atomic Lua scripts (12 embedded scripts)
- Worker pools with configurable concurrency, long-lived workers + spawned handler goroutines
- Timeout hierarchy: job-level → pool-level → global default (30 min)
- Panic recovery per handler goroutine — pool stays operational
- Graceful shutdown with in-flight job completion and configurable timeout
- Functional options API with progressive disclosure (simple → advanced)

**Multi-Pool & Dequeue**
- Explicit pools (`Server.Pool()`) with named pools, multi-queue support
- Implicit pools via `Workers(N)` — auto-creates dedicated pool per job type
- 3 dequeue strategies: strict priority, round-robin, weighted (default)
- Pool-level retry policy with fixed, exponential, and custom interval backoff

**Scheduler & Cron**
- Delayed jobs via `EnqueueAt()` / `EnqueueIn()` using Redis sorted set
- Custom 6-field cron parser (no external dependency)
- Distributed cron locks with timezone support and overlap policies (skip/allow/replace)

**DAG Dependencies**
- Job dependency chains via `DependsOn()` with cycle detection (DFS, depth limit 100)
- Atomic resolution in Lua, cascade cancellation with `AllowFailure()` opt-out
- Idempotent enqueue via `Unique()` option

**Config File**
- YAML config with 20+ validation rules, `NewServerFromConfig()` with code-override
- Catch-all pool (`job_types: ["*"]`), auto-logger from `log_level`

**HTTP API & Auth**
- 32 REST endpoints (read, admin, auth) via `net/http` Go 1.22+ routing
- Dual auth: session cookie (bcrypt) + API key header
- CSRF protection, role-based access (admin/viewer), per-IP rate limiting
- Health endpoint, daily + cumulative stats counters, audit logging

**Web Dashboard**
- Embedded vanilla HTML/CSS/JS dashboard — zero build step
- Pages: Overview, Servers, Queues, Jobs, Workers, Failed/DLQ, Scheduler, DAG
- Customizable via `WithDashboardDir()` or `gqm dashboard export`

**CLI (`gqm`)**
- `init`, `set-password`, `add-api-key`, `revoke-api-key`, `hash-password`, `generate-api-key`
- `dashboard export`, `tui`, `version`

**TUI Monitor** (separate `gqm/tui` module)
- 4 tabs: Queues, Workers, Failed/DLQ, Cron
- Live updates, keyboard actions (pause/resume, retry, trigger cron)

### Security
- Input validation on all HTTP path params
- Request body size limits, brute force protection (Redis rate limit)
- API key constant-time comparison via SHA-256 + `crypto/subtle`
- Session token validation, username enumeration timing protection
- Sanitized error messages, job response field allowlist
- Bounded bulk operations (max 1000), Content-Security-Policy headers

### Dependencies

| Dependency | Purpose |
|---|---|
| `github.com/redis/go-redis/v9` | Redis client |
| `gopkg.in/yaml.v3` | YAML config parsing |
| `golang.org/x/crypto` | bcrypt password hashing |
| `golang.org/x/term` | Interactive password prompt (CLI only) |

TUI module additionally uses `bubbletea` and `lipgloss` (Charm ecosystem).

[0.1.1]: https://github.com/benedict-erwin/gqm/releases/tag/v0.1.1
[0.1.0]: https://github.com/benedict-erwin/gqm/releases/tag/v0.1.0
