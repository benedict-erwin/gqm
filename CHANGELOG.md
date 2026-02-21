# Changelog

All notable changes to this project will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[0.1.0]: https://github.com/benedict-erwin/gqm/releases/tag/v0.1.0
