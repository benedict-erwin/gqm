# Changelog

All notable changes to this project will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Security
- **Dashboard `custom_dir` no longer serves arbitrary files** — the dashboard route is unauthenticated by design, which was safe for the embedded assets but not for an operator-supplied directory. Serving is now restricted to an allowlist of asset extensions with dot-prefixed paths rejected, so a directory that also holds `gqm.yaml`, dotenv files, or backups can no longer leak them. Applies to the embedded assets too
- **Dashboard `custom_dir` no longer follows symlinks out of tree** — switched from `os.DirFS`, which is explicitly not a jail, to `os.Root`. A symlink inside the directory was previously an unauthenticated read of anything on the host
- **Unauthenticated API on a routable address is now refused at startup** — with auth disabled every caller is treated as admin, including on the destructive endpoints. That is still allowed on loopback, but binding to a reachable address without auth or API keys now fails config validation instead of emitting a warning that is easy to miss in a log stream
- **CSRF header is required even when auth is disabled** — previously the check was skipped in that mode, which is exactly where nothing else guards the request. A malicious page could otherwise drive a local unauthenticated instance through a victim's browser. API keys remain exempt, since browsers do not attach them automatically
- **Removing a user now revokes their session immediately** — sessions live in Redis and outlive a config change, so a deleted or renamed user could keep working until their token expired, up to 24 hours. Worse, the unknown username resolved to `admin`, so revoking a viewer promoted them. An unrecognised username is now refused and its orphaned session deleted
- **A user or API key with no explicit role now gets `viewer`, not `admin`** — an omitted role is an operator who did not say what they meant, and reading that as full destructive access is the wrong way to resolve the ambiguity
- **Starting against a password-less Redis prints an unmissable warning** — session tokens are stored in Redis, so an unprotected instance lets anyone who can read it become an authenticated admin without touching the login page. Starting is still allowed; the banner goes to stderr rather than the logger, because a warning that `log_level` can silence is not a warning. It prints once per Redis address per process rather than once per server started — a warning repeated 37 times in a test run is one people learn to ignore
- **Bundled compose files publish Redis on loopback only** — the image runs with `bind * -::*` and protected-mode off, so a `6379:6379` mapping put an unauthenticated session store on every interface. README now documents that production requires `requirepass` (or an ACL) and TLS
- **`gqm init` writes the config as `0600`, not `0644`** — that file is where credentials go, and an API key has to be stored in the clear because key matching compares the raw value. `set-password` and `add-api-key` preserve the mode they find, so a world-readable file created at init stayed world-readable through every later credential write. Those commands now also tighten a group- or world-readable config when they write to it, and say so on stderr rather than changing permissions silently
- **The example config no longer ships a working admin password** — `_examples/09-dev-server/config/gqm.yaml` carried a real bcrypt hash of `admin` with the plaintext in a comment above it. Anyone copying the example into production inherited a publicly known admin credential. It is now a placeholder that fails config validation, so the server refuses to start until it is replaced — a warning comment would have been easy to skim past
- **Logout requires the CSRF header** — it was the only authenticated state-changing route resting on `SameSite` alone, while every other one had a second layer. Not exploitable as it stood, since a cross-site POST carries no `SameSite=Lax` cookie, but defence in depth is not defence if one route quietly opts out. The check moved out of `requireAdmin` into a `requireCSRF` middleware, so future state-changing routes can reuse it; `requireAdmin` behaviour is unchanged and API keys stay exempt
- **`X-Forwarded-Proto` is no longer trusted by default** — a client-supplied header decided whether the session cookie was marked `Secure`. Trusting it failed safe on its own (a client claiming HTTPS over plaintext gets a cookie its own browser refuses to return), but the real exposure ran the other way: behind a proxy that terminates TLS and does not set the header, the cookie went out without `Secure` and with only `SameSite=Lax`, so the browser would send a session token over plain HTTP. Two new options, `WithTrustProxy()` / `api.trust_proxy` and `WithCookieSecure()` / `api.cookie_secure`. Direct TLS connections behave exactly as before with neither set
- **Endpoints that parse a body now require a JSON content type** — a cross-site HTML form can only send `text/plain`, urlencoded or multipart, and `json.Decoder` stops after the first JSON value, so a form field named to open a JSON object produced a body the decoder accepted. `/auth/login` was the notable target: it takes no cookie, so `SameSite` never applied, and the `Set-Cookie` on the response is stored regardless — `SameSite` governs sending, not setting. A victim could be silently logged into the attacker's session. Returns 415; a `charset` parameter is still accepted
- **`stats/daily?queue=` is validated** — the only caller-supplied string that reached a Redis key name without a check, while all 20 path-parameter sites were validated. Not command injection (RESP is length-prefixed) and not arbitrary key reads (the suffix is fixed), but the length was unbounded and one request fans out to as many as 180 `GET`s
- **Response headers completed** — added `base-uri`, `form-action`, `frame-ancestors` and `object-src` to the CSP, none of which fall back to `default-src`. `base-uri` is the substantive one: every asset reference in the dashboard is relative, so an injected `<base href>` could repoint all of them at once and turn a contained HTML injection into script hijacking. Also added `Referrer-Policy`, `Permissions-Policy`, `Cross-Origin-Opener-Policy`, and `Cache-Control: no-store` on `/api/` and `/auth/` only, so authenticated JSON cannot outlive a logout in a shared browser's cache
- **`/api/v1/dag/roots` no longer scans the keyspace without a bound** — Redis applies `MATCH` after iterating, so the cost was O(keys in the database) no matter how few matched, and pagination was applied only after the scan finished, making `limit=1` cost the same as `limit=all`. The endpoint is read-only, so a viewer could turn one request into millions of Redis commands against the instance the workers depend on. Round trips, collected roots, and wall-clock are now all capped, and the response carries `meta.truncated` so a short count cannot be read as a complete one

### Added
- **`AcknowledgeUnprotectedRedis()`** — states in code that a password-less Redis is a deliberate choice, and silences the startup banner for the process. Deliberately a function call rather than a config field or environment variable: it belongs in source, where it appears in a diff, survives review and can be grepped, following `tls.Config.InsecureSkipVerify`. A setting outside the code travels between environments unnoticed, which is how an acknowledgement made for local development ends up silencing production
- **CI security workflow** — `vet`, `gofmt`, `go test -race`, and `govulncheck` on every push and pull request, with the stress suite on a weekly schedule. Actions are pinned to commit SHAs rather than tags, and the token is restricted to `contents: read`. The build also fails when Redis-backed tests *skip*: a suite that skips is not a suite that passed, and `go test` reports `ok` when Redis is unreachable. The TUI is a separate module, so `go test ./...` at the root never reached it — CI now runs it explicitly

### Changed
- **Dependencies updated** — `golang.org/x/crypto` 0.48.0 → 0.54.0, `golang.org/x/sys` 0.41.0 → 0.47.0, `go-redis` 9.17.3 → 9.21.0. `govulncheck` reported nothing this code calls, but 15 advisories in required modules; none was reachable, since only `x/crypto/bcrypt` is imported while the advisories sit in the ssh and FIDO packages. Unreachable still means every module-level scanner flags them for downstream users. Now down to one, `GO-2026-5932`, which has no fix and covers `x/crypto/openpgp` — not vendored and not importable here
- **BREAKING: job IDs may no longer contain a colon** — GQM joins Redis key segments with a colon, and a job owns a bare key as well as suffixed ones (`gqm:job:<id>` alongside `gqm:job:<id>:deps`, `:pending_deps`, `:dependents`). A job with the ID `order-42:deps` therefore occupied exactly the key job `order-42` needed for its dependency set; because the two hold different Redis types, the victim's enqueue failed with `WRONGTYPE`. The `:dependents` variant blocked every child of the targeted parent. `Enqueue` now returns `ErrInvalidJobID` for such an ID, and the API rejects it as a path parameter. **Job types and queue names are unaffected** — they own no sub-keys, so the `namespace:action` convention still works. This restores the contract `ErrInvalidJobID` always documented ("only alphanumeric, hyphen, underscore, dot allowed"); the regex was simply more permissive than the promise. Generated UUIDv7 IDs are unaffected

### Added
- **Job retention** — terminal jobs now expire instead of accumulating forever. `result_ttl` (default 7 days) covers completed jobs; `failure_ttl` (default 30 days) covers dead-lettered, canceled, and stopped ones. Configurable via `app.result_ttl`/`app.failure_ttl`, `WithResultTTL()`/`WithFailureTTL()`, or per job with `ResultTTL()`/`FailureTTL()`. `TTLPermanent` retains forever, `0` deletes on completion
- **Terminal sorted set trimming** — `:completed` and `:dead_letter` are trimmed by score inside the existing terminal Lua scripts, so a set entry never outlives the job hash it points to

### Fixed
- **Unbounded Redis growth** — every terminal job previously left a permanent `gqm:job:<id>` hash plus a sorted set entry, with no reclaim path. Measured cost is ~1.5 KB per retained job
- **Orphaned terminal jobs** — `canceled`, `stopped`, and fallback-`failed` jobs belong to no sorted set, so without an expiry they were unreachable garbage that only a manual `SCAN` could find
- **DAG metadata leak on dead-letter** — `propagateFailure` never deleted the failed parent's `:dependents` set, and its `allow_failure` branch never deleted the promoted dependent's `:deps` set

### Documentation
- **Job payload visibility contract** — README now states plainly that roles limit actions, not visibility: any monitoring credential, `viewer` included, can read every job's `payload`, `result`, `meta` and error message, while `admin` gates only destructive operations. That is deliberate, and it makes the contract run the other way — do not put secrets in a payload. Includes the reference pattern (store an ID, resolve it in the handler) and a test pinning the documented behaviour, so the docs cannot drift from the code in either direction
- **Retention memory tuning** — README documents the measured cost of a retained job (~1.3 KB) and how `hash-max-listpack-value` roughly halves the hash cost with no code change. A single field value over the 64 B default converts the whole job hash from `listpack` to `hashtable`, and a job's `payload` almost always exceeds it. Includes the measured latency trade-off (`listpack` is slightly faster for `HGETALL`, −7% throughput for a worst-case single-field `HGET` at identical p50) and the caveat that the setting is per-instance. GQM never sets it for you

### Notes
- Retention only ever applies to terminal jobs. Non-terminal jobs carry no expiry, which also keeps them outside the candidate set of Redis `volatile-*` eviction policies
- `admin_retry` issues `PERSIST` on the job hash: `HSET` does not clear a TTL, so a retried dead-letter job would otherwise inherit its expiry and could vanish while queued or running
- Measuring retained cost requires `MEMORY USAGE <key> SAMPLES 0`. The default samples 5 fields and extrapolates, which misreports a job hash by up to 49% because `payload` dwarfs the other fields

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
