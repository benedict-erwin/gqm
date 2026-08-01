# Changelog

All notable changes to this project will be documented in this file.

Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Upgrading

**The default `dequeue_strategy` is now `weighted`, was `strict`.** This only
affects pools reading more than one queue, and only when more than one has work
— but there it is a real change. Under `strict` a queue is not read at all until
the one above it runs dry; measured with two full queues and one worker, the
second queue's first job came 301st. Under `weighted` it came 1st, taking about
a third of the throughput.

If you were relying on strict draining order, set it explicitly:

```yaml
pools:
  - name: "payments"
    queues: ["critical", "low"]
    dequeue_strategy: "strict"
```

**`queues[].priority` has been removed.** It was parsed and never read by
anything, so setting it never had an effect. Priority comes from the order of
`pools[].queues`, combined with `dequeue_strategy`. Delete the field — with
unknown keys now rejected, a config still carrying it will refuse to start.

**A pool may only listen on queues declared under `queues:`.** Naming an
undeclared queue used to be accepted, which meant a typo produced a pool that
started, held its workers, and polled a queue nothing ever wrote to — no error,
no log, a healthy-looking pool doing nothing. `default` is exempt and never
needs declaring.

**Unknown keys in `gqm.yaml` are now an error.** They used to be dropped in
silence, which made a typo close to undetectable: the field kept its zero value,
the zero value resolved to a default, and the server came up running numbers
nobody chose. `concurency: 10` gave the pool `runtime.NumCPU()` workers with
nothing in the log to say so. A mistyped `cookie_secure` left the session cookie
without `Secure` behind a TLS proxy.

If your config carries a key GQM does not recognise — a typo, or a leftover from
an older version — the server will now refuse to start and name the field and
line. Load it once and fix what it reports.

One known case: `monitoring.enabled` and `monitoring.addr` were never real
settings, and the bundled `09-dev-server` example used them. The real fields are
`monitoring.api.enabled` and `monitoring.api.addr`; the example has been
corrected.

### Added
- **`WithRedisPoolSize()` and `redis.pool_size`** — the Redis connection pool size was not reachable through GQM's own API: the client was built with `Addr`, `Password`, `DB` and `TLSConfig` and nothing else, so changing it meant constructing a `*redis.Client` by hand and injecting it. Unset still leaves go-redis to its default of `10 x GOMAXPROCS`, which is derived from CPU count and so ignores how many pools and workers are actually configured. Raising it is not like adding workers: a connection is a socket, not a process, so it costs nothing while idle — the real ceilings are the server's `maxclients`, the open-file limit, and buffer memory

### Documentation
- **Connection pool sizing** — README now says plainly not to size the pool to the worker count, with the measurement behind it: 100 workers and instant handlers push ~33,100 jobs/sec on a 4-core container whether the pool is the default 40 or an explicit 100, with no `ErrPoolTimeout` either way. Workers hold a connection for one command, not while waiting, and Redis executes commands one at a time — so past the point where Redis is kept busy, more connections buy nothing. The cases that do call for raising it are slow commands, not many workers
- **DAG chain latency under a burst** — README explains that a resolved dependent is pushed to the back of its queue like any other job, so enqueuing thousands of chains at once puts every first stage ahead of every second stage. The wait that follows reads as slow DAG resolution and is really queue depth: measured across the same code, the gap between stage one and stage two went from 22ms at 300 chains to eight seconds at 4,000. Includes the two ways to avoid it and why the default is not changed
- **Performance table re-measured and reproducible** — every figure is now the median of five runs from one command printed beside it, so the table can be reproduced from a clone. The previous numbers came from a comparison harness that is not in the repository, which is why the batch rows read about 20% higher than anything the shipped benchmark produces. The noise floor is stated too: end-to-end varies 13% across runs and 7% between whole invocations, so smaller differences are not signal. DAG chains are deliberately absent — the obvious measurement is not a stable quantity, since the benchmark enqueues every chain before waiting for any and the per-chain figure falls nearly 3x with a larger iteration count and no code change
- **Examples no longer teach the removed priority field** — `_examples/06-config-driven` annotated its queue list with `priority 10` and `priority 1`, the exact values of the field that has been removed. Both multi-queue examples also presented `strict` without saying what it costs; each now names the starvation and points at the default. The examples are checked in CI from now on: every program is built and every shipped config is loaded, because `_examples` starts with an underscore and Go tooling skips it, so nothing here reached them before

### Changed
- **Default `dequeue_strategy` is `weighted`** — the code defaulted to `strict` while the recorded design decision, the architecture guide and the README all said `weighted`; the code was the outlier. Strict lets a busy queue starve the one below it outright, and someone who has not set a strategy has not asked for that — they have not thought about it. Explicit `strict` is untouched

### Removed
- **`queues[].priority`** — parsed, never read, never affected dispatch. Priority is the order of `pools[].queues`, which is also more expressive than a single number per queue: two pools can rank the same queues differently. A field that looks like it works is worse than no field

### Testing
- **Every job is accounted for under escalating load** — a new conservation check records what each enqueued job was promised and afterwards reads back the recorded status of every one of them, at load levels that double up to 51,200 jobs in a single level. Counting completions cannot find a job that vanished, because the vanished job was never counted. The mix deliberately includes DAG chains and delayed jobs: `DependsOn` and every scheduler call appeared zero times in the existing stress suite, which is why the orphaned-dependent bug fixed in this release went unseen there. It also asserts no duplicate execution, no dependency set surviving the drain, and goroutines back at the level measured before any server existed. Both invariants were proven to fail before being trusted

### Fixed
- **A pool listening on an undeclared queue is now a config error** — the `queues:` block and `pools[].queues` were never checked against each other, so a mistyped queue name gave a pool that ran, held workers, and polled an empty queue forever without a word. `default` stays exempt, since it is the fallback for jobs with no queue and for pools that declare none
- **Mistyped config keys are no longer ignored** — `LoadConfig` now decodes with `KnownFields`, so a key outside the schema fails with the field name and line instead of being dropped. Every optional field has a sensible fallback, which is exactly what made this dangerous: a typo did not stop the server, it started one configured differently from what was written. Found a real instance immediately — the bundled dev-server example set `monitoring.enabled` and `monitoring.addr`, neither of which exists
- **A dependent enqueued after its parent finished is no longer orphaned** — dependency resolution was driven entirely by the parent: on reaching a terminal state it read its `:dependents` set, promoted what it found, and deleted the set. A job enqueued after that moment was invisible to it and sat in `deferred` forever, with no error, no log and no dead-letter entry. Enqueuing a parent and then the work that depends on it is the ordinary way to build a chain, and the window widens the faster the parent runs — so this got *more* likely as a system got healthier. All three terminal states were affected: a completed parent left the child stuck, and a dead-lettered or canceled parent left it stuck rather than cancelling it, including when `AllowFailure` should have released it. `Enqueue` now checks parent status and runs the same resolution the worker would have; the existing `deferred` guard in the Lua makes doing it twice a no-op

## [0.2.0] — 2026-07-31

Security release. Every finding from a full whitebox audit is addressed, and the
defaults that resolved ambiguity toward `admin` now resolve toward least
privilege instead.

**This release changes behaviour that existing deployments depend on.** Read
Upgrading before you take it.

### Upgrading from 0.1.x

**Terminal jobs now expire.** This is the one that fails quietly. Completed jobs
are removed after 7 days and dead-lettered, canceled and stopped jobs after 30 —
previously they were kept forever. Nothing breaks at startup; records simply
begin disappearing a week after you upgrade. To keep the old behaviour:

```yaml
app:
  result_ttl: -1     # keep completed jobs forever
  failure_ttl: -1    # keep dead-lettered, canceled, stopped forever
```

Any value is accepted: seconds as a positive integer, `-1` for permanent, `0` to
delete on completion. The same knobs exist as `WithResultTTL()`/`WithFailureTTL()`
and per job as `ResultTTL()`/`FailureTTL()`, and a per-job value wins over the
server default. Keeping jobs forever means their memory is never reclaimed and
they stay outside Redis `volatile-*` eviction, which only considers keys with an
expiry.

**A server with auth disabled on a routable address now refuses to start.** This
one fails loudly, at startup, before serving anything. With auth off every caller
is treated as admin, including on destructive endpoints. Either enable
`monitoring.auth.enabled`, add `monitoring.api.api_keys`, or bind to loopback
(`127.0.0.1:8080`). Loopback with auth off still starts, which is the local
development case.

**Job IDs may no longer contain a colon.** `Enqueue` returns `ErrInvalidJobID`.
If you derive IDs from an external identifier that may contain one, replace it
with a hyphen or underscore first. Job types and queue names are unaffected, so
the `namespace:action` convention still works.

**A user or API key with no explicit `role` is now `viewer`, not `admin`.** Set
`role: admin` explicitly for the credentials that need it.

**Custom API clients need two headers.** `POST /auth/login` and the batch
endpoints now require `Content-Type: application/json` and return 415 without it.
`POST /auth/logout` now requires `X-GQM-CSRF: 1` for cookie auth and returns 403
without it. The bundled dashboard and TUI already comply; only hand-written
clients are affected. API keys remain exempt from the CSRF requirement.

**`gqm init` writes the config as `0600`**, and `set-password`/`add-api-key`
tighten a group- or world-readable config when they write to it, reporting the
change on stderr.

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
- **Job retention** — terminal jobs now expire instead of accumulating forever. `result_ttl` (default 7 days) covers completed jobs; `failure_ttl` (default 30 days) covers dead-lettered, canceled, and stopped ones. Configurable via `app.result_ttl`/`app.failure_ttl`, `WithResultTTL()`/`WithFailureTTL()`, or per job with `ResultTTL()`/`FailureTTL()`. `TTLPermanent` retains forever, `0` deletes on completion, and a per-job value wins over the server default
- **Terminal sorted set trimming** — `:completed` and `:dead_letter` are trimmed by score inside the existing terminal Lua scripts, so a set entry never outlives the job hash it points to
- **`WithTrustProxy()` / `api.trust_proxy` and `WithCookieSecure()` / `api.cookie_secure`** — control whether `X-Forwarded-Proto` decides the session cookie's `Secure` flag, and force it on behind a TLS-terminating proxy
- **`AcknowledgeUnprotectedRedis()`** — states in code that a password-less Redis is a deliberate choice, and silences the startup banner for the process. Deliberately a function call rather than a config field or environment variable: it belongs in source, where it appears in a diff, survives review and can be grepped, following `tls.Config.InsecureSkipVerify`. A setting outside the code travels between environments unnoticed, which is how an acknowledgement made for local development ends up silencing production
- **CI security workflow** — `vet`, `gofmt`, `go test -race`, and `govulncheck` on every push and pull request, with the stress suite on a weekly schedule. Actions are pinned to commit SHAs rather than tags, and the token is restricted to `contents: read`. The build also fails when Redis-backed tests *skip*: a suite that skips is not a suite that passed, and `go test` reports `ok` when Redis is unreachable. The TUI is a separate module, so `go test ./...` at the root never reached it — CI now runs it explicitly

### Changed
- **BREAKING: job IDs may no longer contain a colon** — GQM joins Redis key segments with a colon, and a job owns a bare key as well as suffixed ones (`gqm:job:<id>` alongside `gqm:job:<id>:deps`, `:pending_deps`, `:dependents`). A job with the ID `order-42:deps` therefore occupied exactly the key job `order-42` needed for its dependency set; because the two hold different Redis types, the victim's enqueue failed with `WRONGTYPE`. The `:dependents` variant blocked every child of the targeted parent. `Enqueue` now returns `ErrInvalidJobID` for such an ID, and the API rejects it as a path parameter. **Job types and queue names are unaffected** — they own no sub-keys, so the `namespace:action` convention still works. This restores the contract `ErrInvalidJobID` always documented ("only alphanumeric, hyphen, underscore, dot allowed"); the regex was simply more permissive than the promise. Generated UUIDv7 IDs are unaffected
- **`meta.truncated`** — pagination envelopes carry it when a bounded scan stopped early, so a short total cannot be read as a complete one. Omitted when false, so no existing response shape changes
- **Dependencies updated** — `golang.org/x/crypto` 0.48.0 → 0.54.0, `golang.org/x/sys` 0.41.0 → 0.47.0, `go-redis` 9.17.3 → 9.21.0. `govulncheck` reported nothing this code calls, but 15 advisories in required modules; none was reachable, since only `x/crypto/bcrypt` is imported while the advisories sit in the ssh and FIDO packages. Unreachable still means every module-level scanner flags them for downstream users. Now down to one, `GO-2026-5932`, which has no fix and covers `x/crypto/openpgp` — not vendored and not importable here

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

[0.2.0]: https://github.com/benedict-erwin/gqm/releases/tag/v0.2.0
[0.1.2]: https://github.com/benedict-erwin/gqm/releases/tag/v0.1.2
[0.1.1]: https://github.com/benedict-erwin/gqm/releases/tag/v0.1.1
[0.1.0]: https://github.com/benedict-erwin/gqm/releases/tag/v0.1.0
