# Contributing to GQM

Thanks for taking the time. This document covers what the project expects from a
change, how to run the same checks CI runs, and the few conventions that are not
obvious from reading the code.

By participating you agree to the [Code of Conduct](CODE_OF_CONDUCT.md).

Found a security problem? Do not open an issue — see [SECURITY.md](SECURITY.md).

## Before you write code

**Open an issue first for anything beyond a bug fix or a typo.** GQM keeps a
deliberately small surface: three production dependencies, no wrapper libraries,
and a public API that is meant to stay small enough to hold in your head. A
feature that does not fit is easier to discuss in an issue than to unwind from a
finished pull request.

Small fixes — a wrong condition, a bad error message, a broken example — go
straight to a pull request.

## Development setup

You need **Go** (the version in `go.mod` — CI uses exactly that) and a **Redis**
instance for the test suite.

```bash
git clone https://github.com/benedict-erwin/gqm
cd gqm

# Redis on loopback only, matching what the test suite expects
docker compose up -d redis

export GQM_TEST_REDIS_ADDR=127.0.0.1:6379
go test ./... -count=1 -timeout 900s
```

A devcontainer is included (`.devcontainer/`) if you prefer it — it brings up Go
and Redis together, with `GQM_TEST_REDIS_ADDR` already set.

Redis-backed tests **skip** when Redis is unreachable, and `go test` still
reports `ok`. CI fails the build when it sees those skip messages, so a green
local run against no Redis proves nothing. Make sure Redis is actually up.

## The checks your PR has to pass

CI (`.github/workflows/security.yml`) runs all of these. Running them locally
before pushing saves a round trip.

```bash
go vet ./...
gofmt -l . | grep -v '^vendor/'          # must print nothing
go build ./...
go test -race ./... -count=1 -timeout 900s
```

Two things are easy to miss:

```bash
# The TUI is a separate Go module. "./..." from the repository root does not
# reach it — go.work changes resolution, not which packages the pattern matches.
cd tui && go test -race ./... -count=1 && cd ..

# Go tooling ignores paths starting with an underscore, so _examples never gets
# built by "go build ./...". An example that stops compiling is a broken doc.
find _examples -name '*.go' -exec dirname {} \; | sort -u | while read -r d; do
  go build -o /dev/null "./$d" || exit 1
done
```

Stress tests are gated behind an environment variable and are not part of the
normal run. They execute on a schedule in CI, but run them yourself if you touched
the worker loop, the scheduler, or anything that reclaims jobs:

```bash
GQM_STRESS_TEST=1 go test -race ./... -count=1 -timeout 1800s
```

## Code conventions

- **No `panic()` or `log.Fatal()` in library code.** Return an `error`. Those are
  allowed only in `main()` and under `cmd/`.
- **Wrap errors with context**: `fmt.Errorf("dequeuing from pool %s: %w", name, err)`.
- **Logging is `log/slog`**, always. GQM is a library — users plug in their own
  `slog.Handler`, so no other logger is acceptable.
- **Public API uses functional options** (`WithRedis(...)`, `Workers(n)`), not
  growing config structs.
- **Comments, godoc, and commit messages are English**, without exception.
- **Comment the *why*, not the *what*.** A comment that restates the code is noise;
  one that records a constraint, a subtle invariant, or why the obvious approach
  fails is what keeps the next change correct.
- **Propagate `context.Context`** through every layer, and respect cancellation.
- Redis keys are prefixed (`gqm:` by default); Lua scripts take every key through
  `KEYS[]` rather than building key names inside the script.

### Dependencies

Production dependencies are limited to three:

| Dependency | Purpose |
|---|---|
| `github.com/redis/go-redis/v9` | Redis client |
| `gopkg.in/yaml.v3` | YAML config parsing |
| `golang.org/x/crypto/bcrypt` | Password hashing (dashboard auth) |

`cmd/gqm` adds `golang.org/x/term`; the TUI module has its own dependencies and
is separate precisely so importing the core library does not pull them in.

**Adding a dependency to the core library requires discussion in an issue first.**
The default answer is stdlib or a small implementation in-tree — UUID v7, the cron
parser, and the HTTP routing are all in-tree for this reason.

### Tests

- Table-driven, named `TestFunctionName_Scenario`.
- Cover the happy path, the error cases, and the edge cases.
- Tests that need Redis must use a unique key prefix or clean up after themselves.
  Never `FLUSHDB` outside a dedicated test database.
- A bug fix should come with a test that fails without the fix. If the test cannot
  be made to fail, say so in the PR and explain why.

## Pull requests

- **Target the `develop` branch.** `main` only receives release merges. A PR against
  `main` will be asked to retarget.
- Keep the change focused. Unrelated cleanup in the same PR makes the actual change
  harder to review — send it separately.
- Commit subjects follow Conventional Commits: `fix(worker): ...`, `feat(monitor): ...`,
  `docs(readme): ...`.
- Update `CHANGELOG.md` under `## [Unreleased]` for anything user-visible — new
  behaviour, changed defaults, fixed bugs. Keep it concise and in English.
- Update the README when you change or add public API. The README is the reference
  documentation for this project.
- Describe how you verified the change. "Tests pass" is weaker than "added
  `TestReaper_ExpiredHash`, which fails on `develop` and passes here".

## Reporting bugs

Use the issue forms. What makes a queue bug tractable is the boring detail:
GQM version, Redis version, the relevant part of your config, whether jobs are
delayed/cron/DAG, and what you saw versus what you expected. A minimal
reproduction beats a description every time.
