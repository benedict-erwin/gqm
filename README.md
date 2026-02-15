# GQM — Go Queue Manager

Redis-based task queue library for Go. Built from scratch with minimal dependencies and progressive disclosure API design.

## Status

**Phase 1 — Foundation** (in progress)

## Features (Planned)

- Redis-backed job queue with atomic operations (Lua scripts)
- Worker pool with configurable concurrency
- Timeout hierarchy (job → pool → global default)
- Panic recovery per handler goroutine
- Graceful shutdown with grace period
- Basic retry with fixed interval
- `log/slog` integration

## Requirements

- Go 1.22+
- Redis 7+

## Installation

```bash
go get github.com/<username>/gqm
```

## Quick Start

```go
// Enqueue a job
client := gqm.NewClient(gqm.WithRedis("localhost:6379"))
client.Enqueue("email:welcome", payload)

// Process jobs
server := gqm.NewServer(gqm.WithRedis("localhost:6379"))
server.Handle("email:welcome", handleWelcomeEmail)
server.Start()
```

> API is subject to change during early development.

## Dependencies

| Dependency | Purpose |
|---|---|
| `github.com/redis/go-redis/v9` | Redis client |
| `gopkg.in/yaml.v3` | Config parsing |
| `golang.org/x/crypto/bcrypt` | Password hashing (dashboard auth) |

## License

MIT
