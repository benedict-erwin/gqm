package gqm

import (
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// safeNameRe matches queue names and job types. Colons are allowed here because
// GQM uses the "namespace:action" convention for job types (e.g. "email:send")
// and implicit pools derive their queue name from the job type.
//
// This is safe for queue names specifically because a queue owns no bare key:
// every queue key carries a structural suffix ("<prefix>queue:<name>:ready" and
// so on), and the suffix set is fixed, so no name can be spelled to land on a
// different queue's key. Job IDs do not have that property — see safeJobIDRe.
var safeNameRe = regexp.MustCompile(`^[a-zA-Z0-9._:-]+$`)

// safeJobIDRe matches job IDs, which may not contain a colon.
//
// An earlier comment here called the colon-collision concern theoretical, on
// the reasoning that key() always appends a fixed structural suffix. That
// reasoning was wrong, and the collision has since been reproduced through
// Client.Enqueue: a job owns a bare key as well as suffixed ones — the hash at
// "<prefix>job:<id>" plus "<prefix>job:<id>:deps", ":pending_deps" and
// ":dependents" — so a job with the id "order-42:deps" occupies exactly the key
// that job "order-42" needs for its dependency set. The two hold different
// Redis types, so the victim's enqueue fails with WRONGTYPE. The ":dependents"
// variant blocks every child of the targeted parent.
//
// A wrong comment about a security property is worse than no comment: it stops
// the next reader from checking.
var safeJobIDRe = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

// validateJobInputs checks queue name, job ID, and dependency IDs for safe characters.
func validateJobInputs(job *Job) error {
	if job.Queue != "" && (len(job.Queue) > 128 || !safeNameRe.MatchString(job.Queue)) {
		return ErrInvalidQueueName
	}
	if len(job.ID) > 256 || !safeJobIDRe.MatchString(job.ID) {
		return ErrInvalidJobID
	}
	// Dependency IDs name other jobs, so they are held to the same rule. A
	// colon here would not corrupt anything by itself, but it can only ever
	// refer to a job that could not have been enqueued.
	for _, depID := range job.DependsOn {
		if depID == "" || !safeJobIDRe.MatchString(depID) {
			return ErrInvalidJobID
		}
	}
	return nil
}

// newLoggerFromLevel creates a slog.Logger at the given level.
// Falls back to slog.Default() if level is empty or unrecognized.
func newLoggerFromLevel(level string) *slog.Logger {
	if level == "" {
		return slog.Default()
	}
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "info":
		lvl = slog.LevelInfo
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		return slog.Default()
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
}

func parseInt(s string) int {
	v, err := strconv.Atoi(s)
	if err != nil && s != "" {
		slog.Warn("parseInt: invalid integer value", "value", s, "error", err)
	}
	return v
}

func parseInt64(s string) int64 {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil && s != "" {
		slog.Warn("parseInt64: invalid integer value", "value", s, "error", err)
	}
	return v
}
