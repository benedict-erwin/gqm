package gqm

import (
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"
)

// safeNameRe matches strings containing only safe characters for Redis key components.
var safeNameRe = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

// validateJobInputs checks queue name, job ID, and dependency IDs for safe characters.
func validateJobInputs(job *Job) error {
	if job.Queue != "" && (len(job.Queue) > 128 || !safeNameRe.MatchString(job.Queue)) {
		return ErrInvalidQueueName
	}
	if len(job.ID) > 256 || !safeNameRe.MatchString(job.ID) {
		return ErrInvalidJobID
	}
	for _, depID := range job.DependsOn {
		if depID == "" || !safeNameRe.MatchString(depID) {
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
	v, _ := strconv.Atoi(s)
	return v
}

func parseInt64(s string) int64 {
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}
