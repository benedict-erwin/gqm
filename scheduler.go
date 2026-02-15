package gqm

import (
	"context"
	"log/slog"
	"time"
)

const (
	schedulerPollInterval = 1 * time.Second
	schedulerBatchSize    = 100
)

// retryScheduler polls the scheduled sorted set and moves ready jobs to their queues.
// This handles the retry mechanism: jobs with status "retry" are in the scheduled set
// with a score = retry timestamp. When the time comes, they get moved back to ready.
type retryScheduler struct {
	server *Server
	logger *slog.Logger
}

func newRetryScheduler(server *Server) *retryScheduler {
	return &retryScheduler{
		server: server,
		logger: server.logger.With("component", "scheduler"),
	}
}

// run starts the scheduler poll loop.
func (rs *retryScheduler) run(ctx context.Context) {
	ticker := time.NewTicker(schedulerPollInterval)
	defer ticker.Stop()

	rs.logger.Debug("retry scheduler started")
	defer rs.logger.Debug("retry scheduler stopped")

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rs.poll(ctx)
		}
	}
}

// poll checks for scheduled jobs that are ready and moves them to their queues.
func (rs *retryScheduler) poll(ctx context.Context) {
	rc := rs.server.rc
	now := time.Now().Unix()

	scheduledKey := rc.Key("scheduled")
	jobPrefix := rc.Key("job") + ":"
	queuePrefix := rc.Key("queue") + ":"

	result := rs.server.scripts.run(ctx, rc.rdb, "schedule_poll",
		[]string{scheduledKey, jobPrefix, queuePrefix},
		now, schedulerBatchSize,
	)
	if result.Err() != nil {
		if ctx.Err() == nil {
			rs.logger.Error("schedule poll failed", "error", result.Err())
		}
		return
	}

	moved, _ := result.Int64()
	if moved > 0 {
		rs.logger.Info("scheduled jobs moved to ready", "count", moved)
	}
}
