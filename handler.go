package gqm

import "context"

// Handler is a function that processes a job.
// It must respect ctx for cancellation and timeout.
type Handler func(ctx context.Context, job *Job) error

// HandleOption configures handler registration.
type HandleOption func(*handlerConfig)

type handlerConfig struct {
	workers int
}

// Workers sets the number of dedicated worker goroutines for this handler.
// This creates an implicit pool with a dedicated queue for this job type.
func Workers(n int) HandleOption {
	return func(cfg *handlerConfig) { cfg.workers = n }
}
