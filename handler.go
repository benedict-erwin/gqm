package gqm

import "context"

// Handler is a function that processes a job.
// It must respect ctx for cancellation and timeout.
type Handler func(ctx context.Context, job *Job) error

// HandleOption configures handler registration.
type HandleOption func(*handlerConfig)

// SuccessCallbackFunc is a callback invoked after a job completes successfully.
type SuccessCallbackFunc func(ctx context.Context, job *Job)

// FailureCallbackFunc is a callback invoked after a job fails (handler error or timeout).
type FailureCallbackFunc func(ctx context.Context, job *Job, err error)

// CompleteCallbackFunc is a callback invoked after every job execution regardless of outcome.
type CompleteCallbackFunc func(ctx context.Context, job *Job, err error)

type handlerConfig struct {
	workers    int
	isFailure  func(error) bool
	onSuccess  SuccessCallbackFunc
	onFailure  FailureCallbackFunc
	onComplete CompleteCallbackFunc
}

// Workers sets the number of dedicated worker goroutines for this handler.
// This creates an implicit pool with a dedicated queue for this job type.
func Workers(n int) HandleOption {
	return func(cfg *handlerConfig) { cfg.workers = n }
}

// MiddlewareFunc is a function that wraps a Handler to add cross-cutting
// behavior such as logging, metrics, tracing, or error handling.
//
// Middleware is registered via Server.Use() and applied in the order
// registered: Use(a, b) executes as a → b → handler.
//
// Example:
//
//	func loggingMiddleware(next gqm.Handler) gqm.Handler {
//	    return func(ctx context.Context, job *gqm.Job) error {
//	        slog.Info("start job", "id", job.ID, "type", job.Type)
//	        err := next(ctx, job)
//	        slog.Info("end job", "id", job.ID, "type", job.Type, "error", err)
//	        return err
//	    }
//	}
type MiddlewareFunc func(Handler) Handler

// IsFailure sets a predicate that classifies handler errors.
//
// When a handler returns an error and the predicate returns false, the
// job is retried WITHOUT incrementing the retry counter. This means
// transient/expected errors (rate limiting, temporary unavailability)
// do not count towards the retry limit, allowing the job to retry
// indefinitely for non-failure errors.
//
// When the predicate returns true (or when no predicate is set), the
// error is treated as a real failure: the retry counter increments
// normally and the job eventually moves to the dead letter queue.
//
// Non-failure retries always use the same retry delay (the first
// interval), since the retry counter is not incremented. Backoff
// progression only applies to failure retries.
//
// If maxRetry is 0, non-failure errors are sent to the DLQ with a
// warning log, since retry is not configured.
//
// If the predicate panics, the error is treated as a failure (safe
// default) and the panic is logged with a stack trace.
//
// Note: ErrSkipRetry always bypasses retry regardless of IsFailure.
// The fn parameter must not be nil.
func IsFailure(fn func(error) bool) HandleOption {
	if fn == nil {
		panic("gqm: IsFailure predicate must not be nil")
	}
	return func(cfg *handlerConfig) { cfg.isFailure = fn }
}

// OnSuccess registers a callback that fires after a job completes
// successfully (handler returned nil). The callback runs synchronously
// in the worker goroutine before the next job is dequeued. Panics are
// recovered and logged; they do not affect the job outcome.
//
// Important: callbacks block the worker. Keep them fast (no heavy I/O,
// no unbounded waits). For async work, spawn a goroutine inside the
// callback. Callbacks fire before Redis state is updated. Do not mutate
// the job struct — it is shared with subsequent Redis operations.
// The ctx is the worker's shutdown context, NOT the handler's timeout
// context. Use your own timeout for I/O inside callbacks.
func OnSuccess(fn SuccessCallbackFunc) HandleOption {
	if fn == nil {
		panic("gqm: OnSuccess callback must not be nil")
	}
	return func(cfg *handlerConfig) { cfg.onSuccess = fn }
}

// OnFailure registers a callback that fires after a handler returns an
// error (including timeout). It fires for ALL handler errors regardless
// of the IsFailure predicate classification. The callback runs
// synchronously in the worker goroutine before retry/DLQ processing.
// Panics are recovered and logged; they do not affect the job outcome.
//
// Important: callbacks block the worker. Keep them fast. Callbacks fire
// before Redis state is updated. Do not mutate the job struct.
func OnFailure(fn FailureCallbackFunc) HandleOption {
	if fn == nil {
		panic("gqm: OnFailure callback must not be nil")
	}
	return func(cfg *handlerConfig) { cfg.onFailure = fn }
}

// OnComplete registers a callback that fires after every job execution,
// regardless of outcome. It is called after OnSuccess or OnFailure. The
// err parameter is nil on success. Panics are recovered and logged; they
// do not affect the job outcome.
//
// Important: callbacks block the worker. Keep them fast. Callbacks fire
// before Redis state is updated. Only fires when a handler is found and
// invoked — internal failures (fetch/parse/no-handler) do not trigger
// callbacks. Do not mutate the job struct.
func OnComplete(fn CompleteCallbackFunc) HandleOption {
	if fn == nil {
		panic("gqm: OnComplete callback must not be nil")
	}
	return func(cfg *handlerConfig) { cfg.onComplete = fn }
}
