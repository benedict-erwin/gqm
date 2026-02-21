package gqm

import "errors"

var (
	// ErrJobNotFound is returned when a job cannot be found in Redis.
	ErrJobNotFound = errors.New("gqm: job not found")

	// ErrQueueEmpty is returned when attempting to dequeue from an empty queue.
	ErrQueueEmpty = errors.New("gqm: queue empty")

	// ErrServerStopped is returned when operations are attempted on a stopped server.
	ErrServerStopped = errors.New("gqm: server stopped")

	// ErrHandlerNotFound is returned when no handler is registered for a job type.
	ErrHandlerNotFound = errors.New("gqm: handler not found")

	// ErrDuplicateHandler is returned when a handler is registered twice for the same job type.
	ErrDuplicateHandler = errors.New("gqm: duplicate handler registration")

	// ErrInvalidJobType is returned when a job type is empty.
	ErrInvalidJobType = errors.New("gqm: invalid job type")

	// ErrMaxRetryExceeded is returned when a job has exceeded its maximum retry count.
	ErrMaxRetryExceeded = errors.New("gqm: max retry exceeded")

	// ErrDuplicatePool is returned when a pool with the same name is registered twice.
	ErrDuplicatePool = errors.New("gqm: duplicate pool name")

	// ErrJobTypeConflict is returned when a job type is assigned to multiple pools.
	ErrJobTypeConflict = errors.New("gqm: job type already assigned to another pool")

	// ErrDuplicateCronEntry is returned when a cron entry with the same ID is registered twice.
	ErrDuplicateCronEntry = errors.New("gqm: duplicate cron entry ID")

	// ErrCyclicDependency is returned when adding a dependency would create a cycle in the DAG.
	ErrCyclicDependency = errors.New("gqm: cyclic dependency detected")

	// ErrDuplicateJobID is returned when enqueueing with Unique() and a job with the same ID already exists.
	ErrDuplicateJobID = errors.New("gqm: duplicate job ID")

	// ErrInvalidQueueName is returned when a queue name contains invalid characters.
	ErrInvalidQueueName = errors.New("gqm: invalid queue name (only alphanumeric, hyphen, underscore, dot allowed; max 128 chars)")

	// ErrInvalidJobID is returned when a job ID contains invalid characters.
	ErrInvalidJobID = errors.New("gqm: invalid job ID (only alphanumeric, hyphen, underscore, dot allowed; max 256 chars)")

	// ErrSkipRetry can be returned (or wrapped) by a handler to skip all
	// remaining retries and move the job directly to the dead letter queue.
	ErrSkipRetry = errors.New("gqm: skip retry")

	// ErrBatchTooLarge is returned when EnqueueBatch receives more items
	// than the maximum allowed batch size.
	ErrBatchTooLarge = errors.New("gqm: batch size exceeds maximum (1000)")

	// ErrBatchDependsOn is returned when DependsOn is used in EnqueueBatch.
	ErrBatchDependsOn = errors.New("gqm: DependsOn is not supported in EnqueueBatch")

	// ErrBatchUnique is returned when Unique is used in EnqueueBatch.
	ErrBatchUnique = errors.New("gqm: Unique is not supported in EnqueueBatch")

	// ErrBatchEnqueueAtFront is returned when EnqueueAtFront is used in EnqueueBatch.
	ErrBatchEnqueueAtFront = errors.New("gqm: EnqueueAtFront is not supported in EnqueueBatch")

	// ErrJobDataTooLarge is returned when a job's serialized data exceeds maxJobDataSize.
	ErrJobDataTooLarge = errors.New("gqm: job data exceeds maximum size (1 MB)")
)
