package gqm

import (
	"math"
	"time"
)

// EnqueueOption configures job enqueue behavior.
type EnqueueOption func(*Job)

// TTLPermanent retains a terminal job's hash forever, opting it out of
// retention. Pass it to ResultTTL or FailureTTL.
//
// Retaining jobs permanently means their memory is never reclaimed, and it also
// keeps them un-evictable under Redis's volatile-* maxmemory policies, since
// those only consider keys that carry an expiry.
const TTLPermanent = time.Duration(-1)

// ResultTTL overrides how long this job's hash is retained after it completes
// successfully, replacing the server default for this job only.
//
// A negative duration means TTLPermanent (retain forever). A zero duration
// deletes the job hash as soon as it completes. Sub-second values are rounded
// up to 1s, so the smallest retention that still keeps the job readable is 1s.
func ResultTTL(d time.Duration) EnqueueOption {
	return func(j *Job) { j.ResultTTL = ttlSeconds(d) }
}

// FailureTTL overrides how long this job's hash is retained after it reaches a
// failure terminal state (dead-lettered, canceled, or stopped), replacing the
// server default for this job only.
//
// Follows the same convention as ResultTTL: negative means retain forever, zero
// deletes immediately.
func FailureTTL(d time.Duration) EnqueueOption {
	return func(j *Job) { j.FailureTTL = ttlSeconds(d) }
}

// ttlSeconds converts a retention duration to the seconds representation stored
// in the job hash, collapsing any negative duration to the permanent sentinel.
func ttlSeconds(d time.Duration) *int {
	if d < 0 {
		secs := -1
		return &secs
	}
	secs := int(math.Ceil(d.Seconds()))
	return &secs
}

// Queue sets the target queue for the job.
func Queue(name string) EnqueueOption {
	return func(j *Job) { j.Queue = name }
}

// MaxRetry sets the maximum number of retries for the job.
// Negative values are clamped to 0 (no retry).
func MaxRetry(n int) EnqueueOption {
	return func(j *Job) {
		if n < 0 {
			n = 0
		}
		j.MaxRetry = n
	}
}

// Timeout sets the job-level timeout. Sub-second values are rounded up to 1s.
func Timeout(d time.Duration) EnqueueOption {
	return func(j *Job) { j.Timeout = int(math.Ceil(d.Seconds())) }
}

// RetryIntervals sets the retry intervals in seconds.
func RetryIntervals(intervals ...int) EnqueueOption {
	return func(j *Job) { j.RetryIntervals = intervals }
}

// JobID overrides the default UUID v7 job ID with a custom value.
// The caller is responsible for ensuring uniqueness — if a job with the
// same ID already exists in Redis, its data will be silently overwritten.
func JobID(id string) EnqueueOption {
	return func(j *Job) { j.ID = id }
}

// Meta sets arbitrary metadata on the job.
func Meta(m Payload) EnqueueOption {
	return func(j *Job) { j.Meta = m }
}

// EnqueuedBy sets the enqueuer identifier.
func EnqueuedBy(name string) EnqueueOption {
	return func(j *Job) { j.EnqueuedBy = name }
}

// DependsOn specifies parent job IDs that must complete before this job runs.
// The job will be created with status "deferred" and moved to ready queue
// only after all dependencies are resolved.
func DependsOn(jobIDs ...string) EnqueueOption {
	return func(j *Job) { j.DependsOn = jobIDs }
}

// AllowFailure controls whether this job should proceed even if a parent
// dependency fails. When false (default), parent failure cascades cancellation.
func AllowFailure(allow bool) EnqueueOption {
	return func(j *Job) { j.AllowFailure = allow }
}

// EnqueueAtFront causes the job to be pushed to the front of the queue
// (instead of the back) when its dependencies are resolved.
func EnqueueAtFront(atFront bool) EnqueueOption {
	return func(j *Job) { j.EnqueueAtFront = atFront }
}

// Unique ensures no existing job with the same ID exists before enqueueing.
// If a duplicate is found, Enqueue returns ErrDuplicateJobID.
// Most useful with JobID() for custom IDs — default UUID v7 IDs are already unique.
func Unique() EnqueueOption {
	return func(j *Job) { j.unique = true }
}
