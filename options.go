package gqm

import "time"

// EnqueueOption configures job enqueue behavior.
type EnqueueOption func(*Job)

// Queue sets the target queue for the job.
func Queue(name string) EnqueueOption {
	return func(j *Job) { j.Queue = name }
}

// MaxRetry sets the maximum number of retries for the job.
func MaxRetry(n int) EnqueueOption {
	return func(j *Job) { j.MaxRetry = n }
}

// Timeout sets the job-level timeout.
func Timeout(d time.Duration) EnqueueOption {
	return func(j *Job) { j.Timeout = int(d.Seconds()) }
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
