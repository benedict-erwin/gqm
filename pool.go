package gqm

import (
	"runtime"
	"time"
)

const (
	defaultGlobalTimeout     = 30 * time.Minute
	defaultGracePeriod       = 10 * time.Second
	defaultShutdownTimeout   = 30 * time.Second
	defaultHeartbeatInterval = 5 * time.Second
	defaultRetryDelay        = 10 * time.Second
	maxBackoffDelay          = 24 * time.Hour
)

// DequeueStrategy determines how a pool selects jobs from multiple queues.
type DequeueStrategy string

const (
	// StrategyStrict always checks the highest-priority queue first.
	// Lower-priority queues may starve if higher ones are always full.
	StrategyStrict DequeueStrategy = "strict"

	// StrategyRoundRobin rotates through queues in order each cycle.
	StrategyRoundRobin DequeueStrategy = "round_robin"

	// StrategyWeighted selects queues probabilistically based on priority weight.
	StrategyWeighted DequeueStrategy = "weighted"
)

// BackoffType determines the retry delay calculation method.
type BackoffType string

const (
	// BackoffFixed uses a constant interval between retries.
	BackoffFixed BackoffType = "fixed"

	// BackoffExponential uses base * 2^attempt, capped at BackoffMax.
	BackoffExponential BackoffType = "exponential"

	// BackoffCustom uses the Intervals slice directly.
	BackoffCustom BackoffType = "custom"
)

// RetryPolicy configures retry behavior at the pool level.
// When set on a pool, it serves as the default for all jobs in that pool
// unless overridden at the job level.
type RetryPolicy struct {
	MaxRetry   int
	Intervals  []int         // Explicit intervals in seconds (for BackoffCustom)
	Backoff    BackoffType   // fixed, exponential, custom
	BackoffBase time.Duration // Base delay for fixed/exponential
	BackoffMax  time.Duration // Maximum delay cap for exponential
}

// PoolConfig is the public configuration for defining an explicit worker pool (Layer 3).
// Use Server.Pool() to register a pool with this configuration.
type PoolConfig struct {
	Name            string
	JobTypes        []string        // Job types handled by this pool
	Queues          []string        // Queues to listen on, ordered by priority
	Concurrency     int             // Number of worker goroutines (default: runtime.NumCPU())
	JobTimeout      time.Duration   // Pool-level job timeout (0 = fall back to global)
	GracePeriod     time.Duration   // Grace period after context cancel (0 = use server default)
	ShutdownTimeout time.Duration   // Shutdown wait time (0 = use server default)
	DequeueStrategy DequeueStrategy // How to select from multiple queues (default: strict)
	RetryPolicy     *RetryPolicy    // Pool-level retry defaults (nil = use job-level)
}

// toInternal converts a public PoolConfig to the internal poolConfig.
func (pc PoolConfig) toInternal(serverGracePeriod time.Duration) *poolConfig {
	queues := pc.Queues
	if len(queues) == 0 {
		queues = []string{"default"}
	}

	concurrency := pc.Concurrency
	if concurrency <= 0 {
		concurrency = runtime.NumCPU()
	}

	gracePeriod := pc.GracePeriod
	if gracePeriod <= 0 {
		gracePeriod = serverGracePeriod
	}

	shutdownTimeout := pc.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = defaultShutdownTimeout
	}

	strategy := pc.DequeueStrategy
	if strategy == "" {
		strategy = StrategyStrict
	}

	return &poolConfig{
		name:            pc.Name,
		queues:          queues,
		concurrency:     concurrency,
		jobTimeout:      pc.JobTimeout,
		gracePeriod:     gracePeriod,
		shutdownTimeout: shutdownTimeout,
		dequeueStrategy: strategy,
		retryPolicy:     pc.RetryPolicy,
	}
}

// poolConfig holds the configuration for a worker pool.
type poolConfig struct {
	name            string
	queues          []string        // Queues to listen on, ordered by priority
	concurrency     int
	jobTimeout      time.Duration
	gracePeriod     time.Duration
	shutdownTimeout time.Duration
	dequeueStrategy DequeueStrategy // How to select from multiple queues
	retryPolicy     *RetryPolicy    // Pool-level retry defaults (nil = use job-level)
}

// newDefaultPoolConfig creates a pool config with sensible defaults.
func newDefaultPoolConfig(name string, queues ...string) *poolConfig {
	if len(queues) == 0 {
		queues = []string{"default"}
	}
	return &poolConfig{
		name:            name,
		queues:          queues,
		concurrency:     runtime.NumCPU(),
		jobTimeout:      0, // falls back to global
		gracePeriod:     defaultGracePeriod,
		shutdownTimeout: defaultShutdownTimeout,
		dequeueStrategy: StrategyWeighted,
	}
}
