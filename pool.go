package gqm

import (
	"runtime"
	"time"
)

const (
	defaultGlobalTimeout  = 30 * time.Minute
	defaultGracePeriod    = 10 * time.Second
	defaultShutdownTimeout = 30 * time.Second
	defaultHeartbeatInterval = 5 * time.Second
)

// poolConfig holds the configuration for a worker pool.
type poolConfig struct {
	name              string
	queue             string
	concurrency       int
	jobTimeout        time.Duration
	gracePeriod       time.Duration
	shutdownTimeout   time.Duration
}

// newDefaultPoolConfig creates a pool config with sensible defaults.
func newDefaultPoolConfig(name, queue string) *poolConfig {
	return &poolConfig{
		name:            name,
		queue:           queue,
		concurrency:     runtime.NumCPU(),
		jobTimeout:      0, // falls back to global
		gracePeriod:     defaultGracePeriod,
		shutdownTimeout: defaultShutdownTimeout,
	}
}
