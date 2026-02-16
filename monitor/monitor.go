// Package monitor provides the HTTP monitoring API and dashboard for GQM.
package monitor

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

// Config holds all configuration needed by the Monitor.
type Config struct {
	APIAddr        string
	AuthEnabled    bool
	AuthSessionTTL int // seconds, default 86400
	AuthUsers      []AuthUser
	APIKeys        []AuthAPIKey
	DashEnabled    bool
	DashPathPrefix string
	DashCustomDir  string
}

// AuthUser represents a user credential.
type AuthUser struct {
	Username     string
	PasswordHash string
}

// AuthAPIKey represents an API key credential.
type AuthAPIKey struct {
	Name string
	Key  string
}

// ServerAdmin is the interface for write operations that the monitor delegates
// back to the Server. This avoids a circular dependency between monitor/ and
// the root gqm package.
type ServerAdmin interface {
	RetryJob(ctx context.Context, jobID string) error
	CancelJob(ctx context.Context, jobID string) error
	DeleteJob(ctx context.Context, jobID string) error
	PauseQueue(ctx context.Context, queue string) error
	ResumeQueue(ctx context.Context, queue string) error
	EmptyQueue(ctx context.Context, queue string) (int64, error)
	RetryAllDLQ(ctx context.Context, queue string) (int64, error)
	ClearDLQ(ctx context.Context, queue string) (int64, error)
	TriggerCron(ctx context.Context, cronID string) (string, error) // returns job ID
	EnableCron(ctx context.Context, cronID string) error
	DisableCron(ctx context.Context, cronID string) error
	IsQueuePaused(ctx context.Context, queue string) (bool, error)
}

// Monitor manages the HTTP monitoring server.
type Monitor struct {
	server    *http.Server
	mux       *http.ServeMux
	rdb       *redis.Client
	prefix    string
	logger    *slog.Logger
	cfg       Config
	admin     ServerAdmin
	startedAt time.Time

	// apiKeyMap is a fast lookup from key string to name.
	apiKeyMap map[string]string
}

// New creates a new Monitor.
// rdb is the Redis client, prefix is the GQM key prefix, logger is the server logger.
// admin is optional; if nil, write endpoints will return 501 Not Implemented.
func New(rdb *redis.Client, prefix string, logger *slog.Logger, cfg Config, admin ServerAdmin) *Monitor {
	m := &Monitor{
		rdb:    rdb,
		prefix: prefix,
		logger: logger.With("component", "monitor"),
		cfg:    cfg,
		admin:  admin,
	}

	// Build API key lookup map
	m.apiKeyMap = make(map[string]string, len(cfg.APIKeys))
	for _, k := range cfg.APIKeys {
		m.apiKeyMap[k.Key] = k.Name
	}

	m.mux = http.NewServeMux()
	m.setupRoutes()

	addr := cfg.APIAddr
	if addr == "" {
		addr = ":8080"
	}

	m.server = &http.Server{
		Addr:         addr,
		Handler:      m.mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	return m
}

// Start starts the HTTP server. Blocks until the server is stopped or errors.
// Returns nil on graceful shutdown.
func (m *Monitor) Start() error {
	m.startedAt = time.Now()
	m.logger.Info("monitor HTTP server starting", "addr", m.server.Addr)
	err := m.server.ListenAndServe()
	if err == http.ErrServerClosed {
		return nil
	}
	return err
}

// Stop gracefully shuts down the HTTP server.
func (m *Monitor) Stop(ctx context.Context) error {
	m.logger.Info("monitor HTTP server stopping")
	return m.server.Shutdown(ctx)
}

// key builds a prefixed Redis key.
func (m *Monitor) key(parts ...string) string {
	k := m.prefix
	for i, p := range parts {
		if i > 0 {
			k += ":"
		}
		k += p
	}
	return k
}
