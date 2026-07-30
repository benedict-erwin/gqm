// Package monitor provides the HTTP monitoring API and dashboard for GQM.
package monitor

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
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
	RateLimit      int // requests/second per IP; 0 = default (100), -1 = disabled
}

// AuthUser represents a user credential.
type AuthUser struct {
	Username     string
	PasswordHash string
	Role         string // "admin" or "viewer"; defaults to "admin" if empty
}

// AuthAPIKey represents an API key credential.
type AuthAPIKey struct {
	Name string
	Key  string
	Role string // "admin" or "viewer"; defaults to "admin" if empty
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
	limiter   *rateLimiter // nil if rate limiting is disabled
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

	m.mux = http.NewServeMux()
	m.setupRoutes()

	// Rate limiting: enabled by default (cfg.RateLimit == 0 means use default).
	// Set cfg.RateLimit to -1 to disable.
	var handler http.Handler = m.mux
	if cfg.RateLimit != -1 {
		m.limiter = newRateLimiter(cfg.RateLimit)
		handler = m.limiter.middleware(handler)
	}

	addr := cfg.APIAddr
	if addr == "" {
		addr = ":8080"
	}

	m.server = &http.Server{
		Addr:           addr,
		Handler:        m.securityHeaders(m.requestLogger(handler)),
		ReadTimeout:    10 * time.Second,
		WriteTimeout:   30 * time.Second,
		IdleTimeout:    60 * time.Second,
		MaxHeaderBytes: 1 << 20, // 1 MB
	}

	return m
}

// Start starts the HTTP server. Blocks until the server is stopped or errors.
// Returns nil on graceful shutdown.
func (m *Monitor) Start() error {
	m.startedAt = time.Now()
	if !m.cfg.AuthEnabled {
		m.logger.Warn("monitor authentication is DISABLED — all endpoints are publicly accessible, enable auth for production use")
	}
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
	if m.limiter != nil {
		m.limiter.close()
	}
	return m.server.Shutdown(ctx)
}

// requestLogger wraps an http.Handler and logs each incoming request and its response status.
func (m *Monitor) requestLogger(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(rw, r)
		m.logger.Debug("http request",
			"method", r.Method,
			"path", r.URL.Path,
			"status", rw.status,
			"duration", time.Since(start).String(),
			"remote", r.RemoteAddr,
		)
	})
}

// securityHeaders adds standard security headers to all responses.
//
// base-uri is the substantive addition here, and it is easy to overlook because
// default-src does not act as a fallback for it. Every asset reference in
// index.html is relative ("css/style.css", "js/app.js", "vendor/*.js"), so an
// injected <base href> would repoint all of them at once — turning an HTML
// injection that the rest of the policy contains into script hijacking.
// form-action and frame-ancestors are likewise not covered by default-src.
//
// style-src still allows 'unsafe-inline'. Removing it needs the dashboard CSS
// refactored, which is deliberately not bundled into this change.
func (m *Monitor) securityHeaders(next http.Handler) http.Handler {
	const csp = "default-src 'self'; " +
		"script-src 'self'; " +
		"style-src 'self' 'unsafe-inline'; " +
		"img-src 'self' data:; " +
		"font-src 'self'; " +
		"connect-src 'self'; " +
		"object-src 'none'; " +
		"base-uri 'none'; " +
		"form-action 'self'; " +
		"frame-ancestors 'none'"

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("Content-Security-Policy", csp)
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Strict-Transport-Security", "max-age=63072000; includeSubDomains")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
		h.Set("Cross-Origin-Opener-Policy", "same-origin")

		// API and auth responses carry job payloads and session identity, which
		// must not sit in a browser or proxy cache and outlive a logout on a
		// shared workstation. Static dashboard assets are deliberately left
		// cacheable — caching those costs nothing and helps.
		if p := r.URL.Path; strings.HasPrefix(p, "/api/") || strings.HasPrefix(p, "/auth/") {
			h.Set("Cache-Control", "no-store")
			h.Set("Pragma", "no-cache")
		}

		next.ServeHTTP(w, r)
	})
}

// statusWriter wraps http.ResponseWriter to capture the response status code.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
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
