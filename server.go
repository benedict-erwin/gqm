package gqm

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// ServerOption configures a Server.
type ServerOption func(*serverConfig)

type serverConfig struct {
	redisOpts       []RedisOption
	globalTimeout   time.Duration
	gracePeriod     time.Duration
	shutdownTimeout time.Duration
	logger          *slog.Logger
}

// WithServerRedis sets the Redis address for the server.
func WithServerRedis(addr string) ServerOption {
	return func(cfg *serverConfig) {
		cfg.redisOpts = append(cfg.redisOpts, WithRedisAddr(addr))
	}
}

// WithServerRedisOpts sets Redis options for the server.
func WithServerRedisOpts(opts ...RedisOption) ServerOption {
	return func(cfg *serverConfig) {
		cfg.redisOpts = append(cfg.redisOpts, opts...)
	}
}

// WithGlobalTimeout sets the global default job timeout.
// Must be > 0; the global timeout cannot be disabled.
func WithGlobalTimeout(d time.Duration) ServerOption {
	return func(cfg *serverConfig) {
		if d > 0 {
			cfg.globalTimeout = d
		}
	}
}

// WithGracePeriod sets the default grace period after context cancellation.
func WithGracePeriod(d time.Duration) ServerOption {
	return func(cfg *serverConfig) {
		if d > 0 {
			cfg.gracePeriod = d
		}
	}
}

// WithShutdownTimeout sets the maximum wait time during graceful shutdown.
func WithShutdownTimeout(d time.Duration) ServerOption {
	return func(cfg *serverConfig) {
		if d > 0 {
			cfg.shutdownTimeout = d
		}
	}
}

// WithLogger sets a custom slog.Logger.
func WithLogger(l *slog.Logger) ServerOption {
	return func(cfg *serverConfig) { cfg.logger = l }
}

// Server manages worker pools and processes jobs.
type Server struct {
	cfg      *serverConfig
	rc       *RedisClient
	scripts  *scriptRegistry
	handlers map[string]Handler
	pools    []*pool
	logger   *slog.Logger

	mu      sync.RWMutex
	running bool
	stopCh  chan struct{}
}

// NewServer creates a new Server with the given options.
func NewServer(opts ...ServerOption) (*Server, error) {
	cfg := &serverConfig{
		globalTimeout:   defaultGlobalTimeout,
		gracePeriod:     defaultGracePeriod,
		shutdownTimeout: defaultShutdownTimeout,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.logger == nil {
		cfg.logger = slog.Default()
	}

	rc, err := NewRedisClient(cfg.redisOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating server redis client: %w", err)
	}

	sr := newScriptRegistry()
	if err := sr.load(); err != nil {
		rc.Close()
		return nil, fmt.Errorf("loading lua scripts: %w", err)
	}

	return &Server{
		cfg:      cfg,
		rc:       rc,
		scripts:  sr,
		handlers: make(map[string]Handler),
		logger:   cfg.logger,
		stopCh:   make(chan struct{}),
	}, nil
}

// Handle registers a handler for a job type.
func (s *Server) Handle(jobType string, handler Handler, opts ...HandleOption) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("cannot register handler while server is running")
	}

	if _, exists := s.handlers[jobType]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateHandler, jobType)
	}

	hcfg := &handlerConfig{}
	for _, opt := range opts {
		opt(hcfg)
	}

	s.handlers[jobType] = handler

	if hcfg.workers > 0 {
		// Implicit pool: dedicated pool + queue per job type
		pcfg := newDefaultPoolConfig(jobType, jobType)
		pcfg.concurrency = hcfg.workers
		pcfg.gracePeriod = s.cfg.gracePeriod
		s.pools = append(s.pools, newPool(pcfg, s))
	}

	return nil
}

// Start begins processing jobs. It blocks until the server is stopped
// via signal (SIGTERM/SIGINT) or the context is cancelled.
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("server already running")
	}
	s.running = true
	s.mu.Unlock()

	if err := s.rc.Ping(ctx); err != nil {
		return fmt.Errorf("redis connection check: %w", err)
	}

	// Ensure a default pool exists for handlers without Workers()
	s.ensureDefaultPool()

	s.logger.Info("server starting",
		"pools", len(s.pools),
		"handlers", len(s.handlers),
	)

	// Create a cancellable context for all pools
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	// Start all pools and the retry scheduler
	var wg sync.WaitGroup

	// Retry scheduler
	sched := newRetryScheduler(s)
	wg.Add(1)
	go func() {
		defer wg.Done()
		sched.run(ctx)
	}()

	for _, p := range s.pools {
		wg.Add(1)
		go func(p *pool) {
			defer wg.Done()
			p.run(ctx)
		}(p)
	}

	// Wait for shutdown signal or context cancellation
	select {
	case sig := <-sigCh:
		s.logger.Info("received signal, initiating shutdown", "signal", sig)
		cancel()
	case <-ctx.Done():
		s.logger.Info("context cancelled, initiating shutdown")
	}

	// Graceful shutdown: wait for pools to finish within timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		s.logger.Info("all pools stopped gracefully")
	case <-time.After(s.cfg.shutdownTimeout):
		s.logger.Warn("shutdown timeout reached, forcing stop",
			"timeout", s.cfg.shutdownTimeout)
	}

	s.mu.Lock()
	s.running = false
	s.mu.Unlock()

	return s.rc.Close()
}

// Stop signals the server to stop.
func (s *Server) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.running {
		close(s.stopCh)
	}
}

// ensureDefaultPool creates a "default" pool for handlers not assigned to any pool.
func (s *Server) ensureDefaultPool() {
	// Collect handlers that don't have an implicit pool
	assignedTypes := make(map[string]bool)
	for _, p := range s.pools {
		assignedTypes[p.cfg.name] = true
	}

	var unassigned []string
	for jt := range s.handlers {
		if !assignedTypes[jt] {
			unassigned = append(unassigned, jt)
		}
	}

	if len(unassigned) == 0 {
		return
	}

	pcfg := newDefaultPoolConfig("default", "default")
	pcfg.gracePeriod = s.cfg.gracePeriod
	s.pools = append(s.pools, newPool(pcfg, s))

	s.logger.Info("created default pool",
		"concurrency", pcfg.concurrency,
		"unassigned_handlers", unassigned,
	)
}

// resolveTimeout returns the effective timeout for a job.
func (s *Server) resolveTimeout(job *Job, pcfg *poolConfig) time.Duration {
	// Job-level timeout (highest priority)
	if job.Timeout > 0 {
		return time.Duration(job.Timeout) * time.Second
	}
	// Pool-level timeout
	if pcfg.jobTimeout > 0 {
		return pcfg.jobTimeout
	}
	// Global default (always set, cannot be disabled)
	return s.cfg.globalTimeout
}
