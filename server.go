package gqm

import (
	"context"
	"encoding/json"
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

	// Phase 5: config-driven fields
	defaultTimezone       string        // fallback timezone for cron entries (IANA)
	schedulerEnabled      bool          // whether to start the scheduler goroutine
	schedulerPollInterval time.Duration // poll interval for scheduled/cron jobs
	logLevel              string        // auto-create logger if no WithLogger (debug/info/warn/error)
	catchAllPool          string        // pool name with job_types: ["*"]
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

// WithDefaultTimezone sets the fallback timezone (IANA name) for cron entries
// that don't specify their own timezone. Defaults to UTC.
func WithDefaultTimezone(tz string) ServerOption {
	return func(cfg *serverConfig) { cfg.defaultTimezone = tz }
}

// WithSchedulerEnabled controls whether the scheduler goroutine is started.
// Defaults to true. Set to false for worker-only instances.
func WithSchedulerEnabled(enabled bool) ServerOption {
	return func(cfg *serverConfig) { cfg.schedulerEnabled = enabled }
}

// WithSchedulerPollInterval sets the poll interval for the scheduler engine.
// Defaults to 1s.
func WithSchedulerPollInterval(d time.Duration) ServerOption {
	return func(cfg *serverConfig) {
		if d > 0 {
			cfg.schedulerPollInterval = d
		}
	}
}

// WithLogLevel sets the log level for the auto-created logger.
// Only takes effect if no WithLogger() is provided.
// Valid values: "debug", "info", "warn", "error".
func WithLogLevel(level string) ServerOption {
	return func(cfg *serverConfig) { cfg.logLevel = level }
}

// Server manages worker pools and processes jobs.
type Server struct {
	cfg      *serverConfig
	rc       *RedisClient
	scripts  *scriptRegistry
	handlers map[string]Handler
	pools    []*pool
	logger   *slog.Logger

	// jobTypePool tracks which pool each job type is assigned to.
	// Key: job type, Value: pool name.
	// Populated by Pool() (explicit) and Handle() with Workers() (implicit).
	jobTypePool map[string]string

	// poolNames tracks registered pool names to detect duplicates.
	poolNames map[string]bool

	// cronEntries holds registered cron entries indexed by ID.
	cronEntries map[string]*CronEntry

	mu      sync.RWMutex
	running bool
	stopCh  chan struct{}
	stopOnce sync.Once
}

// NewServer creates a new Server with the given options.
func NewServer(opts ...ServerOption) (*Server, error) {
	cfg := &serverConfig{
		globalTimeout:         defaultGlobalTimeout,
		gracePeriod:           defaultGracePeriod,
		shutdownTimeout:       defaultShutdownTimeout,
		schedulerEnabled:      true,
		schedulerPollInterval: defaultSchedulerPollInterval,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	if cfg.logger == nil {
		cfg.logger = newLoggerFromLevel(cfg.logLevel)
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
		cfg:         cfg,
		rc:          rc,
		scripts:     sr,
		handlers:    make(map[string]Handler),
		jobTypePool: make(map[string]string),
		poolNames:   make(map[string]bool),
		cronEntries: make(map[string]*CronEntry),
		logger:      cfg.logger,
		stopCh:      make(chan struct{}),
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
		// Check for conflict: job type already assigned to another pool
		if existingPool, ok := s.jobTypePool[jobType]; ok {
			return fmt.Errorf("%w: job type %q already assigned to pool %q, cannot create implicit pool",
				ErrJobTypeConflict, jobType, existingPool)
		}

		// Implicit pool: dedicated pool + queue per job type
		pcfg := newDefaultPoolConfig(jobType, jobType)
		pcfg.concurrency = hcfg.workers
		pcfg.gracePeriod = s.cfg.gracePeriod
		s.pools = append(s.pools, newPool(pcfg, s))
		s.jobTypePool[jobType] = jobType
		s.poolNames[jobType] = true
	}

	return nil
}

// Pool registers an explicit worker pool configuration (Layer 3).
// This allows grouping multiple job types into a single pool with shared
// concurrency, custom queues, dequeue strategy, and retry policy.
//
// Must be called before Start(). Returns an error if the pool name is
// already registered or if the configuration is invalid.
func (s *Server) Pool(cfg PoolConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("cannot register pool while server is running")
	}

	if cfg.Name == "" {
		return fmt.Errorf("pool name must not be empty")
	}

	if s.poolNames[cfg.Name] {
		return fmt.Errorf("%w: %s", ErrDuplicatePool, cfg.Name)
	}

	// Check for conflict: job type already assigned to another pool
	for _, jt := range cfg.JobTypes {
		if existingPool, ok := s.jobTypePool[jt]; ok {
			return fmt.Errorf("%w: job type %q already assigned to pool %q, cannot assign to %q",
				ErrJobTypeConflict, jt, existingPool, cfg.Name)
		}
	}

	pcfg := cfg.toInternal(s.cfg.gracePeriod)
	s.pools = append(s.pools, newPool(pcfg, s))
	s.poolNames[cfg.Name] = true

	// Track job type → pool assignments
	for _, jt := range cfg.JobTypes {
		s.jobTypePool[jt] = cfg.Name
	}

	return nil
}

// Schedule registers a cron entry for recurring job scheduling.
// Must be called before Start(). The entry's cron expression is parsed and
// validated. Duplicate IDs are rejected with ErrDuplicateCronEntry.
func (s *Server) Schedule(entry CronEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return fmt.Errorf("cannot register cron entry while server is running")
	}

	entry.applyDefaults()
	if err := entry.validate(); err != nil {
		return err
	}

	if _, exists := s.cronEntries[entry.ID]; exists {
		return fmt.Errorf("%w: %s", ErrDuplicateCronEntry, entry.ID)
	}

	now := time.Now()
	entry.CreatedAt = now.Unix()
	entry.UpdatedAt = now.Unix()

	s.cronEntries[entry.ID] = &entry
	return nil
}

// Start begins processing jobs. It blocks until the server is stopped
// via signal (SIGTERM/SIGINT) or the context is cancelled.
// The server is single-use: after Stop or Start returns, create a new Server
// instance instead of calling Start again.
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

	// Save cron entries to Redis
	if err := s.saveCronEntries(ctx); err != nil {
		return fmt.Errorf("saving cron entries: %w", err)
	}

	s.logger.Info("server starting",
		"pools", len(s.pools),
		"handlers", len(s.handlers),
		"cron_entries", len(s.cronEntries),
	)

	// Create a cancellable context for all pools
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(sigCh)

	// Start all pools and optionally the scheduler
	var wg sync.WaitGroup

	// Scheduler engine (handles retry/delayed jobs + cron evaluation)
	if s.cfg.schedulerEnabled {
		sched := newSchedulerEngine(s)
		wg.Add(1)
		go func() {
			defer wg.Done()
			sched.run(ctx)
		}()
	}

	for _, p := range s.pools {
		wg.Add(1)
		go func(p *pool) {
			defer wg.Done()
			p.run(ctx)
		}(p)
	}

	// Wait for shutdown signal, context cancellation, or Stop() call.
	select {
	case sig := <-sigCh:
		s.logger.Info("received signal, initiating shutdown", "signal", sig)
		cancel()
	case <-ctx.Done():
		s.logger.Info("context cancelled, initiating shutdown")
	case <-s.stopCh:
		s.logger.Info("Stop() called, initiating shutdown")
		cancel()
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
		s.logger.Warn("shutdown timeout reached, waiting for goroutines before closing Redis",
			"timeout", s.cfg.shutdownTimeout)
		// Wait for goroutines to finish even after timeout, so they don't
		// use a closed Redis connection. Hard limit capped at 10s to avoid
		// doubling the configured shutdown timeout.
		hardLimit := s.cfg.shutdownTimeout
		if hardLimit > 10*time.Second {
			hardLimit = 10 * time.Second
		}
		hardTimeout := time.NewTimer(hardLimit)
		select {
		case <-done:
			hardTimeout.Stop()
		case <-hardTimeout.C:
			s.logger.Error("hard shutdown timeout reached, closing Redis with goroutines still running")
		}
	}

	s.mu.Lock()
	s.running = false
	s.mu.Unlock()

	return s.rc.Close()
}

// Stop signals the server to stop.
func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
}

// ensureDefaultPool creates a "default" pool for handlers not assigned to any pool.
// If a catch-all pool (job_types: ["*"]) is configured, unassigned handlers are
// routed to that pool instead of creating a new default pool.
func (s *Server) ensureDefaultPool() {
	var unassigned []string
	for jt := range s.handlers {
		if _, assigned := s.jobTypePool[jt]; !assigned {
			unassigned = append(unassigned, jt)
		}
	}

	if len(unassigned) == 0 {
		return
	}

	// If a catch-all pool is configured, assign unassigned handlers to it.
	if s.cfg.catchAllPool != "" {
		for _, jt := range unassigned {
			s.jobTypePool[jt] = s.cfg.catchAllPool
		}
		s.logger.Info("assigned unassigned handlers to catch-all pool",
			"pool", s.cfg.catchAllPool,
			"handlers", unassigned,
		)
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

// saveCronEntries persists registered cron entries to Redis.
func (s *Server) saveCronEntries(ctx context.Context) error {
	if len(s.cronEntries) == 0 {
		return nil
	}

	entriesKey := s.rc.Key("cron", "entries")
	pipe := s.rc.rdb.Pipeline()

	for id, entry := range s.cronEntries {
		data, err := json.Marshal(entry)
		if err != nil {
			return fmt.Errorf("marshaling cron entry %q: %w", id, err)
		}
		pipe.HSet(ctx, entriesKey, id, string(data))
	}

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("writing cron entries to redis: %w", err)
	}

	s.logger.Info("cron entries saved to redis", "count", len(s.cronEntries))
	return nil
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
