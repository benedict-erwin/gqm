package gqm

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the top-level YAML configuration file.
type Config struct {
	Redis     RedisYAML       `yaml:"redis"`
	App       AppConfig       `yaml:"app"`
	Queues    []QueueDef      `yaml:"queues"`
	Pools     []PoolYAML      `yaml:"pools"`
	Scheduler SchedulerConfig `yaml:"scheduler"`
}

// RedisYAML holds Redis connection settings from YAML.
type RedisYAML struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
	Prefix   string `yaml:"prefix"`
}

// AppConfig holds application-level settings from YAML.
type AppConfig struct {
	Timezone        string `yaml:"timezone"`
	LogLevel        string `yaml:"log_level"`
	ShutdownTimeout int    `yaml:"shutdown_timeout"`   // seconds
	GlobalJobTimeout int   `yaml:"global_job_timeout"` // seconds
	GracePeriod     int    `yaml:"grace_period"`       // seconds
}

// QueueDef declares a named queue with optional priority metadata.
type QueueDef struct {
	Name     string `yaml:"name"`
	Priority int    `yaml:"priority"`
}

// PoolYAML holds worker pool configuration from YAML.
type PoolYAML struct {
	Name            string     `yaml:"name"`
	JobTypes        []string   `yaml:"job_types"`
	Queues          []string   `yaml:"queues"`
	Concurrency     int        `yaml:"concurrency"`
	JobTimeout      int        `yaml:"job_timeout"`      // seconds
	GracePeriod     int        `yaml:"grace_period"`     // seconds
	ShutdownTimeout int        `yaml:"shutdown_timeout"` // seconds
	DequeueStrategy string     `yaml:"dequeue_strategy"`
	Retry           *RetryYAML `yaml:"retry"`
}

// RetryYAML holds retry policy configuration from YAML.
type RetryYAML struct {
	MaxRetry    int    `yaml:"max_retry"`
	Intervals   []int  `yaml:"intervals"`
	Backoff     string `yaml:"backoff"`
	BackoffBase int    `yaml:"backoff_base"` // seconds
	BackoffMax  int    `yaml:"backoff_max"`  // seconds
}

// SchedulerConfig holds scheduler settings from YAML.
type SchedulerConfig struct {
	Enabled      *bool           `yaml:"enabled"`       // nil = true
	PollInterval int             `yaml:"poll_interval"` // seconds
	CronEntries  []CronEntryYAML `yaml:"cron_entries"`
}

// CronEntryYAML holds a cron entry from YAML.
type CronEntryYAML struct {
	ID            string `yaml:"id"`
	Name          string `yaml:"name"`
	CronExpr      string `yaml:"cron_expr"`
	Timezone      string `yaml:"timezone"`
	JobType       string `yaml:"job_type"`
	Queue         string `yaml:"queue"`
	Payload       string `yaml:"payload"`        // JSON string
	Timeout       int    `yaml:"timeout"`        // seconds
	MaxRetry      int    `yaml:"max_retry"`
	OverlapPolicy string `yaml:"overlap_policy"`
	Enabled       *bool  `yaml:"enabled"` // nil = true
}

// LoadConfig parses YAML bytes and validates the resulting configuration.
func LoadConfig(data []byte) (*Config, error) {
	cfg := &Config{}
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parsing config yaml: %w", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validating config: %w", err)
	}
	return cfg, nil
}

// LoadConfigFile reads a YAML file and returns a validated Config.
func LoadConfigFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}
	return LoadConfig(data)
}

// validate performs structural validation of the configuration.
func (c *Config) validate() error {
	// Redis
	if c.Redis.DB < 0 {
		return fmt.Errorf("redis.db must be >= 0")
	}

	// App
	if c.App.Timezone != "" {
		if _, err := time.LoadLocation(c.App.Timezone); err != nil {
			return fmt.Errorf("app.timezone: invalid IANA timezone %q: %w", c.App.Timezone, err)
		}
	}
	if c.App.LogLevel != "" {
		switch strings.ToLower(c.App.LogLevel) {
		case "debug", "info", "warn", "error":
			// ok
		default:
			return fmt.Errorf("app.log_level: must be one of debug, info, warn, error; got %q", c.App.LogLevel)
		}
	}
	if c.App.ShutdownTimeout < 0 {
		return fmt.Errorf("app.shutdown_timeout must be >= 0")
	}
	if c.App.GlobalJobTimeout < 0 {
		return fmt.Errorf("app.global_job_timeout must be >= 0")
	}
	if c.App.GracePeriod < 0 {
		return fmt.Errorf("app.grace_period must be >= 0")
	}

	// Queues
	queueNames := make(map[string]bool, len(c.Queues))
	for i, q := range c.Queues {
		if q.Name == "" {
			return fmt.Errorf("queues[%d].name must not be empty", i)
		}
		if !safeNameRe.MatchString(q.Name) || len(q.Name) > 128 {
			return fmt.Errorf("queues[%d].name %q: invalid characters or too long (max 128)", i, q.Name)
		}
		if queueNames[q.Name] {
			return fmt.Errorf("queues[%d].name %q: duplicate queue name", i, q.Name)
		}
		queueNames[q.Name] = true
	}

	// Pools
	poolNames := make(map[string]bool, len(c.Pools))
	jobTypeOwner := make(map[string]string) // job type → pool name
	catchAllCount := 0
	for i, p := range c.Pools {
		if p.Name == "" {
			return fmt.Errorf("pools[%d].name must not be empty", i)
		}
		if poolNames[p.Name] {
			return fmt.Errorf("pools[%d].name %q: duplicate pool name", i, p.Name)
		}
		poolNames[p.Name] = true

		if len(p.JobTypes) == 0 {
			return fmt.Errorf("pools[%d] %q: job_types must have at least one entry", i, p.Name)
		}

		// Check catch-all
		isCatchAll := len(p.JobTypes) == 1 && p.JobTypes[0] == "*"
		if isCatchAll {
			catchAllCount++
			if catchAllCount > 1 {
				return fmt.Errorf("pools[%d] %q: only one pool may have job_types: [\"*\"]", i, p.Name)
			}
		} else {
			// Check job type uniqueness across pools
			for _, jt := range p.JobTypes {
				if jt == "*" {
					return fmt.Errorf("pools[%d] %q: \"*\" must be the sole entry in job_types", i, p.Name)
				}
				if owner, ok := jobTypeOwner[jt]; ok {
					return fmt.Errorf("pools[%d] %q: job type %q already assigned to pool %q", i, p.Name, jt, owner)
				}
				jobTypeOwner[jt] = p.Name
			}
		}

		if p.Concurrency < 0 {
			return fmt.Errorf("pools[%d] %q: concurrency must be >= 0", i, p.Name)
		}

		if p.DequeueStrategy != "" {
			switch DequeueStrategy(p.DequeueStrategy) {
			case StrategyStrict, StrategyRoundRobin, StrategyWeighted:
				// ok
			default:
				return fmt.Errorf("pools[%d] %q: dequeue_strategy must be strict, round_robin, or weighted; got %q",
					i, p.Name, p.DequeueStrategy)
			}
		}

		if p.Retry != nil {
			if err := p.Retry.validate(i, p.Name); err != nil {
				return err
			}
		}
	}

	// Scheduler
	if c.Scheduler.PollInterval < 0 {
		return fmt.Errorf("scheduler.poll_interval must be >= 0")
	}

	cronIDs := make(map[string]bool, len(c.Scheduler.CronEntries))
	for i, ce := range c.Scheduler.CronEntries {
		if ce.ID == "" {
			return fmt.Errorf("scheduler.cron_entries[%d].id must not be empty", i)
		}
		if cronIDs[ce.ID] {
			return fmt.Errorf("scheduler.cron_entries[%d].id %q: duplicate cron entry ID", i, ce.ID)
		}
		cronIDs[ce.ID] = true

		if ce.CronExpr == "" {
			return fmt.Errorf("scheduler.cron_entries[%d] %q: cron_expr must not be empty", i, ce.ID)
		}
		if _, err := ParseCronExpr(ce.CronExpr); err != nil {
			return fmt.Errorf("scheduler.cron_entries[%d] %q: invalid cron_expr: %w", i, ce.ID, err)
		}

		if ce.JobType == "" {
			return fmt.Errorf("scheduler.cron_entries[%d] %q: job_type must not be empty", i, ce.ID)
		}

		if ce.Timezone != "" {
			if _, err := time.LoadLocation(ce.Timezone); err != nil {
				return fmt.Errorf("scheduler.cron_entries[%d] %q: invalid timezone %q: %w", i, ce.ID, ce.Timezone, err)
			}
		}

		if ce.OverlapPolicy != "" {
			switch OverlapPolicy(ce.OverlapPolicy) {
			case OverlapSkip, OverlapAllow, OverlapReplace:
				// ok
			default:
				return fmt.Errorf("scheduler.cron_entries[%d] %q: overlap_policy must be skip, allow, or replace; got %q",
					i, ce.ID, ce.OverlapPolicy)
			}
		}

		if ce.Payload != "" {
			if !json.Valid([]byte(ce.Payload)) {
				return fmt.Errorf("scheduler.cron_entries[%d] %q: payload must be valid JSON", i, ce.ID)
			}
		}
	}

	return nil
}

// validate checks a RetryYAML for consistency.
func (r *RetryYAML) validate(poolIdx int, poolName string) error {
	if r.Backoff != "" {
		switch BackoffType(r.Backoff) {
		case BackoffFixed, BackoffExponential, BackoffCustom:
			// ok
		default:
			return fmt.Errorf("pools[%d] %q: retry.backoff must be fixed, exponential, or custom; got %q",
				poolIdx, poolName, r.Backoff)
		}
	}
	if BackoffType(r.Backoff) == BackoffCustom && len(r.Intervals) == 0 {
		return fmt.Errorf("pools[%d] %q: retry.intervals required when backoff=custom", poolIdx, poolName)
	}
	return nil
}

// toPoolConfig converts a PoolYAML to the public PoolConfig.
func (p *PoolYAML) toPoolConfig() PoolConfig {
	pc := PoolConfig{
		Name:     p.Name,
		JobTypes: p.JobTypes,
		Queues:   p.Queues,
	}
	if p.Concurrency > 0 {
		pc.Concurrency = p.Concurrency
	}
	if p.JobTimeout > 0 {
		pc.JobTimeout = time.Duration(p.JobTimeout) * time.Second
	}
	if p.GracePeriod > 0 {
		pc.GracePeriod = time.Duration(p.GracePeriod) * time.Second
	}
	if p.ShutdownTimeout > 0 {
		pc.ShutdownTimeout = time.Duration(p.ShutdownTimeout) * time.Second
	}
	if p.DequeueStrategy != "" {
		pc.DequeueStrategy = DequeueStrategy(p.DequeueStrategy)
	}
	if p.Retry != nil {
		pc.RetryPolicy = p.Retry.toRetryPolicy()
	}
	return pc
}

// toRetryPolicy converts a RetryYAML to a RetryPolicy.
func (r *RetryYAML) toRetryPolicy() *RetryPolicy {
	rp := &RetryPolicy{
		MaxRetry:  r.MaxRetry,
		Intervals: r.Intervals,
	}
	if r.Backoff != "" {
		rp.Backoff = BackoffType(r.Backoff)
	}
	if r.BackoffBase > 0 {
		rp.BackoffBase = time.Duration(r.BackoffBase) * time.Second
	}
	if r.BackoffMax > 0 {
		rp.BackoffMax = time.Duration(r.BackoffMax) * time.Second
	}
	return rp
}

// toCronEntry converts a CronEntryYAML to a CronEntry.
func (ce *CronEntryYAML) toCronEntry() CronEntry {
	entry := CronEntry{
		ID:       ce.ID,
		Name:     ce.Name,
		CronExpr: ce.CronExpr,
		Timezone: ce.Timezone,
		JobType:  ce.JobType,
		Queue:    ce.Queue,
		Timeout:  ce.Timeout,
		MaxRetry: ce.MaxRetry,
	}

	if ce.Payload != "" {
		var p Payload
		// Payload was already validated as valid JSON in validate().
		_ = json.Unmarshal([]byte(ce.Payload), &p)
		entry.Payload = p
	}

	if ce.OverlapPolicy != "" {
		entry.OverlapPolicy = OverlapPolicy(ce.OverlapPolicy)
	}

	// nil = true (enabled by default)
	entry.Enabled = ce.Enabled == nil || *ce.Enabled

	return entry
}

// NewServerFromConfig creates a Server from a Config, with optional code overrides.
// The config serves as the base configuration; ServerOption values always win.
func NewServerFromConfig(cfg *Config, opts ...ServerOption) (*Server, error) {
	// Build Redis options from config
	var redisOpts []RedisOption
	if cfg.Redis.Addr != "" {
		redisOpts = append(redisOpts, WithRedisAddr(cfg.Redis.Addr))
	}
	if cfg.Redis.Password != "" {
		redisOpts = append(redisOpts, WithRedisPassword(cfg.Redis.Password))
	}
	if cfg.Redis.DB != 0 {
		redisOpts = append(redisOpts, WithRedisDB(cfg.Redis.DB))
	}
	if cfg.Redis.Prefix != "" {
		redisOpts = append(redisOpts, WithPrefix(cfg.Redis.Prefix))
	}

	// Build ServerOptions from config (these form the base; user opts override)
	var serverOpts []ServerOption

	if len(redisOpts) > 0 {
		serverOpts = append(serverOpts, WithServerRedisOpts(redisOpts...))
	}

	if cfg.App.GlobalJobTimeout > 0 {
		serverOpts = append(serverOpts, WithGlobalTimeout(time.Duration(cfg.App.GlobalJobTimeout)*time.Second))
	}
	if cfg.App.GracePeriod > 0 {
		serverOpts = append(serverOpts, WithGracePeriod(time.Duration(cfg.App.GracePeriod)*time.Second))
	}
	if cfg.App.ShutdownTimeout > 0 {
		serverOpts = append(serverOpts, WithShutdownTimeout(time.Duration(cfg.App.ShutdownTimeout)*time.Second))
	}
	if cfg.App.Timezone != "" {
		serverOpts = append(serverOpts, WithDefaultTimezone(cfg.App.Timezone))
	}
	if cfg.App.LogLevel != "" {
		serverOpts = append(serverOpts, WithLogLevel(cfg.App.LogLevel))
	}

	// Scheduler
	if cfg.Scheduler.Enabled != nil {
		serverOpts = append(serverOpts, WithSchedulerEnabled(*cfg.Scheduler.Enabled))
	}
	if cfg.Scheduler.PollInterval > 0 {
		serverOpts = append(serverOpts, WithSchedulerPollInterval(time.Duration(cfg.Scheduler.PollInterval)*time.Second))
	}

	// Detect catch-all pool
	for _, p := range cfg.Pools {
		if len(p.JobTypes) == 1 && p.JobTypes[0] == "*" {
			serverOpts = append(serverOpts, func(sc *serverConfig) {
				sc.catchAllPool = p.Name
			})
			break
		}
	}

	// User options come last (override config)
	serverOpts = append(serverOpts, opts...)

	server, err := NewServer(serverOpts...)
	if err != nil {
		return nil, fmt.Errorf("creating server from config: %w", err)
	}

	// Register pools (skip catch-all — it's handled via ensureDefaultPool)
	for _, py := range cfg.Pools {
		isCatchAll := len(py.JobTypes) == 1 && py.JobTypes[0] == "*"
		if isCatchAll {
			// Catch-all pool: register with empty job types.
			// The actual assignment happens in ensureDefaultPool at Start().
			pc := py.toPoolConfig()
			pc.JobTypes = nil // Clear so Pool() doesn't track specific types
			if err := server.Pool(pc); err != nil {
				server.rc.Close()
				return nil, fmt.Errorf("registering catch-all pool %q: %w", py.Name, err)
			}
			continue
		}
		pc := py.toPoolConfig()
		if err := server.Pool(pc); err != nil {
			server.rc.Close()
			return nil, fmt.Errorf("registering pool %q: %w", py.Name, err)
		}
	}

	// Register cron entries
	for _, ceYAML := range cfg.Scheduler.CronEntries {
		ce := ceYAML.toCronEntry()
		if err := server.Schedule(ce); err != nil {
			server.rc.Close()
			return nil, fmt.Errorf("registering cron entry %q: %w", ceYAML.ID, err)
		}
	}

	return server, nil
}
