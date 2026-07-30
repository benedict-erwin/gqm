package gqm

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const defaultPrefix = "gqm:"

// RedisConfig holds Redis connection configuration.
type RedisConfig struct {
	Addr      string
	Password  string
	DB        int
	Prefix    string
	TLSConfig *tls.Config

	// existingClient allows injecting a pre-configured *redis.Client,
	// bypassing the built-in connection setup. When set, Addr, Password,
	// DB, and TLSConfig are ignored (only Prefix is still used).
	existingClient *redis.Client
}

// RedisClient wraps a go-redis client with GQM-specific helpers.
type RedisClient struct {
	rdb    *redis.Client
	prefix string
	owned  bool // true if GQM created the client (and should close it)
}

// NewRedisClient creates a new RedisClient with the given options.
// If WithRedisClient was used to inject an existing *redis.Client,
// it is used directly and connection options (Addr, Password, DB,
// TLSConfig) are ignored.
func NewRedisClient(opts ...RedisOption) (*RedisClient, error) {
	cfg := &RedisConfig{
		Addr:   "localhost:6379",
		Prefix: defaultPrefix,
	}
	for _, opt := range opts {
		opt(cfg)
	}

	rdb := cfg.existingClient
	owned := rdb == nil
	if rdb == nil {
		rdb = redis.NewClient(&redis.Options{
			Addr:      cfg.Addr,
			Password:  cfg.Password,
			DB:        cfg.DB,
			TLSConfig: cfg.TLSConfig,
		})
	}

	return &RedisClient{rdb: rdb, prefix: cfg.Prefix, owned: owned}, nil
}

// Ping checks the Redis connection.
func (rc *RedisClient) Ping(ctx context.Context) error {
	if err := rc.rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis ping: %w", err)
	}
	return nil
}

// Close closes the underlying Redis connection. If the client was
// injected via WithRedisClient, Close is a no-op — the caller retains
// ownership and is responsible for closing it.
func (rc *RedisClient) Close() error {
	if !rc.owned {
		return nil
	}
	return rc.rdb.Close()
}

// Key returns a prefixed Redis key.
func (rc *RedisClient) Key(parts ...string) string {
	key := rc.prefix
	for i, p := range parts {
		if i > 0 {
			key += ":"
		}
		key += p
	}
	return key
}

// Unwrap returns the underlying go-redis client for advanced operations.
func (rc *RedisClient) Unwrap() *redis.Client {
	return rc.rdb
}

// Prefix returns the key prefix used by this client.
func (rc *RedisClient) Prefix() string {
	return rc.prefix
}

// RedisOption configures a RedisConfig.
type RedisOption func(*RedisConfig)

// WithRedisAddr sets the Redis server address.
func WithRedisAddr(addr string) RedisOption {
	return func(cfg *RedisConfig) { cfg.Addr = addr }
}

// WithRedisPassword sets the Redis password.
func WithRedisPassword(password string) RedisOption {
	return func(cfg *RedisConfig) { cfg.Password = password }
}

// WithRedisDB sets the Redis database number.
func WithRedisDB(db int) RedisOption {
	return func(cfg *RedisConfig) { cfg.DB = db }
}

// WithPrefix sets the key prefix for all GQM keys.
func WithPrefix(prefix string) RedisOption {
	return func(cfg *RedisConfig) { cfg.Prefix = prefix }
}

// WithRedisTLS enables TLS for the Redis connection. Pass nil for default TLS
// configuration (system CA pool), or provide a custom *tls.Config for
// client certificates, custom CA, or other TLS settings.
func WithRedisTLS(tc *tls.Config) RedisOption {
	return func(cfg *RedisConfig) {
		if tc == nil {
			tc = &tls.Config{} //nolint:gosec // empty = system CA pool
		}
		cfg.TLSConfig = tc
	}
}

// WithRedisClient injects a pre-configured *redis.Client, bypassing
// the built-in connection setup. This enables Redis Sentinel, Cluster,
// or any custom configuration supported by go-redis.
//
// When used, connection options (WithRedisAddr, WithRedisPassword,
// WithRedisDB, WithRedisTLS) are ignored — only WithPrefix is still
// applied.
//
// Ownership: the caller retains ownership of rdb. GQM will NOT close
// it — you must close it yourself after the Client/Server is done.
// This is safe for sharing a single *redis.Client across multiple GQM
// instances.
//
// Example (Sentinel):
//
//	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
//	    MasterName:    "mymaster",
//	    SentinelAddrs: []string{"sentinel1:26379", "sentinel2:26379"},
//	})
//	defer rdb.Close()
//	client, _ := gqm.NewClient(gqm.WithRedisClient(rdb))
func WithRedisClient(rdb *redis.Client) RedisOption {
	return func(cfg *RedisConfig) { cfg.existingClient = rdb }
}

// queueRetention adds a retention expiry for a job that has just reached a
// terminal state to the given pipeline.
//
// ttl follows the convention used throughout retention handling: a positive
// value sets an expiry in seconds, 0 deletes the record immediately, and a
// negative value retains it permanently. Callers must only use this on terminal
// jobs — an expiry on a job that is still queued or running would be lost work.
func queueRetention(ctx context.Context, pipe redis.Pipeliner, jobKey string, ttl int) {
	switch {
	case ttl == 0:
		pipe.Del(ctx, jobKey)
	case ttl > 0:
		pipe.Expire(ctx, jobKey, time.Duration(ttl)*time.Second)
	}
}

// warnIfUnprotected writes a deliberately loud banner when Redis is reachable
// without a password.
//
// This is not a hypothetical: GQM stores dashboard session tokens in Redis as
// gqm:session:<token>. Anyone who can read the database can lift a token and
// use it as a cookie, which bypasses the authentication layer entirely rather
// than attacking it. Job payloads are readable the same way, and a writer can
// inject jobs that the application's own handlers will then execute.
//
// Starting without a password stays allowed — plenty of setups run Redis on a
// private network — but the operator should make that choice knowingly rather
// than inherit it from a default.
//
// The banner goes to the given writer (os.Stderr in practice) instead of the
// configured slog logger, because a warning that log_level can silence is not a
// warning. Production configurations commonly set the level to error, which is
// exactly the situation where this most needs to be seen. It is not logged at
// Error level either: this is not an error, and misclassifying it would pollute
// error metrics and alerting.
func (rc *RedisClient) warnIfUnprotected(w io.Writer, authEnabled bool) {
	opts := rc.rdb.Options()
	if opts == nil || opts.Password != "" {
		return
	}

	rule := strings.Repeat("!", 74)
	var b strings.Builder
	b.WriteString("\n" + rule + "\n")
	b.WriteString("!! REDIS HAS NO PASSWORD  \u2014  addr: " + opts.Addr + "\n!!\n")
	if isLoopbackAddr(opts.Addr) {
		b.WriteString("!! Any process on this host can read and write every queue.\n")
	} else {
		b.WriteString("!! This address is NOT loopback. Anything that can route to it has full\n")
		b.WriteString("!! read/write access to every queue, with no credentials at all.\n")
	}
	if authEnabled {
		b.WriteString("!!\n")
		b.WriteString("!! Dashboard session tokens are stored in Redis. Reading one is enough to\n")
		b.WriteString("!! become an authenticated admin \u2014 the login page is bypassed, not broken.\n")
	}
	b.WriteString("!!\n")
	b.WriteString("!! Job payloads are readable, and injected jobs run in your own handlers.\n")
	b.WriteString("!!\n")
	b.WriteString("!! Fix: set redis.password (or Redis ACLs); add redis.tls for remote Redis.\n")
	b.WriteString(rule + "\n\n")
	_, _ = io.WriteString(w, b.String())
}
