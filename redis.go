package gqm

import (
	"context"
	"crypto/tls"
	"fmt"

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
