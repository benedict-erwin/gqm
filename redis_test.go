package gqm

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func testRedisClient(t *testing.T) *RedisClient {
	t.Helper()
	addr := os.Getenv("GQM_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	prefix := fmt.Sprintf("gqm:test:%d:", time.Now().UnixNano())
	rc, err := NewRedisClient(
		WithRedisAddr(addr),
		WithPrefix(prefix),
	)
	if err != nil {
		t.Fatalf("creating redis client: %v", err)
	}

	ctx := context.Background()
	if err := rc.Ping(ctx); err != nil {
		t.Skipf("requires Redis: %v", err)
	}

	t.Cleanup(func() { rc.Close() })
	return rc
}

func TestRedisClient_Ping(t *testing.T) {
	rc := testRedisClient(t)
	if err := rc.Ping(context.Background()); err != nil {
		t.Fatalf("ping failed: %v", err)
	}
}

func TestRedisClient_Key(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		parts  []string
		want   string
	}{
		{
			name:   "default prefix single part",
			prefix: "gqm:",
			parts:  []string{"job", "abc123"},
			want:   "gqm:job:abc123",
		},
		{
			name:   "custom prefix",
			prefix: "myapp:",
			parts:  []string{"queue", "email", "ready"},
			want:   "myapp:queue:email:ready",
		},
		{
			name:   "single part",
			prefix: "gqm:",
			parts:  []string{"queues"},
			want:   "gqm:queues",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc := &RedisClient{prefix: tt.prefix}
			got := rc.Key(tt.parts...)
			if got != tt.want {
				t.Errorf("Key(%v) = %q, want %q", tt.parts, got, tt.want)
			}
		})
	}
}

func TestNewRedisClient_Defaults(t *testing.T) {
	rc, err := NewRedisClient()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer rc.Close()

	if rc.prefix != defaultPrefix {
		t.Errorf("prefix = %q, want %q", rc.prefix, defaultPrefix)
	}
}

func TestNewRedisClient_WithOptions(t *testing.T) {
	rc, err := NewRedisClient(
		WithRedisAddr("localhost:6380"),
		WithRedisPassword("secret"),
		WithRedisDB(2),
		WithPrefix("custom:"),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer rc.Close()

	if rc.prefix != "custom:" {
		t.Errorf("prefix = %q, want %q", rc.prefix, "custom:")
	}
}

func TestNewRedisClient_WithTLS(t *testing.T) {
	rc, err := NewRedisClient(
		WithRedisAddr("localhost:6380"),
		WithRedisTLS(nil),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer rc.Close()

	// Verify TLS is configured on the underlying client
	opts := rc.Unwrap().Options()
	if opts.TLSConfig == nil {
		t.Error("TLSConfig should not be nil when WithRedisTLS is used")
	}
}

func TestNewRedisClient_WithExistingClient(t *testing.T) {
	addr := os.Getenv("GQM_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	// Create a raw go-redis client (simulates redis.NewFailoverClient).
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("requires Redis: %v", err)
	}

	// Inject it into GQM with a custom prefix.
	rc, err := NewRedisClient(WithRedisClient(rdb), WithPrefix("sentinel-test:"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify prefix is applied.
	if rc.prefix != "sentinel-test:" {
		t.Errorf("prefix = %q, want %q", rc.prefix, "sentinel-test:")
	}

	// Verify it uses the same underlying client.
	if rc.Unwrap() != rdb {
		t.Error("Unwrap() should return the injected client")
	}

	// Verify Close() is a no-op for injected clients.
	if err := rc.Close(); err != nil {
		t.Fatalf("Close() error: %v", err)
	}

	// The original client should still be usable after GQM's Close().
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("injected client should still work after GQM Close(): %v", err)
	}

	// Clean up: caller is responsible for closing.
	rdb.Close()
}

func TestNewRedisClient_OwnedClose(t *testing.T) {
	// When GQM creates the client, owned=true and Close() actually closes.
	rc, err := NewRedisClient(WithRedisAddr("localhost:6379"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !rc.owned {
		t.Error("owned should be true for internally-created client")
	}

	// When user injects a client, owned=false.
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	defer rdb.Close()

	rc2, err := NewRedisClient(WithRedisClient(rdb))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rc2.owned {
		t.Error("owned should be false for injected client")
	}
}

func TestNewRedisClient_WithNilClient(t *testing.T) {
	// WithRedisClient(nil) should fall through to normal client creation.
	rc, err := NewRedisClient(WithRedisClient(nil), WithPrefix("niltest:"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer rc.Close()

	if !rc.owned {
		t.Error("owned should be true when WithRedisClient(nil) is used")
	}
	if rc.prefix != "niltest:" {
		t.Errorf("prefix = %q, want %q", rc.prefix, "niltest:")
	}
	if rc.Unwrap() == nil {
		t.Error("Unwrap() should not be nil")
	}
}

func TestNewRedisClient_WithExistingClient_Ping(t *testing.T) {
	addr := os.Getenv("GQM_TEST_REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		t.Skipf("requires Redis: %v", err)
	}
	defer rdb.Close()

	// Verify GQM's Ping works through the injected client.
	rc, err := NewRedisClient(WithRedisClient(rdb))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := rc.Ping(context.Background()); err != nil {
		t.Fatalf("Ping through injected client failed: %v", err)
	}
}
