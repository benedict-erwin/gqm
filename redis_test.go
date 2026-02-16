package gqm

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
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
