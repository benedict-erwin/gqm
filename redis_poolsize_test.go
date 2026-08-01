package gqm

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// PoolSize was previously unreachable: NewRedisClient passed Addr, Password, DB
// and TLSConfig to go-redis and nothing else, so the only way to change it was
// to build a *redis.Client by hand and inject it — which means reaching past
// GQM's own API into go-redis.
//
// These check the value actually arrives at the client, not merely that it was
// stored on the config struct. A setting that is accepted and then dropped is
// worse than one that does not exist, because it looks like it worked.

func TestWithRedisPoolSize_ReachesTheClient(t *testing.T) {
	rc, err := NewRedisClient(WithRedisAddr("localhost:6379"), WithRedisPoolSize(137))
	if err != nil {
		t.Fatalf("NewRedisClient: %v", err)
	}
	defer rc.Close()

	if got := rc.rdb.Options().PoolSize; got != 137 {
		t.Errorf("PoolSize = %d, want 137", got)
	}
}

// Unset must keep go-redis's own default rather than silently becoming zero,
// which go-redis would then reinterpret.
func TestWithRedisPoolSize_UnsetKeepsGoRedisDefault(t *testing.T) {
	rc, err := NewRedisClient(WithRedisAddr("localhost:6379"))
	if err != nil {
		t.Fatalf("NewRedisClient: %v", err)
	}
	defer rc.Close()

	want := 10 * runtime.GOMAXPROCS(0)
	if got := rc.rdb.Options().PoolSize; got != want {
		t.Errorf("PoolSize = %d, want the go-redis default of %d", got, want)
	}
}

// An injected client is used as-is; GQM must not overwrite the pool the caller
// already sized.
func TestWithRedisPoolSize_InjectedClientIsNotOverridden(t *testing.T) {
	rc, err := NewRedisClient(WithRedisAddr("localhost:6379"), WithRedisPoolSize(200))
	if err != nil {
		t.Fatalf("NewRedisClient: %v", err)
	}
	defer rc.Close()

	rc2, err := NewRedisClient(WithRedisClient(rc.rdb), WithRedisPoolSize(9))
	if err != nil {
		t.Fatalf("NewRedisClient with injected client: %v", err)
	}
	if got := rc2.rdb.Options().PoolSize; got != 200 {
		t.Errorf("PoolSize = %d, want 200 — an injected client must not be re-sized", got)
	}
}

func TestConfig_RedisPoolSize(t *testing.T) {
	dir := t.TempDir()
	write := func(t *testing.T, body string) string {
		t.Helper()
		p := filepath.Join(dir, strings.ReplaceAll(t.Name(), "/", "_")+".yaml")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatalf("writing config: %v", err)
		}
		return p
	}

	t.Run("accepted and applied", func(t *testing.T) {
		cfg, err := LoadConfigFile(write(t, `
redis:
  addr: "localhost:6379"
  pool_size: 64
`))
		if err != nil {
			t.Fatalf("LoadConfigFile: %v", err)
		}
		if cfg.Redis.PoolSize != 64 {
			t.Fatalf("parsed pool_size = %d, want 64", cfg.Redis.PoolSize)
		}
		rc, err := NewRedisClient(WithRedisAddr(cfg.Redis.Addr), WithRedisPoolSize(cfg.Redis.PoolSize))
		if err != nil {
			t.Fatalf("NewRedisClient: %v", err)
		}
		defer rc.Close()
		if got := rc.rdb.Options().PoolSize; got != 64 {
			t.Errorf("PoolSize = %d, want 64", got)
		}
	})

	t.Run("negative is rejected", func(t *testing.T) {
		_, err := LoadConfigFile(write(t, `
redis:
  addr: "localhost:6379"
  pool_size: -1
`))
		if err == nil {
			t.Fatal("a negative pool_size was accepted")
		}
		if !strings.Contains(err.Error(), "pool_size") {
			t.Errorf("error does not name the offending field: %v", err)
		}
	})

	t.Run("omitted leaves the default", func(t *testing.T) {
		cfg, err := LoadConfigFile(write(t, `
redis:
  addr: "localhost:6379"
`))
		if err != nil {
			t.Fatalf("LoadConfigFile: %v", err)
		}
		if cfg.Redis.PoolSize != 0 {
			t.Errorf("pool_size = %d with nothing configured, want 0 so go-redis decides", cfg.Redis.PoolSize)
		}
	})
}
