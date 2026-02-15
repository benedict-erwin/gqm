package gqm

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
	"github.com/benedict-erwin/gqm/lua"
)

// scriptRegistry holds loaded Lua scripts keyed by name.
type scriptRegistry struct {
	scripts map[string]*redis.Script
}

func newScriptRegistry() *scriptRegistry {
	return &scriptRegistry{
		scripts: make(map[string]*redis.Script),
	}
}

// load reads all .lua files from the embedded FS and registers them.
func (sr *scriptRegistry) load() error {
	entries, err := lua.Scripts.ReadDir(".")
	if err != nil {
		return fmt.Errorf("reading lua scripts dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		data, err := lua.Scripts.ReadFile(name)
		if err != nil {
			return fmt.Errorf("reading lua script %s: %w", name, err)
		}
		// Strip .lua extension for the key
		key := name[:len(name)-4]
		sr.scripts[key] = redis.NewScript(string(data))
	}

	return nil
}

// run executes a named Lua script.
func (sr *scriptRegistry) run(ctx context.Context, rdb *redis.Client, name string, keys []string, args ...any) *redis.Cmd {
	script, ok := sr.scripts[name]
	if !ok {
		cmd := redis.NewCmd(ctx)
		cmd.SetErr(fmt.Errorf("lua script %q not found", name))
		return cmd
	}
	return script.Run(ctx, rdb, keys, args...)
}
