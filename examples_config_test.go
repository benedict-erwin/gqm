package gqm

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

// The example directory is named _examples, and Go tooling ignores any path
// component starting with an underscore. "go build ./..." and "go vet ./..."
// therefore never reach it, so an example can stop compiling or stop loading
// and nothing turns red.
//
// That is not hypothetical. Removing queues[].priority and rejecting unknown
// YAML keys were both schema changes that could have invalidated a shipped
// config, and the only reason we knew they had not was a manual audit. An
// example a user copies is documentation that runs; it should be gated like it.
//
// Compilation is checked in CI, where a shell loop over the directories is the
// natural fit. Config loading is checked here instead, so it fails on a
// developer's machine before it ever reaches CI.

var passwordHashRe = regexp.MustCompile(`(password_hash:\s*")([^"]*)(")`)

func exampleConfigPaths(t *testing.T) []string {
	t.Helper()

	var paths []string
	err := filepath.WalkDir("_examples", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if ext := filepath.Ext(path); ext == ".yaml" || ext == ".yml" {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walking _examples: %v", err)
	}

	// A gate that silently finds nothing to check is worse than no gate: it
	// reports success forever. If the directory is renamed or the configs move,
	// this must fail rather than quietly pass.
	if len(paths) == 0 {
		t.Fatal("no example configs found under _examples — either they moved or this test is no longer checking anything")
	}
	return paths
}

// Every shipped example config must satisfy the current schema. A config that
// no longer loads is a broken instruction manual.
func TestExampleConfigs_Load(t *testing.T) {
	for _, path := range exampleConfigPaths(t) {
		t.Run(path, func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading %s: %v", path, err)
			}

			// One example deliberately ships an invalid password_hash so the
			// server refuses to start until the reader generates their own.
			// Validation stops at the first error, so without substituting a
			// usable hash this file would only ever prove that its placeholder
			// is rejected, hiding any schema breakage behind it.
			data, _ = withUsablePasswordHash(t, data)

			if _, err := LoadConfig(data); err != nil {
				t.Errorf("example config no longer loads: %v", err)
			}
		})
	}
}

// An example config is copied verbatim by people starting out. A working
// bcrypt hash committed in one is a publicly known admin password for every
// deployment that copied it — the reader cannot tell it is meant to be
// replaced, because it works.
//
// The invariant is not "the placeholder must be rejected" but "no example may
// ship a usable credential". Those differ exactly where it matters: someone
// replacing the placeholder with a real hash to make the example start would
// satisfy the first and violate the second.
func TestExampleConfigs_ShipNoUsablePasswordHash(t *testing.T) {
	for _, path := range exampleConfigPaths(t) {
		t.Run(path, func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading %s: %v", path, err)
			}
			for _, m := range passwordHashRe.FindAllSubmatch(data, -1) {
				if strings.HasPrefix(string(m[2]), "$2") {
					t.Errorf("ships a working bcrypt hash: anyone who copied this file has a known admin password. Use a placeholder and let the reader run 'gqm hash-password'.")
				}
			}
		})
	}
}

// Replaces any password_hash that is not a bcrypt hash with one that is,
// reporting whether it had to. The hash is generated rather than hardcoded so
// no credential-shaped constant lives in the repository.
func withUsablePasswordHash(t *testing.T, data []byte) ([]byte, bool) {
	t.Helper()

	replaced := false
	out := passwordHashRe.ReplaceAllFunc(data, func(m []byte) []byte {
		groups := passwordHashRe.FindSubmatch(m)
		if strings.HasPrefix(string(groups[2]), "$2") {
			return m
		}
		hash, err := bcrypt.GenerateFromPassword([]byte("example-config-test"), bcrypt.MinCost)
		if err != nil {
			t.Fatalf("generating bcrypt hash: %v", err)
		}
		replaced = true
		return append(append(append([]byte{}, groups[1]...), hash...), groups[3]...)
	})
	return out, replaced
}
