package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestGenerateAPIKey_Format(t *testing.T) {
	key, err := generateAPIKey()
	if err != nil {
		t.Fatalf("generateAPIKey: %v", err)
	}
	if !strings.HasPrefix(key, "gqm_ak_") {
		t.Errorf("key = %q, want gqm_ak_ prefix", key)
	}
	// gqm_ak_ (7) + 48 hex chars (24 bytes) = 55 total
	if len(key) != 55 {
		t.Errorf("key length = %d, want 55", len(key))
	}
}

func TestGenerateAPIKey_Unique(t *testing.T) {
	key1, _ := generateAPIKey()
	key2, _ := generateAPIKey()
	if key1 == key2 {
		t.Error("two consecutive keys should be different")
	}
}

func TestRunGenerateAPIKey_Valid(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runGenerateAPIKey(nil, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr.String())
	}

	key := strings.TrimSpace(stdout.String())
	if !strings.HasPrefix(key, "gqm_ak_") {
		t.Errorf("output = %q, want gqm_ak_ prefix", key)
	}
}

func TestRunGenerateAPIKey_ExtraArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runGenerateAPIKey([]string{"extra"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "Usage:") {
		t.Error("expected usage message on stderr")
	}
}
