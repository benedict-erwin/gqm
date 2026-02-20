package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInjectAPIKey_New(t *testing.T) {
	dir := t.TempDir()
	path := writeMinimalConfig(t, dir)

	if err := injectAPIKey(path, "grafana", "gqm_ak_test123", "admin"); err != nil {
		t.Fatalf("injectAPIKey: %v", err)
	}

	doc, _ := loadConfigNode(path)
	root := doc.Content[0]
	apiKeys := mapGet(mapGet(mapGet(root, "monitoring"), "api"), "api_keys")
	if apiKeys == nil {
		t.Fatal("api_keys section not created")
	}

	entry, idx := seqFindMapping(apiKeys, "name", "grafana")
	if entry == nil || idx < 0 {
		t.Fatal("API key not found")
	}
	if key := mapGet(entry, "key"); key == nil || key.Value != "gqm_ak_test123" {
		t.Errorf("key = %v, want 'gqm_ak_test123'", key)
	}
	if role := mapGet(entry, "role"); role == nil || role.Value != "admin" {
		t.Errorf("role = %v, want 'admin'", role)
	}
}

func TestInjectAPIKey_ViewerRole(t *testing.T) {
	dir := t.TempDir()
	path := writeMinimalConfig(t, dir)

	if err := injectAPIKey(path, "readonly", "gqm_ak_ro", "viewer"); err != nil {
		t.Fatalf("injectAPIKey: %v", err)
	}

	doc, _ := loadConfigNode(path)
	root := doc.Content[0]
	apiKeys := mapGet(mapGet(mapGet(root, "monitoring"), "api"), "api_keys")
	entry, _ := seqFindMapping(apiKeys, "name", "readonly")
	if role := mapGet(entry, "role"); role == nil || role.Value != "viewer" {
		t.Errorf("role = %v, want 'viewer'", role)
	}
}

func TestInjectAPIKey_Duplicate(t *testing.T) {
	dir := t.TempDir()
	path := writeMinimalConfig(t, dir)

	// Add first key.
	if err := injectAPIKey(path, "mykey", "gqm_ak_111", "admin"); err != nil {
		t.Fatal(err)
	}

	// Adding same name should fail.
	err := injectAPIKey(path, "mykey", "gqm_ak_222", "admin")
	if err == nil {
		t.Fatal("expected error for duplicate name")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("error = %q, want 'already exists'", err)
	}
}

func TestInjectAPIKey_BadConfig(t *testing.T) {
	err := injectAPIKey("/nonexistent/config.yaml", "key", "gqm_ak_x", "admin")
	if err == nil {
		t.Error("expected error for missing config")
	}
}

func TestRunAddAPIKey_Valid(t *testing.T) {
	dir := t.TempDir()
	path := writeMinimalConfig(t, dir)

	var stdout, stderr bytes.Buffer
	code := runAddAPIKey([]string{"--config", path, "--name", "testkey"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "API key added") {
		t.Error("expected success message")
	}
	if !strings.Contains(stdout.String(), "Key: gqm_ak_") {
		t.Error("expected key output")
	}
	if !strings.Contains(stdout.String(), "Restart") {
		t.Error("expected restart notice")
	}
}

func TestRunAddAPIKey_ViewerRole(t *testing.T) {
	dir := t.TempDir()
	path := writeMinimalConfig(t, dir)

	var stdout, stderr bytes.Buffer
	code := runAddAPIKey([]string{"--config", path, "--name", "ro", "--role", "viewer"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr.String())
	}
}

func TestRunAddAPIKey_NoConfig(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runAddAPIKey([]string{"--name", "test"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestRunAddAPIKey_NoName(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runAddAPIKey([]string{"--config", "test.yaml"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestRunAddAPIKey_InvalidRole(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runAddAPIKey([]string{"--config", "test.yaml", "--name", "x", "--role", "superadmin"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "invalid role") {
		t.Error("expected invalid role error")
	}
}

func TestRunAddAPIKey_DuplicateName(t *testing.T) {
	dir := t.TempDir()
	path := writeMinimalConfig(t, dir)

	var stdout, stderr bytes.Buffer
	// Add first.
	runAddAPIKey([]string{"--config", path, "--name", "dup"}, &stdout, &stderr)

	// Add duplicate.
	stdout.Reset()
	stderr.Reset()
	code := runAddAPIKey([]string{"--config", path, "--name", "dup"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "already exists") {
		t.Error("expected duplicate error")
	}
}

func TestRunAddAPIKey_BadFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runAddAPIKey([]string{"--invalid-flag"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestRunAddAPIKey_BadConfig(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runAddAPIKey([]string{"--config", "/nonexistent/path.yaml", "--name", "x"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestInjectAPIKey_MultipleKeys(t *testing.T) {
	dir := t.TempDir()
	path := writeMinimalConfig(t, dir)

	// Add two different keys.
	if err := injectAPIKey(path, "key1", "gqm_ak_aaa", "admin"); err != nil {
		t.Fatal(err)
	}
	if err := injectAPIKey(path, "key2", "gqm_ak_bbb", "viewer"); err != nil {
		t.Fatal(err)
	}

	doc, _ := loadConfigNode(path)
	root := doc.Content[0]
	apiKeys := mapGet(mapGet(mapGet(root, "monitoring"), "api"), "api_keys")
	if len(apiKeys.Content) != 2 {
		t.Errorf("api_keys count = %d, want 2", len(apiKeys.Content))
	}
}

func TestRunAddAPIKey_ConfigWithExistingMonitoring(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gqm.yaml")
	content := `redis:
  addr: localhost:6379
monitoring:
  api:
    enabled: true
`
	os.WriteFile(path, []byte(content), 0o644)

	var stdout, stderr bytes.Buffer
	code := runAddAPIKey([]string{"--config", path, "--name", "test"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr.String())
	}
}
