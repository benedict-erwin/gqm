package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRemoveAPIKey_Found(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gqm.yaml")
	content := `monitoring:
  api:
    api_keys:
      - name: grafana
        key: gqm_ak_111
        role: admin
      - name: tui
        key: gqm_ak_222
        role: viewer
`
	os.WriteFile(path, []byte(content), 0o644)

	if err := removeAPIKey(path, "grafana"); err != nil {
		t.Fatalf("removeAPIKey: %v", err)
	}

	// Verify only "tui" remains.
	doc, _ := loadConfigNode(path)
	root := doc.Content[0]
	apiKeys := mapGet(mapGet(mapGet(root, "monitoring"), "api"), "api_keys")
	if len(apiKeys.Content) != 1 {
		t.Fatalf("api_keys count = %d, want 1", len(apiKeys.Content))
	}
	if entry, _ := seqFindMapping(apiKeys, "name", "grafana"); entry != nil {
		t.Error("grafana key should have been removed")
	}
	if entry, _ := seqFindMapping(apiKeys, "name", "tui"); entry == nil {
		t.Error("tui key should still exist")
	}
}

func TestRemoveAPIKey_NotFound(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gqm.yaml")
	content := `monitoring:
  api:
    api_keys:
      - name: existing
        key: gqm_ak_111
`
	os.WriteFile(path, []byte(content), 0o644)

	err := removeAPIKey(path, "nonexistent")
	if err == nil {
		t.Fatal("expected error for missing key")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("error = %q, want 'not found'", err)
	}
}

func TestRemoveAPIKey_NoMonitoring(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gqm.yaml")
	os.WriteFile(path, []byte("redis:\n  addr: localhost\n"), 0o644)

	err := removeAPIKey(path, "test")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "no monitoring section") {
		t.Errorf("error = %q, want 'no monitoring section'", err)
	}
}

func TestRemoveAPIKey_NoAPI(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gqm.yaml")
	os.WriteFile(path, []byte("monitoring:\n  auth:\n    enabled: true\n"), 0o644)

	err := removeAPIKey(path, "test")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "no api section") {
		t.Errorf("error = %q, want 'no api section'", err)
	}
}

func TestRemoveAPIKey_NoAPIKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gqm.yaml")
	os.WriteFile(path, []byte("monitoring:\n  api:\n    enabled: true\n"), 0o644)

	err := removeAPIKey(path, "test")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "no api_keys section") {
		t.Errorf("error = %q, want 'no api_keys section'", err)
	}
}

func TestRemoveAPIKey_BadConfig(t *testing.T) {
	err := removeAPIKey("/nonexistent/config.yaml", "test")
	if err == nil {
		t.Error("expected error for missing config")
	}
}

func TestRunRevokeAPIKey_Valid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gqm.yaml")
	content := `monitoring:
  api:
    api_keys:
      - name: mykey
        key: gqm_ak_111
        role: admin
`
	os.WriteFile(path, []byte(content), 0o644)

	var stdout, stderr bytes.Buffer
	code := runRevokeAPIKey([]string{"--config", path, "--name", "mykey"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "revoked") {
		t.Error("expected revoked message")
	}
	if !strings.Contains(stdout.String(), "Restart") {
		t.Error("expected restart notice")
	}
}

func TestRunRevokeAPIKey_NoConfig(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runRevokeAPIKey([]string{"--name", "test"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestRunRevokeAPIKey_NoName(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runRevokeAPIKey([]string{"--config", "test.yaml"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestRunRevokeAPIKey_BadFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runRevokeAPIKey([]string{"--invalid-flag"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestRunRevokeAPIKey_NotFound(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gqm.yaml")
	content := `monitoring:
  api:
    api_keys:
      - name: existing
        key: gqm_ak_111
`
	os.WriteFile(path, []byte(content), 0o644)

	var stdout, stderr bytes.Buffer
	code := runRevokeAPIKey([]string{"--config", path, "--name", "missing"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "not found") {
		t.Error("expected not found error")
	}
}

func TestRunRevokeAPIKey_RemoveOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gqm.yaml")
	content := `monitoring:
  api:
    api_keys:
      - name: only
        key: gqm_ak_single
`
	os.WriteFile(path, []byte(content), 0o644)

	var stdout, stderr bytes.Buffer
	code := runRevokeAPIKey([]string{"--config", path, "--name", "only"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr.String())
	}

	// Verify empty api_keys.
	doc, _ := loadConfigNode(path)
	root := doc.Content[0]
	apiKeys := mapGet(mapGet(mapGet(root, "monitoring"), "api"), "api_keys")
	if len(apiKeys.Content) != 0 {
		t.Errorf("api_keys count = %d, want 0", len(apiKeys.Content))
	}
}
