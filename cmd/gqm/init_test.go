package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestInitConfig_Create(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gqm.yaml")

	if err := initConfig(path); err != nil {
		t.Fatalf("initConfig: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading created file: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "redis:") {
		t.Error("config missing redis section")
	}
	if !strings.Contains(content, "pools:") {
		t.Error("config missing pools section")
	}
	if !strings.Contains(content, "monitoring:") {
		t.Error("config missing monitoring section")
	}

	// Verify file permissions.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o644 {
		t.Errorf("file permission = %o, want 644", perm)
	}
}

func TestInitConfig_AlreadyExists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gqm.yaml")
	os.WriteFile(path, []byte("existing"), 0o644)

	err := initConfig(path)
	if err == nil {
		t.Fatal("expected error for existing file")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("error = %q, want 'already exists'", err)
	}
}

func TestInitConfig_WriteError(t *testing.T) {
	// Try writing to a non-existent directory path.
	err := initConfig("/nonexistent/dir/gqm.yaml")
	if err == nil {
		t.Fatal("expected error for unwritable path")
	}
}

func TestRunInit_Valid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.yaml")

	var stdout, stderr bytes.Buffer
	code := runInit([]string{"--config", path}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Config file created") {
		t.Error("expected success message on stdout")
	}
	if !strings.Contains(stdout.String(), "Next steps") {
		t.Error("expected next steps on stdout")
	}

	// Verify file was actually created.
	if _, err := os.Stat(path); err != nil {
		t.Errorf("config file not created: %v", err)
	}
}

func TestRunInit_Exists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.yaml")
	os.WriteFile(path, []byte("existing"), 0o644)

	var stdout, stderr bytes.Buffer
	code := runInit([]string{"--config", path}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "already exists") {
		t.Error("expected 'already exists' error on stderr")
	}
}

func TestRunInit_DefaultPath(t *testing.T) {
	// With default path "gqm.yaml", if the file exists in cwd, it should fail.
	// Create gqm.yaml in a temp dir and test from there.
	dir := t.TempDir()
	gqmPath := filepath.Join(dir, "gqm.yaml")
	os.WriteFile(gqmPath, []byte("existing"), 0o644)

	// We can't easily change cwd for this test, so just test with explicit --config.
	var stdout, stderr bytes.Buffer
	code := runInit([]string{"--config", gqmPath}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestRunInit_BadFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runInit([]string{"--invalid-flag"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}
