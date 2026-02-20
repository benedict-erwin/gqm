package main

import (
	"bytes"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/benedict-erwin/gqm/monitor/dashboard"
)

func TestExportDashboard(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "export")

	if err := exportDashboard(target); err != nil {
		t.Fatalf("exportDashboard: %v", err)
	}

	// Collect all files from the embedded FS.
	var wantFiles []string
	fs.WalkDir(dashboard.Assets, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			wantFiles = append(wantFiles, path)
		}
		return nil
	})

	if len(wantFiles) == 0 {
		t.Fatal("embedded dashboard has no files")
	}

	for _, f := range wantFiles {
		dest := filepath.Join(target, f)
		info, err := os.Stat(dest)
		if err != nil {
			t.Errorf("expected file %s: %v", f, err)
			continue
		}
		if info.IsDir() {
			t.Errorf("%s should be a file, not a directory", f)
			continue
		}

		// Verify content matches the embedded source.
		embedded, _ := fs.ReadFile(dashboard.Assets, f)
		exported, _ := os.ReadFile(dest)
		if len(embedded) != len(exported) {
			t.Errorf("%s: size mismatch: embedded=%d, exported=%d", f, len(embedded), len(exported))
		}
	}
}

func TestExportDashboard_CreatesDir(t *testing.T) {
	dir := t.TempDir()
	// Target is a nested path that doesn't exist yet.
	target := filepath.Join(dir, "a", "b", "c")

	if err := exportDashboard(target); err != nil {
		t.Fatalf("exportDashboard: %v", err)
	}

	// Verify the directory was created and contains index.html.
	if _, err := os.Stat(filepath.Join(target, "index.html")); err != nil {
		t.Errorf("index.html not found: %v", err)
	}
}

func TestRunDashboard_Valid(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "out")

	var stdout, stderr bytes.Buffer
	code := runDashboard([]string{"export", target}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Dashboard exported") {
		t.Error("expected success message")
	}

	// Verify files were exported.
	if _, err := os.Stat(filepath.Join(target, "index.html")); err != nil {
		t.Errorf("index.html not found: %v", err)
	}
}

func TestRunDashboard_NoSubcommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runDashboard(nil, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "Usage:") {
		t.Error("expected usage on stderr")
	}
}

func TestRunDashboard_InvalidSubcommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runDashboard([]string{"invalid"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestRunDashboard_OnlyExport(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runDashboard([]string{"export"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1 (missing dir arg)", code)
	}
}

func TestExportDashboard_ReadOnlyDir(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("test requires non-root user (file permission checks are bypassed by root)")
	}

	dir := t.TempDir()
	roDir := filepath.Join(dir, "readonly")
	os.Mkdir(roDir, 0o555)
	t.Cleanup(func() { os.Chmod(roDir, 0o755) })

	target := filepath.Join(roDir, "nested", "output")
	err := exportDashboard(target)
	if err == nil {
		t.Error("expected error for read-only directory")
	}
}

func TestExportDashboard_WriteFileError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("test requires non-root user (file permission checks are bypassed by root)")
	}

	dir := t.TempDir()
	target := filepath.Join(dir, "locked")
	// Create the target dir first, then make it read-only.
	// MkdirAll for "." succeeds (dir exists) but WriteFile fails.
	os.Mkdir(target, 0o755)
	os.Chmod(target, 0o555)
	t.Cleanup(func() { os.Chmod(target, 0o755) })

	err := exportDashboard(target)
	if err == nil {
		t.Error("expected error when writing to read-only directory")
	}
}

func TestRunDashboard_ExportError(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("test requires non-root user (file permission checks are bypassed by root)")
	}

	dir := t.TempDir()
	roDir := filepath.Join(dir, "readonly")
	os.Mkdir(roDir, 0o555)
	t.Cleanup(func() { os.Chmod(roDir, 0o755) })

	var stdout, stderr bytes.Buffer
	code := runDashboard([]string{"export", filepath.Join(roDir, "out")}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "export dashboard") {
		t.Error("expected export error on stderr")
	}
}
