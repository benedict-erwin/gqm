package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
	"gopkg.in/yaml.v3"
)

// stubPasswordReader returns a function that supplies the given password.
func stubPasswordReader(password string) func() (string, error) {
	return func() (string, error) { return password, nil }
}

// stubPasswordReaderErr returns a function that returns an error.
func stubPasswordReaderErr(err error) func() (string, error) {
	return func() (string, error) { return "", err }
}

func withPasswordReader(t *testing.T, reader func() (string, error)) {
	t.Helper()
	old := passwordReader
	passwordReader = reader
	t.Cleanup(func() { passwordReader = old })
}

// writeMinimalConfig creates a minimal YAML config file for testing.
func writeMinimalConfig(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "gqm.yaml")
	content := "redis:\n  addr: localhost:6379\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestInjectPassword_NewUser(t *testing.T) {
	dir := t.TempDir()
	path := writeMinimalConfig(t, dir)

	if err := injectPassword(path, "admin", "$2a$10$hash"); err != nil {
		t.Fatalf("injectPassword: %v", err)
	}

	// Reload and verify.
	doc, err := loadConfigNode(path)
	if err != nil {
		t.Fatal(err)
	}
	root := doc.Content[0]
	monitoring := mapGet(root, "monitoring")
	if monitoring == nil {
		t.Fatal("monitoring section not created")
	}
	auth := mapGet(monitoring, "auth")
	if auth == nil {
		t.Fatal("auth section not created")
	}

	// Verify auth.enabled = true.
	enabled := mapGet(auth, "enabled")
	if enabled == nil || enabled.Value != "true" {
		t.Errorf("auth.enabled = %v, want true", enabled)
	}

	users := mapGet(auth, "users")
	if users == nil {
		t.Fatal("users section not created")
	}

	entry, idx := seqFindMapping(users, "username", "admin")
	if entry == nil || idx < 0 {
		t.Fatal("user entry not found")
	}
	hash := mapGet(entry, "password_hash")
	if hash == nil || hash.Value != "$2a$10$hash" {
		t.Errorf("password_hash = %v, want '$2a$10$hash'", hash)
	}
	role := mapGet(entry, "role")
	if role == nil || role.Value != "admin" {
		t.Errorf("role = %v, want 'admin'", role)
	}
}

func TestInjectPassword_UpdateExisting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gqm.yaml")
	content := `redis:
  addr: localhost:6379
monitoring:
  auth:
    enabled: true
    users:
      - username: admin
        password_hash: "$2a$10$oldhash"
        role: admin
`
	os.WriteFile(path, []byte(content), 0o644)

	if err := injectPassword(path, "admin", "$2a$10$newhash"); err != nil {
		t.Fatalf("injectPassword: %v", err)
	}

	doc, _ := loadConfigNode(path)
	root := doc.Content[0]
	users := mapGet(mapGet(mapGet(root, "monitoring"), "auth"), "users")
	entry, _ := seqFindMapping(users, "username", "admin")
	hash := mapGet(entry, "password_hash")
	if hash.Value != "$2a$10$newhash" {
		t.Errorf("password_hash = %q, want '$2a$10$newhash'", hash.Value)
	}
}

func TestInjectPassword_BadConfig(t *testing.T) {
	err := injectPassword("/nonexistent/config.yaml", "admin", "hash")
	if err == nil {
		t.Error("expected error for missing config")
	}
}

func TestInjectPassword_EnablesAuth(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gqm.yaml")
	content := `monitoring:
  auth:
    enabled: false
`
	os.WriteFile(path, []byte(content), 0o644)

	if err := injectPassword(path, "admin", "hash"); err != nil {
		t.Fatal(err)
	}

	doc, _ := loadConfigNode(path)
	root := doc.Content[0]
	enabled := mapGet(mapGet(mapGet(root, "monitoring"), "auth"), "enabled")
	if enabled.Value != "true" {
		t.Errorf("auth.enabled = %q, want 'true'", enabled.Value)
	}
}

func TestRunSetPassword_Valid(t *testing.T) {
	dir := t.TempDir()
	path := writeMinimalConfig(t, dir)
	withPasswordReader(t, stubPasswordReader("securePass123"))

	var stdout, stderr bytes.Buffer
	code := runSetPassword([]string{"--config", path, "--user", "admin"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "Password updated") {
		t.Error("expected success message")
	}
	if !strings.Contains(stdout.String(), "Restart") {
		t.Error("expected restart notice")
	}

	// Verify the password was actually set with bcrypt.
	doc, _ := loadConfigNode(path)
	root := doc.Content[0]
	users := mapGet(mapGet(mapGet(root, "monitoring"), "auth"), "users")
	entry, _ := seqFindMapping(users, "username", "admin")
	hash := mapGet(entry, "password_hash")
	if err := bcrypt.CompareHashAndPassword([]byte(hash.Value), []byte("securePass123")); err != nil {
		t.Errorf("bcrypt verify failed: %v", err)
	}
}

func TestRunSetPassword_NoConfig(t *testing.T) {
	withPasswordReader(t, stubPasswordReader("test"))

	var stdout, stderr bytes.Buffer
	code := runSetPassword([]string{"--user", "admin"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestRunSetPassword_NoUser(t *testing.T) {
	withPasswordReader(t, stubPasswordReader("test"))

	var stdout, stderr bytes.Buffer
	code := runSetPassword([]string{"--config", "some.yaml"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestRunSetPassword_EmptyPassword(t *testing.T) {
	withPasswordReader(t, stubPasswordReader("   "))

	dir := t.TempDir()
	path := writeMinimalConfig(t, dir)

	var stdout, stderr bytes.Buffer
	code := runSetPassword([]string{"--config", path, "--user", "admin"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "must not be empty") {
		t.Error("expected empty password error")
	}
}

func TestRunSetPassword_ReadError(t *testing.T) {
	withPasswordReader(t, stubPasswordReaderErr(fmt.Errorf("terminal not available")))

	dir := t.TempDir()
	path := writeMinimalConfig(t, dir)

	var stdout, stderr bytes.Buffer
	code := runSetPassword([]string{"--config", path, "--user", "admin"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "reading password") {
		t.Error("expected reading password error")
	}
}

func TestRunSetPassword_InjectError(t *testing.T) {
	withPasswordReader(t, stubPasswordReader("validpass"))

	var stdout, stderr bytes.Buffer
	code := runSetPassword([]string{"--config", "/nonexistent/path.yaml", "--user", "admin"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestRunSetPassword_BadFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runSetPassword([]string{"--invalid-flag"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestRunSetPassword_ResetPasswordAlias(t *testing.T) {
	// Verify reset-password routes to the same handler via run().
	dir := t.TempDir()
	path := writeMinimalConfig(t, dir)
	withPasswordReader(t, stubPasswordReader("mypass"))

	var stdout, stderr bytes.Buffer
	code := run([]string{"reset-password", "--config", path, "--user", "test"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr.String())
	}

	// Verify user was created.
	doc, _ := loadConfigNode(path)
	root := doc.Content[0]
	users := mapGet(mapGet(mapGet(root, "monitoring"), "auth"), "users")
	entry, _ := seqFindMapping(users, "username", "test")
	if entry == nil {
		t.Error("user 'test' not created via reset-password alias")
	}
}

func TestInjectPassword_AuthAlreadyEnabled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gqm.yaml")
	content := `monitoring:
  auth:
    enabled: true
`
	os.WriteFile(path, []byte(content), 0o644)

	if err := injectPassword(path, "admin", "hash"); err != nil {
		t.Fatal(err)
	}

	doc, _ := loadConfigNode(path)
	root := doc.Content[0]
	enabled := mapGet(mapGet(mapGet(root, "monitoring"), "auth"), "enabled")
	if enabled.Value != "true" {
		t.Errorf("auth.enabled changed to %q", enabled.Value)
	}

	// Verify only one "enabled" key.
	auth := mapGet(mapGet(root, "monitoring"), "auth")
	count := 0
	for i := 0; i < len(auth.Content)-1; i += 2 {
		if auth.Content[i].Value == "enabled" {
			count++
		}
	}
	if count != 1 {
		t.Errorf("found %d 'enabled' keys, want 1", count)
	}
}

func TestRunSetPassword_BcryptTooLong(t *testing.T) {
	// bcrypt rejects passwords >72 bytes. This exercises the bcrypt error path.
	longPass := strings.Repeat("a", 100)
	withPasswordReader(t, stubPasswordReader(longPass))

	dir := t.TempDir()
	path := writeMinimalConfig(t, dir)

	var stdout, stderr bytes.Buffer
	code := runSetPassword([]string{"--config", path, "--user", "admin"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "hashing password") {
		t.Error("expected bcrypt error on stderr")
	}
}

func TestSaveConfigNode_StatError(t *testing.T) {
	// saveConfigNode requires the file to exist (for Stat).
	doc := &yaml.Node{
		Kind: yaml.DocumentNode,
		Content: []*yaml.Node{
			{Kind: yaml.MappingNode},
		},
	}
	err := saveConfigNode("/nonexistent/path.yaml", doc)
	if err == nil {
		t.Error("expected error for missing file")
	}
	if !strings.Contains(err.Error(), "stat config file") {
		t.Errorf("error = %q, want stat error", err)
	}
}
