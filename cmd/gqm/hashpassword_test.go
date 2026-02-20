package main

import (
	"bytes"
	"strings"
	"testing"

	"golang.org/x/crypto/bcrypt"
)

func TestHashPassword_Valid(t *testing.T) {
	hash, err := hashPassword("mypassword")
	if err != nil {
		t.Fatalf("hashPassword: %v", err)
	}
	if !strings.HasPrefix(hash, "$2a$") {
		t.Errorf("hash = %q, want bcrypt hash prefix", hash)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte("mypassword")); err != nil {
		t.Errorf("bcrypt verify failed: %v", err)
	}
}

func TestHashPassword_Empty(t *testing.T) {
	hash, err := hashPassword("")
	if err != nil {
		t.Fatalf("hashPassword: %v", err)
	}
	if err := bcrypt.CompareHashAndPassword([]byte(hash), []byte("")); err != nil {
		t.Errorf("bcrypt verify failed for empty password: %v", err)
	}
}

func TestRunHashPassword_Valid(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runHashPassword([]string{"testpass"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("exit code = %d, want 0; stderr: %s", code, stderr.String())
	}

	hash := strings.TrimSpace(stdout.String())
	if !strings.HasPrefix(hash, "$2a$") {
		t.Errorf("output = %q, want bcrypt hash", hash)
	}
}

func TestRunHashPassword_NoArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runHashPassword(nil, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "Usage:") {
		t.Error("expected usage message on stderr")
	}
}

func TestRunHashPassword_TooManyArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runHashPassword([]string{"a", "b"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}

func TestRunHashPassword_BcryptError(t *testing.T) {
	// bcrypt rejects passwords longer than 72 bytes.
	longPass := strings.Repeat("x", 100)
	var stdout, stderr bytes.Buffer
	code := runHashPassword([]string{longPass}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "hash password") {
		t.Error("expected hash password error on stderr")
	}
}

func TestHashPassword_TooLong(t *testing.T) {
	_, err := hashPassword(strings.Repeat("x", 100))
	if err == nil {
		t.Error("expected error for >72 byte password")
	}
}
