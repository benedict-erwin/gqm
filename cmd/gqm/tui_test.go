package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestValidateTUIArgs_Valid(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{"http localhost", "http://localhost:8080"},
		{"https domain", "https://gqm.example.com"},
		{"http with path", "http://localhost:8080/api"},
		{"https with port", "https://example.com:443"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := validateTUIArgs(tt.url); err != nil {
				t.Errorf("validateTUIArgs(%q) = %v, want nil", tt.url, err)
			}
		})
	}
}

func TestValidateTUIArgs_Invalid(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		wantErr string
	}{
		{"empty", "", "required"},
		{"no scheme", "localhost:8080", "valid URL"},
		{"ftp scheme", "ftp://example.com", "valid URL"},
		{"no host", "http://", "valid URL"},
		{"just path", "/api/v1", "valid URL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTUIArgs(tt.url)
			if err == nil {
				t.Fatalf("validateTUIArgs(%q) = nil, want error", tt.url)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestRunTUI_NoURL(t *testing.T) {
	// Unset env vars for this test.
	t.Setenv("GQM_API_URL", "")
	t.Setenv("GQM_API_KEY", "")

	var stdout, stderr bytes.Buffer
	code := runTUI(nil, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "required") {
		t.Error("expected 'required' error")
	}
}

func TestRunTUI_InvalidURL(t *testing.T) {
	t.Setenv("GQM_API_URL", "")

	var stdout, stderr bytes.Buffer
	code := runTUI([]string{"--api-url", "not-a-url"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "valid URL") {
		t.Error("expected 'valid URL' error")
	}
}

func TestRunTUI_HealthCheckFails(t *testing.T) {
	t.Setenv("GQM_API_URL", "")

	var stdout, stderr bytes.Buffer
	// Use a URL that won't have a server running.
	code := runTUI([]string{"--api-url", "http://127.0.0.1:19999"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "cannot connect") {
		t.Error("expected 'cannot connect' error")
	}
}

func TestRunTUI_EnvVarFallback(t *testing.T) {
	t.Setenv("GQM_API_URL", "http://127.0.0.1:19998")
	t.Setenv("GQM_API_KEY", "gqm_ak_test")

	var stdout, stderr bytes.Buffer
	// No flags — should use env vars. Health check will fail (no server).
	code := runTUI(nil, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	// Should reach health check (not validation error).
	if !strings.Contains(stderr.String(), "cannot connect") {
		t.Errorf("expected health check error, got: %s", stderr.String())
	}
}

func TestRunTUI_FlagOverridesEnv(t *testing.T) {
	t.Setenv("GQM_API_URL", "http://env-url:8080")

	var stdout, stderr bytes.Buffer
	// Flag should override env var. Health check will fail.
	code := runTUI([]string{"--api-url", "http://127.0.0.1:19997"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), "127.0.0.1:19997") {
		t.Error("expected flag URL in error, not env URL")
	}
}

func TestRunTUI_BadFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runTUI([]string{"--invalid-flag"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
}
