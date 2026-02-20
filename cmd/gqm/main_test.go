package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestRun_NoArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run(nil, &stdout, &stderr)
	if code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
	if !strings.Contains(stdout.String(), "GQM") {
		t.Error("expected usage output on stdout")
	}
}

func TestRun_Help(t *testing.T) {
	for _, cmd := range []string{"help", "-h", "--help"} {
		t.Run(cmd, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run([]string{cmd}, &stdout, &stderr)
			if code != 0 {
				t.Errorf("exit code = %d, want 0", code)
			}
			if !strings.Contains(stdout.String(), "gqm <command>") {
				t.Error("expected usage text")
			}
		})
	}
}

func TestRun_Version(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"version"}, &stdout, &stderr)
	if code != 0 {
		t.Errorf("exit code = %d, want 0", code)
	}
	if !strings.Contains(stdout.String(), "gqm dev") {
		t.Errorf("stdout = %q, want version output", stdout.String())
	}
}

func TestRun_UnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"nonexistent"}, &stdout, &stderr)
	if code != 1 {
		t.Errorf("exit code = %d, want 1", code)
	}
	if !strings.Contains(stderr.String(), `unknown command "nonexistent"`) {
		t.Errorf("stderr = %q, want unknown command error", stderr.String())
	}
}

func TestRun_Dispatch(t *testing.T) {
	tests := []struct {
		name string
		args []string
		code int
	}{
		{"hash-password no args", []string{"hash-password"}, 1},
		{"generate-api-key extra args", []string{"generate-api-key", "extra"}, 1},
		{"init bad flag", []string{"init", "--invalid"}, 1},
		{"dashboard no subcommand", []string{"dashboard"}, 1},
		{"set-password no flags", []string{"set-password"}, 1},
		{"add-api-key no flags", []string{"add-api-key"}, 1},
		{"revoke-api-key no flags", []string{"revoke-api-key"}, 1},
		{"tui no url", []string{"tui"}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			code := run(tt.args, &stdout, &stderr)
			if code != tt.code {
				t.Errorf("exit code = %d, want %d; stderr: %s", code, tt.code, stderr.String())
			}
		})
	}
}

func TestPrintUsage(t *testing.T) {
	var buf bytes.Buffer
	printUsage(&buf)
	output := buf.String()

	keywords := []string{
		"init", "set-password", "add-api-key", "revoke-api-key",
		"dashboard", "hash-password", "generate-api-key", "tui", "version", "help",
	}
	for _, kw := range keywords {
		if !strings.Contains(output, kw) {
			t.Errorf("usage missing keyword %q", kw)
		}
	}
}
