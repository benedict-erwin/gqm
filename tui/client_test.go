package tui

import "testing"

func TestJobStr(t *testing.T) {
	j := Job{
		"id":     "abc123",
		"count":  float64(42),
		"status": "ready",
	}

	tests := []struct {
		key  string
		want string
	}{
		{"id", "abc123"},
		{"count", "42"},
		{"status", "ready"},
		{"missing", ""},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			if got := j.str(tt.key); got != tt.want {
				t.Errorf("Job.str(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}

func TestJobInt64val(t *testing.T) {
	tests := []struct {
		name string
		job  Job
		key  string
		want int64
	}{
		{"float64 value", Job{"ts": float64(1234567890)}, "ts", 1234567890},
		{"string integer", Job{"ts": "9876543210"}, "ts", 9876543210},
		{"string non-integer", Job{"ts": "not-a-number"}, "ts", 0},
		{"missing key", Job{}, "ts", 0},
		{"empty string", Job{"ts": ""}, "ts", 0},
		{"bool value", Job{"ts": true}, "ts", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.job.int64val(tt.key); got != tt.want {
				t.Errorf("Job.int64val(%q) = %d, want %d", tt.key, got, tt.want)
			}
		})
	}
}

func TestWorkerStr(t *testing.T) {
	w := Worker{
		"pool":    "email",
		"count":   float64(5),
		"queues":  []any{"critical", "default"},
		"missing": nil,
	}

	tests := []struct {
		key  string
		want string
	}{
		{"pool", "email"},
		{"count", "5"},
		{"queues", "critical, default"},
		{"nonexistent", ""},
	}
	for _, tt := range tests {
		t.Run(tt.key, func(t *testing.T) {
			if got := w.str(tt.key); got != tt.want {
				t.Errorf("Worker.str(%q) = %q, want %q", tt.key, got, tt.want)
			}
		})
	}
}

func TestCronEntryStr(t *testing.T) {
	e := CronEntry{"id": "daily-backup", "count": float64(10)}

	if got := e.str("id"); got != "daily-backup" {
		t.Errorf("CronEntry.str('id') = %q, want 'daily-backup'", got)
	}
	if got := e.str("count"); got != "" {
		t.Errorf("CronEntry.str('count') = %q, want '' (non-string)", got)
	}
	if got := e.str("missing"); got != "" {
		t.Errorf("CronEntry.str('missing') = %q, want ''", got)
	}
}

func TestCronEntryEnabled(t *testing.T) {
	tests := []struct {
		name string
		e    CronEntry
		want bool
	}{
		{"bool true", CronEntry{"enabled": true}, true},
		{"bool false", CronEntry{"enabled": false}, false},
		{"string true", CronEntry{"enabled": "true"}, true},
		{"string false", CronEntry{"enabled": "false"}, false},
		{"missing defaults true", CronEntry{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.e.enabled(); got != tt.want {
				t.Errorf("CronEntry.enabled() = %v, want %v", got, tt.want)
			}
		})
	}
}
