package tui

import "testing"

func TestTruncate(t *testing.T) {
	tests := []struct {
		name string
		s    string
		max  int
		want string
	}{
		{"short string", "hello", 10, "hello"},
		{"exact length", "hello", 5, "hello"},
		{"truncate with ellipsis", "hello world", 8, "hello..."},
		{"very short max", "hello", 3, "hel"},
		{"max 2", "hello", 2, "he"},
		{"max 1", "hello", 1, "h"},
		{"empty string", "", 5, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncate(tt.s, tt.max)
			if got != tt.want {
				t.Errorf("truncate(%q, %d) = %q, want %q", tt.s, tt.max, got, tt.want)
			}
		})
	}
}

func TestFormatNanoTimestamp(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want string
	}{
		{"empty", "", "-"},
		{"invalid", "notanumber", "notanumber"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := formatNanoTimestamp(tt.s)
			if got != tt.want {
				t.Errorf("formatNanoTimestamp(%q) = %q, want %q", tt.s, got, tt.want)
			}
		})
	}

	// A very far-future nanosecond timestamp should produce "just now" is wrong,
	// but a zero timestamp (epoch) should produce some "Xh ago" string.
	t.Run("epoch", func(t *testing.T) {
		got := formatNanoTimestamp("0")
		if got == "-" || got == "0" {
			t.Errorf("formatNanoTimestamp(\"0\") = %q, expected a relative time", got)
		}
	})
}
