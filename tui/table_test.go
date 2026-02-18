package tui

import "testing"

func TestStripAnsi(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want string
	}{
		{"no ansi", "hello", "hello"},
		{"with color", "\x1b[31mred\x1b[0m", "red"},
		{"bold", "\x1b[1mbold\x1b[0m", "bold"},
		{"empty", "", ""},
		{"multiple escapes", "\x1b[31mhello \x1b[32mworld\x1b[0m", "hello world"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stripAnsi(tt.s); got != tt.want {
				t.Errorf("stripAnsi(%q) = %q, want %q", tt.s, got, tt.want)
			}
		})
	}
}

func TestVisibleLen(t *testing.T) {
	tests := []struct {
		name string
		s    string
		want int
	}{
		{"plain text", "hello", 5},
		{"with ansi", "\x1b[31mred\x1b[0m", 3},
		{"empty", "", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := visibleLen(tt.s); got != tt.want {
				t.Errorf("visibleLen(%q) = %d, want %d", tt.s, got, tt.want)
			}
		})
	}
}

func TestTruncateAnsi(t *testing.T) {
	tests := []struct {
		name       string
		s          string
		maxVisible int
		wantLen    int // expected visible length
	}{
		{"short enough", "hello", 10, 5},
		{"exact", "hello", 5, 5},
		{"truncate plain", "hello world", 8, 8},
		{"zero max", "hello", 0, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := truncateAnsi(tt.s, tt.maxVisible)
			gotLen := visibleLen(got)
			if gotLen > tt.maxVisible {
				t.Errorf("truncateAnsi(%q, %d) visible len = %d, exceeds max", tt.s, tt.maxVisible, gotLen)
			}
		})
	}

	t.Run("preserves ansi", func(t *testing.T) {
		s := "\x1b[31mhello world\x1b[0m"
		got := truncateAnsi(s, 8)
		// Should contain ANSI reset at end.
		if got[len(got)-4:] != "\x1b[0m" {
			t.Errorf("truncateAnsi should end with ANSI reset, got suffix %q", got[len(got)-4:])
		}
	})
}

func TestAllocWidths_NoShrink(t *testing.T) {
	tbl := newTable(100,
		colDef{header: "Name", flex: false},
		colDef{header: "Status", flex: false},
	)
	tbl.addRow("email", "active")

	alloc := tbl.allocWidths()
	if len(alloc) != 2 {
		t.Fatalf("got %d widths, want 2", len(alloc))
	}
	// Natural widths: "email" = 5, "Status" = 6
	if alloc[0] < 4 || alloc[1] < 6 {
		t.Errorf("alloc = %v, expected >= [4, 6]", alloc)
	}
}

func TestAllocWidths_FlexShrink(t *testing.T) {
	tbl := newTable(30,
		colDef{header: "ID", flex: false},
		colDef{header: "Description", flex: true, min: 5},
	)
	tbl.addRow("abc", "a very long description that exceeds width")

	alloc := tbl.allocWidths()
	total := alloc[0] + alloc[1] + colGapWidth
	if total > 30 {
		t.Errorf("total width %d exceeds maxWidth 30", total)
	}
	if alloc[1] < 5 {
		t.Errorf("flex column shrunk below min: %d < 5", alloc[1])
	}
}

func TestAllocWidths_Unlimited(t *testing.T) {
	tbl := newTable(0,
		colDef{header: "Name", flex: true, min: 4},
	)
	tbl.addRow("a very long name indeed")

	alloc := tbl.allocWidths()
	if alloc[0] != 23 {
		t.Errorf("unlimited width: got %d, want 23", alloc[0])
	}
}

func TestNewTable_MinDefault(t *testing.T) {
	tbl := newTable(80,
		colDef{header: "Status", flex: true},
	)
	// min should default to header length (6) when not set.
	if tbl.cols[0].min != 6 {
		t.Errorf("min = %d, want 6 (header length)", tbl.cols[0].min)
	}
}
