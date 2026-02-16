package gqm

import (
	"testing"
	"time"
)

func TestParseCronExpr_Valid(t *testing.T) {
	tests := []struct {
		name   string
		expr   string
		second []int
		minute []int
		hour   []int
	}{
		{
			name:   "6-field every second",
			expr:   "* * * * * *",
			second: seq(0, 59),
			minute: seq(0, 59),
			hour:   seq(0, 23),
		},
		{
			name:   "5-field adds second=0",
			expr:   "* * * * *",
			second: []int{0},
			minute: seq(0, 59),
			hour:   seq(0, 23),
		},
		{
			name:   "specific values",
			expr:   "30 15 3 1 6 5",
			second: []int{30},
			minute: []int{15},
			hour:   []int{3},
		},
		{
			name:   "range",
			expr:   "0 0 9-17 * * *",
			second: []int{0},
			minute: []int{0},
			hour:   seq(9, 17),
		},
		{
			name:   "step",
			expr:   "*/15 */10 */6 * * *",
			second: []int{0, 15, 30, 45},
			minute: []int{0, 10, 20, 30, 40, 50},
			hour:   []int{0, 6, 12, 18},
		},
		{
			name:   "range with step",
			expr:   "0 10-30/5 * * * *",
			second: []int{0},
			minute: []int{10, 15, 20, 25, 30},
			hour:   seq(0, 23),
		},
		{
			name:   "list",
			expr:   "0,30 0,15,30,45 * * * *",
			second: []int{0, 30},
			minute: []int{0, 15, 30, 45},
			hour:   seq(0, 23),
		},
		{
			name:   "mixed list range step",
			expr:   "0 0 1,3,5-8,*/12 * * *",
			second: []int{0},
			minute: []int{0},
			hour:   []int{0, 1, 3, 5, 6, 7, 8, 12},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := ParseCronExpr(tt.expr)
			if err != nil {
				t.Fatalf("ParseCronExpr(%q) error = %v", tt.expr, err)
			}
			assertInts(t, "second", c.second, tt.second)
			assertInts(t, "minute", c.minute, tt.minute)
			assertInts(t, "hour", c.hour, tt.hour)
		})
	}
}

func TestParseCronExpr_DayOfWeek7(t *testing.T) {
	// 7 should be normalized to 0 (both mean Sunday)
	c, err := ParseCronExpr("0 0 0 * * 7")
	if err != nil {
		t.Fatalf("ParseCronExpr error = %v", err)
	}
	assertInts(t, "dayOfWeek", c.dayOfWeek, []int{0})

	// 5-7 should become 0,5,6
	c, err = ParseCronExpr("0 0 0 * * 5-7")
	if err != nil {
		t.Fatalf("ParseCronExpr error = %v", err)
	}
	assertInts(t, "dayOfWeek", c.dayOfWeek, []int{0, 5, 6})

	// 0,7 should deduplicate to just 0
	c, err = ParseCronExpr("0 0 0 * * 0,7")
	if err != nil {
		t.Fatalf("ParseCronExpr error = %v", err)
	}
	assertInts(t, "dayOfWeek", c.dayOfWeek, []int{0})
}

func TestParseCronExpr_DomDowFlags(t *testing.T) {
	c, err := ParseCronExpr("0 0 0 * * *")
	if err != nil {
		t.Fatal(err)
	}
	if c.domSet || c.dowSet {
		t.Error("both * should set domSet=false, dowSet=false")
	}

	c, err = ParseCronExpr("0 0 0 15 * 1")
	if err != nil {
		t.Fatal(err)
	}
	if !c.domSet || !c.dowSet {
		t.Errorf("domSet=%v dowSet=%v, want both true", c.domSet, c.dowSet)
	}
}

func TestParseCronExpr_Invalid(t *testing.T) {
	tests := []struct {
		name string
		expr string
	}{
		{"too few fields", "* * *"},
		{"too many fields", "* * * * * * *"},
		{"empty", ""},
		{"bad second", "60 * * * * *"},
		{"bad minute", "0 60 * * * *"},
		{"bad hour", "0 0 24 * * *"},
		{"bad dom", "0 0 0 0 * *"},
		{"bad dom high", "0 0 0 32 * *"},
		{"bad month zero", "0 0 0 * 0 *"},
		{"bad month high", "0 0 0 * 13 *"},
		{"bad dow", "0 0 0 * * 8"},
		{"bad range", "0 0 10-5 * * *"},
		{"bad step", "0 */0 * * * *"},
		{"bad value", "0 abc * * * *"},
		{"negative step", "0 */-1 * * * *"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseCronExpr(tt.expr)
			if err == nil {
				t.Errorf("ParseCronExpr(%q) expected error", tt.expr)
			}
		})
	}
}

func TestCronExpr_Next(t *testing.T) {
	utc := time.UTC
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, utc) // Thursday

	tests := []struct {
		name string
		expr string
		from time.Time
		want time.Time
	}{
		{
			name: "every second",
			expr: "* * * * * *",
			from: base,
			want: time.Date(2026, 1, 1, 0, 0, 1, 0, utc),
		},
		{
			name: "every minute (5-field)",
			expr: "* * * * *",
			from: base,
			want: time.Date(2026, 1, 1, 0, 1, 0, 0, utc),
		},
		{
			name: "specific time today",
			expr: "0 30 14 * * *",
			from: time.Date(2026, 1, 1, 12, 0, 0, 0, utc),
			want: time.Date(2026, 1, 1, 14, 30, 0, 0, utc),
		},
		{
			name: "specific time past today wraps to tomorrow",
			expr: "0 30 14 * * *",
			from: time.Date(2026, 1, 1, 15, 0, 0, 0, utc),
			want: time.Date(2026, 1, 2, 14, 30, 0, 0, utc),
		},
		{
			name: "second field",
			expr: "30 * * * * *",
			from: base,
			want: time.Date(2026, 1, 1, 0, 0, 30, 0, utc),
		},
		{
			name: "step minutes */15",
			expr: "0 */15 * * * *",
			from: time.Date(2026, 1, 1, 0, 7, 0, 0, utc),
			want: time.Date(2026, 1, 1, 0, 15, 0, 0, utc),
		},
		{
			name: "day of month",
			expr: "0 0 0 15 * *",
			from: base,
			want: time.Date(2026, 1, 15, 0, 0, 0, 0, utc),
		},
		{
			name: "specific month",
			expr: "0 0 0 1 6 *",
			from: base,
			want: time.Date(2026, 6, 1, 0, 0, 0, 0, utc),
		},
		{
			name: "year wrap",
			expr: "0 0 0 1 1 *",
			from: time.Date(2026, 6, 1, 0, 0, 0, 0, utc),
			want: time.Date(2027, 1, 1, 0, 0, 0, 0, utc),
		},
		{
			name: "leap year Feb 29",
			expr: "0 0 0 29 2 *",
			from: base, // 2026-01-01
			want: time.Date(2028, 2, 29, 0, 0, 0, 0, utc),
		},
		{
			name: "Feb 31 never matches",
			expr: "0 0 0 31 2 *",
			from: base,
			want: time.Time{}, // zero
		},
		{
			name: "day of week Monday",
			expr: "0 0 0 * * 1",
			from: base, // Thursday Jan 1
			want: time.Date(2026, 1, 5, 0, 0, 0, 0, utc), // Monday
		},
		{
			name: "day of week Sunday via 7",
			expr: "0 0 0 * * 7",
			from: base, // Thursday Jan 1
			want: time.Date(2026, 1, 4, 0, 0, 0, 0, utc), // Sunday
		},
		{
			name: "dom AND dow OR semantics: 1st OR Monday",
			expr: "0 0 0 1 * 1",
			from: base, // Thursday Jan 1 00:00:00, Next starts from 00:00:01
			want: time.Date(2026, 1, 5, 0, 0, 0, 0, utc), // Monday Jan 5
		},
		{
			name: "hour range 9-17",
			expr: "0 0 9-17 * * *",
			from: time.Date(2026, 1, 1, 8, 0, 0, 0, utc),
			want: time.Date(2026, 1, 1, 9, 0, 0, 0, utc),
		},
		{
			name: "dow list Mon Wed Fri",
			expr: "0 0 0 * * 1,3,5",
			from: base, // Thursday Jan 1
			want: time.Date(2026, 1, 2, 0, 0, 0, 0, utc), // Friday
		},
		{
			name: "complex: sec=15 min=30 every 2h",
			expr: "15 30 */2 * * *",
			from: base,
			want: time.Date(2026, 1, 1, 0, 30, 15, 0, utc),
		},
		{
			name: "from exactly on match advances to next",
			expr: "0 0 0 * * *",
			from: base, // exactly midnight
			want: time.Date(2026, 1, 2, 0, 0, 0, 0, utc),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := ParseCronExpr(tt.expr)
			if err != nil {
				t.Fatalf("ParseCronExpr(%q) error = %v", tt.expr, err)
			}
			got := c.Next(tt.from)
			if !got.Equal(tt.want) {
				t.Errorf("Next(%v) = %v, want %v", tt.from.Format(time.RFC3339), got.Format(time.RFC3339), tt.want.Format(time.RFC3339))
			}
		})
	}
}

func TestCronExpr_Next_Timezone(t *testing.T) {
	jakarta, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		t.Skip("timezone Asia/Jakarta not available")
	}

	// "Every day at 03:00 WIB" — from 2026-01-01 04:00 WIB
	c, err := ParseCronExpr("0 0 3 * * *")
	if err != nil {
		t.Fatal(err)
	}

	from := time.Date(2026, 1, 1, 4, 0, 0, 0, jakarta)
	got := c.Next(from)
	want := time.Date(2026, 1, 2, 3, 0, 0, 0, jakarta)

	if !got.Equal(want) {
		t.Errorf("Next() = %v, want %v", got, want)
	}
}

func TestCronExpr_String(t *testing.T) {
	expr := "30 */5 9-17 * * 1-5"
	c, err := ParseCronExpr(expr)
	if err != nil {
		t.Fatal(err)
	}
	if c.String() != expr {
		t.Errorf("String() = %q, want %q", c.String(), expr)
	}
}

// --- helpers ---

func seq(min, max int) []int {
	s := make([]int, 0, max-min+1)
	for i := min; i <= max; i++ {
		s = append(s, i)
	}
	return s
}

func assertInts(t *testing.T, name string, got, want []int) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: len = %d, want %d\n  got:  %v\n  want: %v", name, len(got), len(want), got, want)
		return
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("%s[%d] = %d, want %d\n  got:  %v\n  want: %v", name, i, got[i], want[i], got, want)
			return
		}
	}
}
