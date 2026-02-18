package tui

import (
	"fmt"
	"strconv"
	"time"
)

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	if max <= 3 {
		return s[:max]
	}
	return s[:max-3] + "..."
}

// formatNanoTimestamp formats a nanosecond Unix timestamp to relative time (e.g., "3s ago").
func formatNanoTimestamp(s string) string {
	if s == "" {
		return "-"
	}
	ns, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return s
	}
	t := time.Unix(0, ns)
	d := time.Since(t)
	if d < 0 {
		return "just now"
	}
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	default:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	}
}
