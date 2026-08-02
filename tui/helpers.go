package tui

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

func truncate(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	if max <= 3 {
		return string(r[:max])
	}
	return string(r[:max-3]) + "..."
}

func intToString(n int64) string {
	return strconv.FormatInt(n, 10)
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

// heartbeatAge returns the age of a nanosecond Unix timestamp string.
// Returns -1 when the value is absent or unparseable.
func heartbeatAge(s string) time.Duration {
	if s == "" {
		return -1
	}
	ns, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return -1
	}
	return time.Since(time.Unix(0, ns))
}

// heartbeatCell renders a heartbeat timestamp with staleness coloring: fresh
// heartbeats are green, anything past staleAfter is red — a five-hour-dead
// worker must not look like a four-second-old one.
func heartbeatCell(s string, staleAfter time.Duration) string {
	rel := formatNanoTimestamp(s)
	age := heartbeatAge(s)
	if age < 0 {
		return mutedStyle.Render(rel)
	}
	if age > staleAfter {
		return statusFailed.Render(rel)
	}
	return statusReady.Render(rel)
}

// cronHuman describes common cron expressions in words. Returns "" for
// anything it can't confidently describe — the raw expression is always shown
// beside it, so a blank is never a loss of information.
func cronHuman(expr string) string {
	p := strings.Fields(expr)
	if len(p) != 5 {
		return ""
	}
	min, hour, dom, mon, dow := p[0], p[1], p[2], p[3], p[4]
	dows := []string{"Sundays", "Mondays", "Tuesdays", "Wednesdays", "Thursdays", "Fridays", "Saturdays"}
	isNum := func(s string) bool {
		_, err := strconv.Atoi(s)
		return err == nil
	}
	pad2 := func(s string) string {
		if len(s) < 2 {
			return "0" + s
		}
		return s
	}
	switch {
	case min == "*" && hour == "*" && dom == "*" && mon == "*" && dow == "*":
		return "every minute"
	case isNum(min) && hour == "*" && dom == "*" && mon == "*" && dow == "*":
		if min == "0" {
			return "every hour"
		}
		return "hourly at :" + pad2(min)
	case isNum(min) && isNum(hour) && dom == "*" && mon == "*":
		t := pad2(hour) + ":" + pad2(min)
		if dow == "*" {
			return "daily at " + t
		}
		if n, err := strconv.Atoi(dow); err == nil && n >= 0 && n <= 6 {
			return dows[n] + " at " + t
		}
	case strings.HasPrefix(min, "*/") && hour == "*" && dom == "*" && mon == "*" && dow == "*":
		return "every " + min[2:] + " min"
	}
	return ""
}

// formatSeconds renders a duration given in (possibly fractional) seconds.
func formatSeconds(s string) string {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return s
	}
	switch {
	case v < 1:
		return fmt.Sprintf("%dms", int(v*1000))
	case v < 60:
		return fmt.Sprintf("%.1fs", v)
	case v < 3600:
		return fmt.Sprintf("%dm %ds", int(v)/60, int(v)%60)
	default:
		return fmt.Sprintf("%dh %dm", int(v)/3600, (int(v)%3600)/60)
	}
}

// formatUnixTime renders a second-resolution Unix timestamp. Same-day
// timestamps show the clock only; older ones carry the date so yesterday's
// entries are distinguishable.
func formatUnixTime(ts int64, now time.Time) string {
	if ts <= 0 {
		return "-"
	}
	t := time.Unix(ts, 0)
	if t.Year() == now.Year() && t.YearDay() == now.YearDay() {
		return t.Format("15:04:05")
	}
	return t.Format("Jan 2, 15:04")
}
