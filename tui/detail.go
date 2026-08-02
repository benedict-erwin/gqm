package tui

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// detailView is the drill-down view for a single job (opened with enter).
type detailView struct {
	job      Job
	from     string // breadcrumb context, e.g. queue name
	scroll   int    // payload pane scroll offset
	bodyRows int    // payload lines rendered last frame (for clamping)
	err      error
}

func (v *detailView) clampScroll(visible int) {
	max := v.bodyRows - visible
	if max < 0 {
		max = 0
	}
	if v.scroll > max {
		v.scroll = max
	}
	if v.scroll < 0 {
		v.scroll = 0
	}
}

// render draws the job detail: breadcrumb, compact meta, then a scrollable
// payload pane. maxRows bounds the total content height.
func (v *detailView) render(width, maxRows int) string {
	if v.err != nil {
		return errStyle.Render("Failed to load job: " + v.err.Error())
	}
	j := v.job
	if j == nil {
		return mutedStyle.Render("  Loading job...")
	}

	var b strings.Builder
	id := j.str("id")
	crumb := mutedStyle.Render(fmt.Sprintf("%s / %s / ", v.from, j.str("queue")))
	b.WriteString(" " + crumb + boldStyle.Render(id) + "  " + styleStatus(j.str("status")) + "\n\n")

	// Compact meta line(s)
	meta := func(k, val string) string {
		if val == "" {
			return ""
		}
		return mutedStyle.Render(k+" ") + val + "   "
	}
	rc := j.str("retry_count")
	if rc == "" {
		rc = "0"
	}
	mr := j.str("max_retry")
	if mr == "" {
		mr = "0"
	}
	retry := fmt.Sprintf("%s / %s", rc, mr)
	line := " " + meta("Type", j.str("type")) + meta("Retry", statusFailed.Render(retry)) +
		meta("Worker", j.str("worker_id"))
	if d := j.floatStr("execution_duration"); d != "" {
		line += meta("Duration", formatSeconds(d))
	}
	b.WriteString(truncateAnsi(line, width-1) + "\n")
	created := formatUnixTime(j.int64val("created_at"), time.Now())
	b.WriteString(" " + meta("Created", created) + "\n")
	if e := j.str("error"); e != "" {
		b.WriteString(" " + mutedStyle.Render("Error ") + errStyle.Render(e) + "\n")
	}
	b.WriteString("\n")

	// Payload pane
	header := " " + headerText.Render("PAYLOAD") + " "
	rule := width - visibleLen(header) - 1
	if rule < 0 {
		rule = 0
	}
	b.WriteString(header + dimStyle.Render(strings.Repeat("─", rule)) + "\n")

	lines := renderJSONLines(j["payload"])
	v.bodyRows = len(lines)

	// Rows already used above: crumb(1)+blank(1)+meta(2)+err(0/1)+blank(1)+header(1)+scroll hint(1)
	used := 7
	if j.str("error") != "" {
		used++
	}
	visible := maxRows - used
	if visible < 3 {
		visible = 3
	}
	v.clampScroll(visible)

	end := v.scroll + visible
	if end > len(lines) {
		end = len(lines)
	}
	for _, l := range lines[v.scroll:end] {
		b.WriteString(" " + truncateAnsi(l, width-2) + "\n")
	}
	if rest := len(lines) - end; rest > 0 {
		b.WriteString(scrollStyle.Render(fmt.Sprintf("  ↓ %d more lines", rest)) + "\n")
	}

	return b.String()
}

// renderJSONLines pretty-prints a payload value as colorized lines.
// Non-JSON payloads render as plain text.
func renderJSONLines(payload any) []string {
	if payload == nil {
		return []string{dimStyle.Render("— no payload")}
	}
	// If the payload arrived as a JSON string, try to decode it first.
	if s, ok := payload.(string); ok {
		var decoded any
		if err := json.Unmarshal([]byte(s), &decoded); err == nil {
			payload = decoded
		} else {
			return strings.Split(s, "\n")
		}
	}
	raw, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return []string{fmt.Sprintf("%v", payload)}
	}
	lines := strings.Split(string(raw), "\n")
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = colorizeJSONLine(l)
	}
	return out
}

// colorizeJSONLine styles a single line of pretty-printed JSON: keys muted,
// strings green, numbers blue, punctuation dim.
func colorizeJSONLine(line string) string {
	trimmed := strings.TrimLeft(line, " ")
	indent := line[:len(line)-len(trimmed)]

	// Split "key": value when present.
	key, rest := "", trimmed
	if strings.HasPrefix(trimmed, "\"") {
		if idx := strings.Index(trimmed, "\": "); idx >= 0 {
			key = trimmed[:idx+2]
			rest = trimmed[idx+3:]
		}
	}

	var b strings.Builder
	b.WriteString(indent)
	if key != "" {
		b.WriteString(jsonKeyStyle.Render(key) + " ")
	}

	// Trailing comma stays dim.
	val := rest
	comma := ""
	if strings.HasSuffix(val, ",") {
		val = val[:len(val)-1]
		comma = jsonPunStyle.Render(",")
	}

	switch {
	case val == "{" || val == "}" || val == "[" || val == "]" || val == "{}" || val == "[]":
		b.WriteString(jsonPunStyle.Render(val))
	case strings.HasPrefix(val, "\""):
		b.WriteString(jsonStrStyle.Render(val))
	case val == "true" || val == "false" || val == "null":
		b.WriteString(jsonNumStyle.Render(val))
	case val != "" && (val[0] == '-' || (val[0] >= '0' && val[0] <= '9')):
		b.WriteString(jsonNumStyle.Render(val))
	default:
		b.WriteString(val)
	}
	b.WriteString(comma)
	return b.String()
}
