package tui

import (
	"fmt"
	"strings"
	"time"
)

// filterByID returns the jobs whose value under idKey contains the query
// (case-insensitive). An empty query returns the input unchanged.
func filterByID(jobs []Job, idKey, query string) []Job {
	if query == "" {
		return jobs
	}
	q := strings.ToLower(query)
	var out []Job
	for _, j := range jobs {
		if strings.Contains(strings.ToLower(j.str(idKey)), q) {
			out = append(out, j)
		}
	}
	return out
}

// searchLine renders the active "/" filter line above a table.
func searchLine(query string, typing bool, matches int) string {
	if query == "" && !typing {
		return ""
	}
	cur := ""
	if typing {
		cur = "▏"
	}
	return " " + keyStyle.Render("/") + " " + query + cur + "  " +
		mutedStyle.Render(fmt.Sprintf("(%d match, esc clears)", matches)) + "\n\n"
}

// jobsView is the queue drill-down: recent jobs across every status.
type jobsView struct {
	queue  string
	jobs   []Job
	cursor int
	search string
	typing bool
	err    error
}

func (v *jobsView) visible() []Job {
	return filterByID(v.jobs, "id", v.search)
}

func (v *jobsView) render(width, maxRows int) string {
	if v.err != nil {
		return " " + errStyle.Render("Failed to load jobs: "+v.err.Error()) + "\n"
	}

	var out string
	out += " " + mutedStyle.Render("Queues / ") + boldStyle.Render(v.queue) + "  " +
		mutedStyle.Render(fmt.Sprintf("%d recent jobs", len(v.jobs))) + "\n\n"

	jobs := v.visible()
	out += searchLine(v.search, v.typing, len(jobs))

	now := time.Now()
	t := newTable(width,
		colDef{header: "ID", flex: true, min: 10},
		colDef{header: "TYPE", flex: true, min: 8},
		colDef{header: "STATUS"},
		colDef{header: "CREATED"},
		colDef{header: "ERROR", flex: true, min: 10},
	)
	for _, j := range jobs {
		errText := j.str("error")
		if errText != "" {
			errText = errStyle.Render(errText)
		}
		t.addRow(
			j.str("id"),
			j.str("type"),
			styleStatus(j.str("status")),
			mutedStyle.Render(formatUnixTime(j.int64val("created_at"), now)),
			errText,
		)
	}
	out += t.render(v.cursor, maxRows)
	return out
}

func (v *jobsView) clampCursor() {
	n := len(v.visible())
	if v.cursor >= n {
		v.cursor = n - 1
	}
	if v.cursor < 0 {
		v.cursor = 0
	}
}

func (v *jobsView) selectedJob() *Job {
	jobs := v.visible()
	if v.cursor >= 0 && v.cursor < len(jobs) {
		return &jobs[v.cursor]
	}
	return nil
}

// cronHistView is the cron drill-down: recent trigger records.
type cronHistView struct {
	cronID string
	items  []Job // {job_id, triggered_at}
	cursor int
	search string
	typing bool
	err    error
}

func (v *cronHistView) visible() []Job {
	return filterByID(v.items, "job_id", v.search)
}

func (v *cronHistView) render(width, maxRows int) string {
	if v.err != nil {
		return " " + errStyle.Render("Failed to load history: "+v.err.Error()) + "\n"
	}

	var out string
	out += " " + mutedStyle.Render("Cron / ") + boldStyle.Render(v.cronID) + "  " +
		mutedStyle.Render(fmt.Sprintf("last %d triggers", len(v.items))) + "\n\n"

	items := v.visible()
	out += searchLine(v.search, v.typing, len(items))

	if len(v.items) == 0 {
		out += mutedStyle.Render("  No history yet") + "\n"
		return out
	}

	now := time.Now()
	t := newTable(width,
		colDef{header: "JOB ID", flex: true, min: 10},
		colDef{header: "TRIGGERED"},
	)
	for _, r := range items {
		t.addRow(
			r.str("job_id"),
			mutedStyle.Render(formatUnixTime(r.int64val("triggered_at"), now)),
		)
	}
	out += t.render(v.cursor, maxRows)
	return out
}

func (v *cronHistView) clampCursor() {
	n := len(v.visible())
	if v.cursor >= n {
		v.cursor = n - 1
	}
	if v.cursor < 0 {
		v.cursor = 0
	}
}

func (v *cronHistView) selectedJobID() string {
	items := v.visible()
	if v.cursor >= 0 && v.cursor < len(items) {
		return items[v.cursor].str("job_id")
	}
	return ""
}
