package tui

import (
	"fmt"
	"time"
)

type failedView struct {
	queues   []Queue // for selecting which queue to view
	jobs     []Job
	cursor   int
	queueIdx int // selected queue
	search   string
	typing   bool
	err      error
}

func (v *failedView) visible() []Job {
	return filterByID(v.jobs, "id", v.search)
}

func (v *failedView) render(width, maxRows int) string {
	var out string

	// Queue selector — truncate to fit terminal width
	if len(v.queues) > 0 {
		line := mutedStyle.Render("Queue: ")
		for i, q := range v.queues {
			label := fmt.Sprintf(" %s (%d) ", q.Name, q.DeadLetter)
			var tab string
			if i == v.queueIdx {
				tab = activeTab.Render(label)
			} else {
				tab = inactiveTab.Render(label)
			}
			candidate := line + tab
			if width > 0 && visibleLen(candidate) > width-1 && i > 0 {
				line += scrollStyle.Render(fmt.Sprintf("+%d more", len(v.queues)-i))
				break
			}
			line += tab
		}
		out += line + "\n\n"
	}

	jobs := v.visible()
	out += searchLine(v.search, v.typing, len(jobs))

	now := time.Now()
	t := newTable(width,
		colDef{header: "ID", flex: true, min: 10},
		colDef{header: "TYPE", flex: true, min: 8},
		colDef{header: "STATUS"},
		colDef{header: "ERROR", flex: true, min: 10},
		colDef{header: "RETRY"},
		colDef{header: "CREATED"},
	)
	for _, j := range jobs {
		created := formatUnixTime(j.int64val("created_at"), now)
		retry := fmt.Sprintf("%s/%s", j.str("retry_count"), j.str("max_retry"))
		// A job in the DLQ has exhausted its retries — color the counter red.
		t.addRow(
			j.str("id"),
			j.str("type"),
			styleStatus(j.str("status")),
			j.str("error"),
			statusFailed.Render(retry),
			mutedStyle.Render(created),
		)
	}
	out += t.render(v.cursor, maxRows)
	return out
}

func (v *failedView) clampCursor() {
	n := len(v.visible())
	if v.cursor >= n {
		v.cursor = n - 1
	}
	if v.cursor < 0 {
		v.cursor = 0
	}
}

func (v *failedView) selectedJob() *Job {
	jobs := v.visible()
	if v.cursor >= 0 && v.cursor < len(jobs) {
		return &jobs[v.cursor]
	}
	return nil
}

func (v *failedView) selectedQueue() string {
	if v.queueIdx >= 0 && v.queueIdx < len(v.queues) {
		return v.queues[v.queueIdx].Name
	}
	return ""
}
