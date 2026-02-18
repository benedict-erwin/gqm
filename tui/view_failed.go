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
	err      error
}

func (v *failedView) render(width, maxRows int) string {
	var out string

	// Queue selector — truncate to fit terminal width
	if len(v.queues) > 0 {
		line := "Queue: "
		for i, q := range v.queues {
			label := fmt.Sprintf("[%s (%d)]", q.Name, q.DeadLetter)
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

	t := newTable(width,
		colDef{header: "ID", flex: true, min: 10},
		colDef{header: "TYPE", flex: true, min: 8},
		colDef{header: "STATUS"},
		colDef{header: "ERROR", flex: true, min: 10},
		colDef{header: "RETRY"},
		colDef{header: "CREATED"},
	)
	for _, j := range v.jobs {
		created := "-"
		if ts := j.int64val("created_at"); ts > 0 {
			created = time.Unix(ts, 0).Format("15:04:05")
		}
		retry := fmt.Sprintf("%s/%s", j.str("retry_count"), j.str("max_retry"))
		t.addRow(
			j.str("id"),
			j.str("type"),
			styleStatus(j.str("status")),
			j.str("error"),
			retry,
			created,
		)
	}
	out += t.render(v.cursor, maxRows)
	return out
}

func (v *failedView) clampCursor() {
	if v.cursor >= len(v.jobs) {
		v.cursor = len(v.jobs) - 1
	}
	if v.cursor < 0 {
		v.cursor = 0
	}
}

func (v *failedView) selectedJob() *Job {
	if v.cursor >= 0 && v.cursor < len(v.jobs) {
		return &v.jobs[v.cursor]
	}
	return nil
}

func (v *failedView) selectedQueue() string {
	if v.queueIdx >= 0 && v.queueIdx < len(v.queues) {
		return v.queues[v.queueIdx].Name
	}
	return ""
}
