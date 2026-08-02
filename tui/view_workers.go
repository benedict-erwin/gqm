package tui

import "time"

// workerStaleAfter mirrors the dashboard's worker staleness threshold.
const workerStaleAfter = 30 * time.Second

type workersView struct {
	workers []Worker
	cursor  int
	err     error
}

func (v *workersView) render(width, maxRows int) string {
	t := newTable(width,
		colDef{header: "POOL", flex: true, min: 8},
		colDef{header: "STATUS"},
		colDef{header: "CONCURRENCY"},
		colDef{header: "ACTIVE"},
		colDef{header: "QUEUES", flex: true, min: 10},
		colDef{header: "HEARTBEAT"},
	)
	for _, w := range v.workers {
		// The API reports no explicit worker state; derive it from the
		// heartbeat the same way the dashboard does.
		status := w.str("status")
		if status == "" {
			if age := heartbeatAge(w.str("last_heartbeat")); age > workerStaleAfter {
				status = "stale"
			} else {
				status = "active"
			}
		}
		pool := w.str("pool")
		if pool == "" {
			pool = w.str("pool_id")
		}
		if pool == "" {
			pool = w.str("id")
		}
		active := w.str("active_jobs")
		if active == "" || active == "0" {
			active = dimStyle.Render("·")
		} else {
			active = statusProcessing.Render(active)
		}
		t.addRow(
			pool,
			styleStatus(status),
			w.str("concurrency"),
			active,
			w.str("queues"),
			heartbeatCell(w.str("last_heartbeat"), workerStaleAfter),
		)
	}
	return t.render(v.cursor, maxRows)
}

func (v *workersView) clampCursor() {
	if v.cursor >= len(v.workers) {
		v.cursor = len(v.workers) - 1
	}
	if v.cursor < 0 {
		v.cursor = 0
	}
}
