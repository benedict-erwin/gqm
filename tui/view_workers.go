package tui

type workersView struct {
	workers []Worker
	cursor  int
	err     error
}

func (v *workersView) render(width, maxRows int) string {
	t := newTable(width,
		colDef{header: "ID", flex: true, min: 10},
		colDef{header: "POOL", flex: true, min: 8},
		colDef{header: "STATUS"},
		colDef{header: "CONCURRENCY"},
		colDef{header: "QUEUES", flex: true, min: 10},
		colDef{header: "HEARTBEAT"},
	)
	for _, w := range v.workers {
		t.addRow(
			w.str("id"),
			w.str("pool"),
			styleStatus(w.str("status")),
			w.str("concurrency"),
			w.str("queues"),
			formatNanoTimestamp(w.str("last_heartbeat")),
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
