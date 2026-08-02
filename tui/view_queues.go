package tui

type queuesView struct {
	queues []Queue
	cursor int
	err    error
}

func (v *queuesView) render(width, maxRows int) string {
	t := newTable(width,
		colDef{header: "QUEUE", flex: true, min: 10},
		colDef{header: "STATE"},
		colDef{header: "READY"},
		colDef{header: "PROCESSING"},
		colDef{header: "COMPLETED"},
		colDef{header: "DLQ"},
	)
	for _, q := range v.queues {
		state := ""
		if q.Paused {
			state = statusPaused.Render("PAUSED")
		}
		t.addRow(
			q.Name,
			state,
			dimZero(q.Ready, statusReady),
			dimZero(q.Processing, statusProcessing),
			dimZero(q.Completed, statusCompleted),
			dimZero(q.DeadLetter, statusDLQ),
		)
	}
	return t.render(v.cursor, maxRows)
}

func (v *queuesView) clampCursor() {
	if v.cursor >= len(v.queues) {
		v.cursor = len(v.queues) - 1
	}
	if v.cursor < 0 {
		v.cursor = 0
	}
}
