package tui

import "fmt"

type queuesView struct {
	queues []Queue
	cursor int
	err    error
}

func (v *queuesView) render(width, maxRows int) string {
	t := newTable(width,
		colDef{header: "QUEUE", flex: true, min: 10},
		colDef{header: "PAUSED"},
		colDef{header: "READY"},
		colDef{header: "PROCESSING"},
		colDef{header: "COMPLETED"},
		colDef{header: "DLQ"},
	)
	for _, q := range v.queues {
		paused := ""
		if q.Paused {
			paused = statusPaused.Render("PAUSED")
		}
		t.addRow(
			q.Name,
			paused,
			statusReady.Render(fmt.Sprintf("%d", q.Ready)),
			statusProcessing.Render(fmt.Sprintf("%d", q.Processing)),
			fmt.Sprintf("%d", q.Completed),
			statusDLQ.Render(fmt.Sprintf("%d", q.DeadLetter)),
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
