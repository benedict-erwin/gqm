package tui

type cronView struct {
	entries []CronEntry
	cursor  int
	err     error
}

func (v *cronView) render(width, maxRows int) string {
	t := newTable(width,
		colDef{header: "ID", flex: true, min: 8},
		colDef{header: "NAME", flex: true, min: 10},
		colDef{header: "EXPRESSION"},
		colDef{header: "SCHEDULE", flex: true, min: 8},
		colDef{header: "TZ"},
		colDef{header: "ENABLED"},
	)
	for _, e := range v.entries {
		enabled := statusReady.Render("on")
		if !e.enabled() {
			enabled = mutedStyle.Render("off")
		}
		tz := e.str("timezone")
		if tz == "" {
			tz = "UTC"
		}
		expr := e.str("cron_expr")
		t.addRow(
			e.str("id"),
			e.str("name"),
			expr,
			mutedStyle.Render(cronHuman(expr)),
			mutedStyle.Render(tz),
			enabled,
		)
	}
	return t.render(v.cursor, maxRows)
}

func (v *cronView) clampCursor() {
	if v.cursor >= len(v.entries) {
		v.cursor = len(v.entries) - 1
	}
	if v.cursor < 0 {
		v.cursor = 0
	}
}
