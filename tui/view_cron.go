package tui

import "github.com/charmbracelet/lipgloss"

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
		colDef{header: "TIMEZONE"},
		colDef{header: "JOB TYPE", flex: true, min: 8},
		colDef{header: "ENABLED"},
	)
	for _, e := range v.entries {
		enabled := statusReady.Render("yes")
		if !e.enabled() {
			enabled = lipgloss.NewStyle().Foreground(colorMuted).Render("no")
		}
		tz := e.str("timezone")
		if tz == "" {
			tz = "UTC"
		}
		t.addRow(
			e.str("id"),
			e.str("name"),
			e.str("cron_expr"),
			tz,
			e.str("job_type"),
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
