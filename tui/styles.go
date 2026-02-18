package tui

import "github.com/charmbracelet/lipgloss"

var (
	// Colors
	colorPrimary   = lipgloss.Color("39")  // blue
	colorSecondary = lipgloss.Color("245") // gray
	colorSuccess   = lipgloss.Color("42")  // green
	colorDanger    = lipgloss.Color("196") // red
	colorWarning   = lipgloss.Color("214") // orange
	colorMuted     = lipgloss.Color("240") // dark gray

	// Tab bar
	activeTab = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.Color("0")).
			Background(colorPrimary).
			Padding(0, 2)

	inactiveTab = lipgloss.NewStyle().
			Foreground(colorSecondary).
			Padding(0, 2)

	// Status bar
	statusBar = lipgloss.NewStyle().
			Foreground(colorMuted).
			MarginTop(1)

	// Table
	selectedRow = lipgloss.NewStyle().
			Background(lipgloss.Color("236"))

	// Status badges
	statusReady      = lipgloss.NewStyle().Foreground(colorSuccess)
	statusProcessing = lipgloss.NewStyle().Foreground(colorPrimary)
	statusFailed     = lipgloss.NewStyle().Foreground(colorDanger)
	statusDLQ        = lipgloss.NewStyle().Foreground(colorDanger).Bold(true)
	statusScheduled  = lipgloss.NewStyle().Foreground(colorWarning)
	statusPaused     = lipgloss.NewStyle().Foreground(colorWarning).Bold(true)

	// Error/info messages
	errStyle  = lipgloss.NewStyle().Foreground(colorDanger)
	infoStyle = lipgloss.NewStyle().Foreground(colorSuccess)
)

func styleStatus(status string) string {
	switch status {
	case "ready":
		return statusReady.Render(status)
	case "processing":
		return statusProcessing.Render(status)
	case "failed":
		return statusFailed.Render(status)
	case "dead_letter":
		return statusDLQ.Render("DLQ")
	case "scheduled":
		return statusScheduled.Render(status)
	case "completed":
		return lipgloss.NewStyle().Foreground(colorMuted).Render(status)
	default:
		return status
	}
}
