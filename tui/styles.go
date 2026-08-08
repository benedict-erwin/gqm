package tui

import "github.com/charmbracelet/lipgloss"

// Palette. Every color is an AdaptiveColor so the TUI is readable on light
// terminals too — lipgloss picks the variant from the terminal background.
// Values are ANSI-256 steps chosen to mirror the dashboard's design language:
// teal accent, one distinct hue per job status.
var (
	colorAccent    = lipgloss.AdaptiveColor{Light: "30", Dark: "43"}   // teal
	colorSecondary = lipgloss.AdaptiveColor{Light: "241", Dark: "251"} // gray text (headers, crumbs)
	colorMuted     = lipgloss.AdaptiveColor{Light: "247", Dark: "247"} // de-emphasized text
	colorDim       = lipgloss.AdaptiveColor{Light: "250", Dark: "243"} // separators, zero values

	colorReady      = lipgloss.AdaptiveColor{Light: "29", Dark: "78"}   // green
	colorProcessing = lipgloss.AdaptiveColor{Light: "32", Dark: "75"}   // blue
	colorCompleted  = lipgloss.AdaptiveColor{Light: "245", Dark: "249"} // gray
	colorFailed     = lipgloss.AdaptiveColor{Light: "160", Dark: "167"} // red
	colorDLQ        = lipgloss.AdaptiveColor{Light: "162", Dark: "205"} // magenta
	colorScheduled  = lipgloss.AdaptiveColor{Light: "31", Dark: "80"}   // cyan
	colorDeferred   = lipgloss.AdaptiveColor{Light: "97", Dark: "141"}  // violet
	colorCanceled   = lipgloss.AdaptiveColor{Light: "244", Dark: "249"} // gray
	colorWarning    = lipgloss.AdaptiveColor{Light: "130", Dark: "179"} // amber

	selectedBg = lipgloss.AdaptiveColor{Light: "254", Dark: "236"}

	// Tab bar
	activeTab = lipgloss.NewStyle().
			Bold(true).
			Foreground(lipgloss.AdaptiveColor{Light: "231", Dark: "16"}).
			Background(colorAccent).
			Padding(0, 2)

	inactiveTab = lipgloss.NewStyle().
			Foreground(colorSecondary).
			Padding(0, 2)

	// Status bar
	statusBar = lipgloss.NewStyle().
			Foreground(colorMuted).
			MarginTop(1)

	// Table
	selectedRow = lipgloss.NewStyle().Background(selectedBg)
	cursorBar   = lipgloss.NewStyle().Foreground(colorAccent).Background(selectedBg)

	// Brand / header
	brandStyle = lipgloss.NewStyle().Bold(true).Foreground(colorAccent)
	mutedStyle = lipgloss.NewStyle().Foreground(colorMuted)
	dimStyle   = lipgloss.NewStyle().Foreground(colorDim)

	// Connection indicator states
	connOK    = lipgloss.NewStyle().Foreground(colorReady)
	connSlow  = lipgloss.NewStyle().Foreground(colorWarning)
	connDown  = lipgloss.NewStyle().Foreground(colorFailed)
	keyStyle  = lipgloss.NewStyle().Foreground(colorAccent)
	boldStyle = lipgloss.NewStyle().Bold(true)

	// Status styles
	statusReady      = lipgloss.NewStyle().Foreground(colorReady)
	statusProcessing = lipgloss.NewStyle().Foreground(colorProcessing)
	statusCompleted  = lipgloss.NewStyle().Foreground(colorCompleted)
	statusFailed     = lipgloss.NewStyle().Foreground(colorFailed)
	statusDLQ        = lipgloss.NewStyle().Foreground(colorDLQ).Bold(true)
	statusScheduled  = lipgloss.NewStyle().Foreground(colorScheduled)
	statusDeferred   = lipgloss.NewStyle().Foreground(colorDeferred)
	statusCanceled   = lipgloss.NewStyle().Foreground(colorCanceled)
	statusPaused     = lipgloss.NewStyle().Foreground(colorWarning).Bold(true)
	statusActive     = lipgloss.NewStyle().Foreground(colorReady).Bold(true)
	statusStale      = lipgloss.NewStyle().Foreground(colorFailed).Bold(true)
	statusWarn       = lipgloss.NewStyle().Foreground(colorWarning)

	// Error/info messages
	errStyle  = lipgloss.NewStyle().Foreground(colorFailed)
	infoStyle = lipgloss.NewStyle().Foreground(colorReady)
	warnStyle = lipgloss.NewStyle().Foreground(colorWarning)

	// JSON pretty-printing (job detail payload pane)
	jsonKeyStyle = lipgloss.NewStyle().Foreground(colorSecondary)
	jsonStrStyle = lipgloss.NewStyle().Foreground(colorReady)
	jsonNumStyle = lipgloss.NewStyle().Foreground(colorProcessing)
	jsonPunStyle = lipgloss.NewStyle().Foreground(colorMuted)
)

// styleStatus renders a status word in its color. Every status the API can
// produce has a case; unknown values fall back to muted rather than unstyled
// so nothing renders as bare default text.
func styleStatus(status string) string {
	switch status {
	case "ready":
		return statusReady.Render(status)
	case "processing":
		return statusProcessing.Render(status)
	case "completed":
		return statusCompleted.Render(status)
	case "failed":
		return statusFailed.Render(status)
	case "dead_letter":
		return statusDLQ.Render("DLQ")
	case "scheduled":
		return statusScheduled.Render(status)
	case "deferred":
		return statusDeferred.Render(status)
	case "canceled":
		return statusCanceled.Render(status)
	case "active":
		return statusActive.Render(status)
	case "stale":
		return statusStale.Render(status)
	case "paused":
		return statusPaused.Render("PAUSED")
	case "draining":
		return statusWarn.Render(status)
	case "stopped":
		return statusCanceled.Render(status)
	default:
		return mutedStyle.Render(status)
	}
}

// jobStatusCell renders a job's status, replacing "processing" with a stale
// marker when the API reports the claim as abandoned. A job whose worker
// process died must not read as work in flight.
func jobStatusCell(j Job) string {
	if j.stale() {
		return statusStale.Render("stale")
	}
	return styleStatus(j.str("status"))
}

// statusBorderColor returns the border color for a DAG node with this status.
func statusBorderColor(status string) lipgloss.AdaptiveColor {
	switch status {
	case "ready":
		return colorReady
	case "processing":
		return colorProcessing
	case "completed":
		return colorCompleted
	case "failed":
		return colorFailed
	case "dead_letter":
		return colorDLQ
	case "scheduled":
		return colorScheduled
	case "deferred":
		return colorDeferred
	case "canceled":
		return colorCanceled
	default:
		return colorMuted
	}
}

// dimZero renders a count: zero becomes a muted dot so populated cells stand
// out, non-zero gets the given style.
func dimZero(n int64, style lipgloss.Style) string {
	if n == 0 {
		return dimStyle.Render("·")
	}
	return style.Render(intToString(n))
}
