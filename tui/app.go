package tui

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

const refreshInterval = 3 * time.Second
const clockInterval = 1 * time.Second

// Tab indices
const (
	tabQueues  = 0
	tabWorkers = 1
	tabFailed  = 2
	tabCron    = 3
)

var tabNames = []string{"Queues", "Workers", "Failed", "Cron"}

// messages
type tickMsg time.Time
type clockMsg time.Time
type dataMsg struct {
	queues  []Queue
	workers []Worker
	cron    []CronEntry
	err     error
}
type failedJobsMsg struct {
	jobs []Job
	err  error
}
type actionMsg struct {
	desc string
	err  error
}

// Model is the main bubbletea model for the GQM TUI.
type Model struct {
	client      *Client
	tab         int
	queues      queuesView
	workers     workersView
	failed      failedView
	cron        cronView
	width       int
	height      int
	lastErr     string
	lastRefresh time.Time
	now         time.Time
	message     string // transient action feedback
	messageTTL  int    // ticks remaining for message
}

// NewModel creates a new TUI model.
func NewModel(client *Client) Model {
	return Model{
		client: client,
		now:    time.Now(),
	}
}

// Run starts the TUI application.
func Run(client *Client) error {
	p := tea.NewProgram(NewModel(client), tea.WithAltScreen())
	_, err := p.Run()
	return err
}

func (m Model) Init() tea.Cmd {
	return tea.Batch(fetchData(m.client), tickCmd(), clockCmd())
}

func (m Model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		return m.handleKey(msg)

	case tea.WindowSizeMsg:
		m.width = msg.Width
		m.height = msg.Height
		return m, nil

	case clockMsg:
		m.now = time.Time(msg)
		if m.messageTTL > 0 {
			m.messageTTL--
			if m.messageTTL == 0 {
				m.message = ""
			}
		}
		return m, clockCmd()

	case tickMsg:
		cmds := []tea.Cmd{tickCmd(), fetchData(m.client)}
		if m.tab == tabFailed && m.failed.selectedQueue() != "" {
			cmds = append(cmds, fetchFailedJobs(m.client, m.failed.selectedQueue()))
		}
		return m, tea.Batch(cmds...)

	case dataMsg:
		m.lastRefresh = time.Now()
		if msg.err != nil {
			m.lastErr = msg.err.Error()
		} else {
			m.lastErr = ""
			m.queues.queues = msg.queues
			m.queues.clampCursor()
			m.workers.workers = msg.workers
			m.workers.clampCursor()
			m.cron.entries = msg.cron
			m.cron.clampCursor()
			// Update failed view queue list.
			m.failed.queues = msg.queues
			// Auto-select first queue if none selected.
			if m.failed.selectedQueue() == "" && len(msg.queues) > 0 {
				m.failed.queueIdx = 0
				return m, fetchFailedJobs(m.client, msg.queues[0].Name)
			}
		}
		return m, nil

	case failedJobsMsg:
		if msg.err != nil {
			m.failed.err = msg.err
		} else {
			m.failed.err = nil
			m.failed.jobs = msg.jobs
			m.failed.clampCursor()
		}
		return m, nil

	case actionMsg:
		if msg.err != nil {
			m.setMessage(fmt.Sprintf("Error: %v", msg.err))
		} else {
			m.setMessage(msg.desc)
		}
		// Refresh data after action.
		cmds := []tea.Cmd{fetchData(m.client)}
		if m.tab == tabFailed && m.failed.selectedQueue() != "" {
			cmds = append(cmds, fetchFailedJobs(m.client, m.failed.selectedQueue()))
		}
		return m, tea.Batch(cmds...)
	}

	return m, nil
}

func (m *Model) setMessage(s string) {
	m.message = s
	m.messageTTL = 5 // 5 clock ticks = 5 seconds
}

func (m Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "q", "ctrl+c":
		return m, tea.Quit

	case "tab":
		m.tab = (m.tab + 1) % len(tabNames)
		if m.tab == tabFailed {
			return m, m.loadFailedIfNeeded()
		}
		return m, nil

	case "shift+tab":
		m.tab = (m.tab - 1 + len(tabNames)) % len(tabNames)
		if m.tab == tabFailed {
			return m, m.loadFailedIfNeeded()
		}
		return m, nil

	case "1":
		m.tab = tabQueues
		return m, nil
	case "2":
		m.tab = tabWorkers
		return m, nil
	case "3":
		m.tab = tabFailed
		return m, m.loadFailedIfNeeded()
	case "4":
		m.tab = tabCron
		return m, nil

	case "up", "k":
		m.moveCursor(-1)
		return m, nil
	case "down", "j":
		m.moveCursor(1)
		return m, nil

	case "left", "h":
		if m.tab == tabFailed && len(m.failed.queues) > 0 {
			m.failed.queueIdx = (m.failed.queueIdx - 1 + len(m.failed.queues)) % len(m.failed.queues)
			m.failed.cursor = 0
			return m, fetchFailedJobs(m.client, m.failed.selectedQueue())
		}
		return m, nil

	case "right", "l":
		if m.tab == tabFailed && len(m.failed.queues) > 0 {
			m.failed.queueIdx = (m.failed.queueIdx + 1) % len(m.failed.queues)
			m.failed.cursor = 0
			return m, fetchFailedJobs(m.client, m.failed.selectedQueue())
		}
		return m, nil

	// Actions
	case "p":
		if m.tab == tabQueues {
			return m.togglePauseQueue()
		}
		return m, nil

	case "r":
		if m.tab == tabFailed {
			if j := m.failed.selectedJob(); j != nil {
				jobID := j.str("id")
				m.setMessage(fmt.Sprintf("Retrying %s...", truncate(jobID, 16)))
				return m, doAction(m.client, "retry", jobID, func(c *Client) error {
					return c.RetryJob(jobID)
				})
			}
		}
		return m, nil

	case "t":
		if m.tab == tabCron {
			return m.triggerCron()
		}
		return m, nil

	case "e":
		if m.tab == tabCron {
			return m.toggleCron()
		}
		return m, nil

	case "f5":
		// Force refresh
		cmds := []tea.Cmd{fetchData(m.client)}
		if m.tab == tabFailed && m.failed.selectedQueue() != "" {
			cmds = append(cmds, fetchFailedJobs(m.client, m.failed.selectedQueue()))
		}
		m.setMessage("Refreshing...")
		return m, tea.Batch(cmds...)
	}

	return m, nil
}

func (m Model) togglePauseQueue() (tea.Model, tea.Cmd) {
	if m.queues.cursor < 0 || m.queues.cursor >= len(m.queues.queues) {
		return m, nil
	}
	q := m.queues.queues[m.queues.cursor]
	if q.Paused {
		m.setMessage(fmt.Sprintf("Resuming %s...", q.Name))
		return m, doAction(m.client, fmt.Sprintf("Resumed queue %s", q.Name), "", func(c *Client) error {
			return c.ResumeQueue(q.Name)
		})
	}
	m.setMessage(fmt.Sprintf("Pausing %s...", q.Name))
	return m, doAction(m.client, fmt.Sprintf("Paused queue %s", q.Name), "", func(c *Client) error {
		return c.PauseQueue(q.Name)
	})
}

func (m Model) triggerCron() (tea.Model, tea.Cmd) {
	if m.cron.cursor < 0 || m.cron.cursor >= len(m.cron.entries) {
		return m, nil
	}
	e := m.cron.entries[m.cron.cursor]
	id := e.str("id")
	m.setMessage(fmt.Sprintf("Triggering %s...", id))
	return m, doAction(m.client, fmt.Sprintf("Triggered cron %s", id), "", func(c *Client) error {
		return c.TriggerCron(id)
	})
}

func (m Model) toggleCron() (tea.Model, tea.Cmd) {
	if m.cron.cursor < 0 || m.cron.cursor >= len(m.cron.entries) {
		return m, nil
	}
	e := m.cron.entries[m.cron.cursor]
	id := e.str("id")
	if e.enabled() {
		m.setMessage(fmt.Sprintf("Disabling %s...", id))
		return m, doAction(m.client, fmt.Sprintf("Disabled cron %s", id), "", func(c *Client) error {
			return c.DisableCron(id)
		})
	}
	m.setMessage(fmt.Sprintf("Enabling %s...", id))
	return m, doAction(m.client, fmt.Sprintf("Enabled cron %s", id), "", func(c *Client) error {
		return c.EnableCron(id)
	})
}

func (m *Model) moveCursor(delta int) {
	switch m.tab {
	case tabQueues:
		m.queues.cursor += delta
		m.queues.clampCursor()
	case tabWorkers:
		m.workers.cursor += delta
		m.workers.clampCursor()
	case tabFailed:
		m.failed.cursor += delta
		m.failed.clampCursor()
	case tabCron:
		m.cron.cursor += delta
		m.cron.clampCursor()
	}
}

func (m Model) loadFailedIfNeeded() tea.Cmd {
	if q := m.failed.selectedQueue(); q != "" {
		return fetchFailedJobs(m.client, q)
	}
	return nil
}

func (m Model) View() string {
	var b strings.Builder

	// Title + clock
	title := lipgloss.NewStyle().Bold(true).Foreground(colorPrimary).Render("GQM Monitor")
	clock := lipgloss.NewStyle().Foreground(colorMuted).Render(m.now.Format("15:04:05"))
	refreshAgo := ""
	if !m.lastRefresh.IsZero() {
		d := m.now.Sub(m.lastRefresh)
		refreshAgo = lipgloss.NewStyle().Foreground(colorMuted).Render(
			fmt.Sprintf("  updated %ds ago", int(d.Seconds())))
	}
	b.WriteString(title + "  " + clock + refreshAgo + "\n\n")

	// Tab bar
	for i, name := range tabNames {
		if i == m.tab {
			b.WriteString(activeTab.Render(fmt.Sprintf(" %d %s ", i+1, name)))
		} else {
			b.WriteString(inactiveTab.Render(fmt.Sprintf(" %d %s ", i+1, name)))
		}
	}
	b.WriteString("\n\n")

	// Action message
	if m.message != "" {
		b.WriteString(infoStyle.Render(m.message) + "\n\n")
	}

	// Error banner
	if m.lastErr != "" {
		b.WriteString(errStyle.Render("Error: "+m.lastErr) + "\n\n")
	}

	// Calculate available rows for table data.
	// Fixed overhead: title(1) + blank(1) + tabs(1) + blank(1) + status bar(2) = 6
	// Table overhead: header(1) + separator(1) = 2
	overhead := 8
	if m.message != "" {
		overhead += 2
	}
	if m.lastErr != "" {
		overhead += 2
	}
	if m.tab == tabFailed {
		overhead += 2 // queue selector + blank
		if m.failed.err != nil {
			overhead += 1
		}
	}
	maxRows := m.height - overhead
	if maxRows < 3 {
		maxRows = 3
	}

	// Content
	switch m.tab {
	case tabQueues:
		b.WriteString(m.queues.render(m.width, maxRows))
	case tabWorkers:
		b.WriteString(m.workers.render(m.width, maxRows))
	case tabFailed:
		if m.failed.err != nil {
			b.WriteString(errStyle.Render(m.failed.err.Error()) + "\n")
		}
		b.WriteString(m.failed.render(m.width, maxRows))
	case tabCron:
		b.WriteString(m.cron.render(m.width, maxRows))
	}

	// Status bar with contextual help
	var help string
	switch m.tab {
	case tabQueues:
		help = "tab/1-4: switch  ↑↓/jk: navigate  p: pause/resume  F5: refresh  q: quit"
	case tabWorkers:
		help = "tab/1-4: switch  ↑↓/jk: navigate  F5: refresh  q: quit"
	case tabFailed:
		help = "tab/1-4: switch  ←→/hl: queue  ↑↓/jk: navigate  r: retry  F5: refresh  q: quit"
	case tabCron:
		help = "tab/1-4: switch  ↑↓/jk: navigate  t: trigger  e: enable/disable  F5: refresh  q: quit"
	}
	// Truncate help text to prevent line wrapping
	if m.width > 0 && len(help) > m.width {
		help = help[:m.width]
	}
	b.WriteString(statusBar.Render(help))

	// Pad output to fill terminal height so resize clears stale content.
	// Visual rows = newline count + 1, so pad to m.height-1 newlines.
	output := b.String()
	if m.height > 0 {
		newlines := strings.Count(output, "\n")
		for i := newlines; i < m.height-1; i++ {
			output += "\n"
		}
	}

	return output
}

// Commands

func tickCmd() tea.Cmd {
	return tea.Tick(refreshInterval, func(t time.Time) tea.Msg {
		return tickMsg(t)
	})
}

func clockCmd() tea.Cmd {
	return tea.Tick(clockInterval, func(t time.Time) tea.Msg {
		return clockMsg(t)
	})
}

func fetchData(c *Client) tea.Cmd {
	return func() tea.Msg {
		queues, err := c.ListQueues()
		if err != nil {
			return dataMsg{err: err}
		}
		workers, err := c.ListWorkers()
		if err != nil {
			return dataMsg{err: err}
		}
		cron, err := c.ListCron()
		if err != nil {
			return dataMsg{err: err}
		}
		return dataMsg{queues: queues, workers: workers, cron: cron}
	}
}

func fetchFailedJobs(c *Client, queue string) tea.Cmd {
	return func() tea.Msg {
		dlq, err := c.ListDLQ(queue)
		if err != nil {
			return failedJobsMsg{err: err}
		}
		return failedJobsMsg{jobs: dlq}
	}
}

func doAction(c *Client, desc, _ string, fn func(*Client) error) tea.Cmd {
	return func() tea.Msg {
		err := fn(c)
		return actionMsg{desc: desc, err: err}
	}
}
