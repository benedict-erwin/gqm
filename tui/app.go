package tui

import (
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

const refreshInterval = 3 * time.Second
const clockInterval = 1 * time.Second

// Tab indices
const (
	tabQueues  = 0
	tabWorkers = 1
	tabFailed  = 2
	tabCron    = 3
	tabDAG     = 4
)

var tabNames = []string{"Queues", "Workers", "Failed", "Cron", "DAG"}

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
type jobDetailMsg struct {
	job Job
	err error
}
type queueJobsMsg struct {
	queue string
	jobs  []Job
	err   error
}
type cronHistMsg struct {
	id    string
	items []Job
	err   error
}
type dagRootsMsg struct {
	jobs []Job
	err  error
}
type dagGraphMsg struct {
	forID string
	graph *DagGraph
	err   error
}

// pendingConfirm is an inline [y/N] prompt for a mutating action.
type pendingConfirm struct {
	prompt      string
	pending     string  // optimistic "doing..." message shown once confirmed
	cmd         tea.Cmd // fired on confirm
	closeDetail bool    // close the job detail view on confirm (deletes)
}

// Model is the main bubbletea model for the GQM TUI.
type Model struct {
	client      *Client
	tab         int
	queues      queuesView
	workers     workersView
	failed      failedView
	cron        cronView
	dag         dagView
	queueJobs   *jobsView     // non-nil = queue drill-down open
	cronHist    *cronHistView // non-nil = cron history open
	detail      *detailView   // non-nil = job drill-down open
	confirm     *pendingConfirm
	helpOpen    bool
	width       int
	height      int
	lastErr     string
	lastRefresh time.Time
	now         time.Time
	message     string // transient action feedback
	messageErr  bool   // style the message as an error
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
				m.messageErr = false
			}
		}
		return m, clockCmd()

	case tickMsg:
		cmds := []tea.Cmd{tickCmd(), fetchData(m.client)}
		if m.tab == tabFailed && m.failed.selectedQueue() != "" {
			cmds = append(cmds, fetchFailedJobs(m.client, m.failed.selectedQueue()))
		}
		if m.tab == tabDAG && !m.dag.showGraph {
			cmds = append(cmds, fetchDAGRoots(m.client))
		}
		if m.queueJobs != nil && m.detail == nil {
			cmds = append(cmds, fetchQueueJobs(m.client, m.queueJobs.queue))
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

	case dagRootsMsg:
		if msg.err != nil {
			m.dag.err = msg.err
		} else {
			m.dag.err = nil
			m.dag.roots = msg.jobs
			m.dag.clampCursor()
		}
		return m, nil

	case dagGraphMsg:
		m.dag.graphErr = msg.err
		m.dag.graph = msg.graph
		m.dag.graphFor = msg.forID
		m.dag.treeMode = false
		m.dag.treeForced = false
		m.dag.treeScroll = 0
		if msg.graph != nil && len(msg.graph.Nodes) > 0 {
			// Select the root node initially.
			m.dag.nodeSel = 0
			for i, n := range msg.graph.Nodes {
				if n.ID == msg.graph.RootID {
					m.dag.nodeSel = i
					break
				}
			}
		}
		return m, nil

	case jobDetailMsg:
		if m.detail != nil {
			m.detail.job = msg.job
			m.detail.err = msg.err
		}
		return m, nil

	case queueJobsMsg:
		if m.queueJobs != nil && m.queueJobs.queue == msg.queue {
			m.queueJobs.jobs = msg.jobs
			m.queueJobs.err = msg.err
			m.queueJobs.clampCursor()
		}
		return m, nil

	case cronHistMsg:
		if m.cronHist != nil && m.cronHist.cronID == msg.id {
			m.cronHist.items = msg.items
			m.cronHist.err = msg.err
			m.cronHist.clampCursor()
		}
		return m, nil

	case actionMsg:
		if msg.err != nil {
			m.setError(fmt.Sprintf("Error: %v", msg.err))
		} else {
			m.setMessage(msg.desc)
		}
		// Refresh data after action.
		cmds := []tea.Cmd{fetchData(m.client)}
		if m.tab == tabFailed && m.failed.selectedQueue() != "" {
			cmds = append(cmds, fetchFailedJobs(m.client, m.failed.selectedQueue()))
		}
		if m.queueJobs != nil {
			cmds = append(cmds, fetchQueueJobs(m.client, m.queueJobs.queue))
		}
		if m.cronHist != nil {
			cmds = append(cmds, fetchCronHist(m.client, m.cronHist.cronID))
		}
		return m, tea.Batch(cmds...)
	}

	return m, nil
}

func (m *Model) setMessage(s string) {
	m.message = s
	m.messageErr = false
	m.messageTTL = 5 // 5 clock ticks = 5 seconds
}

func (m *Model) setError(s string) {
	m.message = s
	m.messageErr = true
	m.messageTTL = 5
}

// ask arms the inline [y/N] confirmation for a mutating action.
func (m *Model) ask(prompt, pending string, cmd tea.Cmd) {
	m.confirm = &pendingConfirm{prompt: prompt, pending: pending, cmd: cmd}
}

// searchTarget returns the filter fields of the view that owns "/" in the
// current context, or nils when search doesn't apply (detail, graph, etc.).
func (m *Model) searchTarget() (query *string, typing *bool, clamp func()) {
	switch {
	case m.detail != nil:
		return nil, nil, nil
	case m.queueJobs != nil:
		return &m.queueJobs.search, &m.queueJobs.typing, m.queueJobs.clampCursor
	case m.cronHist != nil:
		return &m.cronHist.search, &m.cronHist.typing, m.cronHist.clampCursor
	case m.tab == tabFailed:
		return &m.failed.search, &m.failed.typing, m.failed.clampCursor
	case m.tab == tabDAG && !m.dag.showGraph:
		return &m.dag.search, &m.dag.typing, m.dag.clampCursor
	}
	return nil, nil, nil
}

func (m Model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	key := msg.String()

	// Confirmation prompt eats every key: y runs, anything else cancels.
	if m.confirm != nil {
		c := m.confirm
		m.confirm = nil
		if key == "y" || key == "Y" {
			if c.pending != "" {
				m.setMessage(c.pending)
			}
			if c.closeDetail {
				m.detail = nil
			}
			return m, c.cmd
		}
		return m, nil
	}

	// Help overlay: any key closes it.
	if m.helpOpen {
		m.helpOpen = false
		return m, nil
	}

	// Search input mode: keys type into the active view's filter.
	if query, typing, clamp := m.searchTarget(); typing != nil && *typing {
		if key == "ctrl+c" {
			return m, tea.Quit
		}
		switch msg.Type {
		case tea.KeyEsc:
			*query = ""
			*typing = false
			clamp()
		case tea.KeyEnter:
			*typing = false
		case tea.KeyBackspace:
			r := []rune(*query)
			if len(r) > 0 {
				*query = string(r[:len(r)-1])
			}
			clamp()
		case tea.KeyRunes:
			*query += string(msg.Runes)
			clamp()
		}
		return m, nil
	}

	// Global quit always works.
	if key == "q" || key == "ctrl+c" {
		return m, tea.Quit
	}
	if key == "?" {
		m.helpOpen = true
		return m, nil
	}
	if key == "/" {
		if _, typing, _ := m.searchTarget(); typing != nil {
			*typing = true
			return m, nil
		}
	}
	// Esc clears an active (committed) filter before anything else.
	if key == "esc" {
		if query, typing, clamp := m.searchTarget(); typing != nil && *query != "" {
			*query = ""
			clamp()
			return m, nil
		}
	}

	// Tab switching is global: it also closes any open drill-down, so the
	// user is never trapped inside a detail or graph screen.
	switchTab := func(tab int) (tea.Model, tea.Cmd) {
		m.detail = nil
		m.queueJobs = nil
		m.cronHist = nil
		m.dag.showGraph = false
		m.tab = tab
		return m, m.onTabEnter()
	}
	switch key {
	case "tab":
		return switchTab((m.tab + 1) % len(tabNames))
	case "shift+tab":
		return switchTab((m.tab - 1 + len(tabNames)) % len(tabNames))
	case "1":
		return switchTab(tabQueues)
	case "2":
		return switchTab(tabWorkers)
	case "3":
		return switchTab(tabFailed)
	case "4":
		return switchTab(tabCron)
	case "5":
		return switchTab(tabDAG)
	}

	// Job detail drill-down.
	if m.detail != nil {
		return m.handleDetailKey(key)
	}

	// Queue jobs drill-down.
	if m.queueJobs != nil {
		return m.handleQueueJobsKey(key)
	}

	// Cron history drill-down.
	if m.cronHist != nil {
		return m.handleCronHistKey(key)
	}

	// DAG graph screen.
	if m.tab == tabDAG && m.dag.showGraph {
		return m.handleGraphKey(key)
	}

	switch key {
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

	case "enter":
		return m.handleEnter()

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
				m.ask(
					fmt.Sprintf("Retry job %s?", truncate(jobID, 16)),
					fmt.Sprintf("Retrying %s...", truncate(jobID, 16)),
					doAction(m.client, "Retried "+truncate(jobID, 16), func(c *Client) error {
						return c.RetryJob(jobID)
					}),
				)
			}
		}
		return m, nil

	case "d":
		if m.tab == tabFailed {
			if j := m.failed.selectedJob(); j != nil {
				jobID := j.str("id")
				m.ask(
					fmt.Sprintf("Delete job %s permanently?", truncate(jobID, 16)),
					fmt.Sprintf("Deleting %s...", truncate(jobID, 16)),
					doAction(m.client, "Deleted "+truncate(jobID, 16), func(c *Client) error {
						return c.DeleteJob(jobID)
					}),
				)
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
		if m.tab == tabDAG {
			cmds = append(cmds, fetchDAGRoots(m.client))
		}
		m.setMessage("Refreshing...")
		return m, tea.Batch(cmds...)
	}

	return m, nil
}

// onTabEnter fires the load a tab needs when it becomes active.
func (m *Model) onTabEnter() tea.Cmd {
	switch m.tab {
	case tabFailed:
		return m.loadFailedIfNeeded()
	case tabDAG:
		return fetchDAGRoots(m.client)
	}
	return nil
}

// handleEnter opens the drill-down for the focused row.
func (m Model) handleEnter() (tea.Model, tea.Cmd) {
	switch m.tab {
	case tabQueues:
		if m.queues.cursor >= 0 && m.queues.cursor < len(m.queues.queues) {
			q := m.queues.queues[m.queues.cursor]
			m.queueJobs = &jobsView{queue: q.Name}
			return m, fetchQueueJobs(m.client, q.Name)
		}
	case tabFailed:
		if j := m.failed.selectedJob(); j != nil {
			id := j.str("id")
			m.detail = &detailView{from: "Failed"}
			return m, fetchJobDetail(m.client, id)
		}
	case tabCron:
		if m.cron.cursor >= 0 && m.cron.cursor < len(m.cron.entries) {
			e := m.cron.entries[m.cron.cursor]
			id := e.str("id")
			m.cronHist = &cronHistView{cronID: id}
			return m, fetchCronHist(m.client, id)
		}
	case tabDAG:
		if r := m.dag.selectedRoot(); r != nil {
			id := r.str("id")
			m.dag.showGraph = true
			m.dag.graph = nil
			m.dag.graphErr = nil
			m.dag.graphFor = id
			return m, fetchDAGGraph(m.client, id)
		}
	}
	return m, nil
}

// handleQueueJobsKey handles keys while the queue jobs drill-down is open.
func (m Model) handleQueueJobsKey(key string) (tea.Model, tea.Cmd) {
	switch key {
	case "esc":
		m.queueJobs = nil
		return m, nil
	case "up", "k":
		m.queueJobs.cursor--
		m.queueJobs.clampCursor()
		return m, nil
	case "down", "j":
		m.queueJobs.cursor++
		m.queueJobs.clampCursor()
		return m, nil
	case "enter":
		if j := m.queueJobs.selectedJob(); j != nil {
			m.detail = &detailView{from: "Queues"}
			return m, fetchJobDetail(m.client, j.str("id"))
		}
		return m, nil
	}
	return m, nil
}

// handleCronHistKey handles keys while the cron history drill-down is open.
func (m Model) handleCronHistKey(key string) (tea.Model, tea.Cmd) {
	switch key {
	case "esc":
		m.cronHist = nil
		return m, nil
	case "up", "k":
		m.cronHist.cursor--
		m.cronHist.clampCursor()
		return m, nil
	case "down", "j":
		m.cronHist.cursor++
		m.cronHist.clampCursor()
		return m, nil
	case "enter":
		if id := m.cronHist.selectedJobID(); id != "" {
			m.detail = &detailView{from: "Cron"}
			return m, fetchJobDetail(m.client, id)
		}
		return m, nil
	}
	return m, nil
}

// handleDetailKey handles keys while the job detail view is open.
func (m Model) handleDetailKey(key string) (tea.Model, tea.Cmd) {
	switch key {
	case "esc":
		m.detail = nil
		return m, nil
	case "up", "k":
		m.detail.scroll--
		if m.detail.scroll < 0 {
			m.detail.scroll = 0
		}
		return m, nil
	case "down", "j":
		m.detail.scroll++
		return m, nil
	case "d":
		if j := m.detail.job; j != nil {
			id := j.str("id")
			m.ask(
				fmt.Sprintf("Delete job %s permanently?", truncate(id, 16)),
				fmt.Sprintf("Deleting %s...", truncate(id, 16)),
				doAction(m.client, "Deleted "+truncate(id, 16), func(c *Client) error {
					return c.DeleteJob(id)
				}),
			)
			m.confirm.closeDetail = true
		}
		return m, nil
	case "r":
		if j := m.detail.job; j != nil {
			id := j.str("id")
			st := j.str("status")
			if st == "dead_letter" || st == "failed" || st == "canceled" {
				m.ask(
					fmt.Sprintf("Retry job %s?", truncate(id, 16)),
					fmt.Sprintf("Retrying %s...", truncate(id, 16)),
					doAction(m.client, "Retried "+truncate(id, 16), func(c *Client) error {
						return c.RetryJob(id)
					}),
				)
			}
		}
		return m, nil
	}
	return m, nil
}

// handleGraphKey handles keys while the DAG graph screen is open.
func (m Model) handleGraphKey(key string) (tea.Model, tea.Cmd) {
	switch key {
	case "esc":
		m.dag.showGraph = false
		return m, nil
	case "up", "k":
		if m.dag.treeMode {
			m.dag.treeScroll--
			if m.dag.treeScroll < 0 {
				m.dag.treeScroll = 0
			}
		} else {
			m.dag.moveNodeSel(0, -1, m.width)
		}
		return m, nil
	case "down", "j":
		if m.dag.treeMode {
			m.dag.treeScroll++
		} else {
			m.dag.moveNodeSel(0, 1, m.width)
		}
		return m, nil
	case "left", "h":
		if !m.dag.treeMode {
			m.dag.moveNodeSel(-1, 0, m.width)
		}
		return m, nil
	case "right", "l":
		if !m.dag.treeMode {
			m.dag.moveNodeSel(1, 0, m.width)
		}
		return m, nil
	case "t":
		m.dag.treeMode = true
		return m, nil
	case "g":
		if !m.dag.treeForced {
			m.dag.treeMode = false
		}
		return m, nil
	case "enter":
		if n := m.dag.selectedNode(); n != nil && !m.dag.treeMode {
			m.detail = &detailView{from: "DAG"}
			return m, fetchJobDetail(m.client, n.ID)
		}
		return m, nil
	}
	return m, nil
}

func (m Model) togglePauseQueue() (tea.Model, tea.Cmd) {
	if m.queues.cursor < 0 || m.queues.cursor >= len(m.queues.queues) {
		return m, nil
	}
	q := m.queues.queues[m.queues.cursor]
	if q.Paused {
		m.ask(
			fmt.Sprintf("Resume queue %s?", q.Name),
			fmt.Sprintf("Resuming %s...", q.Name),
			doAction(m.client, fmt.Sprintf("Resumed queue %s", q.Name), func(c *Client) error {
				return c.ResumeQueue(q.Name)
			}),
		)
		return m, nil
	}
	m.ask(
		fmt.Sprintf("Pause queue %s?", q.Name),
		fmt.Sprintf("Pausing %s...", q.Name),
		doAction(m.client, fmt.Sprintf("Paused queue %s", q.Name), func(c *Client) error {
			return c.PauseQueue(q.Name)
		}),
	)
	return m, nil
}

func (m Model) triggerCron() (tea.Model, tea.Cmd) {
	if m.cron.cursor < 0 || m.cron.cursor >= len(m.cron.entries) {
		return m, nil
	}
	e := m.cron.entries[m.cron.cursor]
	id := e.str("id")
	m.ask(
		fmt.Sprintf("Trigger cron %s now?", id),
		fmt.Sprintf("Triggering %s...", id),
		doAction(m.client, fmt.Sprintf("Triggered cron %s", id), func(c *Client) error {
			return c.TriggerCron(id)
		}),
	)
	return m, nil
}

func (m Model) toggleCron() (tea.Model, tea.Cmd) {
	if m.cron.cursor < 0 || m.cron.cursor >= len(m.cron.entries) {
		return m, nil
	}
	e := m.cron.entries[m.cron.cursor]
	id := e.str("id")
	if e.enabled() {
		m.ask(
			fmt.Sprintf("Disable cron %s?", id),
			fmt.Sprintf("Disabling %s...", id),
			doAction(m.client, fmt.Sprintf("Disabled cron %s", id), func(c *Client) error {
				return c.DisableCron(id)
			}),
		)
		return m, nil
	}
	m.ask(
		fmt.Sprintf("Enable cron %s?", id),
		fmt.Sprintf("Enabling %s...", id),
		doAction(m.client, fmt.Sprintf("Enabled cron %s", id), func(c *Client) error {
			return c.EnableCron(id)
		}),
	)
	return m, nil
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
	case tabDAG:
		m.dag.cursor += delta
		m.dag.clampCursor()
	}
}

func (m Model) loadFailedIfNeeded() tea.Cmd {
	if q := m.failed.selectedQueue(); q != "" {
		return fetchFailedJobs(m.client, q)
	}
	return nil
}

// connIndicator renders the ● connection/staleness dot for the header.
func (m Model) connIndicator() string {
	if m.lastErr != "" {
		return connDown.Render("●")
	}
	if m.lastRefresh.IsZero() {
		return mutedStyle.Render("●")
	}
	if m.now.Sub(m.lastRefresh) > 2*refreshInterval+time.Second {
		return connSlow.Render("●")
	}
	return connOK.Render("●")
}

// hostLabel is the API host shown in the header.
func (m Model) hostLabel() string {
	h := m.client.baseURL
	h = strings.TrimPrefix(h, "http://")
	h = strings.TrimPrefix(h, "https://")
	return strings.TrimSuffix(h, "/")
}

func (m Model) View() string {
	var b strings.Builder

	// Header: brand, connection dot, host, clock, refresh age.
	left := " " + brandStyle.Render("GQM") + " " + mutedStyle.Render("·") + " " +
		m.connIndicator() + " " + mutedStyle.Render(m.hostLabel())
	right := mutedStyle.Render(m.now.Format("15:04:05"))
	if !m.lastRefresh.IsZero() {
		d := m.now.Sub(m.lastRefresh)
		right += mutedStyle.Render(fmt.Sprintf(" · updated %ds ago", int(d.Seconds())))
	}
	gap := 2
	if m.width > 0 {
		gap = m.width - visibleLen(left) - visibleLen(right) - 1
		if gap < 2 {
			gap = 2
		}
	}
	b.WriteString(left + strings.Repeat(" ", gap) + right + "\n\n")

	// Help overlay replaces everything below the header.
	if m.helpOpen {
		b.WriteString(m.renderHelp())
		return m.padToHeight(b.String())
	}

	// Tab bar
	b.WriteString(" ")
	for i, name := range tabNames {
		if i == m.tab {
			b.WriteString(activeTab.Render(fmt.Sprintf("%d %s", i+1, name)))
		} else {
			b.WriteString(inactiveTab.Render(fmt.Sprintf("%d %s", i+1, name)))
		}
	}
	b.WriteString("\n\n")

	// Action message
	if m.message != "" {
		style := infoStyle
		if m.messageErr {
			style = errStyle
		}
		b.WriteString(" " + style.Render(m.message) + "\n\n")
	}

	// Error banner
	if m.lastErr != "" {
		b.WriteString(" " + errStyle.Render("Error: "+m.lastErr) + "\n\n")
	}

	// Calculate available rows for table data.
	// Fixed overhead: header(1) + blank(1) + tabs(1) + blank(1) + status bar(2) = 6
	// Table overhead: header(1) + separator(1) = 2
	overhead := 8
	if m.message != "" {
		overhead += 2
	}
	if m.lastErr != "" {
		overhead += 2
	}
	if m.confirm != nil {
		overhead += 2
	}
	if m.detail == nil {
		if m.queueJobs != nil || m.cronHist != nil {
			overhead += 2 // drill-down crumb header + blank
		} else if m.tab == tabFailed {
			overhead += 2 // queue selector + blank
			if m.failed.err != nil {
				overhead += 1
			}
		}
		// Active search filter line + blank.
		if query, typing, _ := m.searchTarget(); typing != nil && (*query != "" || *typing) {
			overhead += 2
		}
	}
	maxRows := m.height - overhead
	if maxRows < 3 {
		maxRows = 3
	}

	// Content
	switch {
	case m.detail != nil:
		b.WriteString(m.detail.render(m.width, maxRows+2))
	case m.queueJobs != nil:
		b.WriteString(m.queueJobs.render(m.width, maxRows))
	case m.cronHist != nil:
		b.WriteString(m.cronHist.render(m.width, maxRows))
	case m.tab == tabQueues:
		b.WriteString(m.queues.render(m.width, maxRows))
	case m.tab == tabWorkers:
		b.WriteString(m.workers.render(m.width, maxRows))
	case m.tab == tabFailed:
		if m.failed.err != nil {
			b.WriteString(" " + errStyle.Render(m.failed.err.Error()) + "\n")
		}
		b.WriteString(m.failed.render(m.width, maxRows))
	case m.tab == tabCron:
		b.WriteString(m.cron.render(m.width, maxRows))
	case m.tab == tabDAG:
		if m.dag.showGraph {
			b.WriteString(m.dag.renderGraph(m.width, maxRows+2))
		} else {
			if m.dag.err != nil {
				b.WriteString(" " + errStyle.Render(m.dag.err.Error()) + "\n")
			}
			b.WriteString(m.dag.render(m.width, maxRows))
		}
	}

	// Inline confirmation prompt
	if m.confirm != nil {
		b.WriteString("\n " + warnStyle.Render("? "+m.confirm.prompt) + " " + mutedStyle.Render("[y/N]") + "\n")
	}

	// Status bar with contextual help
	b.WriteString(statusBar.Render(m.helpLine()))

	return m.padToHeight(b.String())
}

// helpLine builds the contextual key hint bar, truncated rune-safely.
func (m Model) helpLine() string {
	pair := func(k, desc string) string {
		return keyStyle.Render(k) + " " + desc
	}
	sep := mutedStyle.Render("  ")
	var parts []string
	switch {
	case m.detail != nil:
		parts = []string{pair("jk/↑↓", "scroll"), pair("r", "retry"), pair("d", "delete"), pair("esc", "back"), pair("q", "quit")}
	case m.queueJobs != nil:
		parts = []string{pair("↑↓", "move"), pair("enter", "detail"), pair("/", "search"), pair("esc", "back"), pair("q", "quit")}
	case m.cronHist != nil:
		parts = []string{pair("↑↓", "move"), pair("enter", "detail"), pair("/", "search"), pair("esc", "back"), pair("q", "quit")}
	case m.tab == tabDAG && m.dag.showGraph && m.dag.treeMode:
		parts = []string{pair("jk/↑↓", "scroll"), pair("g", "graph view"), pair("esc", "back"), pair("q", "quit")}
	case m.tab == tabDAG && m.dag.showGraph:
		parts = []string{pair("↑↓←→", "select node"), pair("enter", "job detail"), pair("t", "tree view"), pair("esc", "back"), pair("q", "quit")}
	case m.tab == tabQueues:
		parts = []string{pair("tab/1-5", "switch"), pair("↑↓", "move"), pair("enter", "jobs"), pair("p", "pause/resume"), pair("?", "help"), pair("q", "quit")}
	case m.tab == tabWorkers:
		parts = []string{pair("tab/1-5", "switch"), pair("↑↓", "move"), pair("?", "help"), pair("q", "quit")}
	case m.tab == tabFailed:
		parts = []string{pair("←→", "queue"), pair("↑↓", "move"), pair("r", "retry"), pair("d", "delete"), pair("enter", "detail"), pair("/", "search"), pair("q", "quit")}
	case m.tab == tabCron:
		parts = []string{pair("↑↓", "move"), pair("t", "trigger"), pair("e", "on/off"), pair("enter", "history"), pair("?", "help"), pair("q", "quit")}
	case m.tab == tabDAG:
		parts = []string{pair("↑↓", "move"), pair("enter", "graph"), pair("/", "search"), pair("?", "help"), pair("q", "quit")}
	}
	line := " " + strings.Join(parts, sep)
	if m.width > 0 && visibleLen(line) > m.width {
		line = truncateAnsi(line, m.width)
	}
	return line
}

// renderHelp draws the full-screen keyboard reference (the ? overlay).
func (m Model) renderHelp() string {
	row := func(k, desc string) string {
		return fmt.Sprintf("   %s %s\n", keyStyle.Render(fmt.Sprintf("%-9s", k)), desc)
	}
	var b strings.Builder
	b.WriteString(" " + boldStyle.Render("Keyboard") + "\n\n")
	b.WriteString(" " + headerText.Render("GLOBAL") + "\n")
	b.WriteString(row("tab 1-5", "switch tab"))
	b.WriteString(row("↑↓ jk", "move cursor"))
	b.WriteString(row("enter", "open jobs / detail / history / graph"))
	b.WriteString(row("/", "filter by job ID"))
	b.WriteString(row("esc", "back / clear filter"))
	b.WriteString(row("F5", "force refresh"))
	b.WriteString(row("?", "this help"))
	b.WriteString(row("q", "quit"))
	b.WriteString("\n " + headerText.Render("PER TAB") + "\n")
	b.WriteString(row("p", "pause/resume queue  (Queues)"))
	b.WriteString(row("r d", "retry / delete job  (Failed, detail)"))
	b.WriteString(row("←→ hl", "switch queue  (Failed)"))
	b.WriteString(row("t e", "trigger / enable-disable  (Cron)"))
	b.WriteString(row("g t", "graph / tree view  (DAG)"))
	b.WriteString("\n " + mutedStyle.Render("press any key to close") + "\n")
	return b.String()
}

// padToHeight pads output with newlines so resize clears stale content.
func (m Model) padToHeight(output string) string {
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

func fetchDAGRoots(c *Client) tea.Cmd {
	return func() tea.Msg {
		jobs, err := c.ListDAGRoots()
		if err != nil {
			return dagRootsMsg{err: err}
		}
		return dagRootsMsg{jobs: jobs}
	}
}

func fetchDAGGraph(c *Client, id string) tea.Cmd {
	return func() tea.Msg {
		g, err := c.GetDAGGraph(id)
		if err != nil {
			return dagGraphMsg{forID: id, err: err}
		}
		return dagGraphMsg{forID: id, graph: g}
	}
}

func fetchQueueJobs(c *Client, queue string) tea.Cmd {
	return func() tea.Msg {
		jobs, err := c.ListQueueJobsMerged(queue)
		if err != nil {
			return queueJobsMsg{queue: queue, err: err}
		}
		return queueJobsMsg{queue: queue, jobs: jobs}
	}
}

func fetchCronHist(c *Client, id string) tea.Cmd {
	return func() tea.Msg {
		items, err := c.ListCronHistory(id)
		if err != nil {
			return cronHistMsg{id: id, err: err}
		}
		return cronHistMsg{id: id, items: items}
	}
}

func fetchJobDetail(c *Client, id string) tea.Cmd {
	return func() tea.Msg {
		job, err := c.GetJob(id)
		if err != nil {
			return jobDetailMsg{err: err}
		}
		return jobDetailMsg{job: job}
	}
}

func doAction(c *Client, desc string, fn func(*Client) error) tea.Cmd {
	return func() tea.Msg {
		err := fn(c)
		return actionMsg{desc: desc, err: err}
	}
}
