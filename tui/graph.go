package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// maxBoxNodes is the cutoff above which the graph renderer falls back to the
// tree view: a layered box layout stops being readable, and edge routing for
// arbitrary shapes is not worth the complexity in a terminal.
const maxBoxNodes = 15

// layNode is a node placed by the layered layout.
type layNode struct {
	idx     int // index into DagGraph.Nodes
	rank    int
	xCenter int // canvas column of the box center
}

// dagLayout computes layered positions for a graph. ok is false when the
// graph should use the tree fallback instead (too many nodes, too wide, or
// not connected the way the layout expects).
type dagLayout struct {
	ranks  [][]int // rank -> node indices
	placed []layNode
	boxW   int
	width  int
}

// assignRanks computes each node's rank as the longest path from a source.
func assignRanks(g *DagGraph) []int {
	n := len(g.Nodes)
	idxOf := make(map[string]int, n)
	for i, node := range g.Nodes {
		idxOf[node.ID] = i
	}
	parents := make([][]int, n)
	for _, e := range g.Edges {
		s, okS := idxOf[e.Source]
		t, okT := idxOf[e.Target]
		if !okS || !okT {
			continue
		}
		parents[t] = append(parents[t], s)
	}
	ranks := make([]int, n)
	var visit func(i int, depth int) int
	visit = func(i, depth int) int {
		if depth > n { // cycle guard — the API should never produce one
			return 0
		}
		if len(parents[i]) == 0 {
			return 0
		}
		best := 0
		for _, p := range parents[i] {
			if r := visit(p, depth+1) + 1; r > best {
				best = r
			}
		}
		return best
	}
	for i := range g.Nodes {
		ranks[i] = visit(i, 0)
	}
	return ranks
}

func newDagLayout(g *DagGraph, width int) (*dagLayout, bool) {
	if len(g.Nodes) == 0 || len(g.Nodes) > maxBoxNodes {
		return nil, false
	}

	ranks := assignRanks(g)
	maxRank := 0
	for _, r := range ranks {
		if r > maxRank {
			maxRank = r
		}
	}
	byRank := make([][]int, maxRank+1)
	for i, r := range ranks {
		byRank[r] = append(byRank[r], i)
	}

	// Uniform box width: widest content across all nodes.
	boxW := 0
	for _, n := range g.Nodes {
		label := n.Type
		if label == "" {
			label = "unknown"
		}
		sub := shortID(n.ID) + " " + statusLabel(n.Status)
		w := len(label)
		if len(sub) > w {
			w = len(sub)
		}
		if w+4 > boxW { // 2 padding + 2 borders
			boxW = w + 4
		}
	}

	const gap = 4
	lay := &dagLayout{ranks: byRank, boxW: boxW, width: width}
	for r, nodes := range byRank {
		totalW := len(nodes)*boxW + (len(nodes)-1)*gap
		if width > 0 && totalW > width-2 {
			return nil, false
		}
		startX := 1
		if width > 0 {
			startX = (width - totalW) / 2
			if startX < 1 {
				startX = 1
			}
		}
		for c, idx := range nodes {
			x := startX + c*(boxW+gap)
			lay.placed = append(lay.placed, layNode{idx: idx, rank: r, xCenter: x + boxW/2})
		}
	}
	return lay, true
}

func shortID(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func statusLabel(s string) string {
	if s == "dead_letter" {
		return "DLQ"
	}
	if s == "" {
		return "unknown"
	}
	return s
}

// find returns the placed node for a node index.
func (l *dagLayout) find(idx int) *layNode {
	for i := range l.placed {
		if l.placed[i].idx == idx {
			return &l.placed[i]
		}
	}
	return nil
}

// renderDagGraph renders the layered box view. selIdx is the selected node
// index into g.Nodes (-1 for none).
func renderDagGraph(g *DagGraph, lay *dagLayout, selIdx int) string {
	var b strings.Builder

	// child/parent centers per rank pair, for connectors.
	idxOf := make(map[string]int, len(g.Nodes))
	for i, n := range g.Nodes {
		idxOf[n.ID] = i
	}

	for r, nodes := range lay.ranks {
		// Box rows: top, label, sub, bottom.
		rows := [4]strings.Builder{}
		for _, idx := range nodes {
			p := lay.find(idx)
			n := g.Nodes[idx]
			x := p.xCenter - lay.boxW/2
			for i := range rows {
				pad := x - cursorLen(&rows[i])
				if pad > 0 {
					rows[i].WriteString(strings.Repeat(" ", pad))
				}
			}
			style := lipgloss.NewStyle().Foreground(statusBorderColor(n.Status))
			isRoot := n.ID == g.RootID
			tl, tr, bl, br, hz, vt := "┌", "┐", "└", "┘", "─", "│"
			if isRoot {
				tl, tr, bl, br, hz, vt = "╔", "╗", "╚", "╝", "═", "║"
			}
			inner := lay.boxW - 2

			label := n.Type
			if label == "" {
				label = "unknown"
			}
			label = truncate(label, inner-2)
			sub := shortID(n.ID) + " " + statusLabel(n.Status)
			sub = truncate(sub, inner-2)

			labelLine := " " + boldStyle.Render(label) + strings.Repeat(" ", inner-2-len([]rune(label))) + " "
			subLine := " " + mutedStyle.Render(shortID(n.ID)) + " " + style.Render(statusLabel(n.Status)) +
				strings.Repeat(" ", inner-2-len([]rune(sub))) + " "
			if idx == selIdx {
				labelLine = selectedRow.Render(labelLine)
				subLine = selectedRow.Render(subLine)
			}

			rows[0].WriteString(style.Render(tl + strings.Repeat(hz, inner) + tr))
			rows[1].WriteString(style.Render(vt) + labelLine + style.Render(vt))
			rows[2].WriteString(style.Render(vt) + subLine + style.Render(vt))
			rows[3].WriteString(style.Render(bl + strings.Repeat(hz, inner) + br))
		}
		for i := range rows {
			b.WriteString(rows[i].String())
			b.WriteString("\n")
		}

		// Connector section to the next rank.
		if r+1 < len(lay.ranks) {
			var parentXs, childXs []int
			nextRank := map[int]bool{}
			for _, idx := range lay.ranks[r+1] {
				nextRank[idx] = true
			}
			seenP, seenC := map[int]bool{}, map[int]bool{}
			for _, e := range g.Edges {
				s, okS := idxOf[e.Source]
				t, okT := idxOf[e.Target]
				if !okS || !okT || !nextRank[t] {
					continue
				}
				ps, cs := lay.find(s), lay.find(t)
				if ps == nil || cs == nil || ps.rank != r {
					continue
				}
				if !seenP[s] {
					parentXs = append(parentXs, ps.xCenter)
					seenP[s] = true
				}
				if !seenC[t] {
					childXs = append(childXs, cs.xCenter)
					seenC[t] = true
				}
			}
			b.WriteString(connectorLines(parentXs, childXs, lay.width))
		}
	}
	return b.String()
}

func cursorLen(b *strings.Builder) int {
	return visibleLen(b.String())
}

// connectorLines draws the three-line bus between two ranks:
// verticals from parents, a horizontal bus joining every column, and
// arrowheads into the children.
func connectorLines(parentXs, childXs []int, width int) string {
	if len(parentXs) == 0 || len(childXs) == 0 {
		return ""
	}
	maxX := 0
	all := append(append([]int{}, parentXs...), childXs...)
	minX := all[0]
	for _, x := range all {
		if x < minX {
			minX = x
		}
		if x > maxX {
			maxX = x
		}
	}

	line := func(fill func([]rune)) string {
		row := make([]rune, maxX+2)
		for i := range row {
			row[i] = ' '
		}
		fill(row)
		return dimStyle.Render(strings.TrimRight(string(row), " "))
	}

	l1 := line(func(row []rune) {
		for _, x := range parentXs {
			row[x] = '│'
		}
	})
	l2 := line(func(row []rune) {
		for x := minX; x <= maxX; x++ {
			row[x] = '─'
		}
		for _, x := range parentXs {
			row[x] = '┴'
		}
		for _, x := range childXs {
			row[x] = '┬'
		}
		// The bus ends turn into corners: a parent at the end feeds from
		// above (└/┘), a child at the end feeds downward (┌/┐).
		switch row[minX] {
		case '┴':
			row[minX] = '└'
		case '┬':
			row[minX] = '┌'
		}
		switch row[maxX] {
		case '┴':
			row[maxX] = '┘'
		case '┬':
			row[maxX] = '┐'
		}
		if minX == maxX {
			row[minX] = '│'
		}
	})
	l3 := line(func(row []rune) {
		for _, x := range childXs {
			row[x] = '▼'
		}
	})
	return l1 + "\n" + l2 + "\n" + l3 + "\n"
}

// renderDagTree renders the fallback indented tree view.
func renderDagTree(g *DagGraph, scroll, maxRows int) (string, int) {
	idxOf := make(map[string]int, len(g.Nodes))
	for i, n := range g.Nodes {
		idxOf[n.ID] = i
	}
	children := make(map[string][]string)
	hasParent := make(map[string]bool)
	for _, e := range g.Edges {
		children[e.Source] = append(children[e.Source], e.Target)
		hasParent[e.Target] = true
	}

	var lines []string
	visited := map[string]bool{}
	var walk func(id, prefix string, last bool, top bool)
	walk = func(id, prefix string, last, top bool) {
		idx, ok := idxOf[id]
		if !ok {
			return
		}
		n := g.Nodes[idx]
		branch := ""
		childPrefix := prefix
		if !top {
			if last {
				branch = prefix + "└─ "
				childPrefix = prefix + "   "
			} else {
				branch = prefix + "├─ "
				childPrefix = prefix + "│  "
			}
		}
		style := lipgloss.NewStyle().Foreground(statusBorderColor(n.Status))
		label := n.Type
		if label == "" {
			label = "unknown"
		}
		entry := dimStyle.Render(branch) + style.Render("●") + " " + boldStyle.Render(label) +
			" " + mutedStyle.Render(shortID(n.ID)) + " " + style.Render(statusLabel(n.Status))
		if visited[id] {
			lines = append(lines, entry+" "+dimStyle.Render("⧉"))
			return
		}
		visited[id] = true
		lines = append(lines, entry)
		kids := children[id]
		for i, k := range kids {
			walk(k, childPrefix, i == len(kids)-1, false)
		}
	}
	root := g.RootID
	if root == "" && len(g.Nodes) > 0 {
		root = g.Nodes[0].ID
	}
	walk(root, "", true, true)
	// Any disconnected nodes still get listed.
	for _, n := range g.Nodes {
		if !visited[n.ID] && !hasParent[n.ID] {
			walk(n.ID, "", true, true)
		}
	}

	total := len(lines)
	if maxRows > 0 && total > maxRows {
		if scroll > total-maxRows {
			scroll = total - maxRows
		}
		if scroll < 0 {
			scroll = 0
		}
		end := scroll + maxRows
		visibleLines := lines[scroll:end]
		out := strings.Join(visibleLines, "\n") + "\n"
		if end < total {
			out += scrollStyle.Render(fmt.Sprintf("  ↓ %d more", total-end)) + "\n"
		}
		return out, total
	}
	return strings.Join(lines, "\n") + "\n", total
}
