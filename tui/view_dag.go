package tui

import (
	"fmt"
	"time"
)

type dagView struct {
	roots  []Job
	cursor int
	search string
	typing bool
	err    error

	// Graph state — set when a chain has been opened.
	showGraph  bool
	graph      *DagGraph
	graphErr   error
	graphFor   string
	treeMode   bool // true = tree fallback / manual toggle
	treeForced bool // layout didn't fit; g cannot switch back
	nodeSel    int  // selected node index into graph.Nodes
	treeScroll int
}

func (v *dagView) visible() []Job {
	return filterByID(v.roots, "id", v.search)
}

func (v *dagView) render(width, maxRows int) string {
	var out string
	roots := v.visible()
	out += searchLine(v.search, v.typing, len(roots))

	now := time.Now()
	t := newTable(width,
		colDef{header: "ROOT", flex: true, min: 10},
		colDef{header: "TYPE", flex: true, min: 8},
		colDef{header: "STATUS"},
		colDef{header: "QUEUE", flex: true, min: 8},
		colDef{header: "CHILDREN"},
		colDef{header: "CREATED"},
	)
	for _, j := range roots {
		t.addRow(
			j.str("id"),
			j.str("type"),
			styleStatus(j.str("status")),
			j.str("queue"),
			j.str("child_count"),
			mutedStyle.Render(formatUnixTime(j.int64val("created_at"), now)),
		)
	}
	out += t.render(v.cursor, maxRows)
	return out
}

// renderGraph renders the opened chain: box layout when it fits, tree
// otherwise.
func (v *dagView) renderGraph(width, maxRows int) string {
	if v.graphErr != nil {
		return errStyle.Render("Failed to load graph: "+v.graphErr.Error()) + "\n"
	}
	g := v.graph
	if g == nil {
		return mutedStyle.Render("  Loading graph...") + "\n"
	}
	if len(g.Nodes) == 0 {
		return mutedStyle.Render("  Job not found or no graph data available.") + "\n"
	}

	head := " " + mutedStyle.Render("DAG / ") + boldStyle.Render(shortID(v.graphFor)+"…") +
		"  " + mutedStyle.Render(fmt.Sprintf("%d nodes", len(g.Nodes)))
	if g.Truncated {
		head += "  " + warnStyle.Render("(truncated)")
	}
	head += "\n\n"

	if !v.treeMode {
		if lay, ok := newDagLayout(g, width); ok {
			sel := -1
			if v.nodeSel >= 0 && v.nodeSel < len(g.Nodes) {
				sel = v.nodeSel
			}
			return head + renderDagGraph(g, lay, sel)
		}
		// Layout doesn't fit — fall back and remember it, so `g` can't
		// bounce the user into an unreadable render.
		v.treeMode = true
		v.treeForced = true
	}
	body, _ := renderDagTree(g, v.treeScroll, maxRows-3)
	return head + body
}

// selectedNode returns the currently selected graph node, or nil.
func (v *dagView) selectedNode() *DagNode {
	if v.graph == nil || v.nodeSel < 0 || v.nodeSel >= len(v.graph.Nodes) {
		return nil
	}
	return &v.graph.Nodes[v.nodeSel]
}

// moveNodeSel moves the graph node selection: dx within a rank, dy across
// ranks (picking the horizontally nearest node).
func (v *dagView) moveNodeSel(dx, dy int, width int) {
	g := v.graph
	if g == nil || len(g.Nodes) == 0 {
		return
	}
	lay, ok := newDagLayout(g, width)
	if !ok {
		return
	}
	cur := lay.find(v.nodeSel)
	if cur == nil {
		v.nodeSel = lay.ranks[0][0]
		return
	}
	if dy != 0 {
		targetRank := cur.rank + dy
		if targetRank < 0 || targetRank >= len(lay.ranks) {
			return
		}
		best, bestDist := -1, 1<<30
		for _, idx := range lay.ranks[targetRank] {
			p := lay.find(idx)
			d := p.xCenter - cur.xCenter
			if d < 0 {
				d = -d
			}
			if d < bestDist {
				best, bestDist = idx, d
			}
		}
		if best >= 0 {
			v.nodeSel = best
		}
		return
	}
	if dx != 0 {
		row := lay.ranks[cur.rank]
		for i, idx := range row {
			if idx == v.nodeSel {
				ni := i + dx
				if ni >= 0 && ni < len(row) {
					v.nodeSel = row[ni]
				}
				return
			}
		}
	}
}

func (v *dagView) clampCursor() {
	n := len(v.visible())
	if v.cursor >= n {
		v.cursor = n - 1
	}
	if v.cursor < 0 {
		v.cursor = 0
	}
}

func (v *dagView) selectedRoot() *Job {
	roots := v.visible()
	if v.cursor >= 0 && v.cursor < len(roots) {
		return &roots[v.cursor]
	}
	return nil
}
