package tui

import (
	"fmt"
	"strings"

	"github.com/charmbracelet/lipgloss"
)

// colDef defines a table column with optional responsive behavior.
type colDef struct {
	header string
	flex   bool // whether this column can shrink when terminal is narrow
	min    int  // minimum width for flex columns (0 = header length)
}

// table is a responsive table renderer that adapts to terminal width.
type table struct {
	cols     []colDef
	rows     [][]string
	widths   []int // natural (content-based) widths
	maxWidth int   // available terminal width (0 = unlimited)
}

const colGap = "  "
const colGapWidth = 2

func newTable(maxWidth int, cols ...colDef) *table {
	widths := make([]int, len(cols))
	for i, c := range cols {
		w := len(c.header)
		widths[i] = w
		if c.min <= 0 {
			cols[i].min = w
		}
	}
	return &table{cols: cols, widths: widths, maxWidth: maxWidth}
}

func (t *table) addRow(cells ...string) {
	row := make([]string, len(t.cols))
	for i := range row {
		if i < len(cells) {
			row[i] = cells[i]
		}
	}
	for i, c := range row {
		w := visibleLen(c)
		if w > t.widths[i] {
			t.widths[i] = w
		}
	}
	t.rows = append(t.rows, row)
}

// allocWidths computes column widths that fit the terminal.
// When content fits naturally, full widths are used.
// When too wide, flex columns shrink proportionally.
func (t *table) allocWidths() []int {
	alloc := make([]int, len(t.cols))
	copy(alloc, t.widths)

	if t.maxWidth <= 0 {
		return alloc
	}

	totalGap := 0
	if len(t.cols) > 1 {
		totalGap = (len(t.cols) - 1) * colGapWidth
	}
	total := totalGap
	for _, w := range alloc {
		total += w
	}

	if total <= t.maxWidth {
		return alloc // fits naturally, show full content
	}

	// Shrink flex columns proportionally
	excess := total - t.maxWidth
	flexShrinkable := 0
	for i, c := range t.cols {
		if c.flex {
			s := alloc[i] - c.min
			if s > 0 {
				flexShrinkable += s
			}
		}
	}
	if flexShrinkable <= 0 {
		return alloc
	}

	remaining := excess
	for i, c := range t.cols {
		if !c.flex || remaining <= 0 {
			continue
		}
		canShrink := alloc[i] - c.min
		if canShrink <= 0 {
			continue
		}
		share := (canShrink * excess) / flexShrinkable
		if share > remaining {
			share = remaining
		}
		alloc[i] -= share
		remaining -= share
	}

	// Fix rounding remainder
	for remaining > 0 {
		shrunk := false
		for i, c := range t.cols {
			if remaining <= 0 {
				break
			}
			if c.flex && alloc[i] > c.min {
				alloc[i]--
				remaining--
				shrunk = true
			}
		}
		if !shrunk {
			break
		}
	}

	return alloc
}

var (
	headerText     = lipgloss.NewStyle().Bold(true).Foreground(colorPrimary)
	separatorStyle = lipgloss.NewStyle().Foreground(colorMuted)
	scrollStyle    = lipgloss.NewStyle().Foreground(colorMuted)
)

// render renders the table with cursor highlight and optional row limit.
// maxRows limits the number of visible data rows (0 = unlimited).
// When rows exceed maxRows, a viewport with scroll indicators is shown.
func (t *table) render(cursor, maxRows int) string {
	if len(t.rows) == 0 {
		return lipgloss.NewStyle().Foreground(colorMuted).Render("  (empty)")
	}

	alloc := t.allocWidths()
	totalRows := len(t.rows)

	// Determine viewport
	start, end := 0, totalRows
	if maxRows > 0 && totalRows > maxRows {
		// Reserve 2 lines for scroll indicators (up/down)
		dataSlots := maxRows - 2
		if dataSlots < 1 {
			dataSlots = 1
		}

		start = cursor - dataSlots/2
		if start < 0 {
			start = 0
		}
		end = start + dataSlots
		if end > totalRows {
			end = totalRows
			start = end - dataSlots
			if start < 0 {
				start = 0
			}
		}

		// Reclaim unused indicator lines at edges
		if start == 0 {
			end = min(start+dataSlots+1, totalRows)
		}
		if end == totalRows {
			start = max(end-dataSlots-1, 0)
			if start == 0 {
				end = min(start+maxRows, totalRows)
			}
		}
	}

	var b strings.Builder

	// Header line
	for i, c := range t.cols {
		cell := fmt.Sprintf("%-*s", alloc[i], c.header)
		b.WriteString(headerText.Render(cell))
		if i < len(t.cols)-1 {
			b.WriteString(colGap)
		}
	}
	b.WriteString("\n")

	// Separator
	for i, w := range alloc {
		b.WriteString(separatorStyle.Render(strings.Repeat("─", w)))
		if i < len(alloc)-1 {
			b.WriteString(colGap)
		}
	}
	b.WriteString("\n")

	// Scroll up indicator
	if start > 0 {
		b.WriteString(scrollStyle.Render(fmt.Sprintf("  ↑ %d more", start)))
		b.WriteString("\n")
	}

	// Data rows
	for ri := start; ri < end; ri++ {
		row := t.rows[ri]
		var line strings.Builder
		for i, c := range row {
			w := alloc[i]
			vLen := visibleLen(c)

			if vLen > w {
				c = truncateAnsi(c, w)
				vLen = visibleLen(c)
			}

			pad := w - vLen
			if pad < 0 {
				pad = 0
			}
			line.WriteString(c)
			line.WriteString(strings.Repeat(" ", pad))
			if i < len(row)-1 {
				line.WriteString(colGap)
			}
		}
		s := line.String()
		if ri == cursor {
			s = selectedRow.Render(s)
		}
		b.WriteString(s)
		b.WriteString("\n")
	}

	// Scroll down indicator
	if end < totalRows {
		b.WriteString(scrollStyle.Render(fmt.Sprintf("  ↓ %d more", totalRows-end)))
		b.WriteString("\n")
	}

	return b.String()
}

// visibleLen returns the visible length of s, ignoring ANSI escape sequences.
func visibleLen(s string) int {
	return len(stripAnsi(s))
}

// stripAnsi removes ANSI escape sequences for width calculation.
func stripAnsi(s string) string {
	var b strings.Builder
	inEsc := false
	for _, r := range s {
		if r == '\x1b' {
			inEsc = true
			continue
		}
		if inEsc {
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				inEsc = false
			}
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// truncateAnsi truncates s to maxVisible visible characters, preserving ANSI escapes.
func truncateAnsi(s string, maxVisible int) string {
	if maxVisible <= 0 {
		return ""
	}
	if visibleLen(s) <= maxVisible {
		return s
	}

	useEllipsis := maxVisible > 3
	target := maxVisible
	if useEllipsis {
		target = maxVisible - 3
	}

	var b strings.Builder
	visible := 0
	inEsc := false
	for _, r := range s {
		if r == '\x1b' {
			inEsc = true
			b.WriteRune(r)
			continue
		}
		if inEsc {
			b.WriteRune(r)
			if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				inEsc = false
			}
			continue
		}
		if visible >= target {
			break
		}
		b.WriteRune(r)
		visible++
	}
	if useEllipsis {
		b.WriteString("...")
	}
	// Reset ANSI to prevent style bleed from truncated styled text
	b.WriteString("\x1b[0m")
	return b.String()
}
