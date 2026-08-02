package main

import (
	"fmt"
	"io"
	"os"

	"golang.org/x/term"
)

// styledOut / styledErr gate ANSI styling per stream. Both default to false so
// run() callers (tests included) always get plain output; main() enables them
// only for real TTYs with NO_COLOR unset. Machine-readable commands
// (hash-password, generate-api-key) never style their output regardless.
var styledOut, styledErr bool

func initStyling() {
	if os.Getenv("NO_COLOR") != "" {
		return
	}
	styledOut = term.IsTerminal(int(os.Stdout.Fd()))
	styledErr = term.IsTerminal(int(os.Stderr.Fd()))
}

func styledFor(w io.Writer) bool {
	switch w {
	case os.Stdout:
		return styledOut
	case os.Stderr:
		return styledErr
	}
	return false
}

const (
	ansiReset  = "\033[0m"
	ansiBold   = "\033[1m"
	ansiDim    = "\033[2m"
	ansiInvert = "\033[7m"
	ansiRed    = "\033[31m"
	ansiGreen  = "\033[32m"
	ansiYellow = "\033[33m"
	ansiCyan   = "\033[36m"
)

// paint wraps s in the given ANSI code when w is a styled stream.
func paint(w io.Writer, code, s string) string {
	if !styledFor(w) {
		return s
	}
	return code + s + ansiReset
}

// okLine prints a success line: "✓ <message>".
func okLine(w io.Writer, format string, args ...any) {
	fmt.Fprintf(w, "%s %s\n", paint(w, ansiGreen+ansiBold, "✓"), fmt.Sprintf(format, args...))
}

// warnLine prints a warning line: "⚠ <message>".
func warnLine(w io.Writer, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	fmt.Fprintf(w, "%s %s\n", paint(w, ansiYellow+ansiBold, "⚠"), paint(w, ansiYellow, msg))
}

// errLine prints an error line to w: "✗ gqm: <message>".
// The "gqm:" prefix and stderr routing are the CLI's stable error contract.
func errLine(w io.Writer, format string, args ...any) {
	msg := "gqm: " + fmt.Sprintf(format, args...)
	fmt.Fprintf(w, "%s %s\n", paint(w, ansiRed+ansiBold, "✗"), paint(w, ansiRed, msg))
}

// keyBlock prints a secret on its own indented line, inverted on a TTY so it
// stands apart from surrounding text.
func keyBlock(w io.Writer, key string) {
	fmt.Fprintf(w, "\n  %s\n\n", paint(w, ansiInvert, " "+key+" "))
}

// sectionHeader renders an uppercase help-section label.
func sectionHeader(w io.Writer, s string) string {
	return paint(w, ansiBold, s)
}

// accent renders command names and other accented tokens.
func accent(w io.Writer, s string) string {
	return paint(w, ansiCyan, s)
}

// muted renders de-emphasized hint text.
func muted(w io.Writer, s string) string {
	return paint(w, ansiDim, s)
}

// restartWarning prints the shared "restart required" notice.
func restartWarning(w io.Writer) {
	fmt.Fprintln(w)
	warnLine(w, "Restart your GQM server for changes to take effect.")
}

// confirmPrompt asks "<question> [y/N]" on the terminal and returns the answer.
// Non-interactive stdin (scripts, pipes, tests) skips the prompt and returns
// true so automation keeps working unchanged.
func confirmPrompt(question string) bool {
	if !term.IsTerminal(int(os.Stdin.Fd())) {
		return true
	}
	fmt.Fprintf(os.Stderr, "%s %s ", question, "[y/N]")
	var answer string
	fmt.Fscanln(os.Stdin, &answer)
	return answer == "y" || answer == "Y" || answer == "yes"
}
