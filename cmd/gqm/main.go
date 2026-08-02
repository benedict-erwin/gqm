// Binary gqm provides CLI utilities for the GQM queue manager.
//
// Usage:
//
//	gqm <command> [arguments]
//
// Commands:
//
//	dashboard export <dir>                         Export embedded dashboard files
//	set-password --config <file> --user <username> Set/update a user password
//	reset-password                                 Alias for set-password
//	add-api-key --config <file> --name <name>      Generate and add an API key
//	revoke-api-key --config <file> --name <name>   Remove an API key
//	hash-password <password>                       Generate bcrypt hash (stdout)
//	generate-api-key                               Generate API key (stdout)
//	tui                                            Launch the terminal UI monitor
//	version                                        Print the GQM version
//	help                                           Show this help message
package main

import (
	"fmt"
	"io"
	"os"
	"runtime/debug"
)

// version is set at build time via -ldflags.
var version = ""

func init() {
	if version != "" {
		return // set via -ldflags
	}
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Version != "" && info.Main.Version != "(devel)" {
		version = info.Main.Version
	} else {
		version = "dev"
	}
}

func main() {
	initStyling()
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		printUsage(stdout)
		return 0
	}

	switch args[0] {
	case "init":
		return runInit(args[1:], stdout, stderr)
	case "dashboard":
		return runDashboard(args[1:], stdout, stderr)
	case "set-password", "reset-password":
		return runSetPassword(args[1:], stdout, stderr)
	case "add-api-key":
		return runAddAPIKey(args[1:], stdout, stderr)
	case "revoke-api-key":
		return runRevokeAPIKey(args[1:], stdout, stderr)
	case "hash-password":
		return runHashPassword(args[1:], stdout, stderr)
	case "generate-api-key":
		return runGenerateAPIKey(args[1:], stdout, stderr)
	case "tui":
		return runTUI(args[1:], stdout, stderr)
	case "version":
		fmt.Fprintf(stdout, "gqm %s\n", version)
		return 0
	case "help", "-h", "--help":
		printUsage(stdout)
		return 0
	default:
		errLine(stderr, "unknown command %q", args[0])
		fmt.Fprintf(stderr, "  %s %s %s\n",
			muted(stderr, "Run"), accent(stderr, "gqm help"), muted(stderr, "for available commands."))
		return 1
	}
}

func printUsage(w io.Writer) {
	cmd := func(name, desc string) string {
		return fmt.Sprintf("  %s   %s\n", accent(w, fmt.Sprintf("%-16s", name)), desc)
	}
	fmt.Fprintf(w, "%s — Go Queue Manager %s\n\n",
		paint(w, ansiBold+ansiCyan, "GQM"), muted(w, "v"+version))
	fmt.Fprintf(w, "%s\n", sectionHeader(w, "USAGE"))
	fmt.Fprintf(w, "  gqm %s [flags]\n\n", accent(w, "<command>"))

	fmt.Fprintf(w, "%s\n", sectionHeader(w, "SETUP"))
	fmt.Fprint(w, cmd("init", "Generate a config file "+muted(w, "(default: gqm.yaml)")))
	fmt.Fprintln(w)

	fmt.Fprintf(w, "%s\n", sectionHeader(w, "CONFIG"))
	fmt.Fprint(w, cmd("set-password", "Set or update a user password "+muted(w, "(interactive)")))
	fmt.Fprint(w, cmd("reset-password", "Alias for set-password"))
	fmt.Fprint(w, cmd("add-api-key", "Generate and add an API key"))
	fmt.Fprint(w, cmd("revoke-api-key", "Remove an API key"))
	fmt.Fprintln(w)

	fmt.Fprintf(w, "%s\n", sectionHeader(w, "UTILITIES"))
	fmt.Fprint(w, cmd("dashboard export", "Export embedded dashboard files"))
	fmt.Fprint(w, cmd("hash-password", "Bcrypt hash to stdout "+muted(w, "(pipe-safe)")))
	fmt.Fprint(w, cmd("generate-api-key", "API key to stdout "+muted(w, "(pipe-safe)")))
	fmt.Fprintln(w)

	fmt.Fprintf(w, "%s\n", sectionHeader(w, "OTHER"))
	fmt.Fprint(w, cmd("tui", "Launch the terminal UI monitor"))
	fmt.Fprint(w, cmd("version", "Print the GQM version"))
	fmt.Fprint(w, cmd("help", "Show this help"))
	fmt.Fprintln(w)

	fmt.Fprintf(w, "%s gqm %s -h %s\n",
		muted(w, "Run"), accent(w, "<command>"), muted(w, "for details on a command."))
}
