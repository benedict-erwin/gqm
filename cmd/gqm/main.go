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
)

// version is set at build time via -ldflags.
var version = "dev"

func main() {
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
		fmt.Fprintf(stderr, "gqm: unknown command %q\n\n", args[0])
		printUsage(stderr)
		return 1
	}
}

func printUsage(w io.Writer) {
	fmt.Fprint(w, `GQM — Go Queue Manager CLI

Usage:
  gqm <command> [arguments]

Setup:
  init [--config <file>]                            Generate a config file (default: gqm.yaml)

Config Management:
  set-password --config <file> --user <username>    Set or update a user password
  reset-password                                    Alias for set-password
  add-api-key --config <file> --name <name>         Generate and add an API key
  revoke-api-key --config <file> --name <name>      Remove an API key

Utilities:
  dashboard export <dir>   Export embedded dashboard files to a directory
  hash-password <password> Generate bcrypt hash to stdout
  generate-api-key         Generate API key to stdout

Other:
  tui                      Launch the terminal UI monitor
  version                  Print the GQM version
  help                     Show this help message
`)
}
