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
	"os"
)

// version is set at build time via -ldflags.
var version = "dev"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(0)
	}

	switch os.Args[1] {
	case "init":
		runInit(os.Args[2:])
	case "dashboard":
		runDashboard(os.Args[2:])
	case "set-password", "reset-password":
		runSetPassword(os.Args[2:])
	case "add-api-key":
		runAddAPIKey(os.Args[2:])
	case "revoke-api-key":
		runRevokeAPIKey(os.Args[2:])
	case "hash-password":
		runHashPassword(os.Args[2:])
	case "generate-api-key":
		runGenerateAPIKey(os.Args[2:])
	case "tui":
		runTUI(os.Args[2:])
	case "version":
		fmt.Printf("gqm %s\n", version)
	case "help", "-h", "--help":
		printUsage()
	default:
		fmt.Fprintf(os.Stderr, "gqm: unknown command %q\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Print(`GQM — Go Queue Manager CLI

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
