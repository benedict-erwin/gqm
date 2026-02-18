package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/benedict-erwin/gqm/tui"
)

func runTUI(args []string) {
	fs := flag.NewFlagSet("tui", flag.ExitOnError)
	apiURL := fs.String("api-url", "", "GQM API server URL (e.g., http://localhost:8080)")
	apiKey := fs.String("api-key", "", "API key for authentication")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `Usage: gqm tui [--api-url <url>] [--api-key <key>]

Launch the GQM terminal UI monitor.

Flags can also be set via environment variables:
  GQM_API_URL    API server URL
  GQM_API_KEY    API key for authentication

Flags:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		os.Exit(1)
	}

	// Env vars as fallback.
	if *apiURL == "" {
		*apiURL = os.Getenv("GQM_API_URL")
	}
	if *apiKey == "" {
		*apiKey = os.Getenv("GQM_API_KEY")
	}

	if *apiURL == "" {
		fmt.Fprintln(os.Stderr, "gqm: --api-url or GQM_API_URL is required")
		fs.Usage()
		os.Exit(1)
	}

	client := tui.NewClient(*apiURL, *apiKey)

	// Quick health check before launching TUI.
	if err := client.Health(); err != nil {
		fmt.Fprintf(os.Stderr, "gqm: cannot connect to %s: %v\n", *apiURL, err)
		os.Exit(1)
	}

	if err := tui.Run(client); err != nil {
		fmt.Fprintf(os.Stderr, "gqm: %v\n", err)
		os.Exit(1)
	}
}
