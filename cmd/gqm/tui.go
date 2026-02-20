package main

import (
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"

	"github.com/benedict-erwin/gqm/tui"
)

// validateTUIArgs validates the API URL for the TUI command.
func validateTUIArgs(apiURL string) error {
	if apiURL == "" {
		return fmt.Errorf("--api-url or GQM_API_URL is required")
	}

	u, err := url.Parse(apiURL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("--api-url must be a valid URL (e.g., http://localhost:8080)")
	}

	return nil
}

func runTUI(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("tui", flag.ContinueOnError)
	fs.SetOutput(stderr)
	apiURL := fs.String("api-url", "", "GQM API server URL (e.g., http://localhost:8080)")
	apiKey := fs.String("api-key", "", "API key for authentication")
	fs.Usage = func() {
		fmt.Fprintln(stderr, `Usage: gqm tui [--api-url <url>] [--api-key <key>]

Launch the GQM terminal UI monitor.

Flags can also be set via environment variables:
  GQM_API_URL    API server URL
  GQM_API_KEY    API key for authentication

Flags:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return 1
	}

	// Env vars as fallback.
	if *apiURL == "" {
		*apiURL = os.Getenv("GQM_API_URL")
	}
	if *apiKey == "" {
		*apiKey = os.Getenv("GQM_API_KEY")
	}

	if err := validateTUIArgs(*apiURL); err != nil {
		fmt.Fprintf(stderr, "gqm: %v\n", err)
		fs.Usage()
		return 1
	}

	client := tui.NewClient(*apiURL, *apiKey)

	// Quick health check before launching TUI.
	if err := client.Health(); err != nil {
		fmt.Fprintf(stderr, "gqm: cannot connect to %s: %v\n", *apiURL, err)
		return 1
	}

	if err := tui.Run(client); err != nil {
		fmt.Fprintf(stderr, "gqm: %v\n", err)
		return 1
	}
	return 0
}
