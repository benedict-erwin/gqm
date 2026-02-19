package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/benedict-erwin/gqm/monitor/dashboard"
)

func runDashboard(args []string) {
	if len(args) < 2 || args[0] != "export" {
		fmt.Fprintln(os.Stderr, `Usage: gqm dashboard export <dir>

Export the embedded dashboard files to a directory.
The directory will be created if it does not exist.`)
		os.Exit(1)
	}

	targetDir := args[1]

	if err := exportDashboard(targetDir); err != nil {
		fmt.Fprintf(os.Stderr, "gqm: export dashboard: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Dashboard exported to %s\n", targetDir)
}

func exportDashboard(targetDir string) error {
	// Resolve the target directory to its real path to prevent symlink traversal.
	absTarget, err := filepath.Abs(targetDir)
	if err != nil {
		return fmt.Errorf("resolving target directory: %w", err)
	}

	return fs.WalkDir(dashboard.Assets, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		dest := filepath.Join(absTarget, path)

		// Verify the destination stays within the target directory.
		if !strings.HasPrefix(dest, absTarget) {
			return fmt.Errorf("path traversal detected: %s", path)
		}

		if d.IsDir() {
			return os.MkdirAll(dest, 0o755)
		}

		data, err := fs.ReadFile(dashboard.Assets, path)
		if err != nil {
			return fmt.Errorf("reading embedded %s: %w", path, err)
		}

		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return fmt.Errorf("creating directory for %s: %w", dest, err)
		}

		if err := os.WriteFile(dest, data, 0o644); err != nil {
			return fmt.Errorf("writing %s: %w", dest, err)
		}

		return nil
	})
}
