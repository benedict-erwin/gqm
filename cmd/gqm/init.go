package main

import (
	"flag"
	"fmt"
	"io"
	"os"
)

const configTemplate = `# GQM — Go Queue Manager Configuration
# Documentation: https://github.com/benedict-erwin/gqm

# Redis connection settings
redis:
  addr: "localhost:6379"
  password: ""
  db: 0
  prefix: "gqm"   # Key prefix for all GQM data in Redis

# Application settings
app:
  timezone: "UTC"              # IANA timezone (e.g., "Asia/Jakarta")
  log_level: "info"            # debug, info, warn, error
  shutdown_timeout: 30         # seconds — max wait for graceful shutdown
  global_job_timeout: 1800     # seconds — default 30 min, cannot be disabled
  grace_period: 10             # seconds — cleanup time after job timeout

# Queue definitions
queues:
  - name: "default"
    priority: 1
  # - name: "critical"
  #   priority: 10
  # - name: "low"
  #   priority: 0

# Worker pool configuration
pools:
  - name: "default"
    job_types: ["*"]           # "*" = catch-all pool for unassigned job types
    queues: ["default"]
    concurrency: 5
    job_timeout: 300           # seconds — per-job timeout (overrides global)
    # grace_period: 10         # seconds — per-pool override
    # shutdown_timeout: 15     # seconds — per-pool override
    # dequeue_strategy: "weighted"  # strict, round_robin, weighted
    # retry:
    #   max_retry: 3
    #   backoff: "exponential"      # constant, linear, exponential
    #   backoff_base: 10            # seconds
    #   backoff_max: 3600           # seconds

# Scheduler settings
scheduler:
  enabled: true
  poll_interval: 5             # seconds — how often to check scheduled jobs
  # cron_entries:
  #   - id: "cleanup-daily"
  #     name: "Daily cleanup"
  #     cron_expr: "0 0 2 * * *"   # 6-field: sec min hour day month weekday
  #     timezone: "UTC"
  #     job_type: "cleanup"
  #     queue: "default"
  #     # payload: '{"key": "value"}'
  #     # timeout: 600
  #     # max_retry: 3
  #     # overlap_policy: "skip"    # skip, allow, cancel_prev
  #     # enabled: true

# Monitoring (HTTP API + Dashboard)
monitoring:
  auth:
    enabled: false             # Set to true to enable authentication
    session_ttl: 86400         # seconds — 24 hours
    # users:
    #   - username: "admin"
    #     password_hash: ""    # Generate with: gqm hash-password <password>
    #     role: "admin"        # admin or viewer
  api:
    enabled: false             # Set to true to enable HTTP API
    addr: ":8080"
    # api_keys:
    #   - name: "my-key"
    #     key: ""              # Generate with: gqm generate-api-key
    #     role: "admin"        # admin or viewer
  dashboard:
    enabled: false             # Set to true to enable web dashboard
    path_prefix: "/dashboard"
    # custom_dir: ""           # Path to custom dashboard files (overrides embedded)
`

// initConfig creates a new GQM config file at the given path.
// Returns an error if the file already exists.
func initConfig(configPath string) error {
	if _, err := os.Stat(configPath); err == nil {
		return fmt.Errorf("%s already exists (will not overwrite)", configPath)
	}

	if err := os.WriteFile(configPath, []byte(configTemplate), 0o644); err != nil {
		return fmt.Errorf("writing config: %w", err)
	}

	return nil
}

func runInit(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("init", flag.ContinueOnError)
	fs.SetOutput(stderr)
	configPath := fs.String("config", "gqm.yaml", "Path for the new config file")
	fs.Usage = func() {
		fmt.Fprintln(stderr, `Usage: gqm init [--config <file>]

Generate a GQM config file with sensible defaults and documentation comments.
Default output: gqm.yaml in the current directory.

Flags:`)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		return 1
	}

	if err := initConfig(*configPath); err != nil {
		fmt.Fprintf(stderr, "gqm: %v\n", err)
		return 1
	}

	fmt.Fprintf(stdout, "Config file created: %s\n\n", *configPath)
	fmt.Fprintln(stdout, "Next steps:")
	fmt.Fprintln(stdout, "  1. Edit the config file to match your environment")
	fmt.Fprintln(stdout, "  2. Set up authentication:")
	fmt.Fprintln(stdout, "       gqm set-password --config "+*configPath+" --user admin")
	fmt.Fprintln(stdout, "       gqm add-api-key --config "+*configPath+" --name my-key")
	fmt.Fprintln(stdout, "  3. Start your GQM server with the config file")
	return 0
}
