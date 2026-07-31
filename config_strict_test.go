package gqm

import (
	"strings"
	"testing"
)

// A mistyped config key used to be ignored in silence. The field kept its zero
// value, the zero value fell through to a sensible default, and the server came
// up running numbers the operator never chose — with nothing in the logs to
// suggest anything had gone wrong.
//
// That is the worst shape a configuration bug can take. Every optional field
// here has a reasonable fallback, so a typo almost never produces "it will not
// start"; it produces "it started, and it is wrong".

func TestLoadConfig_RejectsUnknownField(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string // substring the error must name
	}{
		{
			name: "misspelled concurrency",
			yaml: `
redis:
  addr: "localhost:6379"
pools:
  - name: "p"
    job_types: ["a.b"]
    queues: ["default"]
    concurency: 10
`,
			want: "concurency",
		},
		{
			name: "misspelled result_ttl",
			yaml: `
redis:
  addr: "localhost:6379"
app:
  result_tll: 3600
`,
			want: "result_tll",
		},
		{
			// The one with a security consequence: a typo here leaves the
			// session cookie without Secure behind a TLS proxy.
			name: "misspelled cookie_secure",
			yaml: `
redis:
  addr: "localhost:6379"
monitoring:
  api:
    enabled: true
    addr: "127.0.0.1:8080"
    cookie_secur: true
`,
			want: "cookie_secur",
		},
		{
			name: "unknown top-level section",
			yaml: `
redis:
  addr: "localhost:6379"
workerz:
  count: 4
`,
			want: "workerz",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadConfig([]byte(tc.yaml))
			if err == nil {
				t.Fatalf("unknown field %q was accepted; it would have silently fallen back to a default", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error does not name the offending field %q, so it does not tell the operator what to fix: %v", tc.want, err)
			}
		})
	}
}

// The whole schema must still load, or the strictness has broken more than it
// fixed. This exercises every section with every documented field set.
func TestLoadConfig_FullSchemaStillLoads(t *testing.T) {
	const full = `
redis:
  addr: "localhost:6379"
  password: "s3cret"
  db: 1
  prefix: "gqm:"
  tls: false
  pool_size: 64
app:
  timezone: "Asia/Jakarta"
  log_level: "info"
  shutdown_timeout: 30
  global_job_timeout: 1800
  grace_period: 10
  result_ttl: 604800
  failure_ttl: 2592000
queues:
  - name: "critical"
    priority: 10
  - name: "default"
    priority: 1
pools:
  - name: "fast"
    job_types: ["email.send"]
    queues: ["critical", "default"]
    concurrency: 10
    job_timeout: 60
    grace_period: 5
    shutdown_timeout: 20
    dequeue_strategy: "weighted"
    retry:
      max_retry: 5
      backoff: "exponential"
      backoff_base: 10
      backoff_max: 3600
      intervals: [1, 2, 3]
scheduler:
  enabled: true
  poll_interval: 1
  cron_entries:
    - id: "nightly"
      name: "Nightly job"
      cron_expr: "0 0 2 * * *"
      timezone: "UTC"
      job_type: "email.send"
      queue: "default"
      payload: '{"k":"v"}'
      timeout: 60
      max_retry: 2
      overlap_policy: "skip"
      enabled: true
monitoring:
  auth:
    enabled: true
    session_ttl: 86400
    users:
      - username: "admin"
        password_hash: "$2a$10$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy"
        role: "admin"
  api:
    enabled: true
    addr: "127.0.0.1:8080"
    rate_limit: 100
    trust_proxy: false
    cookie_secure: false
    api_keys:
      - name: "ci"
        key: "gqm_ak_this_key_is_at_least_32_characters"
        role: "viewer"
  dashboard:
    enabled: true
    path_prefix: "/dashboard"
    custom_dir: ""
`
	if _, err := LoadConfig([]byte(full)); err != nil {
		t.Fatalf("a config using every documented field was rejected: %v", err)
	}
}
