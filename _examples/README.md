# GQM Examples

Runnable examples demonstrating GQM features. Each example is self-contained and can be run independently.

**Prerequisites:** Redis on `localhost:6379` (or set `GQM_TEST_REDIS_ADDR`).

```bash
# Start Redis (if not already running)
docker compose up -d
```

## Examples

| # | Example | Features | Run |
|---|---------|----------|-----|
| 01 | [Email Service](01-email-service/) | Enqueue, retry, backoff, DLQ | `go run ./_examples/01-email-service` |
| 02 | [Image Pipeline](02-image-pipeline/) | DAG linear chain, per-job timeout | `go run ./_examples/02-image-pipeline` |
| 03 | [Scheduled Reports](03-scheduled-reports/) | Cron entries, EnqueueAt, EnqueueIn | `go run ./_examples/03-scheduled-reports` |
| 04 | [Order Fulfillment](04-order-fulfillment/) | DAG diamond, AllowFailure | `go run ./_examples/04-order-fulfillment` |
| 05 | [Multi-tenant Pools](05-multi-tenant/) | Explicit pools, priority, dequeue strategies | `go run ./_examples/05-multi-tenant` |
| 06 | [Config-driven](06-config-driven/) | YAML config, NewServerFromConfig | `cd _examples/06-config-driven && go run .` |
| 07 | [Webhook Delivery](07-webhook-delivery/) | Unique, JobID, custom retry intervals | `go run ./_examples/07-webhook-delivery` |
| 08 | [Monitoring](08-monitoring/) | Dashboard, HTTP API, auth, API keys | `go run ./_examples/08-monitoring` |
| 09 | [Dev Server](09-dev-server/) | All features, dummy data, DAG patterns | `go run ./_examples/09-dev-server` |
| 10 | [Advanced Features](10-advanced-features/) | ErrSkipRetry, IsFailure, middleware, callbacks, batch | `go run ./_examples/10-advanced-features` |
| 11 | [Custom Dashboard](11-custom-dashboard/) | WithDashboardDir, custom UI | `go run ./_examples/11-custom-dashboard` |

> **Note:** Examples 06 and 09/config load a `gqm.yaml` file, so run them from their directory (`cd` first).

## Feature Coverage

| Feature | Examples |
|---------|----------|
| Basic enqueue/dequeue | 01, 05, 08 |
| Retry + DLQ | 01, 07, 09 |
| DAG dependencies | 02, 04, 09 |
| AllowFailure | 04, 09 |
| Cron scheduling | 03, 09 |
| Delayed jobs | 03 |
| Priority queues | 05 |
| Dequeue strategies | 05 |
| Explicit pools | 05, 06 |
| YAML config | 06, 09/config |
| Unique / JobID | 07 |
| Custom retry intervals | 07 |
| Dashboard + auth | 08, 09 |
| Custom dashboard | 11 |
| API keys | 08, 09 |
