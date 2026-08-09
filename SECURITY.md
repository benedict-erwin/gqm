# Security Policy

## Supported Versions

GQM is pre-1.0. Only the latest minor release receives security fixes; there are
no backports to older lines.

| Version | Supported |
| ------- | --------- |
| 0.6.x   | Yes       |
| < 0.6   | No        |

If you are on an older version, upgrading to the latest release is the fix.
`CHANGELOG.md` records behaviour changes per release, including the ones that
require operator action.

## Reporting a Vulnerability

**Do not open a public issue for a security problem.**

Report privately through GitHub Security Advisories:

**https://github.com/benedict-erwin/gqm/security/advisories/new**

That channel keeps the report, the discussion, and any draft patch private
until a fix is released. If GitHub is unavailable to you, email
benedict.erwin@gmail.com instead.

Useful things to include, in rough order of value:

- The affected version (`go list -m github.com/benedict-erwin/gqm`) and Redis version
- What an attacker gains — read a payload, forge a session, execute a job, crash a worker
- Minimal steps or a short Go program that reproduces it
- Any relevant configuration: auth mode, dashboard exposure, Redis ACLs, TLS

### What to expect

This project is maintained by one person, so these are honest targets rather
than guarantees:

- **Acknowledgement** within 7 days
- **Initial assessment** — confirmed, needs more information, or out of scope — within 14 days
- **Fix and release** as soon as the fix is verified; timing depends on severity and complexity
- **Disclosure** coordinated with you. The advisory is published once the fix is
  released, and you are credited unless you ask otherwise.

Every accepted security fix also gets a regression check added to
`scripts/verify-security-fixes.sh`, which runs against a real server rather than
a test harness. A fix that cannot be shown to fail without the patch is not
considered done.

## Scope

**In scope** — anything in this repository: the core library, the HTTP API and
dashboard under `monitor/`, the CLI in `cmd/gqm/`, the TUI module, and the Lua
scripts in `lua/`. Examples:

- Authentication or session handling flaws in the dashboard and API
  (session forgery, fixation, privilege escalation, CSRF)
- Injection through job payloads, queue names, config values, or Lua arguments
- Path traversal or arbitrary file access via config or CLI arguments
- Secrets leaking into logs, API responses, or the dashboard
- Denial of service triggered by input a caller controls

**Out of scope** — deployment decisions the library documents but cannot enforce:

- Redis exposed without a password, without ACLs, or without TLS. Redis holds
  session tokens and every job payload; securing it is the operator's
  responsibility and is documented in the README under *Securing Redis*.
- Running the dashboard on a public interface without authentication enabled
- Handler code in your own application, including what your handlers do with a payload
- Vulnerabilities in Go, Redis, or third-party dependencies — report those upstream.
  If a dependency advisory is reachable from GQM code, that is in scope and worth
  telling us about.
