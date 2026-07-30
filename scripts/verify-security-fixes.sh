#!/usr/bin/env bash
#
# End-to-end verification for the security fixes from the 2026-07-30 whitebox
# audit. Drives a real server over real HTTP rather than httptest, so it proves
# the fixes hold in the shape they actually ship in.
#
# The unit tests in monitor/ pin the same behaviour at the handler level. This
# script exists because a handler test cannot show that a misconfigured server
# refuses to start at all, and cannot show that the file server behaves the same
# once a real listener and a real kernel filesystem are involved.
#
# Run inside the devcontainer, where Go and Redis are reachable:
#   docker exec -w /workspace gqm_devcontainer-dev-1 bash scripts/verify-security-fixes.sh
#
# Exit code 0 means every check passed. Any failure exits non-zero and prints
# what was expected against what happened.
#
# ---------------------------------------------------------------------------
# ADDING A CHECK — required for every security fix
#
# This script must cover every security fix that has ever landed, not just the
# most recent one, so that a regression in an old fix is caught too. When you
# fix a finding:
#
#   1. Add a section headed with the finding id, following the existing shape:
#        head_ "M-03  <one line saying what must hold>"
#        ... probe ...
#        ok "..."   on success
#        bad "..." "<expected>" "<actual>"   on failure
#
#   2. Prove the new check is load-bearing. Revert the fix, run the script, and
#      confirm the new check goes red:
#        git checkout <commit-before-fix> -- <files>
#        bash scripts/verify-security-fixes.sh     # new check must FAIL
#        git checkout HEAD -- <files>              # restore
#      A check that never fails proves nothing. Green is not evidence on its
#      own — that has been misleading more than once in this project.
#
#   3. Assert the fix does not break what it protects. Every hardening section
#      here also checks that the legitimate path still works, which is what
#      catches an over-broad fix.
# ---------------------------------------------------------------------------

set -uo pipefail

REDIS_ADDR="${GQM_TEST_REDIS_ADDR:-redis:6379}"
PORT="${VERIFY_PORT:-18099}"
WORK="$(mktemp -d)"
PASS=0
FAIL=0

cleanup() {
  [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

ok()   { printf '  \033[32mPASS\033[0m  %s\n' "$1"; PASS=$((PASS+1)); }
bad()  { printf '  \033[31mFAIL\033[0m  %s\n'   "$1"; printf '        expected: %s\n        actual:   %s\n' "$2" "$3"; FAIL=$((FAIL+1)); }
head_() { printf '\n\033[1m%s\033[0m\n' "$1"; }

# ---------------------------------------------------------------------------
# Harness: a minimal server that boots from a config file, which is the shape a
# library user's main() takes. Config validation failures must surface as a
# non-zero exit, which is exactly what the H-02 gate is expected to produce.
# ---------------------------------------------------------------------------
mkdir -p "$WORK/harness"
cat > "$WORK/harness/main.go" <<'GOF'
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/benedict-erwin/gqm"
)

func main() {
	cfg, err := gqm.LoadConfigFile(os.Args[1])
	if err != nil {
		fmt.Fprintf(os.Stderr, "CONFIG_REJECTED: %v\n", err)
		os.Exit(2)
	}
	srv, err := gqm.NewServerFromConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "SERVER_ERROR: %v\n", err)
		os.Exit(3)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	fmt.Println("SERVER_STARTED")
	if err := srv.Start(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "START_ERROR: %v\n", err)
		os.Exit(4)
	}
}
GOF

printf 'Building harness...\n'
if ! go build -buildvcs=false -o "$WORK/harness/server" "$WORK/harness/main.go" 2>"$WORK/build.err"; then
  printf '\033[31mharness build failed\033[0m\n'; cat "$WORK/build.err"; exit 1
fi

write_config() { # $1=path  $2=addr  $3=extra yaml under monitoring
  cat > "$1" <<YAML
redis:
  addr: "$REDIS_ADDR"
  prefix: "gqm:verify:"
app:
  log_level: "error"
monitoring:
  api:
    enabled: true
    addr: "$2"
$3
YAML
}

start_server() { # $1=config path -> sets SERVER_PID, returns 1 if it refused to start
  "$WORK/harness/server" "$1" >"$WORK/out.log" 2>"$WORK/err.log" &
  SERVER_PID=$!
  for _ in $(seq 1 50); do
    if grep -q SERVER_STARTED "$WORK/out.log" 2>/dev/null; then
      # listener may lag the log line slightly
      for _ in $(seq 1 50); do
        curl -s -o /dev/null "http://127.0.0.1:$PORT/health" && return 0
        sleep 0.1
      done
      return 0
    fi
    kill -0 "$SERVER_PID" 2>/dev/null || return 1
    sleep 0.1
  done
  return 1
}

stop_server() {
  [[ -n "${SERVER_PID:-}" ]] && kill "$SERVER_PID" 2>/dev/null
  wait "$SERVER_PID" 2>/dev/null
  SERVER_PID=""
}

code() { curl -s -o /dev/null -w '%{http_code}' "$@"; }

# ===========================================================================
head_ "H-02  Unauthenticated API must not be reachable on a routable address"
# ===========================================================================

# The vulnerable configuration: API on, no auth, listening on every interface.
write_config "$WORK/exposed.yaml" ":$PORT" ""
if start_server "$WORK/exposed.yaml"; then
  bad "auth off + all interfaces is refused" "server refuses to start" "server started and is serving"
  stop_server
else
  if grep -q "auth is disabled" "$WORK/err.log"; then
    ok "auth off + all interfaces is refused at startup"
  else
    bad "refusal names the disabled-auth gate" "error mentioning 'auth is disabled'" "$(head -c 200 "$WORK/err.log")"
  fi
fi

write_config "$WORK/exposed4.yaml" "0.0.0.0:$PORT" ""
if start_server "$WORK/exposed4.yaml"; then
  bad "auth off + explicit 0.0.0.0 is refused" "server refuses to start" "server started"
  stop_server
else
  ok "auth off + explicit 0.0.0.0 is refused"
fi

# The same thing on loopback stays allowed: that is the local development case
# the gate is deliberately not breaking.
write_config "$WORK/loopback.yaml" "127.0.0.1:$PORT" ""
if start_server "$WORK/loopback.yaml"; then
  ok "auth off on loopback still starts"
else
  bad "auth off on loopback still starts" "server starts" "refused: $(head -c 200 "$WORK/err.log")"
fi

# ===========================================================================
head_ "H-02  CSRF header required even with auth disabled"
# ===========================================================================
# Still running the loopback instance from above.

if [[ -n "${SERVER_PID:-}" ]]; then
  c=$(code -X DELETE "http://127.0.0.1:$PORT/api/v1/queues/default/empty")
  [[ "$c" == "403" ]] && ok "destructive call without CSRF header is refused (403)" \
                      || bad "destructive call without CSRF header is refused" "403" "$c"

  c=$(code -X DELETE -H 'X-GQM-CSRF: 1' "http://127.0.0.1:$PORT/api/v1/queues/default/empty")
  [[ "$c" != "403" ]] && ok "same call with CSRF header passes the gate (got $c)" \
                      || bad "same call with CSRF header passes the gate" "anything but 403" "$c"

  # Reads are unaffected: the header guards state changes, not queries.
  c=$(code "http://127.0.0.1:$PORT/api/v1/queues")
  [[ "$c" == "200" ]] && ok "read endpoint unaffected by the CSRF requirement" \
                      || bad "read endpoint unaffected" "200" "$c"
  stop_server
else
  bad "CSRF checks" "a running loopback server" "server was not running"
fi

# ===========================================================================
head_ "H-01  Dashboard custom_dir must serve assets only"
# ===========================================================================

DASH="$WORK/dash"
OUTSIDE="$WORK/outside"
mkdir -p "$DASH/css" "$OUTSIDE"
echo '<html>dashboard</html>'                > "$DASH/index.html"
echo 'body{}'                                 > "$DASH/css/style.css"
echo 'api_keys: [{key: LEAKED_API_KEY}]'      > "$DASH/gqm.yaml"
echo 'REDIS_PASSWORD=LEAKED_PASSWORD'         > "$DASH/.env"
echo 'password_hash: LEAKED_HASH'             > "$DASH/config.yaml.bak"
echo 'OUTSIDE_SECRET'                         > "$OUTSIDE/secret.txt"
ln -sf "$OUTSIDE/secret.txt" "$DASH/link.txt"
ln -sf "$OUTSIDE" "$DASH/outside"
echo "1.0.0" > "$DASH/VERSION"

write_config "$WORK/dash.yaml" "127.0.0.1:$PORT" "  dashboard:
    enabled: true
    path_prefix: \"/dashboard\"
    custom_dir: \"$DASH\""

if start_server "$WORK/dash.yaml"; then
  for probe in "gqm.yaml:LEAKED_API_KEY" ".env:LEAKED_PASSWORD" "config.yaml.bak:LEAKED_HASH"; do
    file="${probe%%:*}"; secret="${probe##*:}"
    body=$(curl -s "http://127.0.0.1:$PORT/dashboard/$file")
    if grep -q "$secret" <<<"$body"; then
      bad "$file is not served" "no $secret in the response" "secret present (HTTP $(code "http://127.0.0.1:$PORT/dashboard/$file"))"
    else
      ok "$file is not served"
    fi
  done

  for probe in "link.txt" "outside/secret.txt"; do
    body=$(curl -s "http://127.0.0.1:$PORT/dashboard/$probe")
    if grep -q OUTSIDE_SECRET <<<"$body"; then
      bad "symlink $probe does not escape custom_dir" "no OUTSIDE_SECRET" "escaped the directory"
    else
      ok "symlink $probe does not escape custom_dir"
    fi
  done

  # Traversal was never exploitable; keep proving it.
  for p in "..%2f..%2f..%2fetc/hosts" "%2e%2e%2f%2e%2e%2fetc/hosts"; do
    body=$(curl -s --path-as-is "http://127.0.0.1:$PORT/dashboard/$p")
    grep -q "localhost" <<<"$body" \
      && bad "traversal $p blocked" "no /etc/hosts content" "looks like /etc/hosts was read" \
      || ok "traversal $p blocked"
  done

  # The hardening must not break the dashboard it protects.
  c=$(code "http://127.0.0.1:$PORT/dashboard/css/style.css")
  [[ "$c" == "200" ]] && ok "legitimate asset css/style.css still served" \
                      || bad "legitimate asset still served" "200" "$c"
  body=$(curl -s "http://127.0.0.1:$PORT/dashboard/")
  grep -q "dashboard" <<<"$body" && ok "dashboard index still served" \
                                 || bad "dashboard index still served" "index.html content" "$(head -c 80 <<<"$body")"
  body=$(curl -s "http://127.0.0.1:$PORT/dashboard/queues")
  grep -q "dashboard" <<<"$body" && ok "SPA fallback still works" \
                                 || bad "SPA fallback still works" "index.html content" "$(head -c 80 <<<"$body")"
  stop_server
else
  bad "dashboard server starts" "server starts with custom_dir" "refused: $(head -c 200 "$WORK/err.log")"
fi

# ===========================================================================
printf '\n\033[1mSummary\033[0m\n'
printf '  passed: %d\n  failed: %d\n' "$PASS" "$FAIL"
if (( FAIL > 0 )); then
  printf '\n\033[31mVERIFICATION FAILED\033[0m\n'
  exit 1
fi
printf '\n\033[32mALL CHECKS PASSED\033[0m\n'
