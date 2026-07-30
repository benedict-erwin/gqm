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
#   2. Prove the new check is load-bearing. Disable the fix, run the script, and
#      confirm the new check goes red:
#        cp <file> /tmp/backup-<file>            # ALWAYS back up first
#        # ...disable the fix, e.g. git checkout <commit> -- <file>...
#        bash scripts/verify-security-fixes.sh   # the new check must FAIL
#        cp /tmp/backup-<file> <file>            # restore from the backup
#      A check that never fails proves nothing. Green is not evidence on its
#      own — that has been misleading more than once in this project.
#
#      `git checkout -- <file>` overwrites the working copy with no way back and
#      will DESTROY uncommitted work. It is only safe once the fix is committed.
#      Backing up first costs nothing and has already saved this exact mistake.
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

# A tiny Redis helper, because redis-cli is not installed in the devcontainer
# and the checks need to seed and inspect keys directly.
mkdir -p "$WORK/redisctl"
cat > "$WORK/redisctl/main.go" <<'GOF'
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: os.Args[1]})
	ctx := context.Background()
	switch os.Args[2] {
	case "set": // set <key> <value>
		if err := rdb.Set(ctx, os.Args[3], os.Args[4], time.Hour).Err(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "exists": // exists <key> -> prints 0 or 1
		n, err := rdb.Exists(ctx, os.Args[3]).Result()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println(n)
	case "del": // del <key>
		rdb.Del(ctx, os.Args[3])
	case "seedroots": // seedroots <prefix> <n> — n job hashes that each look like a DAG root
		n, _ := strconv.Atoi(os.Args[4])
		pipe := rdb.Pipeline()
		for i := 0; i < n; i++ {
			id := fmt.Sprintf("root%06d", i)
			pipe.HSet(ctx, os.Args[3]+"job:"+id, "id", id, "type", "t", "status", "completed")
			pipe.SAdd(ctx, os.Args[3]+"job:"+id+":dependents", "child")
			if i%1000 == 999 {
				if _, err := pipe.Exec(ctx); err != nil {
					fmt.Fprintln(os.Stderr, err)
					os.Exit(1)
				}
				pipe = rdb.Pipeline()
			}
		}
		if _, err := pipe.Exec(ctx); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "delpattern": // delpattern <pattern>
		iter := rdb.Scan(ctx, 0, os.Args[3], 1000).Iterator()
		var batch []string
		for iter.Next(ctx) {
			batch = append(batch, iter.Val())
			if len(batch) >= 1000 {
				rdb.Del(ctx, batch...)
				batch = batch[:0]
			}
		}
		if len(batch) > 0 {
			rdb.Del(ctx, batch...)
		}
	}
}
GOF

# A probe that drives the real client, because the M-05 collision is only
# reachable through Enqueue — it cannot be shown from the HTTP surface.
mkdir -p "$WORK/jobidprobe"
cat > "$WORK/jobidprobe/main.go" <<'GOF'
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/benedict-erwin/gqm"
)

// Reproduces both variants of the DAG metadata collision. Prints one line per
// probe so the shell can assert on them.
func main() {
	c, err := gqm.NewClient(gqm.WithRedisAddr(os.Args[1]), gqm.WithPrefix("gqm:verify:"))
	if err != nil {
		fmt.Println("CLIENT_ERROR", err)
		os.Exit(1)
	}
	defer c.Close()
	ctx := context.Background()

	// Variant 1: squat the victim's :deps key.
	_, err = c.Enqueue(ctx, "probe.job", gqm.Payload{}, gqm.JobID("order-42:deps"))
	switch {
	case errors.Is(err, gqm.ErrInvalidJobID):
		fmt.Println("DEPS_REJECTED", err)
	case err == nil:
		fmt.Println("DEPS_ACCEPTED")
	default:
		fmt.Println("DEPS_OTHER_ERROR", err)
	}

	// Variant 2: squat a parent's :dependents key.
	_, err = c.Enqueue(ctx, "probe.job", gqm.Payload{}, gqm.JobID("order-99:dependents"))
	switch {
	case errors.Is(err, gqm.ErrInvalidJobID):
		fmt.Println("DEPENDENTS_REJECTED")
	case err == nil:
		fmt.Println("DEPENDENTS_ACCEPTED")
	default:
		fmt.Println("DEPENDENTS_OTHER_ERROR", err)
	}

	// The victim DAG must now enqueue cleanly. If a squatter got in first this
	// is where WRONGTYPE surfaces.
	if _, err := c.Enqueue(ctx, "probe.job", gqm.Payload{}, gqm.JobID("order-42")); err != nil {
		fmt.Println("VICTIM_PARENT_FAILED", err)
	} else {
		fmt.Println("VICTIM_PARENT_OK")
	}
	if _, err := c.Enqueue(ctx, "probe.job", gqm.Payload{},
		gqm.JobID("order-42-child"), gqm.DependsOn("order-42")); err != nil {
		fmt.Println("VICTIM_CHILD_FAILED", err)
	} else {
		fmt.Println("VICTIM_CHILD_OK")
	}

	// Colons remain legal where the namespace convention needs them.
	if _, err := c.Enqueue(ctx, "email:send", gqm.Payload{},
		gqm.JobID("probe-legit"), gqm.Queue("email:send")); err != nil {
		fmt.Println("NAMESPACE_REJECTED", err)
	} else {
		fmt.Println("NAMESPACE_OK")
	}
}
GOF

printf 'Building harness...\n'
if ! go build -buildvcs=false -o "$WORK/harness/server" "$WORK/harness/main.go" 2>"$WORK/build.err"; then
  printf '\033[31mharness build failed\033[0m\n'; cat "$WORK/build.err"; exit 1
fi
if ! go build -buildvcs=false -o "$WORK/redisctl/redisctl" "$WORK/redisctl/main.go" 2>>"$WORK/build.err"; then
  printf '\033[31mredisctl build failed\033[0m\n'; cat "$WORK/build.err"; exit 1
fi
if ! go build -buildvcs=false -o "$WORK/jobidprobe/jobidprobe" "$WORK/jobidprobe/main.go" 2>>"$WORK/build.err"; then
  printf '\033[31mjobidprobe build failed\033[0m\n'; cat "$WORK/build.err"; exit 1
fi
rctl() { "$WORK/redisctl/redisctl" "$REDIS_ADDR" "$@"; }

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
head_ "M-01  A revoked user's session must stop working immediately"
# ===========================================================================
# Sessions live in Redis and survive config changes, so removing a user only
# counts as revocation if the session token stops authenticating.

cat > "$WORK/revoke.yaml" <<YAML
redis:
  addr: "$REDIS_ADDR"
  prefix: "gqm:verify:"
app:
  log_level: "error"
monitoring:
  api:
    enabled: true
    addr: "127.0.0.1:$PORT"
  auth:
    enabled: true
    users:
      - username: "alice"
        password_hash: "\$2a\$10\$N9qo8uLOickgx2ZMRZoMyeIjZAgcfl7p92ldGxad68LJZdL17lhWy"
        role: "viewer"
YAML

# "bob" is not in that config. Seed a session for him, as if he had been
# removed after logging in.
BOB_TOKEN="aaaaaaaabbbbbbbbccccccccddddddddeeeeeeeeffffffff0000000011111111"
rctl set "gqm:verify:session:$BOB_TOKEN" "bob"

if start_server "$WORK/revoke.yaml"; then
  c=$(code -X DELETE -H "X-GQM-CSRF: 1" -H "Cookie: gqm_session=$BOB_TOKEN" \
        "http://127.0.0.1:$PORT/api/v1/queues/default/empty")
  [[ "$c" == "401" ]] && ok "revoked user's session is rejected (401)" \
                      || bad "revoked user's session is rejected" "401" "$c"

  left=$(rctl exists "gqm:verify:session:$BOB_TOKEN")
  [[ "$left" == "0" ]] && ok "orphaned session is deleted, not merely refused" \
                       || bad "orphaned session is deleted" "key gone" "still present"
  stop_server
else
  bad "revocation server starts" "server starts" "refused: $(head -c 200 "$WORK/err.log")"
fi
rctl del "gqm:verify:session:$BOB_TOKEN"

# ===========================================================================
head_ "I-06  Redis without a password must warn loudly"
# ===========================================================================
# Allowed by decision, but the operator has to be told: session tokens live in
# Redis, so an unprotected Redis bypasses authentication rather than breaking it.

write_config "$WORK/nopass.yaml" "127.0.0.1:$PORT" ""
if start_server "$WORK/nopass.yaml"; then
  ok "server still starts with an unprotected Redis (allowed by design)"
  if grep -q "REDIS HAS NO PASSWORD" "$WORK/err.log" "$WORK/out.log" 2>/dev/null; then
    ok "startup warns that Redis has no password"
  else
    bad "startup warns that Redis has no password" "a REDIS HAS NO PASSWORD banner" "no such warning in the logs"
  fi
  stop_server
else
  bad "unprotected Redis still allowed to start" "server starts" "refused: $(head -c 200 "$WORK/err.log")"
fi

# ===========================================================================
head_ "M-05  A job ID must not be able to occupy another job's DAG metadata key"
# ===========================================================================
# GQM joins Redis key segments with a colon, and a job owns a bare key as well
# as suffixed ones, so an id like "order-42:deps" lands on job order-42's
# dependency set. Different Redis types, so the victim's enqueue died with
# WRONGTYPE. Driven through the real client: this is not reachable over HTTP.

rctl delpattern "gqm:verify:job:order-*"
probe_out=$("$WORK/jobidprobe/jobidprobe" "$REDIS_ADDR" 2>&1)

grep -q "^DEPS_REJECTED" <<<"$probe_out" \
  && ok "enqueue of the ':deps' squatter id is refused" \
  || bad "enqueue of the ':deps' squatter id is refused" "DEPS_REJECTED" "$(grep '^DEPS_' <<<"$probe_out" | head -1)"

grep -q "^DEPENDENTS_REJECTED" <<<"$probe_out" \
  && ok "enqueue of the ':dependents' squatter id is refused" \
  || bad "enqueue of the ':dependents' squatter id is refused" "DEPENDENTS_REJECTED" "$(grep '^DEPENDENTS_' <<<"$probe_out" | head -1)"

# The refusal has to say why, or a caller sees an ordinary-looking id rejected
# with nothing to act on.
grep -q "^DEPS_REJECTED.*colon" <<<"$probe_out" \
  && ok "refusal explains that colons are the key separator" \
  || bad "refusal explains why" "an error mentioning colons" "$(grep '^DEPS_REJECTED' <<<"$probe_out")"

# Both halves of the PoC must now be impossible, which means the victim's DAG
# enqueues cleanly rather than hitting WRONGTYPE.
grep -q "^VICTIM_PARENT_OK" <<<"$probe_out" \
  && ok "victim parent job enqueues without WRONGTYPE" \
  || bad "victim parent job enqueues" "VICTIM_PARENT_OK" "$(grep '^VICTIM_PARENT' <<<"$probe_out")"
grep -q "^VICTIM_CHILD_OK" <<<"$probe_out" \
  && ok "victim child with DependsOn enqueues without WRONGTYPE" \
  || bad "victim child enqueues" "VICTIM_CHILD_OK" "$(grep '^VICTIM_CHILD' <<<"$probe_out")"

# The namespace convention must survive: colons stay legal in job types and
# queue names, which is where they are actually needed.
grep -q "^NAMESPACE_OK" <<<"$probe_out" \
  && ok "colons still allowed in job type and queue name" \
  || bad "colons still allowed in job type and queue name" "NAMESPACE_OK" "$(grep '^NAMESPACE' <<<"$probe_out")"

# The HTTP surface must agree with the library, or it would offer a lookup path
# to keys no legitimate job can own.
write_config "$WORK/jobid.yaml" "127.0.0.1:$PORT" ""
if start_server "$WORK/jobid.yaml"; then
  c=$(code "http://127.0.0.1:$PORT/api/v1/jobs/order-42:deps")
  [[ "$c" == "400" ]] && ok "API rejects a colon job ID path param (400)" \
                      || bad "API rejects a colon job ID path param" "400" "$c"
  # Queue names keep their colons on the same surface.
  c=$(code "http://127.0.0.1:$PORT/api/v1/queues/email:send/stats")
  [[ "$c" != "400" ]] && ok "API still accepts a colon queue name (got $c)" \
                      || bad "API still accepts a colon queue name" "anything but 400" "$c"
  stop_server
else
  bad "job id server starts" "server starts" "refused: $(head -c 200 "$WORK/err.log")"
fi
rctl delpattern "gqm:verify:job:order-*"
rctl delpattern "gqm:verify:job:probe-*"

# ===========================================================================
head_ "M-03  /api/v1/dag/roots must bound its scan and admit when it truncates"
# ===========================================================================
# The endpoint is read-only, so a viewer reaches it. It used to SCAN the whole
# keyspace with pagination applied only afterwards, so limit=1 cost the same as
# limit=all. Seeded past the 5000-root cap so the bound is exercised at its real
# production value, not a shrunken test one.

rctl delpattern "gqm:verify:job:root*"
rctl seedroots "gqm:verify:" 6000

write_config "$WORK/dagroots.yaml" "127.0.0.1:$PORT" ""
if start_server "$WORK/dagroots.yaml"; then
  body=$(curl -s "http://127.0.0.1:$PORT/api/v1/dag/roots?limit=1")
  if grep -q '"truncated":true' <<<"$body"; then
    ok "scan stops at the root cap and reports truncated"
  else
    bad "scan stops at the root cap and reports truncated" \
        '"truncated":true in the response meta' "$(head -c 200 <<<"$body")"
  fi
  if grep -qE '"total":5000\b' <<<"$body"; then
    ok "collected roots are capped at 5000, not the full 6000"
  else
    bad "collected roots are capped at 5000" '"total":5000' \
        "$(grep -o '\"total\":[0-9]*' <<<"$body" | head -1)"
  fi

  # Regression guard, not proof of the deadline: this database is far too small
  # to make an unbounded scan slow, so it stays green with the caps removed. It
  # is here to catch a fix that makes the endpoint pathologically slow instead.
  t0=$(date +%s)
  code -o /dev/null "http://127.0.0.1:$PORT/api/v1/dag/roots?limit=1" >/dev/null
  elapsed=$(( $(date +%s) - t0 ))
  (( elapsed <= 10 )) && ok "endpoint still responds promptly (${elapsed}s)" \
                      || bad "endpoint still responds promptly" "<= 10s" "${elapsed}s"
  stop_server
else
  bad "dag roots server starts" "server starts" "refused: $(head -c 200 "$WORK/err.log")"
fi
rctl delpattern "gqm:verify:job:root*"

# ===========================================================================
head_ "M-02  Committed compose files must not publish Redis on every interface"
# ===========================================================================
# The Redis image runs with `bind * -::*` and protected-mode off, so the publish
# binding is the only thing between an unauthenticated session store and the
# local network. This is a static check of the committed files: the running
# container keeps whatever binding it was created with until it is recreated.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
for f in docker-compose.yml .devcontainer/docker-compose.yml; do
  path="$REPO_ROOT/$f"
  if [[ ! -f "$path" ]]; then
    bad "$f exists to be checked" "the file to be present" "not found at $path"
    continue
  fi
  # Port mappings are list items; anything else mentioning 6379 (healthcheck,
  # command) is not a publish directive.
  bare=$(grep -nE '^[[:space:]]*-[[:space:]]*"?[^"#]*6379' "$path" \
         | grep -vE '"?127\.0\.0\.1:|"?localhost:|"?\[::1\]:' || true)
  if [[ -z "$bare" ]]; then
    ok "$f publishes Redis on loopback only (or not at all)"
  else
    bad "$f publishes Redis on loopback only" \
        "every 6379 mapping prefixed with 127.0.0.1, or no mapping" \
        "$(tr '\n' ' ' <<<"$bare")"
  fi
done

# ===========================================================================
printf '\n\033[1mSummary\033[0m\n'
printf '  passed: %d\n  failed: %d\n' "$PASS" "$FAIL"
if (( FAIL > 0 )); then
  printf '\n\033[31mVERIFICATION FAILED\033[0m\n'
  exit 1
fi
printf '\n\033[32mALL CHECKS PASSED\033[0m\n'
