#!/usr/bin/env bash
#
# Reports what a develop -> main merge would contain, and which version bump it
# implies. Run it before merging.
#
#   bash scripts/release-check.sh [base] [head]     # defaults: main develop
#
# This does not decide for you and does not tag anything. It gathers the
# evidence that the decision should rest on, because "does this need a minor
# bump?" is a question people answer from memory and get wrong. For v0.2.0 the
# answer was not obvious: zero public API was removed, which looks like a patch
# release, while six behaviours changed in ways that broke existing
# deployments.
#
# What it cannot see: behavioural changes that leave no trace in a signature or
# a commit subject. A changed default, a new expiry, a stricter validation —
# those only surface if the commit says so. Which is why the last section asks
# you to look, rather than reporting a verdict.

set -uo pipefail

BASE="${1:-main}"
HEAD_REF="${2:-develop}"

bold() { printf '\033[1m%s\033[0m\n' "$1"; }
dim()  { printf '\033[2m%s\033[0m\n' "$1"; }

if ! git rev-parse --verify --quiet "$BASE" >/dev/null; then
  echo "no such ref: $BASE" >&2; exit 1
fi
if ! git rev-parse --verify --quiet "$HEAD_REF" >/dev/null; then
  echo "no such ref: $HEAD_REF" >&2; exit 1
fi

RANGE="$BASE..$HEAD_REF"
COUNT=$(git rev-list --count "$RANGE")

bold "Release check: $RANGE"
echo "  commits ahead: $COUNT"
if [[ "$COUNT" == "0" ]]; then
  echo "  nothing to merge."
  exit 0
fi
LAST_TAG=$(git describe --tags --abbrev=0 "$BASE" 2>/dev/null || echo "none")
echo "  last tag on $BASE: $LAST_TAG"
echo

# ---------------------------------------------------------------------------
bold "Breaking markers in commit subjects"
# ---------------------------------------------------------------------------
# Conventional-commit "!" and an explicit BREAKING CHANGE footer are the two
# signals an author can leave deliberately. Both are opt-in, so their absence
# proves nothing.
breaking=$(git log --format='%h %s' "$RANGE" | grep -E '^[0-9a-f]+ [a-z]+(\([^)]*\))?!:' || true)
footers=$(git log --format='%h%n%B' "$RANGE" | grep -iE 'BREAKING[ -]CHANGE' || true)
if [[ -n "$breaking" || -n "$footers" ]]; then
  [[ -n "$breaking" ]] && sed 's/^/  ! /' <<<"$breaking"
  [[ -n "$footers" ]] && sed 's/^/  footer: /' <<<"$footers"
  echo
  echo "  -> at least MINOR (0.x) or MAJOR (>=1.0)"
else
  echo "  none found"
fi
echo

# ---------------------------------------------------------------------------
bold "Public API surface"
# ---------------------------------------------------------------------------
# Exported top-level declarations only. Method sets, struct fields and changed
# signatures are not covered — this is a smoke test, not an API differ.
api_removed=$(git diff "$RANGE" --unified=0 -- '*.go' ':!*_test.go' ':!vendor/*' 2>/dev/null \
  | grep -E '^-(func|type|const|var) [A-Z]' | sed 's/^-/  /' | sort -u || true)
api_added=$(git diff "$RANGE" --unified=0 -- '*.go' ':!*_test.go' ':!vendor/*' 2>/dev/null \
  | grep -E '^\+(func|type|const|var) [A-Z]' | sed 's/^+/  /' | sort -u || true)

if [[ -n "$api_removed" ]]; then
  echo "  REMOVED (breaking):"
  echo "$api_removed"
else
  echo "  removed: none"
fi
if [[ -n "$api_added" ]]; then
  echo "  added:"
  echo "$api_added"
else
  echo "  added: none"
fi
echo

# ---------------------------------------------------------------------------
bold "Changed areas"
# ---------------------------------------------------------------------------
git diff --stat "$RANGE" -- ':!vendor/*' 2>/dev/null | tail -1 | sed 's/^/  /'
for area in "config.go:config schema" "lua/:Lua scripts" "monitor/:HTTP API" \
            "cmd/:CLI" "*.md:docs"; do
  path="${area%%:*}"; label="${area##*:}"
  n=$(git diff --name-only "$RANGE" -- "$path" 2>/dev/null | grep -v '^vendor/' | wc -l | tr -d ' ')
  [[ "$n" != "0" ]] && printf '  %-16s %s file(s)\n' "$label" "$n"
done
echo

# ---------------------------------------------------------------------------
bold "Commits"
# ---------------------------------------------------------------------------
git log --format='  %h %s' "$RANGE"
echo

# ---------------------------------------------------------------------------
bold "Decide"
# ---------------------------------------------------------------------------
cat <<'TXT'
  Under SemVer 0.x, breaking changes bump the MINOR. Past 1.0 they bump MAJOR.

  Signatures are the easy half. Before calling this a patch release, check the
  half no tool sees:

    - Does anything now expire, get deleted, or get cleaned up that did not?
    - Does any default value change?
    - Does validation reject input it used to accept?
    - Will a config that worked before now fail to start?
    - Do existing API clients need to send anything new?

  A "yes" to any of those is breaking, even with an untouched public API.
  That is exactly what 0.2.0 was: nothing removed, six behaviours changed.
TXT
