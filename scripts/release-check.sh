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
bold "TUI module vs its newest tag"
# ---------------------------------------------------------------------------
# scripts/check-tui-pin.sh covers the other half of this: that the root go.mod
# requires the newest tui/vX.Y.Z tag. It compares two version strings and never
# reads the tui source tree, so it stays green while tui/ moves past the tag it
# names. Release in that state and every `go install` of cmd/gqm gets the older
# TUI — the v0.7.0 bug reached from the other direction, and only visible to
# consumers, since go.work substitutes the local tui/ directory for everyone
# working here.
#
# Compared against $HEAD_REF rather than the working tree's HEAD: every other
# section reports on $BASE..$HEAD_REF, and the ref being released is the one
# whose tui/ sources consumers do or do not receive. Uncommitted edits under
# tui/ are invisible here, which is correct — they are not part of the release
# either.
#
# Tag filtering follows check-tui-pin.sh exactly: plain vX.Y.Z only, because a
# release candidate would make "newest" a judgement call, and sort -V rather
# than sort, under which v0.10.0 comes before v0.9.0.
tui_tags=$(git tag --list 'tui/v*' | grep -E '^tui/v[0-9]+\.[0-9]+\.[0-9]+$' || true)
if [[ -z "$tui_tags" ]]; then
  echo "  no tui/vX.Y.Z tags in this checkout — nothing to compare tui/ against."
  echo "  a clone fetched without tags looks exactly like a repo that never"
  echo "  tagged tui, so treat this as unanswered rather than as no drift."
else
  tui_tag="tui/$(sed 's|^tui/||' <<<"$tui_tags" | sort -V | tail -n1)"
  # Status captured rather than discarded with `|| true`: a failed diff also
  # produces no filenames, and reporting that as "unchanged" would be this
  # section confidently answering a question it never asked. $BASE and $HEAD_REF
  # are rev-parse-verified at the top of the script; the tag arrives from the
  # tag list unverified, and a tag on a non-commit object, or a partial clone
  # missing the objects, lands here.
  tui_files=$(git diff --name-only "$tui_tag" "$HEAD_REF" -- 'tui/' 2>/dev/null)
  tui_status=$?
  if [[ "$tui_status" != "0" ]]; then
    echo "  could not diff $tui_tag against $HEAD_REF (git exited $tui_status) —"
    echo "  tui/ is unanswered here, not unchanged. Re-run it by hand to see why;"
    echo "  a failed comparison is not evidence of no drift."
  elif [[ -z "$tui_files" ]]; then
    echo "  tui/ is unchanged since $tui_tag — that tag still describes these sources."
  else
    echo "  tui/ has moved since $tui_tag:"
    sed 's/^/    /' <<<"$tui_files"
    tui_commits=$(git log --format='    %h %s' "$tui_tag..$HEAD_REF" -- 'tui/' 2>/dev/null || true)
    if [[ -n "$tui_commits" ]]; then
      echo "  commits touching tui/:"
      echo "$tui_commits"
    fi
    echo
    echo "  -> releasing now ships $tui_tag to everyone installing the CLI, and the"
    echo "     work above reaches nobody. Tag tui first, then bump the root require"
    echo "     (go mod edit -require=.../tui@vX.Y.Z && go mod tidy) and re-run this."
    echo "     Deliberately releasing the root without re-tagging tui is a valid"
    echo "     choice; this reports it so that it stays a choice."
  fi
fi
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
