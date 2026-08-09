#!/usr/bin/env bash
#
# Fails when the root go.mod requires an older tui than the newest tui tag.
#
#   bash scripts/check-tui-pin.sh
#
# The TUI is a separate Go module, and the root module depends on it like any
# third-party library: a require line naming one exact version. That version is
# what `go install github.com/benedict-erwin/gqm/cmd/gqm@latest` compiles into
# the binary, and nothing moves that line when the tui module gets tagged.
#
# It stopped moving for six months. The root required tui v0.1.0 (February)
# while the module had been tagged through v0.3.0, so every install shipped a
# TUI with no DAG tab, no drill-downs, none of the 0.4.0 restyle and none of
# the 0.5.0 stale markers. It was fixed in v0.7.0; nothing prevented it from
# happening again, which is what this script is for.
#
# No test can catch this, and that is the whole difficulty. go.work substitutes
# the tui/ directory on disk for the module, so every local build, every
# `go test ./...`, every hand-run of the CLI in this repository uses current
# tui sources and looks entirely correct. Only a consumer resolving through the
# module proxy — who has no go.work — receives the pinned version. The one
# environment that can observe the bug is the one nobody working here is in.
#
# Only one direction of drift is an error:
#
#   require line BEHIND an existing tag  -> ships stale code to every user,
#                                           always fails here.
#   tui/ sources AHEAD of the newest tag -> ordinary unreleased work on
#                                           develop, not reported at all.
#
# Consequently this compares two version strings and never looks at the tui
# source tree.

set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)

fail() {
  # ::error:: puts the message on the job summary and against the file in the
  # GitHub UI; outside Actions it is noise, so it is only emitted there.
  if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
    echo "::error::$1"
  fi
  echo "FAIL: $1" >&2
  exit 1
}

# `go mod edit -json` reads go.mod with the toolchain's own parser and reads
# the file itself, so a workspace cannot redirect it the way it redirects
# `go list -m` — under go.work that would report the local directory and an
# empty version, which is exactly the substitution that hides this bug in the
# first place. Grepping the require line would instead break on a single-line
# require, a trailing comment, or an `// indirect` marker.
mod_json=$(go mod edit -json "$ROOT/go.mod")
root_path=$(jq -r '.Module.Path' <<<"$mod_json")

# Derived rather than hardcoded: if the repository is ever renamed, the derived
# path stops matching the require line and this check fails loudly instead of
# quietly comparing nothing.
tui_path="$root_path/tui"

required=$(jq -r --arg p "$tui_path" '.Require[]? | select(.Path == $p) | .Version' <<<"$mod_json")

if [[ -z "$required" ]]; then
  fail "go.mod has no require for $tui_path, so this check is no longer checking anything. If the CLI genuinely stopped depending on the tui module, delete this script and its CI job; otherwise the require line went missing."
fi

# The tag set has to actually be present. actions/checkout clones shallow and
# without tags by default, and against an empty tag set every comparison below
# passes — a check that is green because it found nothing to compare is the
# failure mode this repository keeps running into, so zero tags is an error,
# never a pass. In CI the fix is `fetch-depth: 0` on the checkout step.
#
# Restricted to plain vX.Y.Z: a release candidate or a mistyped tag would make
# "newest" a judgement call, and `sort -V` has no opinion about prerelease
# precedence.
tags=$(git -C "$ROOT" tag --list 'tui/v*' | grep -E '^tui/v[0-9]+\.[0-9]+\.[0-9]+$' || true)

if [[ -z "$tags" ]]; then
  fail "no tui/vX.Y.Z tags found in this checkout, so there is nothing to compare the pin against. This is almost always a shallow clone without tags rather than a repository with no releases — in CI, set fetch-depth: 0 on actions/checkout."
fi

# sort -V, never plain sort: lexically v0.10.0 sorts before v0.9.0, so a
# lexical comparison would call the pin current while it sat a minor version
# behind — the same silent pass this script exists to prevent.
latest=$(sed 's|^tui/||' <<<"$tags" | sort -V | tail -n1)

echo "root go.mod requires $tui_path $required"
echo "newest tui tag:      tui/$latest"

if [[ "$required" == "$latest" ]]; then
  echo "OK: the root module pins the newest tui tag."
  exit 0
fi

# Which of the two is newer, decided by the same version sort.
newest=$(printf '%s\n%s\n' "$required" "$latest" | sort -V | tail -n1)

if [[ "$newest" == "$required" ]]; then
  # The require line names something no tag matches — a pseudo-version, or a
  # tag that has not been pushed yet. Not stale code, and not silent either:
  # a consumer's `go install` fails to resolve it immediately and loudly. Worth
  # saying, not worth failing a build over.
  echo "NOTE: the require line is ahead of every tui tag. That resolves for consumers only once tui/$required is pushed."
  exit 0
fi

fail "root go.mod requires $tui_path $required but tui/$latest is tagged, so every 'go install' of cmd/gqm ships the older TUI while everything built here uses current sources via go.work. Bump it: go mod edit -require=$tui_path@$latest && go mod tidy"
