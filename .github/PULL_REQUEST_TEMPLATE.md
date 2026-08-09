<!--
Target branch: develop. main only receives release merges.
Security fixes do not start as a public PR — see SECURITY.md.
-->

## What this changes

<!-- One or two sentences. What behaviour is different after this merges? -->

Closes #

## Why

<!-- The problem being solved. If there is an issue with the discussion, linking it is enough. -->

## How it was verified

<!--
Be specific. "Tests pass" says less than "added TestReaper_ExpiredHash, which
fails on develop and passes here". Note anything you could not verify.
-->

## Checks

- [ ] `go vet ./...` clean
- [ ] `gofmt -l .` reports nothing outside `vendor/`
- [ ] `go test -race ./... -count=1` passes with Redis reachable (not skipped)
- [ ] `cd tui && go test -race ./...` passes — the TUI is a separate module and `./...` from the root does not reach it
- [ ] Examples under `_examples/` still build
- [ ] `CHANGELOG.md` updated under `## [Unreleased]` if the change is user-visible
- [ ] README updated if the public API or configuration changed
- [ ] No new production dependency, or one agreed in an issue first

## Compatibility

<!--
Anything an existing user has to do differently after upgrading: changed
defaults, stricter validation, new Redis keys, altered timing. Behaviour that
changes without an API change is still breaking — say so here.

Write "none" if there is nothing.
-->
