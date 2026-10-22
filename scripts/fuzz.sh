#!/usr/bin/env bash
#
# fuzz.sh — Run every Go fuzz target in the module for a fixed duration
#
# Usage: scripts/fuzz.sh [fuzztime]   (default 30s per target)
#
# Targets are discovered from `func FuzzXxx(` declarations in test files, so
# a new target needs no registration. Go runs one fuzz target per
# invocation, hence the loop. A failing input is written to the package's
# testdata/fuzz directory by `go test`; commit it as a regression seed.
#
# Exit codes:
#   0 — every target ran without a failure
#   1 — at least one target failed
#
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FUZZTIME="${1:-30s}"

cd "$REPO_ROOT" || exit 1

# run_target fuzzes one target and prints its output. The Go fuzzing engine
# occasionally reports "context deadline exceeded" when the time budget ends
# while workers are still coordinating; that is not a finding (no failing
# input is written), so such a run is repeated once, with a notice.
run_target() {
    local pkg="$1" target="$2" out
    out="$(go test "$pkg" -run '^$' -fuzz "^${target}\$" -fuzztime "$FUZZTIME" 2>&1)" && { echo "$out"; return 0; }
    echo "$out"
    if grep -q 'context deadline exceeded' <<<"$out" && ! grep -q 'Failing input written' <<<"$out"; then
        echo "-- $target: engine deadline error with no failing input; repeating once"
        go test "$pkg" -run '^$' -fuzz "^${target}\$" -fuzztime "$FUZZTIME"
        return $?
    fi
    return 1
}

failed=0
while IFS=: read -r file target; do
    pkg="./$(dirname "$file")"
    echo "== $pkg $target ($FUZZTIME)"
    if ! run_target "$pkg" "$target"; then
        failed=1
    fi
done < <(grep -rhoE --include='*_test.go' '^func (Fuzz[A-Za-z0-9_]+)\(' lode internal \
    --with-filename | sed -E 's/^([^:]+):func (Fuzz[A-Za-z0-9_]+)\(/\1:\2/' | sort -u)

exit "$failed"
