#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$root/lean"

lake build drr_check

status=0
for expected in fixtures/drr/*.coverage.expected; do
  csv="${expected%.coverage.expected}.csv"
  tmp="$(mktemp -p .)"
  if .lake/build/bin/drr_check --coverage-out "$tmp" "$csv" >/dev/null 2>&1; then
    actual_exit=0
  else
    actual_exit=$?
  fi
  if [ "$actual_exit" -ne 0 ]; then
    echo "coverage fixture failed: $(basename "$csv")"
    echo "expected checker exit: 0"
    echo "actual checker exit:   $actual_exit"
    status=1
  else
    expected_text="$(cat "$expected")"
    actual_text="$(cat "$tmp")"
    if [ "$expected_text" != "$actual_text" ]; then
    echo "coverage fixture failed: $(basename "$csv")"
    echo "expected coverage:"
    cat "$expected"
    echo
    echo "actual coverage:"
    cat "$tmp"
    echo
    status=1
    else
    echo "ok: $(basename "$csv")"
    fi
  fi
  rm -f "$tmp"
done

exit "$status"
