#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
lean_dir="$(cd "$script_dir/.." && pwd)"

cd "$lean_dir"
lake build sp_check

checker="$lean_dir/.lake/build/bin/sp_check"
fixture_dir="$lean_dir/fixtures/sp"
failures=0
checked=0

for csv in "$fixture_dir"/*.csv; do
  expected="${csv%.csv}.expected"
  name="$(basename "$csv")"
  checked=$((checked + 1))

  if [[ ! -f "$expected" ]]; then
    echo "missing expected file for $name" >&2
    failures=$((failures + 1))
    continue
  fi

  expected_exit="$(sed -n '1s/^exit=//p' "$expected")"
  expected_output="$(sed '1d' "$expected")"

  set +e
  actual_output="$("$checker" "$csv" 2>&1)"
  actual_exit=$?
  set -e

  if [[ "$actual_exit" != "$expected_exit" || "$actual_output" != "$expected_output" ]]; then
    echo "fixture failed: $name" >&2
    echo "expected exit: $expected_exit" >&2
    echo "actual exit:   $actual_exit" >&2
    echo "expected output:" >&2
    printf '%s\n' "$expected_output" >&2
    echo "actual output:" >&2
    printf '%s\n' "$actual_output" >&2
    failures=$((failures + 1))
  else
    echo "ok: $name"
  fi
done

for expected in "$fixture_dir"/*.coverage.expected; do
  csv="${expected%.coverage.expected}.csv"
  name="$(basename "$csv")"
  coverage="$(mktemp -p "$lean_dir")"
  checked=$((checked + 1))

  set +e
  "$checker" --coverage-out "$coverage" "$csv" >/dev/null 2>&1
  actual_exit=$?
  set -e

  expected_coverage="$(<"$expected")"
  actual_coverage="$(<"$coverage")"
  if [[ "$actual_exit" -ne 0 || "$expected_coverage" != "$actual_coverage" ]]; then
    echo "coverage fixture failed: $name" >&2
    echo "expected coverage:" >&2
    sed -n '1,20p' "$expected" >&2
    echo "actual coverage:" >&2
    sed -n '1,20p' "$coverage" >&2
    failures=$((failures + 1))
  else
    echo "ok: $name coverage"
  fi
  rm -f "$coverage"
done

echo "SP campaign checks: $checked"
exit "$failures"
