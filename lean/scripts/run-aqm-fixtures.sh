#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
lean_dir="$(cd "$script_dir/.." && pwd)"

cd "$lean_dir"

lake build aqm_check

checker="$lean_dir/.lake/build/bin/aqm_check"
fixture_dir="$lean_dir/fixtures/aqm"

failures=0

for csv in "$fixture_dir"/*.csv; do
  expected="${csv%.csv}.expected"
  name="$(basename "$csv")"

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

if [[ "$failures" -ne 0 ]]; then
  exit 1
fi
