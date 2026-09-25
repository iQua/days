#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
lean_dir="$(cd "$script_dir/.." && pwd)"

cd "$lean_dir"
lake build p10c_aqm_check

checker="$lean_dir/.lake/build/bin/p10c_aqm_check"
fixture_dir="$lean_dir/fixtures/p10c"
failures=0
checked=0

for csv in "$fixture_dir"/aqm_*.csv; do
  expected="${csv%.csv}.expected"
  checked=$((checked + 1))
  expected_exit="$(sed -n '1s/^exit=//p' "$expected")"
  expected_output="$(sed '1d' "$expected")"

  set +e
  actual_output="$("$checker" "$csv" 2>&1)"
  actual_exit=$?
  set -e

  if [[ "$actual_exit" != "$expected_exit" || "$actual_output" != "$expected_output" ]]; then
    echo "fixture failed: $(basename "$csv")" >&2
    echo "expected exit: $expected_exit" >&2
    echo "actual exit:   $actual_exit" >&2
    echo "expected output: $expected_output" >&2
    echo "actual output:   $actual_output" >&2
    failures=$((failures + 1))
  else
    echo "ok: $(basename "$csv")"
  fi
done

echo "P10c executor AQM campaign checks: $checked"
exit "$failures"
