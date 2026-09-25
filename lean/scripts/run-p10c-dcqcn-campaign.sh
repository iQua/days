#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
lean_dir="$(cd "$script_dir/.." && pwd)"

cd "$lean_dir"
lake build p10c_dcqcn_check

checker="$lean_dir/.lake/build/bin/p10c_dcqcn_check"
fixture_dir="$lean_dir/fixtures/p10c"
failures=0
checked=0

check_case() {
  local label="$1"
  local csv="$2"
  local expected_exit="$3"
  local expected_output="$4"
  checked=$((checked + 1))

  set +e
  actual_output="$("$checker" "$csv" 2>&1)"
  actual_exit=$?
  set -e

  if [[ "$actual_exit" != "$expected_exit" || "$actual_output" != "$expected_output" ]]; then
    echo "fixture failed: $label" >&2
    echo "expected exit: $expected_exit" >&2
    echo "actual exit:   $actual_exit" >&2
    echo "expected output: $expected_output" >&2
    echo "actual output:   $actual_output" >&2
    failures=$((failures + 1))
  else
    echo "ok: $label"
  fi
}

for csv in "$fixture_dir"/dcqcn_*_accept.csv; do
  expected="${csv%.csv}.expected"
  expected_exit="$(sed -n '1s/^exit=//p' "$expected")"
  expected_output="$(sed '1d' "$expected")"
  check_case "$(basename "$csv")" "$csv" "$expected_exit" "$expected_output"
done

campaign_tmp="$(mktemp -d)"
campaign_case="$campaign_tmp/mutated.csv"
trap 'rm -f "$campaign_case"; rmdir "$campaign_tmp"' EXIT

mutate_case() {
  local label="$1"
  local source="$2"
  local program="$3"
  local expected_output="$4"
  awk -F, -v OFS=, "$program" "$source" > "$campaign_case"
  check_case "$label" "$campaign_case" 1 "$expected_output"
}

anchor="$fixture_dir/dcqcn_exact_anchor_accept.csv"
repeated="$fixture_dir/dcqcn_repeated_cnp_accept.csv"
stages="$fixture_dir/dcqcn_stages_and_bytes_accept.csv"
minimum="$fixture_dir/dcqcn_min_clamp_accept.csv"

mutate_case "cnp-applied-result-mutation" "$repeated" \
  'NR == 6 { $8 = 0 } { print }' \
  'REJECT: line 6: DCQCN transition result mismatch'
mutate_case "cnp-ignored-result-mutation" "$anchor" \
  'NR == 3 { $8 = 1 } { print }' \
  'REJECT: line 3: DCQCN transition result mismatch'
mutate_case "timer-seen-result-mutation" "$anchor" \
  'NR == 4 { $8 = 1 } { print }' \
  'REJECT: line 4: DCQCN transition result mismatch'
mutate_case "timer-quiet-result-mutation" "$anchor" \
  'NR == 5 { $8 = 0 } { print }' \
  'REJECT: line 5: DCQCN transition result mismatch'
mutate_case "fast-recovery-result-mutation" "$stages" \
  'NR == 5 { $8 = 0 } { print }' \
  'REJECT: line 5: DCQCN transition result mismatch'
mutate_case "additive-result-mutation" "$stages" \
  'NR == 10 { $8 = 0 } { print }' \
  'REJECT: line 10: DCQCN transition result mismatch'
mutate_case "hyper-result-mutation" "$stages" \
  'NR == 15 { $8 = 0 } { print }' \
  'REJECT: line 15: DCQCN transition result mismatch'
mutate_case "byte-trigger-result-mutation" "$stages" \
  'NR == 17 { $8 = 0 } { print }' \
  'REJECT: line 17: DCQCN transition result mismatch'
mutate_case "minimum-clamp-mutation" "$minimum" \
  'NR == 2 { $30 = 1001 } { print }' \
  'REJECT: line 2: DCQCN after-state mismatch'
mutate_case "maximum-clamp-mutation" "$stages" \
  'NR == 17 { $31 = 19999 } { print }' \
  'REJECT: line 17: DCQCN after-state mismatch'
mutate_case "duplicate-key" "$anchor" \
  'NR == 3 { $1 = 0; $2 = 0; $3 = 9; $4 = 0 } { print }' \
  'REJECT: line 3: duplicate or backward canonical event key'
mutate_case "backward-key" "$anchor" \
  'NR == 3 { $1 = 0; $2 = 0; $3 = 8 } { print }' \
  'REJECT: line 3: duplicate or backward canonical event key'
mutate_case "config-splice" "$anchor" \
  'NR == 5 { $10 = 8001 } { print }' \
  'REJECT: line 5: DCQCN config discontinuity for source (node_id=1, flow_id=7)'
mutate_case "state-splice" "$anchor" \
  'NR == 5 { $20 = 250000001 } { print }' \
  'REJECT: line 5: DCQCN state discontinuity for source (node_id=1, flow_id=7)'
mutate_case "initial-state-splice" "$minimum" \
  'NR == 2 { $25 = "fast_recovery" } { print }' \
  'REJECT: line 2: DCQCN first state is not initial for source (node_id=1, flow_id=7)'
mutate_case "u64-parser-bound" "$minimum" \
  'NR == 2 { $3 = "18446744073709551616" } { print }' \
  "REJECT: line 2: value exceeds u64: '18446744073709551616'"
mutate_case "u64-byte-counter-overflow" "$stages" \
  'NR == 17 { $9 = "18446744073709551615" } { print }' \
  'REJECT: line 17: DCQCN byte counter exceeds u64'
mutate_case "invalid-ppb-config" "$minimum" \
  'NR == 2 { $15 = 1000000001 } { print }' \
  'REJECT: line 2: invalid DCQCN configuration'
mutate_case "typed-cnp-phase" "$minimum" \
  'NR == 2 { $2 = 3 } { print }' \
  'REJECT: line 2: DCQCN CNP arrival must have phase 0'
mutate_case "invalid-hyper-stage-counter" "$stages" \
  'NR == 17 { $35 = 1 } { print }' \
  'REJECT: line 17: invalid DCQCN after-state'

echo "P10c exact-integer DCQCN campaign checks: $checked"
exit "$failures"
