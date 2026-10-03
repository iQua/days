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

# P16: the Mellanox-form controller (schema days-gpu/plans/briefs/p16/dcqcn-schema.md). Columns:
# 1 time_ns, 2 event_phase, 3 origin node, 6 flow_id, 7 kind, 8 bound_ns, 9 frozen,
# 10-12 counts, 18 g_q63, 19 alpha_interval_ns, 24-34 before state (24 alpha_q63, 25 current,
# 26 target), 35-45 after state (35 alpha_q63, 36 current, 38 next_alpha, 39 next_decrease,
# 41 stage, 44 decrease_pending).
qp="$fixture_dir/dcqcn_qp_timeout_accept.csv"
ticks="$fixture_dir/dcqcn_t26_ticks_accept.csv"
coincident="$fixture_dir/dcqcn_coincident_prefix_accept.csv"

# awk computes in doubles, so Q63 values (above 2^53) are mutated as strings: the last digit
# moves by one modulo 10.
mutate_case "alpha-last-digit" "$qp" \
  'NR == 4 { n = length($35); $35 = substr($35, 1, n - 1) ((substr($35, n) + 1) % 10) } { print }' \
  'REJECT: line 4: DCQCN after-state mismatch'
mutate_case "missing-alpha-tick" "$qp" \
  'NR == 4 { $38 = $38 - $19 } { print }' \
  'REJECT: line 4: DCQCN after-state mismatch'
mutate_case "decrease-grid-without-one-ns" "$qp" \
  'NR == 2 { $39 = $39 - 1 } { print }' \
  'REJECT: line 2: DCQCN after-state mismatch'
mutate_case "skipped-cut" "$qp" \
  'NR == 3 { $36 = $25; $44 = 1 } { print }' \
  'REJECT: line 3: DCQCN after-state mismatch'
mutate_case "increase-average-off-by-one" "$qp" \
  'NR == 6 { $36 = $36 + 1 } { print }' \
  'REJECT: line 6: DCQCN after-state mismatch'
mutate_case "tick-reads-stale-rate" "$ticks" \
  'NR == 8 { $36 = $25 } { print }' \
  'REJECT: line 8: DCQCN after-state mismatch'
mutate_case "counts-mismatch" "$coincident" \
  'NR == 6 { $11 = $11 + 1 } { print }' \
  'REJECT: line 6: DCQCN transition counts mismatch'
mutate_case "wrong-bound" "$qp" \
  'NR == 3 { $8 = $8 + 1 } { print }' \
  'REJECT: line 3: DCQCN bound is not the event time (arrival) or the next nanosecond (timer)'
mutate_case "feedback-on-frozen" "$qp" \
  'NR == 4 { $9 = 1 } { print }' \
  'REJECT: line 4: DCQCN feedback on a frozen controller'
mutate_case "echo-after-completion" "$qp" \
  '{ print } END { $1 = $1 + 1; $2 = 0; $7 = "feedback"; $8 = $1; $9 = 0; for (i = 24; i <= 34; i++) $i = $(i + 11); print }' \
  "REJECT: line $(( $(wc -l < "$qp") + 1 )): DCQCN row after the freeze of source (node_id=2, flow_id=1)"
mutate_case "feedback-phase" "$qp" \
  'NR == 4 { $2 = 1; $8 = $1 + 1 } { print }' \
  'REJECT: line 4: DCQCN feedback must be an arrival (phase 0)'
mutate_case "tick-phase" "$ticks" \
  'NR == 4 { $2 = 0; $8 = $1 } { print }' \
  'REJECT: line 4: DCQCN pacing tick must have phase 1'
mutate_case "advance-without-instant" "$ticks" \
  'NR == 2 { $7 = "advance" } { print }' \
  'REJECT: line 2: DCQCN advance row applied no rate instant and froze nothing'
mutate_case "duplicate-key-flow" "$coincident" \
  'NR == 2 { print } { print }' \
  'REJECT: line 3: duplicate or backward (event key, flow)'
mutate_case "backward-key" "$qp" \
  'NR == 3 { $1 = 600000 } { print }' \
  'REJECT: line 3: duplicate or backward (event key, flow)'
mutate_case "config-splice" "$qp" \
  'NR == 5 { $19 = $19 + 1 } { print }' \
  'REJECT: line 5: DCQCN config discontinuity for source (node_id=2, flow_id=1)'
mutate_case "state-splice" "$qp" \
  'NR == 5 { n = length($24); $24 = substr($24, 1, n - 1) ((substr($24, n) + 1) % 10) } { print }' \
  'REJECT: line 5: DCQCN state discontinuity for source (node_id=2, flow_id=1)'
mutate_case "initial-not-pristine" "$ticks" \
  'NR == 2 { $24 = 1 } { print }' \
  'REJECT: line 2: DCQCN first state is not pristine for source (node_id=0, flow_id=0)'
mutate_case "u64-parser-bound" "$ticks" \
  'NR == 2 { $3 = "18446744073709551616" } { print }' \
  "REJECT: line 2: value exceeds u64: '18446744073709551616'"
mutate_case "invalid-g" "$ticks" \
  'NR == 2 { $18 = "9223372036854775809" } { print }' \
  'REJECT: line 2: invalid DCQCN configuration'
mutate_case "stage-above-saturation" "$qp" \
  'NR == 8 { $41 = 3 } { print }' \
  'REJECT: line 8: invalid DCQCN after-state'
mutate_case "rate-below-floor" "$qp" \
  'NR == 3 { $36 = 1 } { print }' \
  'REJECT: line 3: invalid DCQCN after-state'

echo "P10c exact-integer DCQCN (Mellanox form, P16) campaign checks: $checked"
exit "$failures"
