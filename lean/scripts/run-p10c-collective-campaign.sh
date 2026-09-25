#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
lean_dir="$(cd "$script_dir/.." && pwd)"

cd "$lean_dir"
lake build p10c_collective_check

checker="$lean_dir/.lake/build/bin/p10c_collective_check"
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

for csv in "$fixture_dir"/collective_*_executor_accept.csv; do
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
  awk -F, -v OFS=, '
    NR == 1 {
      for (i = 1; i <= NF; i++) column[$i] = i
      print
      next
    }
  '"$program"'
    { print }
  ' "$source" > "$campaign_case"
  check_case "$label" "$campaign_case" 1 "$expected_output"
}

allgather="$fixture_dir/collective_allgather_executor_accept.csv"
partial="$fixture_dir/collective_allgather_partial_zero_executor_accept.csv"
allreduce="$fixture_dir/collective_ring_allreduce_executor_accept.csv"

mutate_case "local-cause-predecessor" "$allgather" \
  'NR == 2 { $column["cause_flow_id"] = 99 }' \
  'REJECT: line 2: local completion cause does not match the local predecessor'
mutate_case "local-completion-with-arrival-bytes" "$allgather" \
  'NR == 2 { $column["arrival_bytes"] = 1 }' \
  'REJECT: line 2: local completion must not carry arrival bytes'
mutate_case "local-completion-no-flip" "$partial" \
  'NR == 2 { $column["after_local_complete"] = 0 }' \
  'REJECT: line 2: local completion must change the local prerequisite from incomplete to complete'
mutate_case "local-completion-changes-inbound" "$partial" \
  'NR == 2 { $column["after_inbound_complete"] = 0; $column["after_inbound_bytes"] = 1 }' \
  'REJECT: line 2: local completion changed inbound prerequisite state'
mutate_case "inbound-cause-predecessor" "$allgather" \
  'NR == 6 { $column["cause_flow_id"] = 99 }' \
  'REJECT: line 6: inbound arrival cause does not match the inbound predecessor'
mutate_case "inbound-packet-size" "$allgather" \
  'NR == 6 { $column["arrival_bytes"] = 1; $column["after_inbound_complete"] = 0; $column["after_inbound_bytes"] = 1; $column["activated"] = 0; $column["after_packets_emitted"] = 0; $column["after_bytes_emitted"] = 0; $column["after_status"] = "blocked"; $column["after_next_time_ns"] = 0 }' \
  'REJECT: line 6: inbound arrival size does not match the next predecessor packet'
mutate_case "inbound-byte-total" "$allgather" \
  'NR == 6 { $column["after_inbound_complete"] = 0; $column["after_inbound_bytes"] = 1; $column["activated"] = 0; $column["after_packets_emitted"] = 0; $column["after_bytes_emitted"] = 0; $column["after_status"] = "blocked"; $column["after_next_time_ns"] = 0 }' \
  'REJECT: line 6: inbound arrival byte total mismatch'
mutate_case "inbound-changes-local" "$allgather" \
  'NR == 6 { $column["after_local_complete"] = 0 }' \
  'REJECT: line 6: inbound arrival changed the local prerequisite'
mutate_case "stage-state-discontinuity" "$allgather" \
  'NR == 13 { $column["before_inbound_bytes"] = 2 }' \
  'REJECT: line 13: collective stage before-state does not continue the prior after-state'
mutate_case "first-stage-state" "$allgather" \
  'NR == 2 { $column["before_local_complete"] = 1 }' \
  'REJECT: line 2: first local prerequisite state does not match the predecessor chunk'
mutate_case "missing-activated-bit" "$allgather" \
  'NR == 6 { $column["activated"] = 0 }' \
  'REJECT: line 6: collective activated bit disagrees with prerequisite state'
mutate_case "premature-activated-bit" "$allgather" \
  'NR == 12 { $column["activated"] = 1 } NR == 13 { $column["activated"] = 0 }' \
  'REJECT: line 12: collective activated bit disagrees with prerequisite state'
mutate_case "nonactivated-emission-counter" "$allgather" \
  'NR == 2 { $column["after_packets_emitted"] = 1 }' \
  'REJECT: line 2: nonactivated collective stage has emitted counters'
mutate_case "nonactivated-status" "$allgather" \
  'NR == 2 { $column["after_status"] = "scheduled" }' \
  'REJECT: line 2: nonactivated collective status or deadline mismatch'
mutate_case "zero-stage-status" "$partial" \
  'NR == 2 { $column["after_status"] = "blocked" }' \
  'REJECT: line 2: nonactivated collective status or deadline mismatch'
mutate_case "zero-stage-activation" "$partial" \
  'NR == 2 { $column["activated"] = 1 }' \
  'REJECT: line 2: collective activated bit disagrees with prerequisite state'
mutate_case "partition-offset" "$allgather" \
  '$column["flow_id"] == 1 { $column["chunk_offset_bytes"] = 5 }' \
  'REJECT: line 2: collective chunk does not match EqualRemainderLast'
mutate_case "partition-last-remainder" "$allgather" \
  '$column["flow_id"] == 1 { $column["chunk_bytes"] = 3 }' \
  'REJECT: line 2: collective chunk does not match EqualRemainderLast'
mutate_case "ring-allreduce-allgather-owner-offset" "$allreduce" \
  '$column["flow_id"] == 12 { $column["chunk_offset_bytes"] = 0 }' \
  'REJECT: line 16: collective chunk does not match EqualRemainderLast'
mutate_case "first-packet-count" "$allgather" \
  'NR == 6 { $column["after_packets_emitted"] = 2 }' \
  'REJECT: line 6: first collective packet counters mismatch'
mutate_case "first-packet-bytes" "$allgather" \
  'NR == 13 { $column["after_bytes_emitted"] = 2 }' \
  'REJECT: line 13: first collective packet counters mismatch'
mutate_case "post-activation-status" "$allgather" \
  'NR == 13 { $column["after_status"] = "finished" }' \
  'REJECT: line 13: post-activation status or deadline mismatch'
mutate_case "post-activation-deadline" "$allgather" \
  'NR == 13 { $column["after_next_time_ns"] = 15 }' \
  'REJECT: line 13: post-activation status or deadline mismatch'
mutate_case "deadline-past-stop-status" "$allgather" \
  'NR > 1 { $column["stop_time_ns"] = 13 }' \
  'REJECT: line 13: post-activation status or deadline mismatch'
mutate_case "duplicate-key-and-ordinal" "$allgather" \
  'NR == 3 { $column["event_origin_node"] = 0 }' \
  'REJECT: line 2: duplicate canonical event key and activation ordinal'
mutate_case "first-ordinal-not-zero" "$allgather" \
  'NR == 2 { $column["ordinal"] = 1 }' \
  'REJECT: line 2: first activation ordinal for an event key must be zero'
mutate_case "same-key-ordinal-gap" "$allgather" \
  'NR == 3 { $column["event_origin_node"] = 0; $column["ordinal"] = 2 }' \
  'REJECT: line 3: activation ordinals for an event key must be contiguous'
mutate_case "deleted-entire-stage" "$allgather" \
  '$column["flow_id"] == 1 { next }' \
  'REJECT: line 2: incomplete collective progress coverage for collective_id=0: expected 8, found 7'
mutate_case "deleted-progress-prefix" "$allgather" \
  'NR == 12 { next }' \
  'REJECT: line 12: collective stage before-state does not continue the prior after-state'
mutate_case "deleted-local-progress" "$allgather" \
  'NR == 2 { next }' \
  'REJECT: line 11: first local prerequisite state does not match the predecessor chunk'
mutate_case "deleted-final-progress" "$allgather" \
  'NR == 13 { next }' \
  'REJECT: line 2: collective stage final prerequisite state is incomplete'
mutate_case "deleted-zero-chunk-progress" "$partial" \
  'NR == 2 { next }' \
  'REJECT: line 2: incomplete collective progress coverage for collective_id=0: expected 4, found 3'
mutate_case "duplicate-stage-activation" "$allgather" \
  'NR == 12 { $column["activated"] = 1 }' \
  'REJECT: line 13: collective stage activated more than once'
mutate_case "collective-config-discontinuity" "$allgather" \
  'NR == 3 { $column["packet_size_bytes"] = 4 }' \
  'REJECT: line 3: collective configuration discontinuity for collective_id=0'
mutate_case "paired-inbound-predecessor-renaming" "$partial" \
  'NR == 3 { $column["cause_flow_id"] = 99; $column["inbound_predecessor_flow_id"] = 99 }' \
  'REJECT: line 3: inbound predecessor identity does not match the stage recurrence'
mutate_case "local-predecessor-recurrence" "$allgather" \
  '$column["flow_id"] == 5 { $column["local_predecessor_flow_id"] = 99; if ($column["cause"] == "local_completion") $column["cause_flow_id"] = 99 }' \
  'REJECT: line 7: local predecessor identity does not match the stage recurrence'
mutate_case "rank-to-node-mapping" "$allgather" \
  'NR == 3 { $column["node_id"] = 0 }' \
  'REJECT: line 3: collective rank-to-node mapping is inconsistent'
mutate_case "u64-parser-bound" "$allgather" \
  'NR == 2 { $column["time_ns"] = "18446744073709551616" }' \
  "REJECT: line 2: value exceeds u64: '18446744073709551616'"
mutate_case "u64-next-deadline-overflow" "$allgather" \
  'NR > 1 { $column["stop_time_ns"] = "18446744073709551615" } NR == 13 { $column["time_ns"] = "18446744073709551615"; $column["after_next_time_ns"] = "18446744073709551615" }' \
  'REJECT: line 13: next collective emission deadline exceeds u64'
mutate_case "progress-after-stop" "$allgather" \
  'NR > 1 { $column["stop_time_ns"] = 7 }' \
  'REJECT: line 6: collective progress occurs after the simulation stop time'
mutate_case "allgather-phase-legality" "$allgather" \
  '$column["flow_id"] == 1 { $column["collective_phase"] = "reduce_scatter" }' \
  'REJECT: line 2: collective algorithm, phase, rank, or step is illegal'
mutate_case "ring-total-below-group" "$allreduce" \
  'NR > 1 { $column["declared_total_bytes"] = 1 }' \
  'REJECT: line 2: RingAllReduce declared total must cover every rank'

echo "P10c exact-integer collective campaign checks: $checked"
exit "$failures"
