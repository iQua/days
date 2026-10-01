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
allreduce="$fixture_dir/collective_ring_allreduce_executor_accept.csv"
lossy="$fixture_dir/collective_ring_allreduce_lossy_executor_accept.csv"
chain="$fixture_dir/collective_compute_chain_executor_accept.csv"

# TCP stage progress: local completion (last byte acknowledged) and inbound delivery (in-order
# frontier advance).
mutate_case "local-cause-predecessor" "$allgather" \
  'NR == 7 { $column["cause_flow_id"] = 99 }' \
  'REJECT: line 7: local completion cause does not match the local predecessor'
mutate_case "local-completion-with-arrival-bytes" "$allgather" \
  'NR == 7 { $column["arrival_bytes"] = 1 }' \
  'REJECT: line 7: local completion must not carry arrival bytes'
mutate_case "local-completion-no-flip" "$allgather" \
  'NR == 7 { $column["after_local_complete"] = 0 }' \
  'REJECT: line 7: local completion must change the local prerequisite from incomplete to complete'
mutate_case "local-completion-changes-inbound" "$allgather" \
  'NR == 7 { $column["after_inbound_complete"] = 0; $column["after_inbound_bytes"] = 3 }' \
  'REJECT: line 7: local completion changed inbound prerequisite state'
mutate_case "inbound-cause-predecessor" "$allgather" \
  'NR == 2 { $column["cause_flow_id"] = 99 }' \
  'REJECT: line 2: inbound arrival cause does not match the inbound predecessor'
mutate_case "inbound-frontier-overshoot" "$allgather" \
  'NR == 6 { $column["arrival_bytes"] = 2; $column["after_inbound_bytes"] = 5; $column["after_inbound_complete"] = 0 } NR == 7 { $column["before_inbound_bytes"] = 5; $column["before_inbound_complete"] = 0; $column["after_inbound_bytes"] = 5; $column["after_inbound_complete"] = 0 }' \
  'REJECT: line 6: inbound frontier advance exceeds the undelivered predecessor bytes'
mutate_case "inbound-byte-total" "$allgather" \
  'NR == 2 { $column["after_inbound_bytes"] = 1; $column["after_inbound_complete"] = 0 } NR == 8 { $column["before_inbound_bytes"] = 1; $column["before_inbound_complete"] = 0; $column["after_inbound_bytes"] = 1; $column["after_inbound_complete"] = 0; $column["activated"] = 0; $column["after_packets_emitted"] = 0; $column["after_bytes_emitted"] = 0 }' \
  'REJECT: line 2: inbound arrival byte total mismatch'
mutate_case "inbound-changes-local" "$allgather" \
  'NR == 2 { $column["after_local_complete"] = 1 } NR == 8 { $column["before_local_complete"] = 1 }' \
  'REJECT: line 2: inbound arrival changed the local prerequisite'
mutate_case "inbound-flag-without-bytes" "$allgather" \
  'NR == 5 { $column["after_inbound_complete"] = 1 } NR == 6 { $column["before_inbound_complete"] = 1 }' \
  'REJECT: line 5: after inbound completion flag disagrees with the delivered total'
mutate_case "stage-state-discontinuity" "$allgather" \
  'NR == 6 { $column["before_inbound_bytes"] = 2 }' \
  'REJECT: line 6: collective stage before-state does not continue the prior after-state'
mutate_case "first-local-state" "$allgather" \
  'NR == 2 { $column["before_local_complete"] = 1 }' \
  'REJECT: line 2: first local prerequisite state is not initial'
mutate_case "first-root-inbound-state" "$chain" \
  'NR == 2 { $column["before_inbound_complete"] = 0 }' \
  'REJECT: line 2: first inbound prerequisite state is not initial'
mutate_case "missing-activated-bit" "$allgather" \
  'NR == 7 { $column["activated"] = 0 }' \
  'REJECT: line 7: collective activated bit disagrees with prerequisite state'
mutate_case "premature-activated-bit" "$allgather" \
  'NR == 6 { $column["activated"] = 1 } NR == 7 { $column["activated"] = 0 }' \
  'REJECT: line 6: collective activated bit disagrees with prerequisite state'
mutate_case "nonactivated-emission-counter" "$allgather" \
  'NR == 2 { $column["after_packets_emitted"] = 1 }' \
  'REJECT: line 2: nonactivated collective stage has emitted counters'
mutate_case "nonactivated-status" "$allgather" \
  'NR == 2 { $column["after_status"] = "scheduled" }' \
  'REJECT: line 2: nonactivated collective status or deadline mismatch'

# Partition, dimensions, and TCP stage shape.
mutate_case "partition-offset" "$allgather" \
  '$column["flow_id"] == 1 { $column["chunk_offset_bytes"] = 5 }' \
  'REJECT: line 5: collective chunk does not match EqualRemainderLast'
mutate_case "partition-last-remainder" "$allgather" \
  '$column["flow_id"] == 1 { $column["chunk_bytes"] = 3 }' \
  'REJECT: line 5: collective chunk does not match EqualRemainderLast'
mutate_case "ring-allreduce-allgather-owner-offset" "$allreduce" \
  '$column["flow_id"] == 12 { $column["chunk_offset_bytes"] = 0 }' \
  'REJECT: line 23: collective chunk does not match EqualRemainderLast'
mutate_case "allgather-phase-legality" "$allgather" \
  '$column["flow_id"] == 1 { $column["collective_phase"] = "reduce_scatter" }' \
  'REJECT: line 5: collective algorithm, phase, rank, or step is illegal'
mutate_case "total-below-group" "$allreduce" \
  'NR > 1 { $column["declared_total_bytes"] = 3 }' \
  'REJECT: line 2: TCP collective declared total must cover every rank'
mutate_case "tcp-pacing-interval" "$allgather" \
  'NR > 1 { $column["interval_ns"] = 1 }' \
  'REJECT: line 2: TCP collective stage requires a positive MSS and no pacing interval or duration'
mutate_case "tcp-missing-algorithm" "$allgather" \
  'NR == 2 { $column["algorithm"] = "" }' \
  'REJECT: line 3: collective configuration discontinuity for collective_id=0'

# Release: a TCP stage fills its first window from sequence zero; roots appear only when a
# compute stage gates them, and only a compute timer is a phase-1 cause.
mutate_case "first-window-count" "$allgather" \
  'NR == 7 { $column["after_packets_emitted"] = 3 }' \
  'REJECT: line 7: first TCP window counters mismatch'
mutate_case "first-window-bytes" "$allgather" \
  'NR == 8 { $column["after_bytes_emitted"] = 1 }' \
  'REJECT: line 8: first TCP window counters mismatch'
mutate_case "post-activation-status" "$allgather" \
  'NR == 8 { $column["after_status"] = "finished" }' \
  'REJECT: line 8: post-activation status or deadline mismatch'
mutate_case "post-activation-deadline" "$allgather" \
  'NR == 8 { $column["after_next_time_ns"] = 15 }' \
  'REJECT: line 8: post-activation status or deadline mismatch'
mutate_case "data-arrival-phase" "$allgather" \
  'NR == 2 { $column["event_phase"] = 1 }' \
  'REJECT: line 2: collective progress event phase disagrees with its cause'
mutate_case "root-gate-phase" "$chain" \
  'NR == 2 { $column["event_phase"] = 0 }' \
  'REJECT: line 2: collective progress event phase disagrees with its cause'
mutate_case "ungated-root" "$chain" \
  'NR == 2 { $column["local_predecessor_flow_id"] = "" }' \
  'REJECT: line 2: a root stage is logged only when a compute stage gates it'
mutate_case "root-gate-inside-collective" "$chain" \
  'NR == 2 { $column["local_predecessor_flow_id"] = 6; $column["cause_flow_id"] = 6 }' \
  'REJECT: line 2: a root stage'"'"'s gate must be a compute stage outside its collective'

# Canonical order, ordinals, and certificate structure.
mutate_case "duplicate-key-and-ordinal" "$allgather" \
  'NR == 2 { node = $column["event_origin_node"]; sequence = $column["event_origin_sequence"] } NR == 3 { $column["event_origin_node"] = node; $column["event_origin_sequence"] = sequence }' \
  'REJECT: line 2: duplicate canonical event key and activation ordinal'
mutate_case "first-ordinal-not-zero" "$allgather" \
  'NR == 2 { $column["ordinal"] = 1 }' \
  'REJECT: line 2: first activation ordinal for an event key must be zero'
mutate_case "same-key-ordinal-gap" "$allgather" \
  'NR == 2 { node = $column["event_origin_node"]; sequence = $column["event_origin_sequence"] } NR == 3 { $column["event_origin_node"] = node; $column["event_origin_sequence"] = sequence; $column["ordinal"] = 2 }' \
  'REJECT: line 3: activation ordinals for an event key must be contiguous'
mutate_case "deleted-entire-stage" "$allgather" \
  '$column["flow_id"] == 1 { next }' \
  'REJECT: line 2: incomplete collective progress coverage for collective_id=0: expected 8, found 7'
mutate_case "deleted-progress-prefix" "$allgather" \
  'NR == 5 { next }' \
  'REJECT: line 5: first inbound prerequisite state is not initial'
mutate_case "deleted-final-progress" "$allgather" \
  'NR == 7 { next }' \
  'REJECT: line 2: incomplete collective activation coverage for collective_id=0: expected 8, found 7'
# Row 47 closes a retransmission hole: the frontier jumps 2500 bytes at once, exactly the bytes
# still undelivered. One more byte overshoots the chunk.
mutate_case "lossy-frontier-jump-overshoot" "$lossy" \
  'NR == 47 { $column["arrival_bytes"] = 2501; $column["after_inbound_bytes"] = $column["before_inbound_bytes"] + 2501; $column["after_inbound_complete"] = 0; $column["activated"] = 0; $column["after_packets_emitted"] = 0; $column["after_bytes_emitted"] = 0 }' \
  'REJECT: line 47: inbound frontier advance exceeds the undelivered predecessor bytes'
mutate_case "duplicate-stage-activation" "$allgather" \
  'NR == 6 { $column["activated"] = 1 }' \
  'REJECT: line 7: collective stage activated more than once'
mutate_case "collective-config-discontinuity" "$allgather" \
  'NR == 3 { $column["packet_size_bytes"] = 4 }' \
  'REJECT: line 3: collective configuration discontinuity for collective_id=0'
mutate_case "inbound-predecessor-recurrence" "$allgather" \
  '$column["flow_id"] == 5 { $column["inbound_predecessor_flow_id"] = 99; if ($column["cause"] == "inbound_arrival") $column["cause_flow_id"] = 99 }' \
  'REJECT: line 11: inbound predecessor identity does not match the stage recurrence'
mutate_case "local-predecessor-recurrence" "$allgather" \
  '$column["flow_id"] == 5 { $column["local_predecessor_flow_id"] = 99; if ($column["cause"] == "local_completion") $column["cause_flow_id"] = 99 }' \
  'REJECT: line 11: local predecessor identity does not match the stage recurrence'
mutate_case "rank-to-node-mapping" "$allgather" \
  'NR == 3 { $column["node_id"] = 1 }' \
  'REJECT: line 3: collective rank-to-node mapping is inconsistent'
mutate_case "u64-parser-bound" "$allgather" \
  'NR == 2 { $column["time_ns"] = "18446744073709551616" }' \
  "REJECT: line 2: value exceeds u64: '18446744073709551616'"
mutate_case "progress-after-stop" "$allgather" \
  'NR > 1 { $column["stop_time_ns"] = 7 }' \
  'REJECT: line 2: collective progress occurs after the simulation stop time'
mutate_case "unknown-stage-kind" "$allgather" \
  'NR == 2 { $column["stage_kind"] = "packet_distribution" }' \
  "REJECT: line 2: invalid stage kind: 'packet_distribution'"

# Review F3: certified segments and completing signals.
# R2: rows 5 and 6 merged, so node 0's inbound completes at 12 ns with 4 bytes although the 12 ns
# segment is [0, 3); the replayed frontier is 3 (the 1-byte segment arrives at 13 ns).
mutate_case "inbound-early-completion" "$allgather" \
  'NR == 5 { $column["arrival_bytes"] = 4; $column["after_inbound_complete"] = 1; $column["after_inbound_bytes"] = 4 } NR == 6 { next }' \
  'REJECT: line 5: inbound progress does not match the receiver frontier replayed from the certified segments'
# R3: row 7's local completion moved from 168 ns to 14 ns, before the answered segment (sent at
# 0 ns) and its ACK could make the 168 ns unloaded round trip.
mutate_case "local-completion-before-ack-return" "$allgather" \
  'NR == 7 { $column["time_ns"] = 14 }' \
  'REJECT: line 7: local completion precedes the earliest return of the completing acknowledgment'
mutate_case "ack-number-short" "$allgather" \
  'NR == 7 { $column["ack_number"] = 1 }' \
  'REJECT: line 7: completing acknowledgment does not reach exactly the local predecessor'"'"'s byte total'
mutate_case "segment-past-chunk" "$allgather" \
  'NR == 2 { $column["segment_sequence"] = 1 }' \
  'REJECT: line 2: inbound segment is empty or extends past the predecessor chunk'
mutate_case "inbound-row-local-fields" "$allgather" \
  'NR == 2 { $column["ack_number"] = 2 }' \
  'REJECT: line 2: inbound row carries local completion fields'
# Row 30 certifies an out-of-order segment of flow 4; without it the hole fill at row 47 (line 46
# after the deletion) cannot reach the frontier it claims.
mutate_case "deleted-out-of-order-segment" "$lossy" \
  'NR == 30 { next }' \
  'REJECT: line 46: inbound progress does not match the receiver frontier replayed from the certified segments'
mutate_case "compute-arm-time" "$chain" \
  'NR == 41 { $column["cause_origin_ns"] = 15641 }' \
  'REJECT: line 41: compute timer completion does not occur at arm time plus duration'
mutate_case "root-gate-arm-time" "$chain" \
  'NR == 2 { $column["cause_origin_ns"] = 1; $column["cause_delay_ns"] = 4999 }' \
  'REJECT: line 2: an unlogged root compute stage is armed at time zero'

# Compute (delay-only) stages: timer-only rows whose release sets an exact deadline.
mutate_case "compute-carries-collective-fields" "$chain" \
  '$column["flow_id"] == 12 { $column["step"] = 1 }' \
  'REJECT: line 32: compute stage row carries collective fields'
mutate_case "compute-zero-duration" "$chain" \
  '$column["stage_kind"] == "compute" && $column["collective_id"] == 0 { $column["duration_ns"] = 0 }' \
  'REJECT: line 32: compute stage duration must be positive'
mutate_case "compute-rank-outside-group" "$chain" \
  '$column["flow_id"] == 12 { $column["rank"] = 3 }' \
  'REJECT: line 32: compute stage rank is outside its group'
mutate_case "compute-inbound-bytes" "$chain" \
  '$column["flow_id"] == 12 { $column["inbound_predecessor_bytes"] = 0 }' \
  'REJECT: line 32: compute inbound predecessor and byte count disagree'
mutate_case "compute-timer-deadline" "$chain" \
  'NR == 38 { $column["after_next_time_ns"] = 22641 }' \
  'REJECT: line 38: compute timer status or deadline mismatch'
mutate_case "compute-timer-past-stop" "$chain" \
  'NR > 1 { $column["stop_time_ns"] = 22000 }' \
  'REJECT: line 38: compute timer status or deadline mismatch'
mutate_case "compute-emitted-counters" "$chain" \
  'NR == 38 { $column["after_packets_emitted"] = 1 }' \
  'REJECT: line 38: compute stage has emitted counters'
mutate_case "compute-timer-phase" "$chain" \
  'NR == 41 { $column["event_phase"] = 0 }' \
  'REJECT: line 41: collective progress event phase disagrees with its cause'
mutate_case "compute-local-predecessor-rank" "$chain" \
  '$column["flow_id"] == 18 { $column["local_predecessor_flow_id"] = 13; $column["cause_flow_id"] = 13 }' \
  'REJECT: line 41: compute local predecessor is not the same-rank stage of a compute group'
mutate_case "compute-inbound-not-final" "$chain" \
  '$column["flow_id"] == 12 { $column["inbound_predecessor_flow_id"] = 6; if ($column["cause"] == "inbound_arrival") $column["cause_flow_id"] = 6 }' \
  'REJECT: line 32: compute inbound predecessor is not the previous rank'"'"'s final collective stage'
# Review F2 (R1b): the optimizer releases 640 ns after backward's release instead of at its
# 7000 ns timer deadline (22640); the release rows' own deadlines move consistently.
mutate_case "compute-completion-before-timer" "$chain" \
  'NR >= 41 && NR <= 43 { $column["time_ns"] = 16000; $column["after_next_time_ns"] = 19000 }' \
  'REJECT: line 41: compute local completion does not occur at its predecessor'"'"'s timer deadline'
mutate_case "compute-group-coverage" "$chain" \
  '$column["flow_id"] == 20 { next }' \
  'REJECT: line 41: incomplete collective progress coverage for collective_id=2: expected 3, found 2'

echo "P10c exact-integer collective campaign checks: $checked"
exit "$failures"
