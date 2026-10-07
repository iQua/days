#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
lean_dir="$(cd "$script_dir/.." && pwd)"

cd "$lean_dir"
lake build p10c_collective_check

# P10C_COLLECTIVE_CHECKER substitutes a drop-in checker (run-p10c-collective-differential.sh).
checker="${P10C_COLLECTIVE_CHECKER:-$lean_dir/.lake/build/bin/p10c_collective_check}"
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

# Hand fixtures for the RoCE rules (schema Amendment 4): one accept trace and its reject twins.
for csv in "$fixture_dir"/collective_*_hand_accept.csv "$fixture_dir"/collective_*_hand_*_reject.csv; do
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
# A root with nothing to wait for, logged with its local prerequisite complete from the start.
mutate_case "ungated-root" "$chain" \
  'NR == 2 { $column["local_predecessors"] = ""; $column["local_required"] = 0; $column["before_local_complete"] = 1 }' \
  'REJECT: line 2: a root stage is logged only when a compute stage gates it'
# The gate is flow 6, a 1,000-byte stage of the root's own collective, completed by its ACK.
mutate_case "root-gate-inside-collective" "$chain" \
  'NR == 2 { $column["local_predecessors"] = 6; $column["cause_flow_id"] = 6; $column["cause_total_bytes"] = 1000; $column["cause_kind"] = "tcp"; $column["event_phase"] = 0 }' \
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
  '$column["flow_id"] == 5 { $column["inbound_predecessors"] = 99; if ($column["cause"] == "inbound_arrival") $column["cause_flow_id"] = 99 }' \
  'REJECT: line 11: inbound predecessor identity does not match the stage recurrence'
mutate_case "local-predecessor-recurrence" "$allgather" \
  '$column["flow_id"] == 5 { $column["local_predecessors"] = 99; if ($column["cause"] == "local_completion") $column["cause_flow_id"] = 99 }' \
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
  '$column["flow_id"] == 18 { $column["local_predecessors"] = 13; $column["cause_flow_id"] = 13 }' \
  'REJECT: line 41: compute local predecessor is not a compute stage on its host'
mutate_case "compute-inbound-not-final" "$chain" \
  '$column["flow_id"] == 12 { $column["inbound_predecessors"] = 6; if ($column["cause"] == "inbound_arrival") $column["cause_flow_id"] = 6 }' \
  'REJECT: line 32: compute inbound predecessor is not the previous rank'"'"'s final collective stage'
# Review F2 (R1b): the optimizer releases 640 ns after backward's release instead of at its
# 7000 ns timer deadline (22640); the release rows' own deadlines move consistently.
mutate_case "compute-completion-before-timer" "$chain" \
  'NR >= 41 && NR <= 43 { $column["time_ns"] = 16000; $column["after_next_time_ns"] = 19000 }' \
  'REJECT: line 41: compute local completion does not occur at its predecessor'"'"'s timer deadline'
mutate_case "compute-group-coverage" "$chain" \
  '$column["flow_id"] == 20 { next }' \
  'REJECT: line 41: incomplete collective progress coverage for collective_id=2: expected 3, found 2'

# RoCE stages (schema Amendment 4). The lossy ring certificate is R3's roce_ring_lossy at
# size = 40000 (9bc3c63): Go-back-N drops, NACKs and retransmissions inside stages.
roce_lossy="$fixture_dir/collective_roce_ring_lossy_executor_accept.csv"
roce_dag="$fixture_dir/collective_roce_compute_dag_executor_accept.csv"

# Flow 1 loses PSNs 5000-7000; PSNs 8000 and 9000 arrive out of order (rows 32, 35, advance 0)
# and are discarded; the retransmissions from 5000 complete the chunk at row 46. A receiver that
# buffered them, as TCP's does, would fill the hole at row 42 (PSN 7000: 7000 -> 10000) and log no
# later arrival. TCP's merging replay accepts this; the Go-back-N frontier does not jump.
mutate_case "roce-frontier-jump-out-of-order" "$roce_lossy" \
  'NR == 42 { $column["arrival_bytes"] = 3000; $column["after_inbound_bytes"] = 10000; $column["after_inbound_complete"] = 1 } NR == 44 || NR == 46 { next }' \
  "REJECT: line 42: inbound progress does not match the receiver's Go-back-N frontier"
# Row 76 is a duplicate (PSN 5000 below the frontier 6000). Counting it moves the frontier to 7000,
# so the next packet (PSN 6000, row 77) becomes a duplicate and the chain rejoins the trace.
mutate_case "roce-inbound-counts-duplicate" "$roce_lossy" \
  'NR == 76 { $column["arrival_bytes"] = 1000; $column["after_inbound_bytes"] = 7000 } NR == 77 { $column["before_inbound_bytes"] = 7000; $column["arrival_bytes"] = 0 }' \
  "REJECT: line 76: inbound progress does not match the receiver's Go-back-N frontier"
mutate_case "roce-packet-not-at-psn" "$roce_lossy" \
  'NR == 77 { $column["segment_bytes"] = 999 }' \
  "REJECT: line 77: inbound RoCE packet is not the predecessor queue pair's packet at its PSN"
# Row 34 releases flow 7: its first pacing tick is at the release instant, so nothing is sent yet.
mutate_case "roce-release-emitted-packet" "$roce_lossy" \
  'NR == 34 { $column["after_packets_emitted"] = 1; $column["after_bytes_emitted"] = 1000 }' \
  'REJECT: line 34: RoCE stage release has emitted counters: its first pacing tick is at the release instant'
mutate_case "roce-release-tcp-deadline" "$roce_lossy" \
  'NR == 34 { $column["after_next_time_ns"] = 0 }' \
  'REJECT: line 34: RoCE stage release status or first pacing tick mismatch'
mutate_case "roce-release-finished" "$roce_lossy" \
  'NR == 34 { $column["after_status"] = "finished" }' \
  'REJECT: line 34: RoCE stage release status or first pacing tick mismatch'
mutate_case "roce-zero-pacing-interval" "$roce_lossy" \
  'NR > 1 { $column["interval_ns"] = 0 }' \
  'REJECT: line 2: RoCE collective stage requires a positive MTU and pacing interval and no duration'
mutate_case "roce-missing-algorithm" "$roce_lossy" \
  'NR > 1 { $column["algorithm"] = "" }' \
  'REJECT: line 2: RoCE stage row requires an algorithm and a phase'
# Row 74's ACK returns exactly at origin + unloaded round trip (512288 ns); one nanosecond earlier
# is impossible.
mutate_case "roce-completion-before-round-trip" "$roce_lossy" \
  'NR == 74 { $column["time_ns"] = 512287 }' \
  'REJECT: line 74: local completion precedes the earliest return of the completing acknowledgment'
# The run's receiver log NACKs flow 9 once, acknowledging its frontier 5000 for the packet sent at
# 71000 ns. A NACK acknowledges the frontier below an out-of-order PSN, so it never reaches the
# chunk: row 50 (completed by flow 9) cannot be caused by it.
mutate_case "roce-completion-by-nack" "$roce_lossy" \
  'NR == 50 { $column["ack_number"] = 5000; $column["cause_origin_ns"] = 71000 }' \
  'REJECT: line 50: completing acknowledgment does not reach exactly the local predecessor'"'"'s byte total'
# A compute stage after the RoCE collective receives RoCE packets: its rows replay the Go-back-N
# frontier of its logged predecessor (row 63: flow 27 receives flow 20's PSN 0).
mutate_case "compute-after-roce-packet-not-at-psn" "$roce_dag" \
  'NR == 63 { $column["segment_bytes"] = 999 }' \
  "REJECT: line 63: inbound RoCE packet is not the predecessor queue pair's packet at its PSN"
mutate_case "compute-after-roce-out-of-order-advance" "$roce_dag" \
  'NR == 63 { $column["segment_sequence"] = 1000 }' \
  "REJECT: line 63: inbound progress does not match the receiver's Go-back-N frontier"
# Schema Amendment 5 (fix round 1, review M1): a compute stage names its RoCE inbound predecessor's
# MTU and pacing interval. roce_agc is configs/p15/roce_allgather_compute_lossy.toml with the ring
# at 40,000 B and the AllGather at 20,000 B: the AllGather's stages are unlogged roots. Flow 27
# receives flow 24's PSNs 5000-9000 out of order (rows 37-53), then the Go-back-N resend from 0;
# TCP's merge would jump from 4000 to 10000 at row 72 (PSN 4000) and log no later arrival.
roce_agc="$fixture_dir/collective_roce_allgather_compute_lossy_executor_accept.csv"
mutate_case "compute-after-unlogged-roce-hole-fill" "$roce_agc" \
  'NR == 72 { $column["arrival_bytes"] = 6000; $column["after_inbound_bytes"] = 10000; $column["after_inbound_complete"] = 1 } NR >= 73 && NR <= 77 { next }' \
  "REJECT: line 72: inbound progress does not match the receiver's Go-back-N frontier"
# Without the columns, the RoCE packets of the unlogged predecessor have no MTU to replay them
# with (the cause's carrier, `cause_kind`, says RoCE), so the first such packet is refused.
mutate_case "compute-after-unlogged-roce-zero-columns" "$roce_agc" \
  '$column["stage_kind"] == "compute" { $column["packet_size_bytes"] = 0; $column["interval_ns"] = 0 }' \
  "REJECT: line 10: inbound RoCE packet is not the predecessor queue pair's packet at its PSN"
mutate_case "compute-mtu-disagrees-with-logged-predecessor" "$roce_dag" \
  '$column["stage_kind"] == "compute" { $column["packet_size_bytes"] = 1500 }' \
  'REJECT: line 63: compute stage inbound transport columns disagree with its inbound predecessor'
mutate_case "compute-mtu-without-interval" "$roce_dag" \
  '$column["stage_kind"] == "compute" { $column["interval_ns"] = 0 }' \
  'REJECT: line 63: compute stage inbound transport columns must be both zero or both positive (Amendment 5)'
# P14's chain: the optimizer group follows the backward compute group and has no inbound stage.
mutate_case "compute-transport-without-inbound-predecessor" "$chain" \
  '$column["stage_kind"] == "compute" && $column["inbound_predecessors"] == "" { $column["packet_size_bytes"] = 1000; $column["interval_ns"] = 1000 }' \
  'REJECT: line 41: compute stage without an inbound predecessor carries inbound transport columns'
mutate_case "roce-stage-coverage" "$roce_lossy" \
  '$column["flow_id"] == 7 { next }' \
  'REJECT: line 2: incomplete collective progress coverage for collective_id=0: expected 20, found 19'

# P16 H1 (collops): the operations and counted joins. The certificates are the Scalar traces of
# tests/p16_collops.rs's fixtures (the_certificates_are_scalar_generated).
channels="$fixture_dir/collective_collops_ring_channels_2_executor_accept.csv"
a2a_tcp="$fixture_dir/collective_collops_a2a_uniform_tcp_executor_accept.csv"
a2a_roce="$fixture_dir/collective_collops_a2a_uniform_roce_executor_accept.csv"
seeded="$fixture_dir/collective_collops_a2a_seeded_roce_executor_accept.csv"
stream="$fixture_dir/collective_collops_data_stream_executor_accept.csv"
sendrecv="$fixture_dir/collective_collops_sendrecv_executor_accept.csv"
# Flow 12 is channel 1's root at rank 0: a UniformFloor message of floor(16003 / 4 / 2) = 2000 B.
mutate_case "channel-uniform-floor-size" "$channels" \
  '$column["flow_id"] == 12 { $column["chunk_bytes"] = 2001 }' \
  'REJECT: line 3: collective chunk does not match its chunk policy'
# Flow 4 (rank 1, channel 0, step 2) names rank 2's step-1 stage (flow 6) instead of rank 0's: rank
# 2 would then precede both rank 1 and rank 3 on channel 0.
mutate_case "channel-ring-not-one-ring" "$channels" \
  '$column["flow_id"] == 4 { $column["inbound_predecessors"] = 6; if ($column["cause"] == "inbound_arrival") $column["cause_flow_id"] = 6 }' \
  'REJECT: line 13: channel ring predecessor ranks are not one ring'
# Flow 0 is rank 0's send to rank 1: floor(8002 / 4) = 2000 B.
mutate_case "all-to-all-uniform-floor-size" "$a2a_tcp" \
  '$column["flow_id"] == 0 { $column["chunk_bytes"] = 2001 }' \
  'REJECT: line 2: collective chunk does not match its chunk policy'
# Flow 28 (rank 0's expert compute) joins its three sends (12, 13, 14) and three receives (17, 19,
# 21). Counting two local predecessors breaks the count against the list.
mutate_case "join-local-required" "$a2a_roce" \
  '$column["flow_id"] == 28 { $column["local_required"] = 2 }' \
  'REJECT: line 14: local completion flags disagree with the local completion counts'
# Rank 3's 2,000-byte send to rank 0 (flow 21) claims 2,500 bytes on every row it causes: the
# join's causes then total 6,500 bytes against its 6,000-byte requirement.
mutate_case "join-inbound-cause-totals" "$a2a_roce" \
  '$column["cause_flow_id"] == 21 && $column["cause"] == "inbound_arrival" { $column["cause_total_bytes"] = 2500 }' \
  'REJECT: line 14: a stage'"'"'s inbound predecessors do not deliver its inbound requirement'
mutate_case "cause-total-changes" "$a2a_roce" \
  '$column["flow_id"] == 28 && $column["cause_flow_id"] == 21 && ++seen == 2 { $column["cause_total_bytes"] = 2500 }' \
  'REJECT: line 26: cause byte total disagrees with an earlier row of the same cause'
# Flow 13 (rank 0 -> rank 2, 600 B) of the seeded dispatch is deleted, and the activation
# ordinals of its event keys closed up.
mutate_case "seeded-deleted-pair" "$seeded" \
  '{ key = $column["time_ns"] SUBSEP $column["event_phase"] SUBSEP $column["event_origin_node"] SUBSEP $column["event_origin_sequence"] } $column["flow_id"] == 13 { removed[key]++; next } { $column["ordinal"] -= removed[key] }' \
  'REJECT: line 2: incomplete collective progress coverage for collective_id=1: expected 12, found 11'
# The compute after the dispatch at rank 0 drops its send to rank 2 (flow 13) from its join.
mutate_case "join-missing-local-predecessor" "$seeded" \
  '$column["local_predecessors"] == "12;13;14" { $column["local_predecessors"] = "12;14"; $column["local_required"] = 2; if ($column["after_local_completed"] > 0) $column["after_local_completed"] -= 1; if ($column["before_local_completed"] > 0) $column["before_local_completed"] -= 1 } $column["local_predecessors"] == "12;14" && $column["cause_flow_id"] == 13 { next }' \
  'REJECT: line 15: a stage does not wait for its predecessor collective'"'"'s whole completion at its rank'
# Rank 0's root of the second ReduceScatter follows its compute (flow 32) and the first ReduceScatter
# (flows 2 and 11): the ACK of flow 2 is a phase-0 event.
mutate_case "join-root-ack-phase" "$stream" \
  '$column["flow_id"] == 12 && $column["cause_flow_id"] == 2 { $column["event_phase"] = 1 }' \
  'REJECT: line 94: collective progress event phase disagrees with its cause'
# The Send/Recv receiver's compute stage has no local predecessor, so it starts locally complete.
mutate_case "sendrecv-receiver-initial-state" "$sendrecv" \
  'NR == 3 { $column["before_local_complete"] = 0 }' \
  'REJECT: line 3: first local prerequisite state is not initial'

# P16 H1 x H2: on a Rail topology the intra-server messages of a multi-server collective are stage
# notifies (tests/p16_collops_rail.rs). LeanGuard checks a notify's lead timer at its sender and its
# whole-chunk delivery at its receiver; the NVLink delivery delay (the notify's `duration_ns`) is
# accepted as given.
rail_ring="$fixture_dir/collective_collops_rail_ring_roce_executor_accept.csv"
# The first notify delivery is split into two 2,000-byte halves (the second under a fresh event key).
mutate_case "notify-split-delivery" "$rail_ring" \
  'done == 0 && $column["cause_kind"] == "notify" && $column["cause"] == "inbound_arrival" { done = 1; saved = $0; $column["arrival_bytes"] = 2000; $column["segment_bytes"] = 2000; $column["after_inbound_bytes"] = 2000; $column["after_inbound_complete"] = 0; $column["activated"] = 0; $column["after_status"] = "blocked"; $column["after_next_time_ns"] = 0; print; $0 = saved; $column["event_origin_sequence"] += 1000; $column["segment_sequence"] = 2000; $column["segment_bytes"] = 2000; $column["arrival_bytes"] = 2000; $column["before_inbound_bytes"] = 2000 }' \
  'REJECT: line 16: inbound stage notify does not deliver its whole chunk at once'
# The first notify release arms its lead's timer one nanosecond late.
mutate_case "notify-lead-timer" "$rail_ring" \
  'done == 0 && $column["stage_kind"] == "notify" && $column["cause"] == "local_completion" && $column["activated"] == 1 { done = 1; $column["after_next_time_ns"] += 1 }' \
  'REJECT: line 2: stage notify release does not arm its lead'"'"'s timer'
# A notify delivery claims a RoCE carrier (and drops the notify's origin, delay and collective).
mutate_case "notify-carrier-relabeled-roce" "$rail_ring" \
  'done == 0 && $column["cause_kind"] == "notify" && $column["cause"] == "inbound_arrival" { done = 1; $column["cause_kind"] = "roce"; $column["cause_origin_ns"] = 0; $column["cause_delay_ns"] = 0; $column["cause_collective_id"] = "" }' \
  'REJECT: line 16: cause kind disagrees with the cause stage'
# The receiver of the first notify (line 10) completes locally a nanosecond after its timer.
mutate_case "notify-completion-off-its-timer" "$rail_ring" \
  'NR == 10 { $column["time_ns"] = 1002 }' \
  'REJECT: line 10: compute local completion does not occur at its predecessor'"'"'s timer deadline'

# P16 H1 fix round 2 (review N1): a notify is delivered exactly its delay after its release, both
# named on its delivery rows; a logged notify's are its release row's time and its `duration_ns`
# (accepted as given). Notify flow 0 (released at 1,000 ns, delivered at 1,068 ns, 68 ns) claims a
# 99 ns delay on its own rows (it would arrive 31 ns early), then a 60 ns one (8 ns late).
mutate_case "notify-early-delivery" "$rail_ring" \
  '$column["flow_id"] == 0 { $column["duration_ns"] = 99 }' \
  'REJECT: line 16: stage notify delivery does not name its release and delay'
mutate_case "notify-late-delivery" "$rail_ring" \
  '$column["flow_id"] == 0 { $column["duration_ns"] = 60 }' \
  'REJECT: line 16: stage notify delivery does not name its release and delay'
# A notify released by a counted join leaves at the join's completion: notify flow 0 waits for `fwd`
# (1,000 ns) and `aux` (2,000 ns) and is delivered at 2,134 ns. Claiming a 1,134 ns delay (as if
# released by `fwd` alone) rejects.
rail_join="$fixture_dir/collective_collops_rail_a2a_after_join_executor_accept.csv"
mutate_case "notify-join-release" "$rail_join" \
  '$column["flow_id"] == 0 { $column["duration_ns"] = 1134 }' \
  'REJECT: line 146: stage notify delivery does not name its release and delay'

# An ungated root notify is not logged: it starts with its collective at the collective's initial
# delay (2,000 ns here). Notify flow 0 (rank 0 -> 1) is delivered at 2,134 ns; it is moved 1 ns early,
# and notify flow 26 (rank 3 -> 1) 1 ns late.
rail_ungated="$fixture_dir/collective_collops_rail_a2a_ungated_executor_accept.csv"
mutate_case "ungated-notify-early-delivery" "$rail_ungated" \
  '$column["cause"] == "inbound_arrival" && $column["cause_flow_id"] == 0 { $column["time_ns"] = 2133 }' \
  'REJECT: line 26: stage notify delivery does not occur at its origin plus its delay'
mutate_case "ungated-notify-late-delivery" "$rail_ungated" \
  '$column["cause"] == "inbound_arrival" && $column["cause_flow_id"] == 26 { $column["time_ns"] = 2135 }' \
  'REJECT: line 36: stage notify delivery does not occur at its origin plus its delay'

# Notify flow 26's delivery moves 1 ns late with its origin (still origin + delay): it no longer
# shares its collective's start with the other unlogged notifies.
mutate_case "ungated-notify-shifted-delivery" "$rail_ungated" \
  '$column["cause"] == "inbound_arrival" && $column["cause_flow_id"] == 26 { $column["time_ns"] = 2135; $column["cause_origin_ns"] = 2001 }' \
  'REJECT: line 36: unlogged stage notifies of one collective do not share one origin'
# Notify flow 26 claims a release 1 ns earlier on every row it causes, consistently (its sender's
# timer completion at 2,001 ns with a 2 ns lead, its delivery at 2,134 ns with a 135 ns delay):
# only its collective's shared start rejects it.
mutate_case "ungated-notify-shifted-release" "$rail_ungated" \
  '$column["cause_flow_id"] == 26 && $column["cause"] == "local_completion" { $column["cause_origin_ns"] = 1999; $column["cause_delay_ns"] = 2 } $column["cause_flow_id"] == 26 && $column["cause"] == "inbound_arrival" { $column["cause_origin_ns"] = 1999; $column["cause_delay_ns"] = 135 }' \
  'REJECT: line 12: unlogged stage notifies of one collective do not share one origin'
# The delivery rows of notify flow 0 name another collective.
mutate_case "notify-cause-collective" "$rail_ring" \
  '$column["cause_flow_id"] == 0 && $column["cause_kind"] == "notify" { $column["cause_collective_id"] = 7 }' \
  'REJECT: line 10: stage notify cause names another collective'

# Review N2 (fix round 3): every row a notify causes names one collective. Notify flow 0's delivery
# row is relabelled to collective 1, then to 99 (none), while its sender's timer row keeps 0.
mutate_case "ungated-notify-relabelled-delivery" "$rail_ungated" \
  '$column["cause"] == "inbound_arrival" && $column["cause_flow_id"] == 0 { $column["cause_collective_id"] = 1 }' \
  'REJECT: line 26: the rows a stage notify causes name different collectives'
mutate_case "ungated-notify-relabelled-to-none" "$rail_ungated" \
  '$column["cause"] == "inbound_arrival" && $column["cause_flow_id"] == 0 { $column["cause_collective_id"] = 99 }' \
  'REJECT: line 26: the rows a stage notify causes name different collectives'

# P16 host-matched `after` (the ruling on H3's C1): an entry stage waits, at its host, for the
# groups it follows there, whatever their ranks. In dp-after-ep, `tail` at host 5 (flow 22) follows
# `post` at host 5 (flow 21); naming `post` at host 1 (flow 19, same timer deadline) instead names
# a compute stage on another host.
hostafter_dp="$fixture_dir/collective_hostafter_dp_after_ep_executor_accept.csv"
mutate_case "hostafter-compute-predecessor-on-another-host" "$hostafter_dp" \
  '$column["flow_id"] == 22 { $column["local_predecessors"] = 19; $column["cause_flow_id"] = 19 }' \
  'REJECT: line 57: compute local predecessor is not a compute stage on its host'
# In rail-dp-after-ep-a2a, the DP ring's root at host 4 (flow 1) follows EP instance 1's all-to-all
# there (its sends 16, 17, 18 and receipts 21, 23, 25) and the weight gradient (39); dropping send
# 18 from the join (and its completion row) leaves instance 1's completion at host 4 partial.
hostafter_rail="$fixture_dir/collective_hostafter_rail_dp_after_ep_a2a_executor_accept.csv"
mutate_case "hostafter-join-missing-cross-family-pair" "$hostafter_rail" \
  '$column["flow_id"] == 1 { if ($column["cause_flow_id"] == 18 && $column["cause"] == "local_completion") next; $column["local_predecessors"] = "16;17;39"; $column["local_required"] = 3; if ($column["cause_flow_id"] == 17 && $column["cause"] == "local_completion") { $column["after_local_complete"] = 1 } if ($column["before_local_completed"] == 4) $column["before_local_completed"] = 3; if ($column["after_local_completed"] == 4) $column["after_local_completed"] = 3 }' \
  'REJECT: line 3: a stage does not wait for its predecessor collective'"'"'s whole completion at its rank'

# Review L4 (hostafter lane): a flow has one target host. In sendrecv-across-stages the message
# (flow 0, host 1 -> host 2) is stage 1's inbound predecessor at host 2 (flow 7). The reviewer's
# forgery has stage 1's rank at host 3 (flow 8) claim the message too, with copied arrival rows;
# the second has stage 0's next compute at host 0 (flow 3) claim it.
hostafter_sendrecv="$fixture_dir/collective_hostafter_sendrecv_across_stages_executor_accept.csv"
mutate_case "sendrecv-message-claimed-at-another-host" "$hostafter_sendrecv" \
  'NR == 2 { ps = $column["packet_size_bytes"]; iv = $column["interval_ns"] } $column["flow_id"] == 8 && $column["cause"] == "local_completion" { $column["inbound_predecessors"] = 0; $column["inbound_predecessor_bytes"] = 9000; $column["after_inbound_complete"] = 0; $column["before_inbound_complete"] = 0; $column["activated"] = 0; $column["after_status"] = "blocked"; $column["after_next_time_ns"] = 0; $column["packet_size_bytes"] = ps; $column["interval_ns"] = iv; print; next } $column["flow_id"] == 7 && $column["cause"] == "inbound_arrival" { print; $column["node_id"] = 3; $column["flow_id"] = 8; $column["rank"] = 1; $column["local_predecessors"] = 6; $column["ordinal"] = $column["ordinal"] + 1; $column["before_local_complete"] = 1; $column["after_local_complete"] = 1; print; next }' \
  'REJECT: line 3: an inbound predecessor is delivered to more than one host'
mutate_case "sendrecv-message-claimed-by-another-group" "$hostafter_sendrecv" \
  'NR == 2 { ps = $column["packet_size_bytes"]; iv = $column["interval_ns"] } $column["flow_id"] == 3 && $column["cause"] == "local_completion" { $column["inbound_predecessors"] = 0; $column["inbound_predecessor_bytes"] = 9000; $column["after_inbound_complete"] = 0; $column["before_inbound_complete"] = 0; $column["activated"] = 0; $column["after_status"] = "blocked"; $column["after_next_time_ns"] = 0; $column["packet_size_bytes"] = ps; $column["interval_ns"] = iv; print; next } $column["flow_id"] == 7 && $column["cause"] == "inbound_arrival" { print; $column["node_id"] = 0; $column["flow_id"] = 3; $column["rank"] = 0; $column["collective_id"] = 1; $column["local_predecessors"] = 1; $column["ordinal"] = $column["ordinal"] + 1; $column["before_local_complete"] = 1; $column["after_local_complete"] = 1; print; next }' \
  'REJECT: line 4: an inbound predecessor is delivered to more than one host'

echo "P10c exact-integer collective campaign checks: $checked"
exit "$failures"
