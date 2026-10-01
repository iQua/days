#!/usr/bin/env bash
# P15 RoCE queue-pair LeanGuard campaign: accept fixtures, committed reject fixtures, and
# awk mutations of the accept fixtures, each with its exact expected verdict.
#
# Optional: ROCE_TRACE_DIR=<dir> also checks every executor trace triple
# <name>.roce_sender.csv / <name>.roce_receiver.csv / <name>.dcqcn.csv found there
# (with <name>.stop_time_ns holding the image's stop time, when present). Those must ACCEPT.
#
# Receiver CSV columns (roce_receiver_transitions_csv):
#   1 time_ns  2 event_phase  3 event_origin_node  4 event_origin_sequence  5 node_id  6 flow_id
#   7 total_bytes  8 ack_every_packets  9 nack_interval_ns  10 duplicate_ack  11 ack_size_bytes
#   12 cnp_interval_ns  13 packet_psn  14 packet_bytes  15 packet_sent_time_ns
#   16 packet_retransmission  17 packet_ce  18 action  19 feedback_acknowledgment
#   20 feedback_payload  21 cnp_sent  22 cnp_payload
#   23 before_expected_psn  24 before_packets_since_ack  25 before_last_nack_psn
#   26 before_last_nack_time_ns  27 before_last_cnp_time_ns
#   28 after_expected_psn  29 after_packets_since_ack  30 after_last_nack_psn
#   31 after_last_nack_time_ns  32 after_last_cnp_time_ns
#
# Sender CSV columns (roce_sender_transitions_csv):
#   1 time_ns  2 event_phase  3 event_origin_node  4 event_origin_sequence  5 node_id  6 flow_id
#   7 kind  8 mtu_bytes  9 total_bytes  10 pacing_interval_ns  11 first_pacing_time_ns  12 rto_ns
#   13 rate_bps  14 input_acknowledgment  15 emitted  16 emitted_psn  17 emitted_bytes
#   18 emitted_retransmission  19 emitted_payload
#   20 before_next_psn  21 before_snd_una  22 before_bytes_emitted  23 before_packets_emitted
#   24 before_credit_quanta  25 before_rto_deadline_ns  26 before_pacer  27 before_next_tick_ns
#   28 before_status
#   29 after_next_psn  30 after_snd_una  31 after_bytes_emitted  32 after_packets_emitted
#   33 after_credit_quanta  34 after_rto_deadline_ns  35 after_pacer  36 after_next_tick_ns
#   37 after_status
#
# DCQCN CSV columns: as in run-p10c-dcqcn-campaign.sh (dcqcn_transitions_csv).
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
lean_dir="$(cd "$script_dir/.." && pwd)"

cd "$lean_dir"
lake build p10c_roce_check

checker="$lean_dir/.lake/build/bin/p10c_roce_check"
fixture_dir="$lean_dir/fixtures/p10c"
failures=0
checked=0
mutations=0
mutations_caught=0

check_case() {
  local label="$1"
  local expected_exit="$2"
  local expected_output="$3"
  shift 3
  checked=$((checked + 1))

  set +e
  actual_output="$("$checker" "$@" 2>&1)"
  actual_exit=$?
  set -e

  if [[ "$actual_exit" != "$expected_exit" || "$actual_output" != "$expected_output" ]]; then
    echo "fixture failed: $label" >&2
    echo "expected exit: $expected_exit" >&2
    echo "actual exit:   $actual_exit" >&2
    echo "expected output: $expected_output" >&2
    echo "actual output:   $actual_output" >&2
    failures=$((failures + 1))
    return 1
  fi
  echo "ok: $label"
}

expected_case() {
  local label="$1"
  local expected="$2"
  shift 2
  local expected_exit
  local expected_output
  expected_exit="$(sed -n '1s/^exit=//p' "$expected")"
  expected_output="$(sed '1d' "$expected")"
  check_case "$label" "$expected_exit" "$expected_output" "$@" || true
}

campaign_tmp="$(mktemp -d)"
trap 'rm -rf "$campaign_tmp"' EXIT

# mutate_receiver <label> <source.csv> <awk program> <expected REJECT line>
mutate_receiver() {
  local label="$1"
  local source="$2"
  local program="$3"
  local expected_output="$4"
  local mutated="$campaign_tmp/receiver.csv"
  awk -F, -v OFS=, "$program" "$source" > "$mutated"
  mutations=$((mutations + 1))
  if check_case "receiver/$label" 1 "$expected_output" receiver "$mutated"; then
    mutations_caught=$((mutations_caught + 1))
  fi
}

# --- Receiver: accept and committed reject fixtures -------------------------------------------
for csv in "$fixture_dir"/roce_receiver_*_accept.csv "$fixture_dir"/roce_receiver_*_reject.csv; do
  [[ -e "$csv" ]] || continue
  expected_case "$(basename "$csv")" "${csv%.csv}.expected" receiver "$csv"
done

# --- Receiver: mutations of roce_receiver_rules_accept.csv ------------------------------------
# Rows (NR): 2 t100 f3 in-order below cadence; 3 t150 f5 first NACK; 4 t200 f3 CNP + cadence ACK;
# 5 t250 f5 NACK at the interval edge; 6 t260 f5 in-order; 7 t270 f5 NACK for a new frontier;
# 8 t280 f5 silent duplicate; 9 t300 f3 first NACK, CNP interval closed; 10 t350 f5 in-order;
# 11 t360 f5 CNP + ACK at the end; 12 t400 f3 NACK suppressed; 13 t600 f3 in-order;
# 14 t700 f3 CNP at the interval edge + cadence ACK; 15 t800 f3 duplicate ACK; 16 t900 f3 last ACK.
rules="$fixture_dir/roce_receiver_rules_accept.csv"

mutate_receiver "nack-inside-suppression-interval" "$rules" \
  'NR == 12 { $18 = "nack"; $19 = 2000; $20 = 100; $31 = 400 } { print }' \
  'REJECT: line 12: RoCE receiver action mismatch'
mutate_receiver "nack-suppressed-at-interval-edge" "$rules" \
  'NR == 5 { $18 = "nack_suppressed"; $19 = ""; $20 = ""; $31 = 150 } { print }' \
  'REJECT: line 5: RoCE receiver action mismatch'
mutate_receiver "nack-suppressed-for-new-frontier" "$rules" \
  'NR == 7 { $18 = "nack_suppressed"; $19 = ""; $20 = ""; $29 = 1; $30 = 0; $31 = 250 } { print }' \
  'REJECT: line 7: RoCE receiver action mismatch'
mutate_receiver "missing-ack-at-end" "$rules" \
  'NR == 16 { $18 = "none"; $19 = ""; $20 = ""; $29 = 1 } { print }' \
  'REJECT: line 16: RoCE receiver action mismatch'
mutate_receiver "ack-before-cadence" "$rules" \
  'NR == 2 { $18 = "ack"; $19 = 1000; $20 = 4; $29 = 0 } { print }' \
  'REJECT: line 2: RoCE receiver action mismatch'
mutate_receiver "missing-cadence-ack" "$rules" \
  'NR == 14 { $18 = "none"; $19 = ""; $20 = "" } { print }' \
  'REJECT: line 14: RoCE receiver action mismatch'
mutate_receiver "duplicate-acked-when-disabled" "$rules" \
  'NR == 8 { $18 = "duplicate_ack"; $19 = 1000; $20 = 40 } { print }' \
  'REJECT: line 8: RoCE receiver action mismatch'
mutate_receiver "duplicate-dropped-when-enabled" "$rules" \
  'NR == 15 { $18 = "none"; $19 = ""; $20 = "" } { print }' \
  'REJECT: line 15: RoCE receiver action mismatch'
mutate_receiver "ack-above-frontier" "$rules" \
  'NR == 4 { $19 = 3000 } { print }' \
  'REJECT: line 4: RoCE feedback acknowledgment mismatch'
mutate_receiver "nack-not-at-frontier" "$rules" \
  'NR == 9 { $19 = 3000 } { print }' \
  'REJECT: line 9: RoCE feedback acknowledgment mismatch'
mutate_receiver "frontier-not-advanced" "$rules" \
  'NR == 13 { $28 = 2000 } { print }' \
  'REJECT: line 13: RoCE receiver after-state mismatch'
mutate_receiver "frontier-advanced-on-out-of-order" "$rules" \
  'NR == 9 { $28 = 4000 } { print }' \
  'REJECT: line 9: RoCE receiver after-state mismatch'
mutate_receiver "cadence-not-reset-by-nack" "$rules" \
  'NR == 7 { $29 = 2 } { print }' \
  'REJECT: line 7: RoCE receiver after-state mismatch'
mutate_receiver "suppressed-nack-moves-mark" "$rules" \
  'NR == 12 { $31 = 400 } { print }' \
  'REJECT: line 12: RoCE receiver after-state mismatch'
mutate_receiver "cnp-inside-interval" "$rules" \
  'NR == 9 { $21 = 1; $22 = 45; $32 = 300 } { print }' \
  'REJECT: line 9: RoCE notification-point decision mismatch'
mutate_receiver "cnp-missed-at-interval-edge" "$rules" \
  'NR == 14 { $21 = 0; $22 = ""; $32 = 200 } { print }' \
  'REJECT: line 14: RoCE notification-point decision mismatch'
mutate_receiver "cnp-without-ce" "$rules" \
  'NR == 4 { $17 = 0 } { print }' \
  'REJECT: line 4: RoCE notification-point decision mismatch'
mutate_receiver "ack-allocated-before-cnp" "$rules" \
  'NR == 4 { $20 = 12; $22 = 20 } { print }' \
  'REJECT: line 4: RoCE receiver payloads out of allocation order (node_id=4)'
mutate_receiver "state-splice" "$rules" \
  'NR == 10 { $23 = 1500 } { print }' \
  'REJECT: line 10: RoCE receiver state discontinuity (node_id=4, flow_id=5)'
mutate_receiver "config-splice" "$rules" \
  'NR == 14 { $9 = 999 } { print }' \
  'REJECT: line 14: RoCE receiver config discontinuity (node_id=4, flow_id=3)'
mutate_receiver "initial-state-splice" "$rules" \
  'NR == 2 { $24 = 1; $29 = 2 } { print }' \
  'REJECT: line 2: RoCE receiver first state is not initial (node_id=4, flow_id=3)'
mutate_receiver "packet-beyond-total" "$rules" \
  'NR == 16 { $14 = 2000 } { print }' \
  'REJECT: line 16: RoCE data packet extends beyond the queue pair'"'"'s total bytes'
mutate_receiver "arrival-before-send" "$rules" \
  'NR == 2 { $15 = 101 } { print }' \
  'REJECT: line 2: RoCE data packet arrives before it was sent'
mutate_receiver "typed-arrival-phase" "$rules" \
  'NR == 2 { $2 = 1 } { print }' \
  'REJECT: line 2: RoCE data arrival must have phase 0'
mutate_receiver "zero-ack-cadence" "$rules" \
  'NR == 2 { $8 = 0 } { print }' \
  'REJECT: line 2: invalid RoCE receiver configuration'
mutate_receiver "duplicate-key" "$rules" \
  'NR == 4 { $1 = 150; $2 = 0; $3 = 2; $4 = 0 } { print }' \
  'REJECT: line 4: duplicate or backward canonical event key'
mutate_receiver "u64-parser-bound" "$rules" \
  'NR == 2 { $15 = "18446744073709551616" } { print }' \
  "REJECT: line 2: value exceeds u64: '18446744073709551616'"
mutate_receiver "half-blank-nack-mark" "$rules" \
  'NR == 4 { $30 = 7 } { print }' \
  'REJECT: line 4: after_last_nack_psn and after_last_nack_time_ns must be both present or both blank'

# --- Sender (joined with the DCQCN controller log) --------------------------------------------
gbn="$fixture_dir/roce_sender_gbn_accept.csv"
gbn_dcqcn="$fixture_dir/dcqcn_qp_completion_accept.csv"
gbn_stop=30000
stopped="$fixture_dir/roce_sender_stopped_accept.csv"
stopped_dcqcn="$fixture_dir/roce_sender_stopped_accept.dcqcn.csv"
stopped_stop=2950

check_case "roce_sender_gbn_accept.csv" 0 "ACCEPT" sender "$gbn" "$gbn_dcqcn" "$gbn_stop" || true
check_case "roce_sender_gbn_accept.csv (stop inferred)" 0 "ACCEPT" sender "$gbn" "$gbn_dcqcn" || true
check_case "roce_sender_stopped_accept.csv" 0 "ACCEPT" \
  sender "$stopped" "$stopped_dcqcn" "$stopped_stop" || true
check_case "roce_sender_stopped_accept.csv (stop inferred)" 0 "ACCEPT" \
  sender "$stopped" "$stopped_dcqcn" || true
for csv in "$fixture_dir"/roce_sender_*_reject.csv; do
  [[ -e "$csv" ]] || continue
  expected_case "$(basename "$csv")" "${csv%.csv}.expected" sender "$csv" "${csv%.csv}.dcqcn.csv"
done

# mutate_sender <label> <sender.csv> <dcqcn.csv> <stop or ""> <awk program on the sender CSV>
#   <expected REJECT line>
mutate_sender() {
  local label="$1"
  local source="$2"
  local dcqcn="$3"
  local stop="$4"
  local program="$5"
  local expected_output="$6"
  local mutated="$campaign_tmp/sender.csv"
  awk -F, -v OFS=, "$program" "$source" > "$mutated"
  mutations=$((mutations + 1))
  if check_case "sender/$label" 1 "$expected_output" sender "$mutated" "$dcqcn" $stop; then
    mutations_caught=$((mutations_caught + 1))
  fi
}

# mutate_dcqcn <label> <sender.csv> <dcqcn.csv> <stop> <awk program on the DCQCN CSV>
#   <expected REJECT line>
mutate_dcqcn() {
  local label="$1"
  local sender="$2"
  local source="$3"
  local stop="$4"
  local program="$5"
  local expected_output="$6"
  local mutated="$campaign_tmp/dcqcn.csv"
  awk -F, -v OFS=, "$program" "$source" > "$mutated"
  mutations=$((mutations + 1))
  if check_case "sender/$label" 1 "$expected_output" sender "$sender" "$mutated" $stop; then
    mutations_caught=$((mutations_caught + 1))
  fi
}

# roce_sender_gbn_accept.csv rows (NR): 2 t1000 fresh send; 3 t2000 fresh send; 4 t2500 ACK 1000;
# 5 t2800 NACK 1000 (= snd_una: rewinds); 6 t3000 retransmission; 7 t4000 tick after the CNP at
# 3500 (rate 6 Gb/s; status recomputed to blocked between rows); 8 t5000 last fresh packet,
# parks; 9 t6000 ACK 2000; 10 t11000 timeout, restart on the grid at 12000; 11 t12000
# retransmission, parks; 12 t13000 ACK 3000, finished.
mutate_sender "wrong-rewind-point-nack" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 5 { $29 = 2000 } { print }' \
  'REJECT: sender: line 5: RoCE sender after-state mismatch'
mutate_sender "wrong-rewind-point-timeout" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 10 { $29 = 3000 } { print }' \
  'REJECT: sender: line 10: RoCE sender after-state mismatch'
mutate_sender "stale-nack-applied" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 5 { $14 = 0 } { print }' \
  'REJECT: sender: line 5: RoCE sender after-state mismatch'
mutate_sender "stale-ack-applied" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 4 { $14 = 0 } { print }' \
  'REJECT: sender: line 4: RoCE sender after-state mismatch'
mutate_sender "ack-rewinds" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 4 { $29 = 1000 } { print }' \
  'REJECT: sender: line 4: RoCE sender after-state mismatch'
mutate_sender "ack-above-frontier" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 4 { $14 = 3000 } { print }' \
  "REJECT: sender: line 4: RoCE acknowledgment above the sender's high-water mark"
mutate_sender "credit-not-charged-on-retransmission" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 6 { $33 = 8000000000000 } { print }' \
  'REJECT: sender: line 6: RoCE sender after-state mismatch'
mutate_sender "retransmission-counted-as-fresh" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 6 { $31 = 3000; $32 = 3 } { print }' \
  'REJECT: sender: line 6: RoCE sender after-state mismatch'
mutate_sender "retransmission-bit-cleared" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 6 { $18 = 0 } { print }' \
  'REJECT: sender: line 6: RoCE emission mismatch'
mutate_sender "retransmission-from-wrong-psn" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 11 { $16 = 1000 } { print }' \
  'REJECT: sender: line 11: RoCE emission mismatch'
mutate_sender "emission-without-credit" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 7 { $15 = 1; $16 = 2000; $17 = 1000; $18 = 0; $19 = 19 } { print }' \
  'REJECT: sender: line 7: RoCE emission without a DCQCN byte opportunity'
mutate_sender "emission-differs-from-byte-counter" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 2 { $17 = 999 } { print }' \
  'REJECT: sender: line 2: RoCE emission differs from its DCQCN byte opportunity'
mutate_sender "tick-rate-not-controller-rate" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 7 { $13 = 8000000000 } { print }' \
  "REJECT: sender: line 7: RoCE tick rate differs from the DCQCN controller's current rate"
mutate_sender "status-not-recomputed-at-cnp" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 7 { $28 = "scheduled" } { print }' \
  'REJECT: sender: line 7: RoCE sender state discontinuity (node_id=1, flow_id=3)'
mutate_sender "timeout-not-rearmed" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 10 { $34 = 11000 } { print }' \
  'REJECT: sender: line 10: RoCE sender after-state mismatch'
mutate_sender "timeout-before-deadline" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 10 { $1 = 10999 } { print }' \
  'REJECT: sender: line 10: RoCE timeout fires without an armed deadline at this time'
mutate_sender "restart-at-rewind-instant" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 10 { $36 = 11000 } { print }' \
  'REJECT: sender: line 10: RoCE sender after-state mismatch'
mutate_sender "rto-not-restarted-by-ack" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 4 { $34 = 6000 } { print }' \
  'REJECT: sender: line 4: RoCE sender after-state mismatch'
mutate_sender "rto-not-disarmed-at-completion" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 12 { $34 = 18000 } { print }' \
  'REJECT: sender: line 12: invalid RoCE sender after-state'
mutate_sender "completion-not-finished" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 12 { $37 = "blocked" } { print }' \
  'REJECT: sender: line 12: invalid RoCE sender after-state'
mutate_sender "tick-of-unarmed-pacer" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 7 { $1 = 4500 } { print }' \
  'REJECT: sender: line 7: RoCE tick of a pacer not armed for this time'
mutate_sender "typed-tick-phase" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 2 { $2 = 0 } { print }' \
  'REJECT: sender: line 2: RoCE pacing tick must have phase 1'
mutate_sender "state-splice" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 9 { $24 = 4000000000001 } { print }' \
  'REJECT: sender: line 9: RoCE sender state discontinuity (node_id=1, flow_id=3)'
mutate_sender "config-splice" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 8 { $12 = 5001 } { print }' \
  'REJECT: sender: line 8: RoCE sender config discontinuity (node_id=1, flow_id=3)'
mutate_sender "initial-state-splice" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 2 { $28 = "blocked" } { print }' \
  'REJECT: sender: line 2: RoCE sender first state is not initial (node_id=1, flow_id=3)'
mutate_sender "duplicate-key" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 3 { $1 = 1000; $2 = 1; $3 = 1; $4 = 0 } { print }' \
  'REJECT: sender: line 3: duplicate or backward canonical event key'
mutate_sender "payload-allocation-order" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 3 { $19 = 5 } { print }' \
  'REJECT: sender: line 3: RoCE sender payloads out of allocation order (node_id=1)'
mutate_sender "u128-credit-bound" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 2 { $24 = "340282366920938463463374607431768211456" } { print }' \
  "REJECT: sender: line 2: value exceeds u128: '340282366920938463463374607431768211456'"
mutate_dcqcn "control-rearmed-after-completion" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  '{ print } END { print "29500,1,1,9,1,3,control,0,0,8000000000,1000000000,8000000000,500000000,1000000000,500000000,500000000,0,9500,1000000000000,625000000,4812500000,7000000000,1,22000,fast_recovery,0,0,29500,625000000,4812500000,7000000000,0,22000,fast_recovery,0,0,39000" }' \
  'REJECT: dcqcn: line 11: DCQCN control tick re-armed after queue-pair completion (D2) (node_id=1, flow_id=3)'
mutate_dcqcn "retransmission-not-charged-to-byte-counter" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR != 4 { print }' \
  'REJECT: dcqcn: line 4: DCQCN state discontinuity for source (node_id=1, flow_id=3)'

# roce_sender_stopped_accept.csv rows (NR), timeout off, stop 2950: 2 t1000 flow 3 send; 3 t2000
# flow 3 last packet, parks; 4 t2200 flow 7 tick whose next tick (3200) is beyond stop; 5 t2600
# NACK 0 rewinds a parked pacer whose restart (3000) is beyond stop; 6 t2700 ACK 1000, still
# stopped; 7 t2800 ACK 2000 completes a stopped pacer (parked, finished).
mutate_sender "restart-armed-beyond-stop" "$stopped" "$stopped_dcqcn" "$stopped_stop" \
  'NR == 5 { $35 = "armed"; $37 = "scheduled" } { print }' \
  'REJECT: sender: line 5: RoCE pacer stop decision contradicts stop_time_ns=2950 (tick 3000)'
mutate_sender "inconsistent-inferred-stop" "$stopped" "$stopped_dcqcn" "" \
  'NR == 4 { $35 = "armed"; $37 = "blocked" } { print }' \
  'REJECT: sender: no single stop time fits the pacer decisions: tick 3200 (line 4) is armed and tick 3000 (line 5) is stopped'
mutate_sender "finished-pacer-left-stopped" "$stopped" "$stopped_dcqcn" "$stopped_stop" \
  'NR == 7 { $35 = "stopped"; $36 = 3000 } { print }' \
  'REJECT: sender: line 7: invalid RoCE sender after-state'
mutate_sender "rto-armed-while-off" "$stopped" "$stopped_dcqcn" "$stopped_stop" \
  'NR == 2 { $34 = 6000 } { print }' \
  'REJECT: sender: line 2: invalid RoCE sender after-state'
mutate_sender "event-after-stop" "$stopped" "$stopped_dcqcn" 2750 \
  '{ print }' \
  'REJECT: sender: line 7: event after stop_time_ns=2750'
mutate_sender "stopped-tick-still-armed" "$stopped" "$stopped_dcqcn" "$stopped_stop" \
  'NR == 4 { $35 = "armed"; $37 = "blocked" } { print }' \
  'REJECT: sender: line 4: RoCE pacer stop decision contradicts stop_time_ns=2950 (tick 3200)'

# --- Trace: the three logs of one run, and the cross-role invariants ---------------------------
loss_sender="$fixture_dir/roce_trace_loss_accept.sender.csv"
loss_receiver="$fixture_dir/roce_trace_loss_accept.receiver.csv"
loss_dcqcn="$fixture_dir/roce_trace_loss_accept.dcqcn.csv"
loss_stop=50000

check_case "roce_trace_loss_accept" 0 "ACCEPT" \
  trace "$loss_sender" "$loss_receiver" "$loss_dcqcn" "$loss_stop" || true
# Executor traces committed as fixtures (tests/fixtures-style triples with a .stop_time_ns file).
for sender_csv in "$fixture_dir"/roce_trace_*_executor_accept.sender.csv; do
  [[ -e "$sender_csv" ]] || continue
  base="${sender_csv%.sender.csv}"
  check_case "$(basename "$base")" 0 "ACCEPT" \
    trace "$sender_csv" "$base.receiver.csv" "$base.dcqcn.csv" "$(cat "$base.stop_time_ns")" || true
done
if [[ -n "${ROCE_TRACE_DIR:-}" ]]; then
  for sender_csv in "$ROCE_TRACE_DIR"/*.roce_sender.csv; do
    [[ -e "$sender_csv" ]] || continue
    base="${sender_csv%.roce_sender.csv}"
    stop=""
    [[ -e "$base.stop_time_ns" ]] && stop="$(cat "$base.stop_time_ns")"
    check_case "external/$(basename "$base")" 0 "ACCEPT" \
      trace "$sender_csv" "$base.roce_receiver.csv" "$base.dcqcn.csv" $stop || true
  done
fi

# mutate_trace <label> <sender|receiver|dcqcn> <awk program> <expected REJECT line>
mutate_trace() {
  local label="$1"
  local role="$2"
  local program="$3"
  local expected_output="$4"
  local sender="$loss_sender"
  local receiver="$loss_receiver"
  local dcqcn="$loss_dcqcn"
  local mutated="$campaign_tmp/trace-$role.csv"
  case "$role" in
    sender) awk -F, -v OFS=, "$program" "$loss_sender" > "$mutated"; sender="$mutated" ;;
    receiver) awk -F, -v OFS=, "$program" "$loss_receiver" > "$mutated"; receiver="$mutated" ;;
    dcqcn) awk -F, -v OFS=, "$program" "$loss_dcqcn" > "$mutated"; dcqcn="$mutated" ;;
  esac
  mutations=$((mutations + 1))
  if check_case "trace/$label" 1 "$expected_output" \
      trace "$sender" "$receiver" "$dcqcn" "$loss_stop"; then
    mutations_caught=$((mutations_caught + 1))
  fi
}

# roce_trace_loss_accept: sender rows (NR) 2 t1000 psn 0; 3 t2000 psn 1000 (lost); 4 t2600 ACK
# 1000; 5 t3000 psn 2000, parks; 6 t4200 NACK 1000 rewinds, restart at 5000 (rate 6 Gb/s after
# the CNP at 3900: blocked); 7 t5000 tick without credit; 8 t6000 psn 1000 again; 9 t7000 psn
# 2000 again, parks; 10 t7100 ACK 2000; 11 t8100 ACK 3000, finished. Receiver rows: 2 t1500
# psn 0 -> ACK 1000; 3 t3500 psn 2000 (CE) -> CNP + NACK 1000; 4 t6500 psn 1000 -> ACK 2000;
# 5 t7500 psn 2000 -> ACK 3000. Each mutation keeps every log consistent on its own terms.
mutate_trace "ack-consumed-before-receiver-sent-it" receiver \
  'NR == 2 { $1 = 2700 } { print }' \
  'REJECT: sender: line 4: RoCE ACK carries a value no receiver sent before it (flow_id=3, acknowledgment=1000)'
mutate_trace "nack-consumed-before-receiver-sent-it" receiver \
  'NR == 3 { $1 = 4300; $31 = 4300; $32 = 4300 } NR >= 4 { $26 = 4300; $27 = 4300; $31 = 4300; $32 = 4300 } { print }' \
  'REJECT: sender: line 6: RoCE NACK carries a value no receiver sent before it (flow_id=3, acknowledgment=1000)'
mutate_trace "cnp-applied-before-receiver-sent-it" receiver \
  'NR == 3 { $1 = 4000; $31 = 4000; $32 = 4000 } NR >= 4 { $26 = 4000; $27 = 4000; $31 = 4000; $32 = 4000 } { print }' \
  'REJECT: dcqcn: line 5: DCQCN CNP of a queue pair that no receiver sent before it (flow_id=3)'
mutate_trace "data-never-sent" receiver \
  'NR == 4 { $15 = 5000 } { print }' \
  'REJECT: receiver: line 4: RoCE data arrival matches no sender emission (flow_id=3, sent_time_ns=5000)'
mutate_trace "data-differs-from-emission" receiver \
  'NR == 4 { $16 = 0 } { print }' \
  "REJECT: receiver: line 4: RoCE data arrival differs from the sender's emission (flow_id=3, sent_time_ns=6000)"
mutate_trace "emission-delivered-twice" receiver \
  'NR == 5 { $15 = 3000; $16 = 0 } { print }' \
  'REJECT: receiver: line 5: RoCE emission delivered twice (flow_id=3, sent_time_ns=3000)'
mutate_trace "receiver-total-differs" receiver \
  'NR >= 2 { $7 = 4000 } { print }' \
  "REJECT: receiver: line 2: RoCE receiver total_bytes differs from the sender's (flow_id=3)"
mutate_trace "silent-duplicates-with-timeout-on" receiver \
  'NR >= 2 { $10 = 0 } { print }' \
  "REJECT: receiver: line 2: RoCE receiver drops duplicates silently while the sender's timeout is on (D7) (flow_id=3)"
mutate_trace "receiver-without-sender" receiver \
  'NR >= 2 { $6 = 4 } { print }' \
  'REJECT: receiver: line 2: RoCE receiver of a flow with no sender rows (flow_id=4)'
mutate_trace "receiver-log-checked" receiver \
  'NR == 5 { $18 = "none"; $19 = ""; $20 = "" } { print }' \
  'REJECT: receiver: line 5: RoCE receiver action mismatch'

# --- Mutations of committed executor traces (rows located by pattern, not by number) ----------
# mutate_executor <label> <name> <sender|receiver> <row condition> <awk action> <expected message
# with LINE standing for the mutated line>
mutate_executor() {
  local label="$1"
  local name="$2"
  local role="$3"
  local condition="$4"
  local action="$5"
  local expected_template="$6"
  local base="$fixture_dir/roce_trace_${name}_executor_accept"
  local source="$base.$role.csv"
  local line
  line="$(awk -F, "NR > 1 && ($condition) { print NR; exit }" "$source")"
  mutations=$((mutations + 1))
  if [[ -z "$line" ]]; then
    echo "fixture failed: executor/$label (no row matches: $condition)" >&2
    failures=$((failures + 1))
    return
  fi
  local mutated="$campaign_tmp/executor-$role.csv"
  awk -F, -v OFS=, -v target="$line" "NR == target { $action } { print }" "$source" > "$mutated"
  local sender="$base.sender.csv"
  local receiver="$base.receiver.csv"
  case "$role" in
    sender) sender="$mutated" ;;
    receiver) receiver="$mutated" ;;
  esac
  if check_case "executor/$label" 1 "${expected_template//LINE/$line}" \
      trace "$sender" "$receiver" "$base.dcqcn.csv" "$(cat "$base.stop_time_ns")"; then
    mutations_caught=$((mutations_caught + 1))
  fi
}

mutate_executor "timeout-wrong-rewind-point" timeout sender \
  '$7 == "timeout" && $21 != $22' '$29 = $31' \
  'REJECT: sender: line LINE: RoCE sender after-state mismatch'
mutate_executor "nack-without-rewind" timeout sender \
  '$7 == "nack" && $20 != $14' '$29 = $20' \
  'REJECT: sender: line LINE: RoCE sender after-state mismatch'
mutate_executor "retransmission-not-charged" timeout sender \
  '$15 == 1 && $18 == 1' '$33 = $24' \
  'REJECT: sender: line LINE: RoCE sender after-state mismatch'
mutate_executor "timeout-not-rearmed" timeout sender \
  '$7 == "timeout"' '$34 = $25' \
  'REJECT: sender: line LINE: RoCE sender after-state mismatch'
mutate_executor "nack-inside-suppression-interval" timeout receiver \
  '$18 == "nack_suppressed"' '$18 = "nack"' \
  'REJECT: receiver: line LINE: RoCE receiver action mismatch'
mutate_executor "missing-ack-at-end" timeout receiver \
  '$18 == "ack" && $28 == $7' '$18 = "none"; $19 = ""; $20 = ""' \
  'REJECT: receiver: line LINE: RoCE receiver action mismatch'
mutate_executor "rto-config-spliced-mid-trace" nack_only sender \
  '$7 == "nack"' '$12 = 1000000' \
  'REJECT: sender: line LINE: RoCE sender config discontinuity (node_id=2, flow_id=1)'
mutate_executor "nack-only-suppressed-sent" nack_only receiver \
  '$18 == "nack_suppressed"' '$18 = "nack"' \
  'REJECT: receiver: line LINE: RoCE receiver action mismatch'

# --- Pending events must fire (review H1) -----------------------------------------------------
# A full run executes exactly the events with time <= stop_time_ns (scalar.rs run loop), so a
# pair's pending pacing tick, timeout, or (unless spent by D2) control tick must appear as a row
# before any later row of the pair, and, at the end, if it lies at or before the stop time.
# expect_reject <label> <expected REJECT line> <checker args...>
expect_reject() {
  local label="$1"
  local expected_output="$2"
  shift 2
  mutations=$((mutations + 1))
  if check_case "pending/$label" 1 "$expected_output" "$@"; then
    mutations_caught=$((mutations_caught + 1))
  fi
}
pf="$fixture_dir/roce_pending"
# Hand traces from the review (evidence/P15/leanguard-review): each reject differs from its
# accepted twin only by the missing event.
expect_reject "lost-timeout (M1)" \
  'REJECT: sender: line 3: pending RoCE timeout at 2500 did not fire before this row (node_id=1, flow_id=3)' \
  sender "${pf}_timeout_reject.sender.csv" "${pf}_timeout_reject.dcqcn.csv" 5000
expect_reject "lost-timeout, trace mode (M1)" \
  'REJECT: sender: line 3: pending RoCE timeout at 2500 did not fire before this row (node_id=1, flow_id=3)' \
  trace "${pf}_timeout_reject.sender.csv" "${pf}_timeout_reject.receiver.csv" \
  "${pf}_timeout_reject.dcqcn.csv" 5000
expect_reject "lost-tick (M2)" \
  'REJECT: sender: line 3: pending RoCE pacing tick at 2000 did not fire before this row (node_id=1, flow_id=3)' \
  sender "${pf}_tick_reject.sender.csv" "${pf}_tick_reject.dcqcn.csv" 5000
expect_reject "lost-tick-at-tail (M2b)" \
  'REJECT: sender: pending RoCE pacing tick at 2000 never fired by stop_time_ns=5000 (node_id=1, flow_id=3)' \
  sender "${pf}_tick_tail_reject.sender.csv" "${pf}_tick_reject.dcqcn.csv" 5000
expect_reject "lost-tick-at-tail, stop inferred (M2b)" \
  'REJECT: sender: pending RoCE pacing tick at 2000 never fired although the log implies stop_time_ns >= 2000 (node_id=1, flow_id=3)' \
  sender "${pf}_tick_tail_reject.sender.csv" "${pf}_tick_reject.dcqcn.csv"
expect_reject "lost-control-tick-of-incomplete-pair (M3)" \
  'REJECT: sender: line 3: pending DCQCN control tick at 10500 did not fire before this row (node_id=1, flow_id=3)' \
  sender "${pf}_control_reject.sender.csv" "${pf}_control_reject.dcqcn.csv" 20000
check_case "pending/lost-timeout twin (M1)" 0 "ACCEPT" \
  sender "${pf}_timeout_twin.sender.csv" "${pf}_timeout_twin.dcqcn.csv" 5000 || true
check_case "pending/lost-timeout twin, trace mode (M1)" 0 "ACCEPT" \
  trace "${pf}_timeout_twin.sender.csv" "${pf}_timeout_twin.receiver.csv" \
  "${pf}_timeout_twin.dcqcn.csv" 5000 || true
check_case "pending/lost-tick twin (M2, M2b)" 0 "ACCEPT" \
  sender "${pf}_tick_twin.sender.csv" "${pf}_tick_twin.dcqcn.csv" 5000 || true
check_case "pending/lost-control-tick twin (M3)" 0 "ACCEPT" \
  sender "${pf}_control_reject.sender.csv" "${pf}_control_twin.dcqcn.csv" 20000 || true

# Executor analogues on roce_trace_timeout_executor_accept (one pair: node 2, flow 1).
xb="$fixture_dir/roce_trace_timeout_executor_accept"
xstop="$(cat "$xb.stop_time_ns")"
# truncate_all <time>: the three logs cut before <time> (a run that lost everything from <time>).
truncate_all() {
  local cut="$1"
  for role in sender receiver dcqcn; do
    awk -F, -v cut="$cut" 'NR == 1 || $1 < cut' "$xb.$role.csv" > "$campaign_tmp/cut.$role.csv"
  done
}
first_timeout="$(awk -F, 'NR > 1 && $7 == "timeout" { print $1; exit }' "$xb.sender.csv")"
truncate_all "$first_timeout"
expect_reject "executor: timeout lost at the tail" \
  "REJECT: sender: pending RoCE timeout at $first_timeout never fired by stop_time_ns=$xstop (node_id=2, flow_id=1)" \
  trace "$campaign_tmp/cut.sender.csv" "$campaign_tmp/cut.receiver.csv" "$campaign_tmp/cut.dcqcn.csv" "$xstop"
restart_tick="$(awk -F, 'NR > 1 && $7 == "timeout" { print $36; exit }' "$xb.sender.csv")"
truncate_all "$restart_tick"
expect_reject "executor: restarted tick lost at the tail" \
  "REJECT: sender: pending RoCE pacing tick at $restart_tick never fired by stop_time_ns=$xstop (node_id=2, flow_id=1)" \
  trace "$campaign_tmp/cut.sender.csv" "$campaign_tmp/cut.receiver.csv" "$campaign_tmp/cut.dcqcn.csv" "$xstop"
# Mid-trace tick: cut out a tick that sends nothing, followed by an ACK or NACK with no controller
# row in between; the ACK or NACK then starts from the tick's before-state (consistent logs).
read -r tick_line tick_time next_line <<<"$(awk -F, '
  FNR == NR { if (FNR > 1) dtime[$1] = 1; next }
  FNR > 1 {
    if (cand && ($7 == "ack" || $7 == "nack")) {
      clean = 1
      for (t in dtime) if (t + 0 >= ctime && t + 0 <= $1 + 0) { clean = 0; break }
      if (clean) { print cline, ctime, FNR; exit }
    }
    cand = ($7 == "tick" && $15 == 0); cline = FNR; ctime = $1
  }' "$xb.dcqcn.csv" "$xb.sender.csv")"
awk -F, -v OFS=, -v cut="$tick_line" -v next_row="$next_line" '
  NR == cut { for (i = 20; i <= 28; i++) before[i] = $i; next }
  NR == next_row { for (i = 20; i <= 28; i++) $i = before[i] }
  { print }' "$xb.sender.csv" > "$campaign_tmp/tick-cut.sender.csv"
expect_reject "executor: tick lost mid-trace" \
  "REJECT: sender: line $((next_line - 1)): pending RoCE pacing tick at $tick_time did not fire before this row (node_id=2, flow_id=1)" \
  trace "$campaign_tmp/tick-cut.sender.csv" "$xb.receiver.csv" "$xb.dcqcn.csv" "$xstop"
# Mid-trace control tick: drop the controller log from a control tick that leaves the rate as it
# was (so no status changes) and at whose time no tick sends; the next sender row exposes it.
read -r control_time sender_line <<<"$(awk -F, '
  FNR == NR { if (FNR > 1) { emit[$1] = $15; row[FNR] = $1 }; last = FNR; next }
  FNR > 1 && $7 == "control" && $21 == $30 && emit[$1] != 1 {
    for (i = 2; i <= last; i++) if (row[i] + 0 > $1 + 0) { print $1, i; exit }
  }' "$xb.sender.csv" "$xb.dcqcn.csv")"
awk -F, -v cut="$control_time" 'NR == 1 || $1 < cut' "$xb.dcqcn.csv" > "$campaign_tmp/control-cut.dcqcn.csv"
expect_reject "executor: control tick lost mid-trace" \
  "REJECT: sender: line $sender_line: pending DCQCN control tick at $control_time did not fire before this row (node_id=2, flow_id=1)" \
  trace "$xb.sender.csv" "$xb.receiver.csv" "$campaign_tmp/control-cut.dcqcn.csv" "$xstop"

# --- Amendment 1: class_paused (host-link PFC backpressure) ------------------------------------
# Amended sender layout (Amendments 1 and 3): column 8 is class_paused, column 9 data_class; the
# schema's columns 8-37 become 10-39 (15 rate_bps, 16 input_acknowledgment, 17 emitted, 18-21
# emitted_*, 22-30 before_*, 31-39 after_*: 35 after_credit_quanta, 37 after_pacer,
# 38 after_next_tick_ns, 39 after_status). These fixtures carry their PFC log (--pfc).
# Logs without class_paused (writers before Amendment 1) read class_paused = 0 throughout.
# mutate_amended <label> <sender.csv> <dcqcn.csv> <pfc.csv> <stop> <awk program on the sender CSV>
#   <expected REJECT line>
mutate_amended() {
  local label="$1"
  local source="$2"
  local dcqcn="$3"
  local pfc="$4"
  local stop="$5"
  local program="$6"
  local expected_output="$7"
  local mutated="$campaign_tmp/amended.csv"
  awk -F, -v OFS=, "$program" "$source" > "$mutated"
  mutations=$((mutations + 1))
  if check_case "sender/$label" 1 "$expected_output" \
      sender "$mutated" "$dcqcn" --pfc "$pfc" "$stop"; then
    mutations_caught=$((mutations_caught + 1))
  fi
}
paused="$fixture_dir/roce_sender_paused_accept.csv"
paused_dcqcn="$fixture_dir/roce_sender_paused_accept.dcqcn.csv"
paused_pfc="$fixture_dir/roce_sender_paused_accept.pfc.csv"
paused_stop=8000
check_case "roce_sender_paused_accept.csv" 0 "ACCEPT" \
  sender "$paused" "$paused_dcqcn" --pfc "$paused_pfc" "$paused_stop" || true
check_case "roce_sender_paused_accept.csv (stop inferred)" 0 "ACCEPT" \
  sender "$paused" "$paused_dcqcn" --pfc "$paused_pfc" || true
# Rows (NR): 2 t1000 flow 3 sends psn 0; 3 t1000 flow 5 sends psn 0; 4 t2000 flow 3 tick finds
# the class paused: parks, no credit; 5 t2000 flow 5 likewise; 6 t2500 ACK 1000 restarts flow 3
# on the grid (3000), as any ACK restart; 7 t3000 flow 3's restarted tick parks again; 8 t5000
# flow 5 timeout rewinds and restarts at 6000; 9 t6000 flow 5's restarted tick parks again.
mutate_amended "paused-tick-emits" "$paused" "$paused_dcqcn" "$paused_pfc" "$paused_stop" \
  'NR == 4 { $17 = 1; $18 = 1000; $19 = 1000; $20 = 0; $21 = 17 } { print }' \
  'REJECT: sender: line 4: RoCE paused tick credits or emits'
mutate_amended "paused-tick-adds-credit" "$paused" "$paused_dcqcn" "$paused_pfc" "$paused_stop" \
  'NR == 4 { $35 = 8000000000000 } { print }' \
  'REJECT: sender: line 4: RoCE sender after-state mismatch'
mutate_amended "paused-tick-reads-rate" "$paused" "$paused_dcqcn" "$paused_pfc" "$paused_stop" \
  'NR == 4 { $15 = 8000000000 } { print }' \
  'REJECT: sender: line 4: RoCE paused tick credits or emits'
mutate_amended "paused-tick-flag-cleared" "$paused" "$paused_dcqcn" "$paused_pfc" "$paused_stop" \
  'NR == 4 { $8 = 0 } { print }' \
  'REJECT: sender: line 4: RoCE tick rate present iff the tick credits'
mutate_amended "paused-tick-flag-cleared-with-rate" "$paused" "$paused_dcqcn" "$paused_pfc" "$paused_stop" \
  'NR == 4 { $8 = 0; $15 = 8000000000 } { print }' \
  'REJECT: sender: line 4: RoCE emission mismatch'
mutate_amended "paused-tick-keeps-pacer-armed" "$paused" "$paused_dcqcn" "$paused_pfc" "$paused_stop" \
  'NR == 4 { $37 = "armed"; $38 = 3000; $39 = "scheduled" } { print }' \
  'REJECT: sender: line 4: RoCE sender after-state mismatch'
mutate_amended "class-paused-on-ack" "$paused" "$paused_dcqcn" "$paused_pfc" "$paused_stop" \
  'NR == 6 { $8 = 1 } { print }' \
  'REJECT: sender: line 6: RoCE class_paused set on a non-tick row'
mutate_amended "rewind-while-paused-not-restarting" "$paused" "$paused_dcqcn" "$paused_pfc" "$paused_stop" \
  'NR == 8 { $37 = "parked"; $38 = ""; $39 = "blocked" } { print }' \
  'REJECT: sender: line 8: RoCE sender after-state mismatch'
mutate_amended "class-paused-not-a-bit" "$paused" "$paused_dcqcn" "$paused_pfc" "$paused_stop" \
  'NR == 4 { $8 = 2 } { print }' \
  "REJECT: sender: line 4: invalid bit: '2'"
mutate_amended "paused-tick-lost" "$paused" "$paused_dcqcn" "$paused_pfc" "$paused_stop" \
  'NR == 4 { for (i = 22; i <= 30; i++) before[i] = $i; next } NR == 6 { for (i = 22; i <= 30; i++) $i = before[i] } { print }' \
  'REJECT: sender: line 5: pending RoCE pacing tick at 2000 did not fire before this row (node_id=1, flow_id=3)'

# --- Amendment 2: resume rows (a PFC RESUME restarts pause-parked queue pairs) -----------------
# Amended layout as above. A resume row is a D3 restart of a pair parked by a paused tick, at the
# RESUME's event key (phase 0: a PFC frame arrival); several may share one key, for distinct
# flows of one node in flow_id order.
resume="$fixture_dir/roce_sender_resume_accept.csv"
resume_dcqcn="$fixture_dir/roce_sender_resume_accept.dcqcn.csv"
resume_pfc="$fixture_dir/roce_sender_resume_accept.pfc.csv"
resume_stop=8000
check_case "roce_sender_resume_accept.csv" 0 "ACCEPT" \
  sender "$resume" "$resume_dcqcn" --pfc "$resume_pfc" "$resume_stop" || true
check_case "roce_sender_resume_accept.csv (stop inferred)" 0 "ACCEPT" \
  sender "$resume" "$resume_dcqcn" --pfc "$resume_pfc" || true
# Rows (NR): 2-4 t1000/1000/1500 flows 3, 5, 7 send psn 0 (flow 7's grid starts at 1500);
# 5-7 their next ticks find the class paused and park; 8-10 one RESUME at 4500 restarts flows 3, 5
# and 7 (three rows, one key): 3 and 5 at 5000, 7 at 5500; 11-12 t5000 flows 3 and 5 send their
# last packet and park; 13 t5500 flow 7's tick finds the class paused again; 14 a RESUME at 7600
# restarts flow 7 at 8500, beyond stop 8000: stopped.
resume_order='REJECT: sender: line 9: RoCE resume rows sharing an event key must be of one node in strictly increasing flow_id order'
mutate_amended "resume-of-unpaused-pair" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR == 14 { print "7600,0,9,1,1,3,resume,0,3,1000,2000,1000,1000,0,,,0,,,,,2000,0,2000,2,0,,parked,,blocked,2000,0,2000,2,0,,parked,,blocked" } { print }' \
  'REJECT: sender: line 14: RoCE resume of a queue pair not parked by a pause (node_id=1, flow_id=3)'
mutate_amended "two-resume-rows-for-one-flow" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR == 8 { print } { print }' \
  "$resume_order"
mutate_amended "resume-rows-out-of-flow-order" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR == 8 { held = $0; next } NR == 9 { print; print held; next } { print }' \
  "$resume_order"
mutate_amended "resume-rows-of-two-nodes" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR == 9 { $5 = 2 } { print }' \
  "$resume_order"
mutate_amended "resume-at-wrong-grid-point" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR == 8 { $38 = 6000 } { print }' \
  'REJECT: sender: line 8: RoCE sender after-state mismatch'
mutate_amended "resume-restarts-at-the-resume-instant" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR == 10 { $38 = 4500 } { print }' \
  'REJECT: sender: line 10: RoCE sender after-state mismatch'
mutate_amended "resume-armed-beyond-stop" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR == 14 { $37 = "armed"; $39 = "scheduled" } { print }' \
  'REJECT: sender: line 14: RoCE pacer stop decision contradicts stop_time_ns=8000 (tick 8500)'
mutate_amended "resume-with-rate" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR == 8 { $15 = 8000000000 } { print }' \
  'REJECT: sender: line 8: RoCE resume row credits, emits or carries an acknowledgment'
mutate_amended "resume-with-acknowledgment" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR == 8 { $16 = 1000 } { print }' \
  'REJECT: sender: line 8: RoCE resume row credits, emits or carries an acknowledgment'
mutate_amended "resume-with-class-paused" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR == 8 { $8 = 1 } { print }' \
  'REJECT: sender: line 8: RoCE class_paused set on a non-tick row'
mutate_amended "resume-typed-phase" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR >= 8 && NR <= 10 { $2 = 1 } { print }' \
  'REJECT: sender: line 8: RoCE resume must have phase 0'
mutate_amended "resume-lost" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR != 8 { print }' \
  'REJECT: sender: line 10: RoCE sender state discontinuity (node_id=1, flow_id=3)'

# --- Amendment 3: pauses and resumes against the host PFC records ------------------------------
# PFC CSV columns (pfc_transitions_csv): 1 time_ns, 2-4 key, 5 node_id, 6 queue_id, 7 kind,
# 8 controlled_link, 9 controller, 10 priority, 20 control_action, 21 before_controllers,
# 22 after_controllers. A host's class is paused while its controller set is non-empty; a host
# RESUME is a control row that empties it.
amended_gbn="$fixture_dir/roce_sender_gbn_amended_accept.csv"
amended_gbn_pfc="$fixture_dir/roce_sender_gbn_amended_accept.pfc.csv"
check_case "roce_sender_gbn_amended_accept.csv (no host PFC: header-only PFC log)" 0 "ACCEPT" \
  sender "$amended_gbn" "$gbn_dcqcn" --pfc "$amended_gbn_pfc" "$gbn_stop" || true

# expect_warrant_reject <label> <expected REJECT line> <checker args...>
expect_warrant_reject() {
  local label="$1"
  local expected_output="$2"
  shift 2
  mutations=$((mutations + 1))
  if check_case "warrant/$label" 1 "$expected_output" "$@"; then
    mutations_caught=$((mutations_caught + 1))
  fi
}
# mutate_pfc <label> <sender.csv> <dcqcn.csv> <pfc.csv> <stop> <awk program on the PFC CSV>
#   <expected REJECT line>
mutate_pfc() {
  local label="$1"
  local sender="$2"
  local dcqcn="$3"
  local source="$4"
  local stop="$5"
  local program="$6"
  local expected_output="$7"
  local mutated="$campaign_tmp/pfc.csv"
  awk -F, -v OFS=, "$program" "$source" > "$mutated"
  mutations=$((mutations + 1))
  if check_case "warrant/$label" 1 "$expected_output" \
      sender "$sender" "$dcqcn" --pfc "$mutated" "$stop"; then
    mutations_caught=$((mutations_caught + 1))
  fi
}

# The review's round-2 probes (evidence/P15/leanguard-review/round2), data_class 3 inserted,
# against roce_sender_resume_accept.pfc.csv (PAUSE 1800, RESUME 4500, PAUSE 5200, RESUME 7600).
expect_warrant_reject "review B1: flow 3's resume row and restarted tick missing" \
  'REJECT: pfc: line 3: host RESUME of data_class 3 at node 1 did not restart pause-parked queue pair (flow_id=3)' \
  sender "$fixture_dir/roce_review_b1.sender.csv" "$fixture_dir/roce_review_b1.dcqcn.csv" \
  --pfc "$resume_pfc" "$resume_stop"
expect_warrant_reject "review B2: B1, masked by a later ACK restart" \
  'REJECT: pfc: line 3: host RESUME of data_class 3 at node 1 did not restart pause-parked queue pair (flow_id=3)' \
  sender "$fixture_dir/roce_review_b2.sender.csv" "$fixture_dir/roce_review_b2.dcqcn.csv" \
  --pfc "$resume_pfc" "$resume_stop"
expect_warrant_reject "review S1: a resume, and a pause, at instants no PFC record claims" \
  'REJECT: sender: line 14: RoCE resume row without a host RESUME of data_class 3 at node 1 at this event key' \
  sender "$fixture_dir/roce_review_s1.sender.csv" "$resume_dcqcn" --pfc "$resume_pfc" "$resume_stop"
mutate_amended "resume-row-missing-for-one-pair" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR != 9 { print }' \
  'REJECT: pfc: line 3: host RESUME of data_class 3 at node 1 did not restart pause-parked queue pair (flow_id=5)'
mutate_pfc "pause-without-warrant" "$paused" "$paused_dcqcn" "$paused_pfc" "$paused_stop" \
  'NR == 2 { $1 = 2500 } { print }' \
  'REJECT: sender: line 4: RoCE paused tick while data_class 3 is not paused at node 1'
mutate_pfc "unpaused-tick-while-paused" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR == 4 { $1 = 4900 } { print }' \
  'REJECT: sender: line 11: RoCE unpaused tick while data_class 3 is paused at node 1'
mutate_pfc "resume-at-another-key" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR == 3 { $1 = 4400 } { print }' \
  'REJECT: sender: line 8: RoCE resume row without a host RESUME of data_class 3 at node 1 at this event key'
mutate_pfc "resume-at-another-node" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR == 3 { $5 = 2 } NR == 4 { $21 = 9 } { print }' \
  'REJECT: sender: line 8: RoCE resume row without a host RESUME of data_class 3 at node 1 at this event key'
mutate_pfc "resume-of-another-class" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR == 3 { $10 = 4 } NR == 4 { $21 = 9 } { print }' \
  'REJECT: sender: line 8: RoCE resume row without a host RESUME of data_class 3 at node 1 at this event key'
mutate_pfc "partial-resume-leaves-class-paused" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR == 2 { print; print "1900,0,8,0,1,0,control,10,8,3,,,,,,,,,,pause,9,8;9"; next } NR == 3 { $21 = "8;9"; $22 = "8" } NR == 4 { $21 = "8"; $22 = "8;9" } NR == 5 { $21 = "8;9"; $22 = "8" } { print }' \
  'REJECT: sender: line 8: RoCE resume row without a host RESUME of data_class 3 at node 1 at this event key'
mutate_amended "data-class-out-of-range" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR == 2 { $9 = 8 } { print }' \
  "REJECT: sender: line 2: data_class exceeds 7: '8'"
mutate_amended "data-class-discontinuity" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR == 5 { $9 = 4 } { print }' \
  'REJECT: sender: line 5: RoCE data_class discontinuity (node_id=1, flow_id=3)'
# Input rules: an Amendment 3 log needs its PFC log; pause and resume rows need Amendment 3.
expect_warrant_reject "amended-log-without-pfc-log" \
  'REJECT: sender: the log carries data_class (Amendment 3); pass its PFC log with --pfc' \
  sender "$resume" "$resume_dcqcn" "$resume_stop"
awk -F, -v OFS=, '{ out = $1; for (i = 2; i <= NF; i++) if (i != 9) out = out OFS $i; print out }' \
  "$resume" > "$campaign_tmp/amendment2-layout.csv"
expect_warrant_reject "pause-rows-without-data-class" \
  'REJECT: sender: line 5: class_paused and resume rows need the data_class column and the PFC log (Amendment 3)' \
  sender "$campaign_tmp/amendment2-layout.csv" "$resume_dcqcn" "$resume_stop"
expect_warrant_reject "pfc-log-without-data-class" \
  'REJECT: sender: --pfc needs a sender log with the data_class column (Amendment 3)' \
  sender "$gbn" "$gbn_dcqcn" --pfc "$amended_gbn_pfc" "$gbn_stop"

echo "P10c RoCE campaign checks: $checked; mutations caught: $mutations_caught/$mutations"
exit "$failures"
