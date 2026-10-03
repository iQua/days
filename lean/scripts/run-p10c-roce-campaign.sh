#!/usr/bin/env bash
# P15 RoCE queue-pair LeanGuard campaign (schema Amendment 6 since P16: ECN echo on ACKs and
# NACKs, no CNP, and the Mellanox-form controller's rows joined to the sender rows that make
# them): accept fixtures, committed reject fixtures, and awk mutations of the accept fixtures,
# each with its exact expected verdict.
#
# Optional: ROCE_TRACE_DIR=<dir> also checks every executor trace triple
# <name>.roce_sender.csv / <name>.roce_receiver.csv / <name>.dcqcn.csv found there
# (with <name>.pfc.csv passed as --pfc and <name>.stop_time_ns holding the image's stop time,
# when present). Those must ACCEPT.
# Optional: ADE_TRACE_DIR=<dir> does the same for logs with the Amendment 1-3 columns, adding
# <name>.pfc.csv (--pfc); sender logs above ADE_TRACE_MAX_BYTES (default 64 MiB) are skipped.
#
# Receiver CSV columns (roce_receiver_transitions_csv, Amendment 6):
#   1 time_ns  2 event_phase  3 event_origin_node  4 event_origin_sequence  5 node_id  6 flow_id
#   7 total_bytes  8 ack_every_packets  9 nack_interval_ns  10 duplicate_ack  11 ack_size_bytes
#   12 packet_psn  13 packet_bytes  14 packet_sent_time_ns  15 packet_retransmission
#   16 packet_ce  17 action  18 feedback_acknowledgment  19 feedback_payload  20 feedback_ce_echo
#   21 before_expected_psn  22 before_packets_since_ack  23 before_last_nack_psn
#   24 before_last_nack_time_ns
#   25 after_expected_psn  26 after_packets_since_ack  27 after_last_nack_psn
#   28 after_last_nack_time_ns
#
# Sender CSV columns (roce_sender_transitions_csv without the Amendment 1/3 columns, as the
# unamended hand fixtures have them; Amendment 6's input_ce_echo is column 15):
#   1 time_ns  2 event_phase  3 event_origin_node  4 event_origin_sequence  5 node_id  6 flow_id
#   7 kind  8 mtu_bytes  9 total_bytes  10 pacing_interval_ns  11 first_pacing_time_ns  12 rto_ns
#   13 rate_bps  14 input_acknowledgment  15 input_ce_echo  16 emitted  17 emitted_psn
#   18 emitted_bytes  19 emitted_retransmission  20 emitted_payload
#   21 before_next_psn  22 before_snd_una  23 before_bytes_emitted  24 before_packets_emitted
#   25 before_credit_quanta  26 before_rto_deadline_ns  27 before_pacer  28 before_next_tick_ns
#   29 before_status
#   30 after_next_psn  31 after_snd_una  32 after_bytes_emitted  33 after_packets_emitted
#   34 after_credit_quanta  35 after_rto_deadline_ns  36 after_pacer  37 after_next_tick_ns
#   38 after_status
# The executor writes the amended layout (Amendments 1, 3 and 6; see the Amendment 1 section).
#
# DCQCN CSV columns: as in run-p10c-dcqcn-campaign.sh (dcqcn_transitions_csv, the P16 schema).
# A queue pair's DCQCN rows share the event key of the sender row whose transition made them.
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
  # Fix round 3 (orchestrator ruling): the checker requires the full P16 sender schema unless told
  # otherwise. Most committed sender fixtures predate P16's window and initial-rate columns, so
  # sender and trace cases pass --legacy-sender-format unless the caller sets
  # sender_format=current for a current-format log.
  local format=()
  if [[ "${sender_format:-legacy}" != current && ( "$1" == sender || "$1" == trace ) ]]; then
    format=(--legacy-sender-format)
  fi

  set +e
  actual_output="$("$checker" "$@" ${format[@]+"${format[@]}"} 2>&1)"
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
# Rows (NR): 2 t100 f3 in-order below cadence; 3 t150 f5 first NACK; 4 t200 f3 CE, cadence ACK
# echoing it; 5 t250 f5 NACK at the interval edge; 6 t260 f5 in-order; 7 t270 f5 NACK for a new
# frontier; 8 t280 f5 silent duplicate; 9 t300 f3 first NACK, echoing CE; 10 t350 f5 in-order;
# 11 t360 f5 CE, ACK at the end echoing it; 12 t400 f3 NACK suppressed; 13 t600 f3 in-order;
# 14 t700 f3 CE, cadence ACK echoing it; 15 t800 f3 duplicate ACK; 16 t900 f3 last ACK.
rules="$fixture_dir/roce_receiver_rules_accept.csv"

mutate_receiver "nack-inside-suppression-interval" "$rules" \
  'NR == 12 { $17 = "nack"; $18 = 2000; $19 = 100; $28 = 400 } { print }' \
  'REJECT: line 12: RoCE receiver action mismatch'
mutate_receiver "nack-suppressed-at-interval-edge" "$rules" \
  'NR == 5 { $17 = "nack_suppressed"; $18 = ""; $19 = ""; $28 = 150 } { print }' \
  'REJECT: line 5: RoCE receiver action mismatch'
mutate_receiver "nack-suppressed-for-new-frontier" "$rules" \
  'NR == 7 { $17 = "nack_suppressed"; $18 = ""; $19 = ""; $26 = 1; $27 = 0; $28 = 250 } { print }' \
  'REJECT: line 7: RoCE receiver action mismatch'
mutate_receiver "missing-ack-at-end" "$rules" \
  'NR == 16 { $17 = "none"; $18 = ""; $19 = ""; $26 = 1 } { print }' \
  'REJECT: line 16: RoCE receiver action mismatch'
mutate_receiver "ack-before-cadence" "$rules" \
  'NR == 2 { $17 = "ack"; $18 = 1000; $19 = 4; $26 = 0 } { print }' \
  'REJECT: line 2: RoCE receiver action mismatch'
mutate_receiver "missing-cadence-ack" "$rules" \
  'NR == 14 { $17 = "none"; $18 = ""; $19 = "" } { print }' \
  'REJECT: line 14: RoCE receiver action mismatch'
mutate_receiver "duplicate-acked-when-disabled" "$rules" \
  'NR == 8 { $17 = "duplicate_ack"; $18 = 1000; $19 = 40 } { print }' \
  'REJECT: line 8: RoCE receiver action mismatch'
mutate_receiver "duplicate-dropped-when-enabled" "$rules" \
  'NR == 15 { $17 = "none"; $18 = ""; $19 = "" } { print }' \
  'REJECT: line 15: RoCE receiver action mismatch'
mutate_receiver "ack-above-frontier" "$rules" \
  'NR == 4 { $18 = 3000 } { print }' \
  'REJECT: line 4: RoCE feedback acknowledgment mismatch'
mutate_receiver "nack-not-at-frontier" "$rules" \
  'NR == 9 { $18 = 3000 } { print }' \
  'REJECT: line 9: RoCE feedback acknowledgment mismatch'
mutate_receiver "frontier-not-advanced" "$rules" \
  'NR == 13 { $25 = 2000 } { print }' \
  'REJECT: line 13: RoCE receiver after-state mismatch'
mutate_receiver "frontier-advanced-on-out-of-order" "$rules" \
  'NR == 9 { $25 = 4000 } { print }' \
  'REJECT: line 9: RoCE receiver after-state mismatch'
mutate_receiver "cadence-not-reset-by-nack" "$rules" \
  'NR == 7 { $26 = 2 } { print }' \
  'REJECT: line 7: RoCE receiver after-state mismatch'
mutate_receiver "suppressed-nack-moves-mark" "$rules" \
  'NR == 12 { $28 = 400 } { print }' \
  'REJECT: line 12: RoCE receiver after-state mismatch'
# Amendment 6 (P16 ruling D5): an ACK or NACK echoes the CE mark of the packet that triggered it.
echo_mismatch="RoCE feedback CE echo is not the arriving packet's CE mark (or is present without feedback)"
mutate_receiver "echo-without-ce" "$rules" \
  'NR == 4 { $16 = 0 } { print }' \
  "REJECT: line 4: $echo_mismatch"
mutate_receiver "ce-not-echoed" "$rules" \
  'NR == 14 { $20 = 0 } { print }' \
  "REJECT: line 14: $echo_mismatch"
mutate_receiver "nack-echo-missing" "$rules" \
  'NR == 9 { $20 = "" } { print }' \
  "REJECT: line 9: $echo_mismatch"
mutate_receiver "echo-without-feedback" "$rules" \
  'NR == 2 { $20 = 0 } { print }' \
  "REJECT: line 2: $echo_mismatch"
mutate_receiver "echo-on-suppressed-nack" "$rules" \
  'NR == 12 { $20 = 1 } { print }' \
  "REJECT: line 12: $echo_mismatch"
mutate_receiver "ack-allocated-out-of-order" "$rules" \
  'NR == 4 { $19 = 1 } { print }' \
  'REJECT: line 4: RoCE receiver payloads out of allocation order (node_id=4)'
mutate_receiver "state-splice" "$rules" \
  'NR == 10 { $21 = 1500 } { print }' \
  'REJECT: line 10: RoCE receiver state discontinuity (node_id=4, flow_id=5)'
mutate_receiver "config-splice" "$rules" \
  'NR == 14 { $9 = 999 } { print }' \
  'REJECT: line 14: RoCE receiver config discontinuity (node_id=4, flow_id=3)'
mutate_receiver "initial-state-splice" "$rules" \
  'NR == 2 { $22 = 1; $26 = 2 } { print }' \
  'REJECT: line 2: RoCE receiver first state is not initial (node_id=4, flow_id=3)'
mutate_receiver "packet-beyond-total" "$rules" \
  'NR == 16 { $13 = 2000 } { print }' \
  'REJECT: line 16: RoCE data packet extends beyond the queue pair'"'"'s total bytes'
mutate_receiver "arrival-before-send" "$rules" \
  'NR == 2 { $14 = 101 } { print }' \
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
  'NR == 2 { $14 = "18446744073709551616" } { print }' \
  "REJECT: line 2: value exceeds u64: '18446744073709551616'"
mutate_receiver "half-blank-nack-mark" "$rules" \
  'NR == 4 { $27 = 7 } { print }' \
  'REJECT: line 4: after_last_nack_psn and after_last_nack_time_ns must be both present or both blank'

# --- Sender (joined with the DCQCN controller log) --------------------------------------------
gbn="$fixture_dir/roce_sender_gbn_accept.csv"
gbn_dcqcn="$fixture_dir/roce_sender_gbn_accept.dcqcn.csv"
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
# 5 t2800 NACK 1000 (= snd_una: rewinds), echoing CE: the controller's first feedback (alpha
# interval 500 ns, decrease interval 699 ns, g = 1/2), so a cut is due at 3500; 6 t3000
# retransmission at 8 Gb/s; 7 t4000 tick, which applies the cut first (8 to 6 Gb/s) and so sends
# nothing; no status was recomputed between rows 6 and 7; 8 t5000 last fresh packet, parks;
# 9 t6000 ACK 2000; 10 t11000 timeout, restart on the grid at 12000; 11 t12000 retransmission,
# parks; 12 t13000 ACK 3000, finished, freezing the controller. DCQCN rows (NR): 2 the feedback at
# 2800; 3 the cut, at the tick at 4000 (bound 4001); 4 the freeze at 13000.
mutate_sender "wrong-rewind-point-nack" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 5 { $30 = 2000 } { print }' \
  'REJECT: sender: line 5: RoCE sender after-state mismatch'
mutate_sender "wrong-rewind-point-timeout" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 10 { $30 = 3000 } { print }' \
  'REJECT: sender: line 10: RoCE sender after-state mismatch'
mutate_sender "stale-nack-applied" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 5 { $14 = 0 } { print }' \
  'REJECT: sender: line 5: RoCE sender after-state mismatch'
mutate_sender "stale-ack-applied" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 4 { $14 = 0 } { print }' \
  'REJECT: sender: line 4: RoCE sender after-state mismatch'
mutate_sender "ack-rewinds" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 4 { $30 = 1000 } { print }' \
  'REJECT: sender: line 4: RoCE sender after-state mismatch'
mutate_sender "ack-above-frontier" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 4 { $14 = 3000 } { print }' \
  "REJECT: sender: line 4: RoCE acknowledgment above the sender's high-water mark"
mutate_sender "credit-not-charged-on-retransmission" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 6 { $34 = 8000000000000 } { print }' \
  'REJECT: sender: line 6: RoCE sender after-state mismatch'
mutate_sender "retransmission-counted-as-fresh" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 6 { $32 = 3000; $33 = 3 } { print }' \
  'REJECT: sender: line 6: RoCE sender after-state mismatch'
mutate_sender "retransmission-bit-cleared" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 6 { $19 = 0 } { print }' \
  'REJECT: sender: line 6: RoCE emission mismatch'
mutate_sender "retransmission-from-wrong-psn" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 11 { $17 = 1000 } { print }' \
  'REJECT: sender: line 11: RoCE emission mismatch'
mutate_sender "emission-without-credit" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 7 { $16 = 1; $17 = 2000; $18 = 1000; $19 = 0; $20 = 19 } { print }' \
  'REJECT: sender: line 7: RoCE emission mismatch'
mutate_sender "emission-not-packet-size" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 2 { $18 = 999 } { print }' \
  'REJECT: sender: line 2: RoCE emission mismatch'
mutate_sender "tick-rate-not-controller-rate" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 7 { $13 = 8000000000 } { print }' \
  "REJECT: sender: line 7: RoCE tick rate differs from the DCQCN controller's current rate"
# The lazy controller recomputes nothing between the pair's transitions (P16 ruling D2).
mutate_sender "status-recomputed-between-transitions" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 7 { $29 = "blocked" } { print }' \
  'REJECT: sender: line 7: RoCE sender state discontinuity (node_id=1, flow_id=3)'
mutate_sender "timeout-not-rearmed" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 10 { $35 = 11000 } { print }' \
  'REJECT: sender: line 10: RoCE sender after-state mismatch'
mutate_sender "timeout-before-deadline" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 10 { $1 = 10999 } { print }' \
  'REJECT: sender: line 10: RoCE timeout fires without an armed deadline at this time'
mutate_sender "restart-at-rewind-instant" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 10 { $37 = 11000 } { print }' \
  'REJECT: sender: line 10: RoCE sender after-state mismatch'
mutate_sender "rto-not-restarted-by-ack" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 4 { $35 = 6000 } { print }' \
  'REJECT: sender: line 4: RoCE sender after-state mismatch'
mutate_sender "rto-not-disarmed-at-completion" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 12 { $35 = 18000 } { print }' \
  'REJECT: sender: line 12: invalid RoCE sender after-state'
mutate_sender "completion-not-finished" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 12 { $38 = "blocked" } { print }' \
  'REJECT: sender: line 12: invalid RoCE sender after-state'
mutate_sender "tick-of-unarmed-pacer" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 8 { $1 = 4500 } { print }' \
  'REJECT: sender: line 8: RoCE tick of a pacer not armed for this time'
mutate_sender "typed-tick-phase" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 2 { $2 = 0 } { print }' \
  'REJECT: sender: line 2: RoCE pacing tick must have phase 1'
mutate_sender "state-splice" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 9 { $25 = 4000000000001 } { print }' \
  'REJECT: sender: line 9: RoCE sender state discontinuity (node_id=1, flow_id=3)'
mutate_sender "config-splice" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 8 { $12 = 5001 } { print }' \
  'REJECT: sender: line 8: RoCE sender config discontinuity (node_id=1, flow_id=3)'
mutate_sender "initial-state-splice" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 2 { $29 = "blocked" } { print }' \
  'REJECT: sender: line 2: RoCE sender first state is not initial (node_id=1, flow_id=3)'
mutate_sender "duplicate-key" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 3 { $1 = 1000; $2 = 1; $3 = 1; $4 = 0 } { print }' \
  'REJECT: sender: line 3: duplicate or backward canonical event key'
mutate_sender "payload-allocation-order" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 3 { $20 = 5 } { print }' \
  'REJECT: sender: line 3: RoCE sender payloads out of allocation order (node_id=1)'
mutate_sender "u128-credit-bound" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 2 { $25 = "340282366920938463463374607431768211456" } { print }' \
  "REJECT: sender: line 2: value exceeds u128: '340282366920938463463374607431768211456'"
# Amendment 6: the joins between the sender rows and the controller rows.
mutate_sender "echo-without-feedback-row" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 4 { $15 = 1 } { print }' \
  'REJECT: sender: line 4: RoCE ACK or NACK echoes CE on an incomplete queue pair but has no DCQCN feedback row (node_id=1, flow_id=3)'
mutate_sender "feedback-row-without-echo" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 5 { $15 = 0 } { print }' \
  'REJECT: dcqcn: line 2: DCQCN row at a RoCE transition that brings no ECN echo, completion or due rate instant (node_id=1, flow_id=3)'
mutate_sender "echo-on-a-tick" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 3 { $15 = 0 } { print }' \
  'REJECT: sender: line 3: RoCE input_ce_echo present iff the row is an ACK or NACK'
mutate_sender "ack-without-echo-column-value" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 4 { $15 = "" } { print }' \
  'REJECT: sender: line 4: RoCE input_ce_echo present iff the row is an ACK or NACK'
# The cut due at 3500 folded into the freeze row: the controller log stays consistent on its own
# terms (materialization is path-independent), but the tick at 4000 read a rate the controller
# had not cut.
mutate_dcqcn "due-cut-without-row" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 1 { for (i = 1; i <= NF; i++) name[i] = $i } NR == 2 { for (i = 1; i <= NF; i++) after[name[i]] = $i } NR == 3 { for (i = 1; i <= NF; i++) if (name[i] ~ /^(alpha_ticks|increase_fires|decrease_cuts)$/) add[name[i]] = $i; next } NR == 4 { for (i = 1; i <= NF; i++) { if (name[i] ~ /^before_/) $i = after["after_" substr(name[i], 8)]; if (name[i] in add) $i += add[name[i]] } } { print }' \
  'REJECT: sender: line 7: RoCE transition has no DCQCN row although a controller rate instant is due before 4001 (node_id=1, flow_id=3)'
mutate_dcqcn "completion-without-freeze-row" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR != 4 { print }' \
  'REJECT: sender: line 12: RoCE ACK completes the queue pair but has no DCQCN row freezing its controller (node_id=1, flow_id=3)'
mutate_dcqcn "controller-row-at-no-sender-row" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  'NR == 3 { $4 = 2 } { print }' \
  'REJECT: dcqcn: line 3: DCQCN row of a queue pair at no sender row of the pair at its event key (node_id=1, flow_id=3)'
mutate_dcqcn "row-after-the-freeze (D11)" "$gbn" "$gbn_dcqcn" "$gbn_stop" \
  '{ print } END { $1 = 29500; print }' \
  'REJECT: dcqcn: line 5: DCQCN row after the freeze of source (node_id=1, flow_id=3)'

# roce_sender_stopped_accept.csv rows (NR), timeout off, stop 2950: 2 t1000 flow 3 send; 3 t2000
# flow 3 last packet, parks; 4 t2200 flow 7 tick whose next tick (3200) is beyond stop; 5 t2600
# NACK 0 rewinds a parked pacer whose restart (3000) is beyond stop; 6 t2700 ACK 1000, still
# stopped; 7 t2800 ACK 2000 completes a stopped pacer (parked, finished).
mutate_sender "restart-armed-beyond-stop" "$stopped" "$stopped_dcqcn" "$stopped_stop" \
  'NR == 5 { $36 = "armed"; $38 = "scheduled" } { print }' \
  'REJECT: sender: line 5: RoCE pacer stop decision contradicts stop_time_ns=2950 (tick 3000)'
mutate_sender "inconsistent-inferred-stop" "$stopped" "$stopped_dcqcn" "" \
  'NR == 4 { $36 = "armed"; $38 = "blocked" } { print }' \
  'REJECT: sender: no single stop time fits the pacer decisions: tick 3200 (line 4) is armed and tick 3000 (line 5) is stopped'
mutate_sender "finished-pacer-left-stopped" "$stopped" "$stopped_dcqcn" "$stopped_stop" \
  'NR == 7 { $36 = "stopped"; $37 = 3000 } { print }' \
  'REJECT: sender: line 7: invalid RoCE sender after-state'
mutate_sender "rto-armed-while-off" "$stopped" "$stopped_dcqcn" "$stopped_stop" \
  'NR == 2 { $35 = 6000 } { print }' \
  'REJECT: sender: line 2: invalid RoCE sender after-state'
mutate_sender "event-after-stop" "$stopped" "$stopped_dcqcn" 2750 \
  '{ print }' \
  'REJECT: sender: line 7: event after stop_time_ns=2750'
mutate_sender "stopped-tick-still-armed" "$stopped" "$stopped_dcqcn" "$stopped_stop" \
  'NR == 4 { $36 = "armed"; $38 = "blocked" } { print }' \
  'REJECT: sender: line 4: RoCE pacer stop decision contradicts stop_time_ns=2950 (tick 3200)'

# --- Trace: the three logs of one run, and the cross-role invariants ---------------------------
loss_sender="$fixture_dir/roce_trace_loss_accept.sender.csv"
loss_receiver="$fixture_dir/roce_trace_loss_accept.receiver.csv"
loss_dcqcn="$fixture_dir/roce_trace_loss_accept.dcqcn.csv"
loss_stop=50000

check_case "roce_trace_loss_accept" 0 "ACCEPT" \
  trace "$loss_sender" "$loss_receiver" "$loss_dcqcn" "$loss_stop" || true
# Executor traces committed as fixtures (tests/fixtures-style triples with a .stop_time_ns file):
# Scalar full-observation logs of configs/p15/roce_timeout.toml and roce_nack_only.toml, written
# by days-gpu evidence/P15/leanguard/tooling/p15_lg_csvs.rs. Generated at p15/qp a4387f4;
# regenerated at R1's final head 4624d21 (feat/p15 d97cc3e) byte-identically (F6).
# roce_trace_hostpfc_prefix_executor_accept: the logs of configs/p15/hostpfc_incast_lossless.toml
# at p15/hostpfc ade83b4 (p15_lg_csvs_pfc.rs), cut to the rows with time_ns < 1,871,513 (the 10th
# host RESUME, at 1,871,512, and every row up to its instant): 10 complete host PAUSE/RESUME
# cycles. A prefix is checked with --horizon-ns: the log holds exactly the events before it.
# Collective stages over RoCE (schema Amendment 4; P15 LeanGuard part 3), with the run's
# collective progress log (.collective.csv, checked with --collective): at 9bc3c63, R3's
# configs/p15/roce_compute_dag.toml and roce_tcp_mixed_collectives.toml at size = 8000 and 6000
# (days-gpu evidence/P15/leanguard/tooling/p15_lg3_csvs.rs). roce_trace_roce_dag_prefix keeps
# the rows with time_ns < 150,000: 11 stage releases, 4 of them compute-gated roots under host-link
# PFC, and 2 logged stages not yet released; roce_trace_roce_tcp_mixed is the whole run.
for sender_csv in "$fixture_dir"/roce_trace_*_executor_accept.sender.csv; do
  [[ -e "$sender_csv" ]] || continue
  base="${sender_csv%.sender.csv}"
  options=()
  [[ -e "$base.pfc.csv" ]] && options+=(--pfc "$base.pfc.csv")
  [[ -e "$base.horizon_ns" ]] && options+=(--horizon-ns "$(cat "$base.horizon_ns")")
  [[ -e "$base.collective.csv" ]] && options+=(--collective "$base.collective.csv")
  check_case "$(basename "$base")" 0 "ACCEPT" \
    trace "$sender_csv" "$base.receiver.csv" "$base.dcqcn.csv" ${options[@]+"${options[@]}"} \
    "$(cat "$base.stop_time_ns")" || true
done
if [[ -n "${ROCE_TRACE_DIR:-}" ]]; then
  for sender_csv in "$ROCE_TRACE_DIR"/*.roce_sender.csv; do
    [[ -e "$sender_csv" ]] || continue
    base="${sender_csv%.roce_sender.csv}"
    options=()
    [[ -e "$base.pfc.csv" ]] && options+=(--pfc "$base.pfc.csv")
    [[ -e "$base.stop_time_ns" ]] && options+=("$(cat "$base.stop_time_ns")")
    check_case "external/$(basename "$base")" 0 "ACCEPT" \
      trace "$sender_csv" "$base.roce_receiver.csv" "$base.dcqcn.csv" \
      ${options[@]+"${options[@]}"} || true
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
# 1000 echoing CE: the controller's first feedback (alpha interval 1000 ns, decrease interval
# 1299 ns, g = 1/2), so a cut is due at 3900; 5 t3000 psn 2000, parks; 6 t4200 NACK 1000 applies
# the cut (8 to 6 Gb/s), rewinds, restart at 5000 (blocked at 6 Gb/s); 7 t5000 tick without
# credit; 8 t6000 psn 1000 again; 9 t7000 psn 2000 again, parks; 10 t7100 ACK 2000; 11 t8100 ACK
# 3000, finished, freezing the controller. Receiver rows: 2 t1500 psn 0 (CE) -> ACK 1000 echoing
# it; 3 t3500 psn 2000 -> NACK 1000; 4 t6500 psn 1000 -> ACK 2000; 5 t7500 psn 2000 -> ACK 3000.
# Each mutation keeps every log consistent on its own terms.
mutate_trace "ack-consumed-before-receiver-sent-it" receiver \
  'NR == 2 { $1 = 2700 } { print }' \
  'REJECT: sender: line 4: RoCE ACK carries a value and ECN echo no receiver sent before it (flow_id=3, acknowledgment=1000, ce_echo=1)'
mutate_trace "nack-consumed-before-receiver-sent-it" receiver \
  'NR == 3 { $1 = 4300; $28 = 4300 } NR >= 4 { $24 = 4300; $28 = 4300 } { print }' \
  'REJECT: sender: line 6: RoCE NACK carries a value and ECN echo no receiver sent before it (flow_id=3, acknowledgment=1000, ce_echo=0)'
# Amendment 6: the controller reacts only to CE marks a receiver saw and echoed.
mutate_trace "echo-the-receiver-never-sent" receiver \
  'NR == 2 { $16 = 0; $20 = 0 } { print }' \
  'REJECT: sender: line 4: RoCE ACK carries a value and ECN echo no receiver sent before it (flow_id=3, acknowledgment=1000, ce_echo=1)'
mutate_trace "echo-dropped-by-the-sender" sender \
  'NR == 4 { $15 = 0 } { print }' \
  'REJECT: dcqcn: line 2: DCQCN row at a RoCE transition that brings no ECN echo, completion or due rate instant (node_id=1, flow_id=3)'
mutate_trace "data-never-sent" receiver \
  'NR == 4 { $14 = 5000 } { print }' \
  'REJECT: receiver: line 4: RoCE data arrival matches no sender emission (flow_id=3, sent_time_ns=5000)'
mutate_trace "data-differs-from-emission" receiver \
  'NR == 4 { $15 = 0 } { print }' \
  "REJECT: receiver: line 4: RoCE data arrival differs from the sender's emission (flow_id=3, sent_time_ns=6000)"
mutate_trace "emission-delivered-twice" receiver \
  'NR == 5 { $14 = 3000; $15 = 0 } { print }' \
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
  'NR == 5 { $17 = "none"; $18 = ""; $19 = "" } { print }' \
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
      trace "$sender" "$receiver" "$base.dcqcn.csv" --pfc "$base.pfc.csv" \
      "$(cat "$base.stop_time_ns")"; then
    mutations_caught=$((mutations_caught + 1))
  fi
}

mutate_executor "timeout-wrong-rewind-point" timeout sender \
  '$7 == "timeout" && $24 != $25' '$32 = $34' \
  'REJECT: sender: line LINE: RoCE sender after-state mismatch'
mutate_executor "nack-without-rewind" timeout sender \
  '$7 == "nack" && $23 != $16' '$32 = $23' \
  'REJECT: sender: line LINE: RoCE sender after-state mismatch'
mutate_executor "retransmission-not-charged" timeout sender \
  '$18 == 1 && $21 == 1' '$36 = $27' \
  'REJECT: sender: line LINE: RoCE sender after-state mismatch'
mutate_executor "timeout-not-rearmed" timeout sender \
  '$7 == "timeout"' '$37 = $28' \
  'REJECT: sender: line LINE: RoCE sender after-state mismatch'
mutate_executor "nack-inside-suppression-interval" timeout receiver \
  '$17 == "nack_suppressed"' '$17 = "nack"' \
  'REJECT: receiver: line LINE: RoCE receiver action mismatch'
mutate_executor "missing-ack-at-end" timeout receiver \
  '$17 == "ack" && $25 == $7' '$17 = "none"; $18 = ""; $19 = ""' \
  'REJECT: receiver: line LINE: RoCE receiver action mismatch'
mutate_executor "rto-config-spliced-mid-trace" nack_only sender \
  '$7 == "nack"' '$14 = 1000000' \
  'REJECT: sender: line LINE: RoCE sender config discontinuity (node_id=2, flow_id=1)'
mutate_executor "nack-only-suppressed-sent" nack_only receiver \
  '$17 == "nack_suppressed"' '$17 = "nack"' \
  'REJECT: receiver: line LINE: RoCE receiver action mismatch'

# --- Pending events must fire (review H1) -----------------------------------------------------
# A full run executes exactly the events with time <= stop_time_ns (scalar.rs run loop), so a
# pair's pending pacing tick or timeout must appear as a row before any later row of the pair,
# and, at the end, if it lies at or before the stop time. A pair's controller has no events of its
# own since P16 (ruling D2); a rate instant it skipped is the join's "due" rule above.
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
check_case "pending/lost-timeout twin (M1)" 0 "ACCEPT" \
  sender "${pf}_timeout_twin.sender.csv" "${pf}_timeout_twin.dcqcn.csv" 5000 || true
check_case "pending/lost-timeout twin, trace mode (M1)" 0 "ACCEPT" \
  trace "${pf}_timeout_twin.sender.csv" "${pf}_timeout_twin.receiver.csv" \
  "${pf}_timeout_twin.dcqcn.csv" 5000 || true
check_case "pending/lost-tick twin (M2, M2b)" 0 "ACCEPT" \
  sender "${pf}_tick_twin.sender.csv" "${pf}_tick_twin.dcqcn.csv" 5000 || true

# Executor analogues on roce_trace_timeout_executor_accept (one pair: node 2, flow 1). The
# executor writes the amended sender layout (Amendments 1, 3 and 6), so its header-only PFC log
# comes along.
xb="$fixture_dir/roce_trace_timeout_executor_accept"
xstop="$(cat "$xb.stop_time_ns")"
# truncate_all <time>: the four logs cut before <time> (a run that lost everything from <time>).
truncate_all() {
  local cut="$1"
  for role in sender receiver dcqcn pfc; do
    awk -F, -v cut="$cut" 'NR == 1 || $1 < cut' "$xb.$role.csv" > "$campaign_tmp/cut.$role.csv"
  done
}
first_timeout="$(awk -F, 'NR > 1 && $7 == "timeout" { print $1; exit }' "$xb.sender.csv")"
truncate_all "$first_timeout"
expect_reject "executor: timeout lost at the tail" \
  "REJECT: sender: pending RoCE timeout at $first_timeout never fired by stop_time_ns=$xstop (node_id=2, flow_id=1)" \
  trace "$campaign_tmp/cut.sender.csv" "$campaign_tmp/cut.receiver.csv" "$campaign_tmp/cut.dcqcn.csv" \
  --pfc "$campaign_tmp/cut.pfc.csv" "$xstop"
restart_tick="$(awk -F, 'NR > 1 && $7 == "timeout" { print $39; exit }' "$xb.sender.csv")"
truncate_all "$restart_tick"
expect_reject "executor: restarted tick lost at the tail" \
  "REJECT: sender: pending RoCE pacing tick at $restart_tick never fired by stop_time_ns=$xstop (node_id=2, flow_id=1)" \
  trace "$campaign_tmp/cut.sender.csv" "$campaign_tmp/cut.receiver.csv" "$campaign_tmp/cut.dcqcn.csv" \
  --pfc "$campaign_tmp/cut.pfc.csv" "$xstop"
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
    cand = ($7 == "tick" && $18 == 0); cline = FNR; ctime = $1
  }' "$xb.dcqcn.csv" "$xb.sender.csv")"
awk -F, -v OFS=, -v cut="$tick_line" -v next_row="$next_line" '
  NR == cut { for (i = 23; i <= 31; i++) before[i] = $i; next }
  NR == next_row { for (i = 23; i <= 31; i++) $i = before[i] }
  { print }' "$xb.sender.csv" > "$campaign_tmp/tick-cut.sender.csv"
expect_reject "executor: tick lost mid-trace" \
  "REJECT: sender: line $((next_line - 1)): pending RoCE pacing tick at $tick_time did not fire before this row (node_id=2, flow_id=1)" \
  trace "$campaign_tmp/tick-cut.sender.csv" "$xb.receiver.csv" "$xb.dcqcn.csv" --pfc "$xb.pfc.csv" "$xstop"
# --- Amendment 1: class_paused (host-link PFC backpressure) ------------------------------------
# Amended sender layout (Amendments 1, 3 and 6; the executor's): column 8 is class_paused,
# column 9 data_class; then 10 mtu_bytes, 11 total_bytes, 12 pacing_interval_ns,
# 13 first_pacing_time_ns, 14 rto_ns, 15 rate_bps, 16 input_acknowledgment, 17 input_ce_echo,
# 18 emitted, 19-22 emitted_*, 23-31 before_*, 32-40 after_* (36 after_credit_quanta,
# 38 after_pacer, 39 after_next_tick_ns, 40 after_status). These fixtures carry their PFC log
# (--pfc). Logs without class_paused (writers before Amendment 1) read class_paused = 0
# throughout.
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
  'NR == 4 { $18 = 1; $19 = 1000; $20 = 1000; $21 = 0; $22 = 17 } { print }' \
  'REJECT: sender: line 4: RoCE paused tick credits or emits'
mutate_amended "paused-tick-adds-credit" "$paused" "$paused_dcqcn" "$paused_pfc" "$paused_stop" \
  'NR == 4 { $36 = 8000000000000 } { print }' \
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
  'NR == 4 { $38 = "armed"; $39 = 3000; $40 = "scheduled" } { print }' \
  'REJECT: sender: line 4: RoCE sender after-state mismatch'
mutate_amended "class-paused-on-ack" "$paused" "$paused_dcqcn" "$paused_pfc" "$paused_stop" \
  'NR == 6 { $8 = 1 } { print }' \
  'REJECT: sender: line 6: RoCE class_paused set on a non-tick row'
mutate_amended "rewind-while-paused-not-restarting" "$paused" "$paused_dcqcn" "$paused_pfc" "$paused_stop" \
  'NR == 8 { $38 = "parked"; $39 = ""; $40 = "blocked" } { print }' \
  'REJECT: sender: line 8: RoCE sender after-state mismatch'
mutate_amended "class-paused-not-a-bit" "$paused" "$paused_dcqcn" "$paused_pfc" "$paused_stop" \
  'NR == 4 { $8 = 2 } { print }' \
  "REJECT: sender: line 4: invalid bit: '2'"
mutate_amended "paused-tick-lost" "$paused" "$paused_dcqcn" "$paused_pfc" "$paused_stop" \
  'NR == 4 { for (i = 23; i <= 31; i++) before[i] = $i; next } NR == 6 { for (i = 23; i <= 31; i++) $i = before[i] } { print }' \
  'REJECT: sender: line 5: pending RoCE pacing tick at 2000 did not fire before this row (node_id=1, flow_id=3)'

# --- Fix round 1 (review F3): a pristine pair's rate is its initial_rate_bps -------------------
# roce_sender_paused_accept.csv carries `initial_rate_bps` (appended last, so the numbered columns
# above are unchanged). Its pairs see no echo and have no DCQCN row, so only that column ties
# their credited rate to their configuration. The reviewer's probe
# (days-gpu evidence/P16/dcqcn-review/lean/rate-probe/rerate.py): re-rate flow 3 on every row and
# recompute its credit chain, so the sender log stays consistent on its own terms.
# rerate <rate>: the awk program over named columns.
rerate() {
  printf '%s' 'NR == 1 { for (i = 1; i <= NF; i++) c[$i] = i; print; next }
    $c["flow_id"] == 3 { if (seen) $c["before_credit_quanta"] = credit
      after = $c["before_credit_quanta"]
      if ($c["rate_bps"] != "") { $c["rate_bps"] = '"$1"'; after += '"$1"' * $c["pacing_interval_ns"]
        if ($c["emitted"] == 1) after -= $c["emitted_bytes"] * 8000000000 }
      $c["after_credit_quanta"] = sprintf("%.0f", after); credit = $c["after_credit_quanta"]; seen = 1 }
    { print }'
}
for rate in 8000000001 9000000000; do
  awk -F, -v OFS=, "$(rerate $rate)" "$paused" > "$campaign_tmp/rerated.csv"
  mutations=$((mutations + 1))
  if check_case "sender/pristine-pair-re-rated-to-$rate" 1 \
      'REJECT: sender: line 2: RoCE rate of a pair whose controller is pristine is not its initial_rate_bps (node_id=1, flow_id=3)' \
      sender "$campaign_tmp/rerated.csv" "$paused_dcqcn" --pfc "$paused_pfc" "$paused_stop"; then
    mutations_caught=$((mutations_caught + 1))
  fi
done
awk -F, -v OFS=, 'NR == 1 { for (i = 1; i <= NF; i++) c[$i] = i; print; next }
  $c["flow_id"] == 5 { $c["initial_rate_bps"] = 7000000000 } { print }' "$paused" > "$campaign_tmp/rerated.csv"
mutations=$((mutations + 1))
if check_case "sender/initial-rate-not-the-credited-rate" 1 \
    'REJECT: sender: line 3: RoCE rate of a pair whose controller is pristine is not its initial_rate_bps (node_id=1, flow_id=5)' \
    sender "$campaign_tmp/rerated.csv" "$paused_dcqcn" --pfc "$paused_pfc" "$paused_stop"; then
  mutations_caught=$((mutations_caught + 1))
fi

# Fix round 3 (orchestrator ruling on residual F3): the sender log's format is explicit. By default
# the checker requires every column of the P16 sender schema and names the missing ones; a log from
# before P16's window and initial-rate columns is read only under --legacy-sender-format, which
# check_case passes unless sender_format=current. Deleting or renaming columns therefore cannot
# turn a current log into an untied legacy one.
lacks() {
  echo "REJECT: sender: line 1: sender log lacks P16 sender schema column(s): $1 (a log from before P16's window and initial-rate columns needs --legacy-sender-format)"
}
# drop_column <name>: an awk program printing every row without the named column.
drop_column() {
  printf '%s' 'NR == 1 { for (i = 1; i <= NF; i++) if ($i == "'"$1"'") drop = i }
    { out = ""; sep = ""; for (i = 1; i <= NF; i++) if (i != drop) { out = out sep $i; sep = OFS }; print out }'
}
# rename_column <old> <new>: an awk program renaming one header field.
rename_column() {
  printf '%s' 'NR == 1 { for (i = 1; i <= NF; i++) if ($i == "'"$1"'") $i = "'"$2"'" } { print }'
}
wn_fmt="$fixture_dir/roce_trace_window_prefix_executor_accept"
# current_trace <label> <expected exit> <expected output> <sender log>: the window prefix's trace,
# with no format flag.
current_trace() {
  mutations=$((mutations + $2))
  if sender_format=current check_case "format/$1" "$2" "$3" \
      trace "$4" "$wn_fmt.receiver.csv" "$wn_fmt.dcqcn.csv" --pfc "$wn_fmt.pfc.csv" \
      --horizon-ns "$(cat "$wn_fmt.horizon_ns")" "$(cat "$wn_fmt.stop_time_ns")"; then
    mutations_caught=$((mutations_caught + $2))
  fi
}
current_trace "window-prefix-in-the-current-format" 0 "ACCEPT" "$wn_fmt.sender.csv"
awk -F, -v OFS=, "$(drop_column initial_rate_bps)" "$wn_fmt.sender.csv" > "$campaign_tmp/format.csv"
current_trace "initial-rate-column-deleted" 1 "$(lacks initial_rate_bps)" "$campaign_tmp/format.csv"
awk -F, -v OFS=, "$(drop_column initial_rate_bps)" "$wn_fmt.sender.csv" \
  | awk -F, -v OFS=, "$(drop_column maximum_rate_bps)" > "$campaign_tmp/format.csv"
current_trace "both-rate-columns-deleted" 1 "$(lacks "maximum_rate_bps, initial_rate_bps")" "$campaign_tmp/format.csv"
awk -F, -v OFS=, "$(rename_column maximum_rate_bps maximum_rate)" "$wn_fmt.sender.csv" > "$campaign_tmp/format.csv"
current_trace "marker-renamed" 1 "$(lacks maximum_rate_bps)" "$campaign_tmp/format.csv"
awk -F, -v OFS=, "$(rename_column maximum_rate_bps maximum_rate)" "$wn_fmt.sender.csv" \
  | awk -F, -v OFS=, "$(drop_column initial_rate_bps)" > "$campaign_tmp/format.csv"
current_trace "marker-renamed-and-initial-rate-deleted" 1 "$(lacks "maximum_rate_bps, initial_rate_bps")" "$campaign_tmp/format.csv"
# The reviewer's probe: the paused fixture with maximum_rate_bps added, flow 3 re-rated to 9 Gb/s
# with its credit chain recomputed, and initial_rate_bps deleted. Without the flag it names every
# P16 column it lacks.
awk -F, -v OFS=, 'NR == 1 { print $0 ",maximum_rate_bps"; next } { print $0 ",8000000000" }' "$paused" \
  | awk -F, -v OFS=, "$(rerate 9000000000)" \
  | awk -F, -v OFS=, "$(drop_column initial_rate_bps)" > "$campaign_tmp/format.csv"
mutations=$((mutations + 1))
if sender_format=current check_case "format/re-rated-pair-with-initial-rate-deleted" 1 \
    "$(lacks "window_blocked, window_bytes, variable_window, initial_rate_bps")" \
    sender "$campaign_tmp/format.csv" "$paused_dcqcn" --pfc "$paused_pfc" "$paused_stop"; then
  mutations_caught=$((mutations_caught + 1))
fi
# A legacy fixture ACCEPTs only with the flag.
mutations=$((mutations + 1))
if sender_format=current check_case "format/legacy-fixture-without-the-flag" 1 \
    "$(lacks "class_paused, window_blocked, data_class, window_bytes, variable_window, maximum_rate_bps, initial_rate_bps")" \
    sender "$gbn" "$gbn_dcqcn" "$gbn_stop"; then
  mutations_caught=$((mutations_caught + 1))
fi
check_case "format/legacy-fixture-with-the-flag" 0 "ACCEPT" sender "$gbn" "$gbn_dcqcn" "$gbn_stop" || true

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
  'NR == 14 { print "7600,0,9,1,1,3,resume,0,3,1000,2000,1000,1000,0,,,,0,,,,,2000,0,2000,2,0,,parked,,blocked,2000,0,2000,2,0,,parked,,blocked" } { print }' \
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
  'NR == 8 { $39 = 6000 } { print }' \
  'REJECT: sender: line 8: RoCE sender after-state mismatch'
mutate_amended "resume-restarts-at-the-resume-instant" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR == 10 { $39 = 4500 } { print }' \
  'REJECT: sender: line 10: RoCE sender after-state mismatch'
mutate_amended "resume-armed-beyond-stop" "$resume" "$resume_dcqcn" "$resume_pfc" "$resume_stop" \
  'NR == 14 { $38 = "armed"; $40 = "scheduled" } { print }' \
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
  'REJECT: pfc: line 3: host RESUME of data_class 3 at node 1 did not restart pause-parked queue pair (flow_id=3)'

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

# --- Ruling C6: a pair whose first ticks are class-paused (no rate yet) ------------------------
# A class-paused tick writes no rate, so a pair paused at its first tick has no known rate until
# its first crediting tick or its first DCQCN row reveals it. Until then the rate cannot change
# (only the pair's DCQCN rows change it, and since P16 its controller moves only on the ECN echoes
# of its own ACKs, so in an executor log the first crediting tick always comes first), the armed
# statuses it predicts bound it, and the revealed rate must fall within those bounds. Amended
# layout (see Amendment 1-3 above).
c6="$fixture_dir/roce_sender_c6_accept.csv"
c6_dcqcn="$fixture_dir/roce_sender_c6_accept.dcqcn.csv"
c6_pfc="$fixture_dir/roce_sender_c6_accept.pfc.csv"
c6_stop=7000
c6_one="$fixture_dir/roce_sender_c6_one_accept.csv"
c6_one_dcqcn="$fixture_dir/roce_sender_c6_one_accept.dcqcn.csv"
c6_one_pfc="$fixture_dir/roce_sender_c6_one_accept.pfc.csv"
c6_one_stop=3000
check_case "roce_sender_c6_accept.csv (two paused ticks and two resumes before the first credit)" \
  0 "ACCEPT" sender "$c6" "$c6_dcqcn" --pfc "$c6_pfc" "$c6_stop" || true
check_case "roce_sender_c6_one_accept.csv (one paused tick)" 0 "ACCEPT" \
  sender "$c6_one" "$c6_one_dcqcn" --pfc "$c6_one_pfc" "$c6_one_stop" || true
# roce_sender_c6_accept.csv rows (NR): 2 t1000 first tick, paused (status before: scheduled, so
# rate >= 8 Gb/s); 3 RESUME 1500 restarts at 2000 (scheduled); 4 t2000 paused again; 5 RESUME
# 3000 restarts at 4000 (scheduled); 6 t4000 the first crediting tick reveals the rate, 8 Gb/s,
# and sends PSN 0; 7 t5000 sends PSN 1000, the last packet, and parks. No DCQCN row: the pair
# never sees an echo and never completes.
mutate_amended "c6-statuses-contradict-revealed-rate" "$c6" "$c6_dcqcn" "$c6_pfc" "$c6_stop" \
  'NR == 2 || NR == 4 || NR == 6 { $31 = "blocked" } NR == 3 || NR == 5 { $40 = "blocked" } { print }' \
  'REJECT: sender: line 6: RoCE status predicted before the rate was known contradicts the controller rate 8000000000 (node_id=1, flow_id=3)'
mutate_amended "c6-statuses-fit-no-rate" "$c6" "$c6_dcqcn" "$c6_pfc" "$c6_stop" \
  'NR == 2 { $31 = "blocked" } { print }' \
  'REJECT: sender: line 3: RoCE statuses predicted before the rate was known fit no controller rate (node_id=1, flow_id=3)'
mutate_amended "c6-rate-changes-without-a-controller-row" "$c6" "$c6_dcqcn" "$c6_pfc" "$c6_stop" \
  'NR == 7 { $15 = 9000000000 } { print }' \
  "REJECT: sender: line 7: RoCE tick rate differs from the DCQCN controller's current rate"
mutate_amended "c6-revealed-rate-below-bound" "$c6_one" "$c6_one_dcqcn" "$c6_one_pfc" "$c6_one_stop" \
  'NR == 4 { $15 = 7000000000 } { print }' \
  'REJECT: sender: line 4: RoCE status predicted before the rate was known contradicts the controller rate 7000000000 (node_id=1, flow_id=3)'
mutate_amended "c6-first-row-unpaused-without-rate" "$c6_one" "$c6_one_dcqcn" "$c6_one_pfc" "$c6_one_stop" \
  'NR == 2 { $8 = 0 } { print }' \
  'REJECT: sender: line 2: RoCE queue pair'"'"'s first row is neither a crediting tick nor a class-paused tick (node_id=1, flow_id=3)'
mutate_amended "c6-parked-status-is-exact" "$c6" "$c6_dcqcn" "$c6_pfc" "$c6_stop" \
  'NR == 2 { $40 = "scheduled" } NR == 3 { $31 = "scheduled" } { print }' \
  'REJECT: sender: line 2: invalid RoCE sender after-state'

# --- Host-link PFC executor traces (Task 2 part 2) ----------------------------------------------
hp="$fixture_dir/roce_trace_hostpfc_prefix_executor_accept"
hp_stop="$(cat "$hp.stop_time_ns")"
hp_horizon="$(cat "$hp.horizon_ns")"
# trace_case <label> <expected> <sender> <receiver> <dcqcn> <pfc>: the prefix in trace mode.
expect_hp_reject() {
  local label="$1"
  local expected_output="$2"
  mutations=$((mutations + 1))
  if check_case "hostpfc/$label" 1 "$expected_output" \
      trace "$3" "$4" "$5" --pfc "$6" --horizon-ns "$hp_horizon" "$hp_stop"; then
    mutations_caught=$((mutations_caught + 1))
  fi
}
# The prefix is not a whole run: without the horizon, its armed ticks beyond the cut are pending
# events that never fired.
check_case "hostpfc/prefix without --horizon-ns" 1 \
  "REJECT: sender: pending RoCE pacing tick at 1872000 never fired by stop_time_ns=$hp_stop (node_id=1, flow_id=0)" \
  trace "$hp.sender.csv" "$hp.receiver.csv" "$hp.dcqcn.csv" --pfc "$hp.pfc.csv" "$hp_stop" || true
# Horizon mutations.
awk -F, -v OFS=, -v h="$hp_horizon" '{ print } END { $1 = h; print }' "$hp.sender.csv" > "$campaign_tmp/hp.sender.csv"
expect_hp_reject "row at the horizon" \
  "REJECT: sender: line $(($(wc -l < "$hp.sender.csv") + 1)): event at or after horizon_ns=$hp_horizon" \
  "$campaign_tmp/hp.sender.csv" "$hp.receiver.csv" "$hp.dcqcn.csv" "$hp.pfc.csv"
# A pair's last row, an unpaused tick that sends nothing, removed: its tick before the horizon
# never fired.
read -r last_line last_time last_node last_flow <<<"$(awk -F, '
  NR > 1 { last[$5 "," $6] = NR; row[NR] = $0 }
  END { best = 0; for (k in last) { split(row[last[k]], f, ",");
          if (f[7] == "tick" && f[8] == 0 && f[18] == 0 && (best == 0 || last[k] < best)) best = last[k] }
        split(row[best], f, ","); print best, f[1], f[5], f[6] }' "$hp.sender.csv")"
awk -v cut="$last_line" 'NR != cut' "$hp.sender.csv" > "$campaign_tmp/hp.sender.csv"
expect_hp_reject "a pair's last tick before the horizon lost" \
  "REJECT: sender: pending RoCE pacing tick at $last_time never fired before horizon_ns=$hp_horizon (node_id=$last_node, flow_id=$last_flow)" \
  "$campaign_tmp/hp.sender.csv" "$hp.receiver.csv" "$hp.dcqcn.csv" "$hp.pfc.csv"

# Red capability on the executor prefix (the brief's seven mutations). The first resume row, at
# a key no other resume row shares, and the host RESUME record that warrants it:
read -r r_line r_time r_node r_flow r_restart <<<"$(awk -F, 'NR > 1 && $7 == "resume" { print NR, $1, $5, $6, $39; exit }' "$hp.sender.csv")"
r_key="$(awk -F, -v n="$r_line" 'NR == n { print $1 "," $2 "," $3 "," $4 }' "$hp.sender.csv")"
read -r p_line p_ctl p_link <<<"$(awk -F, -v k="$r_key" -v node="$r_node" 'NR > 1 && $7 == "control" && $5 == node && ($1 "," $2 "," $3 "," $4) == k { print NR, $9, $8; exit }' "$hp.pfc.csv")"
# 1. Drop one resume row: the RESUME did not restart a pause-parked pair (completeness).
awk -v cut="$r_line" 'NR != cut' "$hp.sender.csv" > "$campaign_tmp/hp.sender.csv"
expect_hp_reject "drop a resume row" \
  "REJECT: pfc: line $p_line: host RESUME of data_class 3 at node $r_node did not restart pause-parked queue pair (flow_id=$r_flow)" \
  "$campaign_tmp/hp.sender.csv" "$hp.receiver.csv" "$hp.dcqcn.csv" "$hp.pfc.csv"
# 2. class_paused 1 -> 0 inside a pause, as an executor that ignored the pause would write the
# tick: the first paused tick whose credit would not cover its packet, rewritten as the unpaused
# tick it would otherwise be (class_paused 0; rate_bps the controller's current rate, which is the
# rate of the pair's last crediting tick when the pair has no DCQCN row up to this tick; credit +
# rate x interval; nothing sent; re-armed one interval later with the armed status prediction).
# Every transition rule accepts that row; only the PFC log refutes it (the unpaused-tick warrant).
# awk computes in doubles, so values beyond 2^53 fail this case instead of rounding.
read -r w_line w_node w_rate w_credit w_tick w_status <<<"$(awk -F, '
  FNR == NR { if (FNR > 1) first_dcqcn[$5 "," $6] = (first_dcqcn[$5 "," $6] == "" ? $1 + 0 : first_dcqcn[$5 "," $6]); next }
  FNR > 1 && $15 != "" { rate_of[$5 "," $6] = $15 }
  FNR > 1 && $7 == "tick" && $8 == 1 {
    pair = $5 "," $6; rate = rate_of[pair]
    if (rate == "" || (first_dcqcn[pair] != "" && first_dcqcn[pair] <= $1 + 0)) next
    size = $10 + 0; if ($11 - $23 < size) size = $11 - $23
    cost = size * 8000000000; credit = $27 + rate * $12
    if (credit >= cost || credit + rate * $12 >= 2^53 || cost >= 2^53) next
    status = (credit + rate * $12 >= cost) ? "scheduled" : "blocked"
    printf "%d %s %s %.0f %.0f %s\n", FNR, $5, rate, credit, $1 + $12, status; exit }' \
  "$hp.dcqcn.csv" "$hp.sender.csv")"
if [[ -z "$w_line" ]]; then
  echo "fixture failed: hostpfc/class_paused 1 -> 0 (no paused tick fits the consistent rewrite)" >&2
  failures=$((failures + 1))
else
  awk -F, -v OFS=, -v n="$w_line" -v rate="$w_rate" -v credit="$w_credit" -v tick="$w_tick" \
      -v status="$w_status" \
    'NR == n { $8 = 0; $15 = rate; $36 = credit; $38 = "armed"; $39 = tick; $40 = status } { print }' \
    "$hp.sender.csv" > "$campaign_tmp/hp.sender.csv"
  expect_hp_reject "class_paused 1 -> 0 inside a pause (the tick an executor ignoring the pause writes)" \
    "REJECT: sender: line $w_line: RoCE unpaused tick while data_class 3 is paused at node $w_node" \
    "$campaign_tmp/hp.sender.csv" "$hp.receiver.csv" "$hp.dcqcn.csv" "$hp.pfc.csv"
fi
# 2b. The same flip of the class_paused field alone: the row then claims an unpaused tick that
# carries no rate although it has a packet to send, which the row-shape rule rejects before any
# warrant is consulted. It shows that rule, not the pause warrant (case 2 shows that).
f_line="$(awk -F, 'NR > 1 && $7 == "tick" && $8 == 1 { print NR; exit }' "$hp.sender.csv")"
awk -F, -v OFS=, -v n="$f_line" 'NR == n { $8 = 0 } { print }' "$hp.sender.csv" > "$campaign_tmp/hp.sender.csv"
expect_hp_reject "class_paused 1 -> 0, the field alone" \
  "REJECT: sender: line $f_line: RoCE tick rate present iff the tick credits" \
  "$campaign_tmp/hp.sender.csv" "$hp.receiver.csv" "$hp.dcqcn.csv" "$hp.pfc.csv"
# 3. class_paused 0 -> 1 outside any pause: a pair's last row, an unpaused tick that sends
# nothing, rewritten as a well-formed paused tick (no rate, parked); only the PFC log refutes it.
read -r u_line u_node <<<"$(awk -F, '
  NR > 1 { last[$5 "," $6] = NR; row[NR] = $0 }
  END { best = 0; for (k in last) { split(row[last[k]], f, ",");
          if (f[7] == "tick" && f[8] == 0 && f[18] == 0 && (best == 0 || last[k] < best)) best = last[k] }
        split(row[best], f, ","); print best, f[5] }' "$hp.sender.csv")"
awk -F, -v OFS=, -v n="$u_line" 'NR == n { $8 = 1; $15 = "";
    for (i = 23; i <= 31; i++) $(i + 9) = $i; $38 = "parked"; $39 = ""; $40 = "blocked" } { print }' \
  "$hp.sender.csv" > "$campaign_tmp/hp.sender.csv"
expect_hp_reject "class_paused 0 -> 1 outside any pause" \
  "REJECT: sender: line $u_line: RoCE paused tick while data_class 3 is not paused at node $u_node" \
  "$campaign_tmp/hp.sender.csv" "$hp.receiver.csv" "$hp.dcqcn.csv" "$hp.pfc.csv"
# 4. Change one data_class (on a row that is not the pair's first).
awk -F, -v OFS=, -v n="$r_line" 'NR == n { $9 = 4 } { print }' "$hp.sender.csv" > "$campaign_tmp/hp.sender.csv"
expect_hp_reject "change one data_class" \
  "REJECT: sender: line $r_line: RoCE data_class discontinuity (node_id=$r_node, flow_id=$r_flow)" \
  "$campaign_tmp/hp.sender.csv" "$hp.receiver.csv" "$hp.dcqcn.csv" "$hp.pfc.csv"
# 5. Move a resume row's key off its RESUME record, by 1 ns (the restart grid point is unchanged:
# it lies beyond the new instant, and the next row is later still).
awk -F, -v OFS=, -v n="$r_line" 'NR == n { $1 = $1 + 1 } { print }' "$hp.sender.csv" > "$campaign_tmp/hp.sender.csv"
expect_hp_reject "move a resume row off its RESUME key" \
  "REJECT: sender: line $r_line: RoCE resume row without a host RESUME of data_class 3 at node $r_node at this event key" \
  "$campaign_tmp/hp.sender.csv" "$hp.receiver.csv" "$hp.dcqcn.csv" "$hp.pfc.csv"
# 6. Duplicate a resume row.
awk -v n="$r_line" 'NR == n { print } { print }' "$hp.sender.csv" > "$campaign_tmp/hp.sender.csv"
expect_hp_reject "duplicate a resume row" \
  "REJECT: sender: line $((r_line + 1)): RoCE resume rows sharing an event key must be of one node in strictly increasing flow_id order" \
  "$campaign_tmp/hp.sender.csv" "$hp.receiver.csv" "$hp.dcqcn.csv" "$hp.pfc.csv"
# 7. Delete the host RESUME record, keeping its resume rows. The next control record of that
# queue (a PAUSE by the same controller) becomes an idempotent PAUSE, so the PFC log stays
# consistent and the resume rows have no warrant.
n_line="$(awk -F, -v after="$p_line" -v node="$r_node" 'NR > after && $7 == "control" && $5 == node { print NR; exit }' "$hp.pfc.csv")"
awk -F, -v OFS=, -v cut="$p_line" -v n="$n_line" -v c="$p_ctl" \
  'NR == cut { next } NR == n { $21 = c; $22 = c } { print }' "$hp.pfc.csv" > "$campaign_tmp/hp.pfc.csv"
expect_hp_reject "delete the host RESUME record" \
  "REJECT: sender: line $r_line: RoCE resume row without a host RESUME of data_class 3 at node $r_node at this event key" \
  "$hp.sender.csv" "$hp.receiver.csv" "$hp.dcqcn.csv" "$campaign_tmp/hp.pfc.csv"
# 7b. The same deletion without the repair: the P10c PFC rules see the controller set jump.
awk -v cut="$p_line" 'NR != cut' "$hp.pfc.csv" > "$campaign_tmp/hp.pfc.csv"
expect_hp_reject "delete the host RESUME record (unrepaired)" \
  "REJECT: pfc: line $((n_line - 1)): PFC controller-set discontinuity for queue (node_id=$r_node, queue_id=0, controlled_link=$p_link, priority=3)" \
  "$hp.sender.csv" "$hp.receiver.csv" "$hp.dcqcn.csv" "$campaign_tmp/hp.pfc.csv"

# --- Amendment 6: the queue-pair window (P16 ruling D7) ----------------------------------------
# roce_trace_window_prefix_executor_accept: the logs of configs/p16/dcqcn_mlx_window.toml before
# 400,000 ns (Scalar, full observation; days-gpu evidence/P16/dcqcn-impl/tooling/p16_lg_csvs.rs):
# four queue pairs with fixed (200,000 and 4,000 B) and variable (20,000 and 8,000 B) windows,
# window-blocked ticks, rate cuts that shrink the variable windows, and the 4,000 B pair (paced one
# packet per tick) window-parked through a host PAUSE and RESUME. These mutations locate their
# rows by named columns, so the sender layout (with the window columns) is not numbered here.
wn="$fixture_dir/roce_trace_window_prefix_executor_accept"
# The awk preludes that name the columns (`c["name"]`): one keeps the header (a mutation), one
# drops it (a lookup).
named='NR == 1 { for (i = 1; i <= NF; i++) c[$i] = i; print; next }'
columns='NR == 1 { for (i = 1; i <= NF; i++) c[$i] = i; next }'
# The controller rate a sender row's pair holds as of it: the last DCQCN row of the pair at or
# before its key, or else the rate of the pair's crediting ticks (the initial rate). Reads the
# DCQCN log, then the sender log; `rate_lookup <condition>` prints `line rate` for the first row
# meeting the awk condition (which may read `rate`).
rate_head='
  function le(a, b) { split(a, x, " "); split(b, y, " ");
    for (i = 1; i <= 4; i++) { if (x[i] + 0 < y[i] + 0) return 1; if (x[i] + 0 > y[i] + 0) return 0 }
    return 1 }
  FNR == NR { if (FNR == 1) { for (i = 1; i <= NF; i++) d[$i] = i; next }
    n++; df[n] = $d["flow_id"]; dk[n] = $1 " " $2 " " $3 " " $4; dr[n] = $d["after_current_rate_bps"]; next }
  FNR == 1 { for (i = 1; i <= NF; i++) c[$i] = i; next }
  { f = $c["flow_id"]; key = $1 " " $2 " " $3 " " $4; rate = (f in initial) ? initial[f] : ""
    for (j = 1; j <= n; j++) if (df[j] == f && le(dk[j], key)) rate = dr[j]
    if (rate != "" && ('
rate_tail=')) { print FNR, rate; exit }
    if ($c["rate_bps"] != "" && !(f in initial)) initial[f] = $c["rate_bps"] }'
rate_lookup() {
  awk -F, "$rate_head$1$rate_tail" "$wn.dcqcn.csv" "$wn.sender.csv"
}
# window_reject <label> <awk program over the sender log> <expected REJECT line>
window_reject() {
  awk -F, -v OFS=, "$named $2"' { print }' "$wn.sender.csv" > "$campaign_tmp/window.sender.csv"
  mutations=$((mutations + 1))
  if sender_format=current check_case "window/$1" 1 "$3" \
      trace "$campaign_tmp/window.sender.csv" "$wn.receiver.csv" "$wn.dcqcn.csv" --pfc "$wn.pfc.csv" \
      --horizon-ns "$(cat "$wn.horizon_ns")" "$(cat "$wn.stop_time_ns")"; then
    mutations_caught=$((mutations_caught + 1))
  fi
}
# A. A tick that credits but sends nothing, recorded as window-blocked: its window was open (not
# a pair's first row, which the first-row rule decides).
a_line="$(awk -F, "$columns"' seen[$c["flow_id"]]++ && $c["kind"] == "tick" && $c["window_bytes"] > 0 && $c["window_blocked"] == 0 && $c["class_paused"] == 0 && $c["rate_bps"] != "" && $c["emitted"] == 0 { print NR; exit }' "$wn.sender.csv")"
window_reject "open-window-recorded-as-blocked" \
  "NR == $a_line"' { $c["window_blocked"] = 1; $c["rate_bps"] = "" }' \
  "REJECT: sender: line $a_line: RoCE window-blocked tick finds the window open"
# B. A window-blocked tick of a fixed window recorded as an ordinary tick at the controller's rate:
# it would credit inside the closed window.
read -r b_line b_rate <<<"$(rate_lookup '$c["window_blocked"] == 1 && $c["variable_window"] == 0')"
window_reject "closed-window-recorded-as-crediting" \
  "NR == $b_line"' { $c["window_blocked"] = 0; $c["rate_bps"] = '"$b_rate"' }' \
  "REJECT: sender: line $b_line: RoCE tick credits inside a closed window"
# The same for a variable window, whose size the checker scales by the rate as of the tick.
read -r v_line v_rate <<<"$(rate_lookup '$c["window_blocked"] == 1 && $c["variable_window"] == 1')"
window_reject "closed-variable-window-recorded-as-crediting" \
  "NR == $v_line"' { $c["window_blocked"] = 0; $c["rate_bps"] = '"$v_rate"' }' \
  "REJECT: sender: line $v_line: RoCE tick credits inside a closed window"
# C. A variable window read as a fixed one: at that tick the fixed window is open.
read -r f_line f_flow <<<"$(awk -F, "$columns"' $c["window_blocked"] == 1 && $c["variable_window"] == 1 && $c["before_next_psn"] - $c["before_snd_una"] < $c["window_bytes"] { print NR, $c["flow_id"]; exit }' "$wn.sender.csv")"
window_reject "variable-window-read-as-fixed" \
  '$c["flow_id"] == '"$f_flow"' { $c["variable_window"] = 0 }' \
  "REJECT: sender: line $f_line: RoCE window-blocked tick finds the window open"
# D. The prediction: an armed pacer whose next tick has the credit for its packet but finds the
# window closed predicts Blocked (the 4,000 B pair, one packet of credit per tick).
read -r p_line p_rate <<<"$(rate_lookup '$c["after_pacer"] == "armed" && $c["variable_window"] == 0 && $c["window_bytes"] > 0 && $c["after_next_psn"] - $c["after_snd_una"] >= $c["window_bytes"] && $c["after_credit_quanta"] + rate * $c["pacing_interval_ns"] >= $c["mtu_bytes"] * 8000000000')"
window_reject "prediction-ignores-the-window" \
  "NR == $p_line"' { $c["after_status"] = "scheduled" }' \
  "REJECT: sender: line $p_line: RoCE sender after-state mismatch"
# E. Shape: window_blocked only on ticks, never with class_paused.
w_ack="$(awk -F, "$columns"' $c["kind"] == "ack" { print NR; exit }' "$wn.sender.csv")"
window_reject "window-blocked-ack" \
  "NR == $w_ack"' { $c["window_blocked"] = 1 }' \
  "REJECT: sender: line $w_ack: RoCE window_blocked set on a non-tick row or with class_paused"
w_tick="$(awk -F, "$columns"' $c["window_blocked"] == 1 { print NR; exit }' "$wn.sender.csv")"
window_reject "window-blocked-and-class-paused" \
  "NR == $w_tick"' { $c["class_paused"] = 1 }' \
  "REJECT: sender: line $w_tick: RoCE window_blocked set on a non-tick row or with class_paused"
window_reject "window-blocked-tick-credits" \
  "NR == $w_tick"' { $c["rate_bps"] = 1000000000 }' \
  "REJECT: sender: line $w_tick: RoCE window-blocked tick credits or emits"
# F. The maximum rate that scales a window is the controller's.
read -r m_flow m_node <<<"$(awk -F, "$columns"' $c["variable_window"] == 0 && $c["window_bytes"] == 4000 { print $c["flow_id"], $c["node_id"]; exit }' "$wn.sender.csv")"
m_line="$(awk -F, -v flow="$m_flow" 'NR == 1 { for (i = 1; i <= NF; i++) d[$i] = i; next } $d["flow_id"] == flow { print NR; exit }' "$wn.dcqcn.csv")"
window_reject "maximum-rate-not-the-controllers" \
  '$c["flow_id"] == '"$m_flow"' { $c["maximum_rate_bps"] += 1 }' \
  "REJECT: dcqcn: line $m_line: DCQCN maximum rate differs from the queue pair's maximum_rate_bps (node_id=$m_node, flow_id=$m_flow)"

# --- Collective stages over RoCE (P15 LeanGuard part 3): the --collective joins -----------------
# The DAG prefix: rows 2-5 release the four compute-gated roots at 5,000 ns; row 11 releases flow 7
# at node 1 by the ACK of flow 6 (sender line 67) that completes it; flow 6's previous ACK is at
# sender line 66 (53024 ns, origin 6, sequence 8). A pair's controller has no timer to anchor at
# the release since P16 (ruling D2).
rd="$fixture_dir/roce_trace_roce_dag_prefix_executor_accept"
rd_stop="$(cat "$rd.stop_time_ns")"
rd_horizon="$(cat "$rd.horizon_ns")"
# mutate_stages <label> <collective> <awk program over named columns> <expected REJECT line>
mutate_stages() {
  local label="$1"
  local role="$2"
  local program="$3"
  local expected_output="$4"
  local collective="$rd.collective.csv"
  local dcqcn="$rd.dcqcn.csv"
  local mutated="$campaign_tmp/stages-$role.csv"
  awk -F, -v OFS=, \
    'NR == 1 { for (i = 1; i <= NF; i++) column[$i] = i; print; next } '"$program"' { print }' \
    "$rd.$role.csv" > "$mutated"
  case "$role" in
    collective) collective="$mutated" ;;
    dcqcn) dcqcn="$mutated" ;;
  esac
  mutations=$((mutations + 1))
  if check_case "stages/$label" 1 "$expected_output" \
      trace "$rd.sender.csv" "$rd.receiver.csv" "$dcqcn" --pfc "$rd.pfc.csv" \
      --horizon-ns "$rd_horizon" --collective "$collective" "$rd_stop"; then
    mutations_caught=$((mutations_caught + 1))
  fi
}
mutate_stages "release-before-first-tick" collective \
  'NR == 2 { $column["time_ns"] = 4999; $column["after_next_time_ns"] = 4999 }' \
  "REJECT: collective: line 2: RoCE stage queue pair's first pacing tick is not at its release instant, on a grid anchored there (node_id=0, flow_id=0)"
# Row 11's release predicts Blocked (one tick of credit does not cover the first packet at the
# controller's rate); the collective log alone allows Scheduled or Blocked.
mutate_stages "release-status-not-the-pair-prediction" collective \
  'NR == 11 { $column["after_status"] = "scheduled" }' \
  "REJECT: collective: line 11: RoCE stage release status is not its queue pair's armed status at its first tick (node_id=1, flow_id=7)"
mutate_stages "unreleased-stage-with-queue-pair-rows" collective \
  'NR == 11 { next }' \
  "REJECT: collective: line 6: unreleased RoCE stage has queue-pair rows (node_id=1, flow_id=7)"
mutate_stages "completion-by-an-earlier-ack" collective \
  'NR == 11 { $column["time_ns"] = 53024; $column["event_origin_sequence"] = 8; $column["after_next_time_ns"] = 53024 }' \
  "REJECT: collective: line 11: RoCE local completion is not the ACK that completes its predecessor queue pair (node_id=1, flow_id=6, sender line 66)"
mutate_stages "completion-without-a-sender-row" collective \
  'NR == 11 { $column["event_origin_sequence"] = 99 }' \
  "REJECT: collective: line 11: RoCE local completion without a sender row of its predecessor queue pair at its event key (node_id=1, flow_id=6)"
mutate_stages "collective-row-at-horizon" collective \
  'NR == 29 { print; $column["time_ns"] = 150000 }' \
  "REJECT: collective: line 30: event at or after horizon_ns=150000"

# Host-PFC executor traces kept outside the repository (ADE_TRACE_DIR=<dir>): every
# <name>.roce_sender.csv there with <name>.roce_receiver.csv, <name>.dcqcn.csv, <name>.pfc.csv and
# <name>.stop_time_ns runs in trace mode with the PFC join. Sender logs above
# ADE_TRACE_MAX_BYTES (default 64 MiB) are skipped and named, so a multi-GB log is run on purpose.
if [[ -n "${ADE_TRACE_DIR:-}" ]]; then
  for sender_csv in "$ADE_TRACE_DIR"/*.roce_sender.csv; do
    [[ -e "$sender_csv" ]] || continue
    base="${sender_csv%.roce_sender.csv}"
    if (( $(wc -c < "$sender_csv") > ${ADE_TRACE_MAX_BYTES:-67108864} )); then
      echo "skipped (size): external/$(basename "$base")"
      continue
    fi
    check_case "external/$(basename "$base") (with PFC)" 0 "ACCEPT" \
      trace "$sender_csv" "$base.roce_receiver.csv" "$base.dcqcn.csv" --pfc "$base.pfc.csv" \
      "$(cat "$base.stop_time_ns")" || true
  done
fi

echo "P10c RoCE campaign checks: $checked; mutations caught: $mutations_caught/$mutations"
exit "$failures"
