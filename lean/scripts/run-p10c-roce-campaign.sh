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

echo "P10c RoCE campaign checks: $checked; mutations caught: $mutations_caught/$mutations"
exit "$failures"
