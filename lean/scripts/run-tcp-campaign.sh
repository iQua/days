#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 2 ]]; then
  echo "usage: run-tcp-campaign.sh <scalar-trace-dir> <results-dir>" >&2
  exit 2
fi

trace_dir="$1"
results_dir="$2"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
lean_dir="$(cd "$script_dir/.." && pwd)"
checker="$lean_dir/.lake/build/bin/tcp_check"

mkdir -p "$results_dir"
cd "$lean_dir"
lake build tcp_check

shopt -s nullglob
traces=("$trace_dir"/*-tcp-events.csv)
if [[ "${#traces[@]}" -eq 0 ]]; then
  echo "no *-tcp-events.csv certificates found in $trace_dir" >&2
  exit 2
fi

for trace in "${traces[@]}"; do
  cp "$trace" "$results_dir/baseline-$(basename "$trace")"
done

mutate_first() {
  local input="$1"
  local algorithm="$2"
  local kind="$3"
  local condition_column="$4"
  local condition_value="$5"
  local column="$6"
  local value="$7"
  local output="$8"
  awk -F, -v OFS=, -v algorithm="$algorithm" -v kind="$kind" \
      -v condition_column="$condition_column" -v condition_value="$condition_value" \
      -v column="$column" -v value="$value" '
    NR == 1 {
      for (i = 1; i <= NF; i++) col[$i] = i
      print
      next
    }
    !changed && $col["algorithm"] == algorithm && $col["kind"] == kind &&
        (condition_column == "" || $col[condition_column] == condition_value) {
      $col[column] = value
      changed = 1
    }
    { print }
    END { if (!changed) exit 3 }
  ' "$input" > "$output"
}

reno_recovery="$trace_dir/reno-recovery-tcp-events.csv"
cubic_recovery="$trace_dir/cubic-recovery-tcp-events.csv"

# This mutation leaves all redundant byte/scaled projections internally consistent. It is killed
# only because the recorded recovery_high output no longer follows the third-duplicate input.
mutate_first "$reno_recovery" Reno duplicate_ack before_dupacks 2 \
  recovery_high_input 2560 "$results_dir/semantic_bad_recovery_high.csv"

mutate_first "$reno_recovery" Reno new_ack "" "" \
  time_ns 18446744073709551616 "$results_dir/time_u64_overflow.csv"
mutate_first "$cubic_recovery" CUBIC new_ack "" "" \
  rtt_ns 18446744073709551616 "$results_dir/rtt_u64_overflow.csv"
mutate_first "$cubic_recovery" CUBIC duplicate_ack before_dupacks 2 \
  after_epoch_ns 18446744073709551616 "$results_dir/epoch_u64_overflow.csv"

awk -F, -v OFS=, '
  NR == 1 {
    for (i = 1; i <= NF; i++) col[$i] = i
    print
    next
  }
  NR == 2 {
    time = $col["time_ns"]
    phase = $col["event_phase"]
    origin = $col["event_origin_node"]
    sequence = $col["event_origin_sequence"]
    print
    next
  }
  NR == 3 {
    $col["time_ns"] = time
    $col["event_phase"] = phase
    $col["event_origin_node"] = origin
    $col["event_origin_sequence"] = sequence
  }
  { print }
' "$reno_recovery" > "$results_dir/duplicate_event_key.csv"

awk 'NR == 1 { print; next } NR == 2 { first = $0; next } NR == 3 { print; print first; next } { print }' \
  "$reno_recovery" > "$results_dir/out_of_order_event_key.csv"

summary="$results_dir/campaign-results.txt"
: > "$summary"

run_case() {
  local name="$1"
  local expected_exit="$2"
  local input="$3"
  local coverage="$results_dir/${name}-coverage.json"
  set +e
  output="$($checker --coverage-out "$coverage" "$input" 2>&1)"
  actual_exit=$?
  set -e
  printf '%s expected_exit=%s actual_exit=%s output=%s\n' \
    "$name" "$expected_exit" "$actual_exit" "$output" | tee -a "$summary"
  if [[ "$actual_exit" -ne "$expected_exit" ]]; then
    exit 1
  fi
}

for trace in "${traces[@]}"; do
  stem="$(basename "$trace" .csv)"
  run_case "$stem" 0 "$trace"
done
run_case semantic_bad_recovery_high 1 "$results_dir/semantic_bad_recovery_high.csv"
run_case time_u64_overflow 2 "$results_dir/time_u64_overflow.csv"
run_case rtt_u64_overflow 2 "$results_dir/rtt_u64_overflow.csv"
run_case epoch_u64_overflow 2 "$results_dir/epoch_u64_overflow.csv"
run_case duplicate_event_key 1 "$results_dir/duplicate_event_key.csv"
run_case out_of_order_event_key 1 "$results_dir/out_of_order_event_key.csv"
