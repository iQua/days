#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 2 ]]; then
  echo "usage: run-tcp-campaign.sh <scalar-tcp-events.csv> <results-dir>" >&2
  exit 2
fi

baseline="$1"
results_dir="$2"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
lean_dir="$(cd "$script_dir/.." && pwd)"
checker="$lean_dir/.lake/build/bin/tcp_check"

mkdir -p "$results_dir"
cd "$lean_dir"
lake build tcp_check

cp "$baseline" "$results_dir/baseline.csv"

mutate_first() {
  local algorithm="$1"
  local kind="$2"
  local column="$3"
  local delta="$4"
  local output="$5"
  awk -F, -v OFS=, -v algorithm="$algorithm" -v kind="$kind" \
      -v column="$column" -v delta="$delta" '
    NR == 1 {
      for (i = 1; i <= NF; i++) col[$i] = i
      print
      next
    }
    !changed && $col["algorithm"] == algorithm && $col["kind"] == kind {
      $col[column] = $col[column] + delta
      changed = 1
    }
    { print }
    END { if (!changed) exit 3 }
  ' "$baseline" > "$output"
}

mutate_first Reno new_ack after_cwnd_bytes 1 "$results_dir/reno_bad_window.csv"
mutate_first CUBIC timeout after_w_max_scaled 1 "$results_dir/cubic_bad_timeout.csv"
mutate_first Reno duplicate_ack before_dupacks 1 "$results_dir/bad_continuity.csv"

summary="$results_dir/campaign-results.txt"
: > "$summary"

run_case() {
  local name="$1"
  local expected_exit="$2"
  local path="$3"
  local coverage="$results_dir/${name}-coverage.json"
  set +e
  output="$($checker --coverage-out "$coverage" "$path" 2>&1)"
  actual_exit=$?
  set -e
  printf '%s expected_exit=%s actual_exit=%s output=%s\n' \
    "$name" "$expected_exit" "$actual_exit" "$output" | tee -a "$summary"
  if [[ "$actual_exit" -ne "$expected_exit" ]]; then
    exit 1
  fi
}

run_case baseline 0 "$results_dir/baseline.csv"
run_case reno_bad_window 1 "$results_dir/reno_bad_window.csv"
run_case cubic_bad_timeout 1 "$results_dir/cubic_bad_timeout.csv"
run_case bad_continuity 1 "$results_dir/bad_continuity.csv"
