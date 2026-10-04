#!/usr/bin/env bash
# Differential test of the P10c AQM checker (P16 lane L1): the shipped keyed continuity check and
# the test-only list-scan reference (LeanGuard/P10c/Test/AqmReference.lean) must return the same
# result on every case of the P10c AQM campaign, run through the drop-in differential checker
# (exit 3 on any disagreement), and on a generated mutation set from fixed seeds. Extra AQM CSVs
# given as arguments join the generated set.
#
# aqm_red_fattree4_executor_accept.csv is the first 1,000 rows (39 RED queues) of the
# aqm_transitions_csv of a Scalar run of days at 26dc1d1 on configs/fattree.toml with k = 4,
# 16 flows, a 20-packet capacity and 10 us arrivals for 20 ms.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
lean_dir="$(cd "$script_dir/.." && pwd)"

cd "$lean_dir"
lake build p10c_aqm_diff

diff_checker="$lean_dir/.lake/build/bin/p10c_aqm_diff"
fixture_dir="$lean_dir/fixtures/p10c"

echo "== P10c AQM campaign through the differential checker"
P10C_AQM_CHECKER="$diff_checker" bash "$script_dir/run-p10c-aqm-campaign.sh"

echo "== generated mutation set"
status=0
for seed in 1 2; do
  "$diff_checker" mutate "$seed" 30 "$fixture_dir"/aqm_*.csv "$@" || status=$?
done
exit "$status"
