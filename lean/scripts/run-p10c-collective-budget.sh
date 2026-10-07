#!/usr/bin/env bash
# Deterministic cost budget of the P10c collective checker (P16 lane L1): the small allocations
# (Lean heartbeats) made by the shipped checkRows alone, which grew quadratically in the trace before
# the checker became linear (base 26dc1d1: 225 -> 1,073 RoCE rows cost 23x, 220 -> 1,027 TCP rows
# 22x). Each pair is one scenario at two sizes; the larger trace's heartbeats may grow by at most
# 200% of the row ratio, and no input may exceed the per-row cap.
#
# Budget fixtures (lean/fixtures/p10c/budget/), Scalar traces of days at 26dc1d1:
#   collective_roce_ring_lossy_150000.csv: configs/p15/roce_ring_lossy.toml with size = 150000
#     (1,073 rows); its small twin collective_roce_ring_lossy_executor_accept.csv is the same
#     ring at 40,000 B (225 rows, P15).
#   collective_ring_allreduce_lossy_100000.csv: lossy_ring_config() of
#     tests/collective_certificates.rs with size 100,000 and duration = 60.0 (1,027 rows); at
#     20,000 B the same config is collective_ring_allreduce_lossy_executor_accept.csv (220 rows).
#   collective_collops_a2a_uniform_roce_40000.csv (P16 H1): the a2a-uniform-roce fixture of
#     tests/p16_collops.rs (two RoCE all-to-alls, each joined by a compute stage) with size =
#     40000 (528 rows); its small twin collective_collops_a2a_uniform_roce_executor_accept.csv is
#     the same at 8,000 B (144 rows).
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
lean_dir="$(cd "$script_dir/.." && pwd)"

cd "$lean_dir"
lake build p10c_collective_diff

diff_checker="$lean_dir/.lake/build/bin/p10c_collective_diff"
fixture_dir="$lean_dir/fixtures/p10c"
max_ratio_percent=200
max_per_row=200

status=0
# Heartbeats count allocations, so they miss a scan that allocates nothing (a find? over earlier
# rows). The shipped checker must also hold no whole-trace list scan: rows, previous rows and the
# canonical trace are walked only by `for` passes that build or read keyed state.
echo "== no whole-trace list scan in the shipped checker"
scan_pattern='\b(rows|previous|canonical)[[:space:]]*\.[[:space:]]*(find\?|filter|filterMap|any|all|count|contains|lookup|find)\b|for[[:space:]]+prior[[:space:]]+in'
if grep -nE "$scan_pattern" "$lean_dir/LeanGuard/P10c/CollectiveEventLog.lean"; then
  echo "SCAN AUDIT FAILED: a whole-trace list scan in LeanGuard/P10c/CollectiveEventLog.lean" >&2
  status=1
fi
echo "== RoCE lossy ring"
"$diff_checker" budget "$max_ratio_percent" "$max_per_row" \
  "$fixture_dir/collective_roce_ring_lossy_executor_accept.csv" \
  "$fixture_dir/budget/collective_roce_ring_lossy_150000.csv" || status=1
echo "== TCP lossy ring"
"$diff_checker" budget "$max_ratio_percent" "$max_per_row" \
  "$fixture_dir/collective_ring_allreduce_lossy_executor_accept.csv" \
  "$fixture_dir/budget/collective_ring_allreduce_lossy_100000.csv" || status=1
echo "== RoCE all-to-all joins"
"$diff_checker" budget "$max_ratio_percent" "$max_per_row" \
  "$fixture_dir/collective_collops_a2a_uniform_roce_executor_accept.csv" \
  "$fixture_dir/budget/collective_collops_a2a_uniform_roce_40000.csv" || status=1
exit "$status"
