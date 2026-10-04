#!/usr/bin/env bash
# Differential test of the P10c collective checker (P16 lane L1): the shipped keyed checker and the
# test-only list-scan reference (LeanGuard/P10c/Test/CollectiveReference.lean) must return the same
# result, ACCEPT or the same first REJECT message, on
#   1. every case of the collective campaign (its fixtures and its RED mutations), run through the
#      drop-in differential checker, which exits 3 on any disagreement;
#   2. the larger budget fixtures (lean/fixtures/p10c/budget/), which must be accepted; and
#   3. a generated mutation set: per fixture, per mutation kind, single-edit mutants from fixed seeds.
# Extra collective CSVs given as arguments join the generated set.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
lean_dir="$(cd "$script_dir/.." && pwd)"

cd "$lean_dir"
lake build p10c_collective_diff

diff_checker="$lean_dir/.lake/build/bin/p10c_collective_diff"
fixture_dir="$lean_dir/fixtures/p10c"

echo "== collective campaign through the differential checker"
P10C_COLLECTIVE_CHECKER="$diff_checker" bash "$script_dir/run-p10c-collective-campaign.sh"

echo "== budget fixtures through the differential checker"
for csv in "$fixture_dir"/budget/collective_*.csv; do
  "$diff_checker" "$csv"
done

echo "== generated mutation set"
inputs=("$fixture_dir"/collective_*.csv "$fixture_dir"/roce_trace_*.collective.csv "$@")
status=0
for seed in 1 2; do
  "$diff_checker" mutate "$seed" 10 "${inputs[@]}" || status=$?
done
exit "$status"
