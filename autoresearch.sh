#!/bin/bash
set -euo pipefail

CONFIG=configs/benchmarks/flow/fattree_k32_tcp_f32_mt.toml
LOG_DIR=logs/fattree

rm -rf "$LOG_DIR"

out=$(mktemp)
trap 'rm -f "$out"' EXIT

{ /usr/bin/time -p cargo run --release --bin days "$CONFIG"; } >"$out" 2>&1
cat "$out"

sim_wall_s=$(grep 'Elapsed wall-clock time:' "$out" | tail -n1 | sed -E 's/.*Elapsed wall-clock time: ([0-9.]+) seconds.*/\1/' || true)
command_real_s=$(awk '/^real / { print $2 }' "$out" | tail -n1)

[ -n "$sim_wall_s" ] && echo "METRIC sim_wall_s=$sim_wall_s"
[ -n "$command_real_s" ] && echo "METRIC command_real_s=$command_real_s"
