#!/bin/bash
set -euo pipefail

BIN=./target/release/days
CONFIG=configs/exp_tcp_fattree.toml
LOG_DIR=logs/exp/tcp_fattree/t10_m1000

cargo build --release --features perf_stats --bin days >/dev/null
rm -rf "$LOG_DIR"

out=$(mktemp)
trap 'rm -f "$out"' EXIT

{ /usr/bin/time -p "$BIN" "$CONFIG"; } >"$out" 2>&1
cat "$out"

wall_s=$(awk '/^real / { print $2 }' "$out" | tail -n1)
steps=$(grep -o 'steps=[0-9]*' "$out" | tail -n1 | cut -d= -f2 || true)
avg_groups=$(grep -o 'avg_groups/step=[0-9.]*' "$out" | tail -n1 | cut -d= -f2 || true)
worker_parks=$(grep -o 'worker_parks=[0-9]*' "$out" | tail -n1 | cut -d= -f2 || true)

[ -n "$wall_s" ] && echo "METRIC wall_s=$wall_s"
[ -n "$steps" ] && echo "METRIC steps=$steps"
[ -n "$avg_groups" ] && echo "METRIC avg_groups_per_step=$avg_groups"
[ -n "$worker_parks" ] && echo "METRIC worker_parks=$worker_parks"
