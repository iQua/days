#!/usr/bin/env bash
set -euo pipefail

# One-click sweep runner for Pipeline Parallel P2P experiments (single-message).
#
# Outputs:
#   sim_pp_01.csv   (0 -> 1)
#   sim_pp_12.csv   (1 -> 2)

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Default sweep: NCCL-aligned sizes (bytes)
SIZES="${SIZES:-4096B,16384B,65536B,262144B,1048576B,4194304B,8388608B,16777216B,33554432B,67108864B,134217728B,268435456B,536870912B}"

cd "${ROOT_DIR}"

python3 utils/precision_sweep.py \
  --tp 3 \
  --collective-type Broadcast \
  --template configs/pp_p2p_01_tp3.toml \
  --out sim_pp_01.csv \
  --log-root logs/pp_01 \
  --sizes "${SIZES}"

python3 utils/precision_sweep.py \
  --tp 3 \
  --collective-type Broadcast \
  --template configs/pp_p2p_12_tp3.toml \
  --out sim_pp_12.csv \
  --log-root logs/pp_12 \
  --sizes "${SIZES}"

echo "Done. Outputs: ${ROOT_DIR}/sim_pp_01.csv and ${ROOT_DIR}/sim_pp_12.csv"

