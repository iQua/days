#!/usr/bin/env bash
set -euo pipefail

# One-click sweep runner for Precision experiments.
#
# Examples:
#   ./utils/run_precision_sweeps.sh
#   SIZES="8MiB,64MiB,256MiB,512MiB,1GiB" ./utils/run_precision_sweeps.sh
#
# Outputs:
#   sim_tp2.csv
#   sim_tp3.csv

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Default sweep: NCCL-aligned allreduce sizes (bytes)
SIZES="${SIZES:-4096B,16384B,65536B,262144B,1048576B,4194304B,8388608B,16777216B,33554432B,67108864B,134217728B,268435456B,536870912B}"

cd "${ROOT_DIR}"

python3 utils/precision_sweep.py --tp 2 --sizes "${SIZES}"
python3 utils/precision_sweep.py --tp 3 --sizes "${SIZES}"

echo "Done. Outputs: ${ROOT_DIR}/sim_tp2.csv and ${ROOT_DIR}/sim_tp3.csv"

