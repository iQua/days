#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Group 1: GPT3-13B / 128 GPUs / A100
cargo run --release --manifest-path "${ROOT_DIR}/Cargo.toml" --bin days -- \
  "${ROOT_DIR}/configs/workload/collective/training/gpt3_13b_128_a100.toml"

# Group 2: LLaMA-65B / 512 GPUs / H100
cargo run --release --manifest-path "${ROOT_DIR}/Cargo.toml" --bin days -- \
  "${ROOT_DIR}/configs/workload/collective/training/llama_65b_512_h100.toml"

# Group 3: GPT3-175B / 1024 GPUs / H100
cargo run --release --manifest-path "${ROOT_DIR}/Cargo.toml" --bin days -- \
  "${ROOT_DIR}/configs/workload/collective/training/gpt3_175b_1024_h100.toml"

# Group 4: Inference prefill / 13B / 128 GPUs / A100
cargo run --release --manifest-path "${ROOT_DIR}/Cargo.toml" --bin days -- \
  "${ROOT_DIR}/configs/workload/collective/inference/infer_prefill_13b_128_a100.toml"

# Group 5: Inference decode / 13B / 128 GPUs / A100
cargo run --release --manifest-path "${ROOT_DIR}/Cargo.toml" --bin days -- \
  "${ROOT_DIR}/configs/workload/collective/inference/infer_decode_13b_128_a100.toml"

# Group 6: Inference long-context prefill / 13B / 128 GPUs / A100
cargo run --release --manifest-path "${ROOT_DIR}/Cargo.toml" --bin days -- \
  "${ROOT_DIR}/configs/workload/collective/inference/infer_longctx_prefill_13b_128_a100.toml"
