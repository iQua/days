# Days: A Fast and Exact Discrete-Event Network Simulator on CPUs and GPUs

Days simulates packet networks exactly and in parallel. Its executor, Days AGO,
compiles a TOML scenario into a simulation image, splits the network into
logical processes, and advances them concurrently in rounds bounded by a safe
horizon. Lean proofs (`lean/DaysExecutor/`) show that this round execution
reaches the same result as serial execution, and every backend that runs a
scenario produces byte-identical complete state.

Four backends run the same image:

| Backend | Where it runs |
|---|---|
| Scalar | one CPU thread; the reference oracle |
| CPU | a multicore worker pool |
| Metal | Apple GPUs |
| CUDA | NVIDIA GPUs (built for sm_86, sm_89 and sm_121) |

Modeled mechanisms: closed-loop TCP Reno and CUBIC, DCQCN with CNP, PFC, ECN,
RED, strict-priority, DRR, and exact-rational WFQ scheduling, and ring
all-reduce and all-gather collectives over TCP with delay-only compute stages,
over fat-tree, torus, and dragonfly topologies. DCQCN and PFC run on all four
backends. RED, collectives, and compute stages run on the Scalar and CPU
backends only; Metal and CUDA reject those scenarios at validation with a
message naming the backend, never with a silent fallback.

## Quick start

Run from the repository root.

```bash
# Scalar reference run
cargo run --release --bin days -- \
  configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml --engine scalar

# Multicore CPU run
cargo run --release --bin days -- \
  configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml \
  --engine cpu --workers 2 --repetitions 1

# GPU run on Apple Metal
cargo run --release --features metal --bin days -- \
  configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml --engine metal

# GPU run on NVIDIA
cargo run --release --features cuda --bin days -- \
  configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml --engine cuda
```

Every engine ends with a `record=days_result` line carrying the complete-state
byte length and its FNV-1a fingerprint; equal fingerprints mean byte-identical
results. `--engine metal` or `--engine cuda` without its feature is an error.

## Documentation

The documentation site is at [days.sh/docs](https://days.sh/docs/). To build it
locally:

```bash
cd docs/
bun install
bun dev
```

## Tests

The four-package default matrix:

```bash
cargo test -p days-executor -- --show-output
cargo test -p days --features test -- --show-output
cargo test -p days-legacy --features test -- --show-output
cargo test -p days-validation --features test -- --show-output
```

GPU surfaces add `--features test,metal` (Apple) or `--features test,cuda`
(NVIDIA, CUDA 13 toolkit). See the testing page in the documentation for the
long-running gates.

## Feature flags

- `cuda`: the CUDA backend (requires a CUDA 13 `nvcc`)
- `metal`: the Metal backend
- `test`: extra assertions and test helpers
- `cuda-planner-test`: host-only CUDA plan-equality tests; builds without `nvcc` (no kernels)
- `metal-test-hooks`, `cuda-test-hooks`: device conformance hooks

The legacy crate has its own `l2`, `l2_pfc`, `dcqcn`, and `lean` features.

## Legacy Days

The original actor-model simulator (Nexosim coroutines, versions up to 0.4.3)
is frozen in `legacy/` as the `days-legacy` crate and still runs:

```bash
cargo run --release -p days-legacy --bin days-legacy -- configs/simple.toml
```

Its last commit on `main` before Days AGO is tagged `legacy-main-final`.

## Repository layout

- `src/`: scenario compiler, topologies, and runners
- `executor/`: the Days AGO executor and its four backends
- `lean/`: executor theorems and LeanGuard protocol checkers
- `legacy/`: the frozen actor-model simulator
- `validation/`: cross-engine tests against legacy Days
- `configs/`: scenarios, benchmarks, and fixtures
- `docs/`: the documentation site

## License

AGPL-3.0-only.
