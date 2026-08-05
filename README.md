# Days: A Performant Discrete-Event Simulator for Network Simulations

Days is a discrete-event network simulator written in Rust. It models network components as actors (async coroutines) that communicate via message passing, with pluggable schedulers, flow models (packet distributions, TCP, optional DCQCN), and optional link-layer PFC support.

## Quick start

```bash
cargo run --release -p days-legacy --bin days -- configs/simple.toml
```

Simulation outputs are written under `log_path` (default: `./output/`) as CSV files.

## Documentation

All design and configuration documentation lives under `docs/`:

```bash
cd docs/
bun install
bun dev
```

Alternatively, one can directly visit the [documentation website](https://days.sh/docs/).

## Examples

- Config-driven runs: `cargo run --release -p days-legacy --bin days -- configs/tcp_simple.toml`
- Rust examples: `cargo run --release -p days-legacy --example basic`

## Tests

```bash
cargo test -p days-executor -- --show-output
cargo test -p days --features test -- --show-output
cargo test -p days-legacy --features test -- --show-output
cargo test -p days-validation --features test -- --show-output
```

The device-planner equality gate also runs in the standard backend test surfaces:

```bash
# Apple Metal toolchain
cargo test -p days --features test,metal-spike --test t20e_planner_bit_equal -- --show-output

# CUDA toolchain; constructs and compares host plans without executing a GPU
cargo test -p days --features test,cuda --test t20e_planner_bit_equal -- --show-output
```

The CUDA surface still requires the normal CUDA build toolchain because the backend is compiled.

## Feature flags

- `l2` / `l2_pfc`: optional legacy L2/PFC pipeline
- `dcqcn`: DCQCN flow type and models
- `lean`: additional DCQCN event logging for the Lean checker
- `test`: extra assertions and test helpers
