# Days: A Performant Discrete-Event Simulator for Network Simulations

Days is a discrete-event network simulator written in Rust. It models network components as actors (async coroutines) that communicate via message passing, with pluggable schedulers, flow models (packet distributions, TCP, optional DCQCN), and optional link-layer PFC support.

## Quick start

```bash
cargo run --release --bin days -- configs/simple.toml
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

- Config-driven runs: `cargo run --release --bin days -- configs/tcp_simple.toml`
- Rust examples: `cargo run --release --example basic`

## Tests

```bash
cargo nextest run --all-features
```

## Feature flags

- `l2` / `l2_pfc`: optional L2/PFC pipeline
- `dcqcn`: DCQCN flow type and models
- `lean`: additional DCQCN event logging for the Lean checker
- `test`: extra assertions and test helpers
