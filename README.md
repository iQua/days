# Days: A Performant Discrete-Event Simulator for Network Simulations

Days is a discrete-event network simulator written in Rust. It models network components as actors (async coroutines) that communicate via message passing, with pluggable schedulers, flow models (packet distributions, TCP, optional DCQCN), and optional link-layer PFC support.

## Quick start

```bash
RUST_LOG=info cargo run -- configs/simple.toml
```

Simulation outputs are written under `log_path` (default: `./output/`) as CSV files.

## Documentation (MkDocs)

All design and configuration documentation lives under `docs/`, built with MkDocs Material.

```bash
pip install -r docs/requirements.txt
mkdocs serve -f docs/mkdocs.yml
```

## Examples

- Config-driven runs: `cargo run -- configs/tcp_simple.toml`
- Rust examples: `cargo run --example basic`

## Tests

```bash
cargo test --features test -- --show-output
```

## Feature flags

- `l2` / `l2_pfc`: optional L2/PFC pipeline
- `dcqcn`: DCQCN flow type and models
- `lean`: additional DCQCN event logging for the Lean checker
- `test`: extra assertions and test helpers
