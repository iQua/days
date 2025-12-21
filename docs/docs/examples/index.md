# Examples

Days supports two “example styles”:

1. **Config-driven simulations** (`configs/*.toml`) run via the CLI (`cargo run -- <config>`).
2. **Rust examples** (`examples/*.rs`) run via `cargo run --example <name>`.

## Config examples

Run a config from the repo root:

```bash
RUST_LOG=info cargo run -- configs/simple.toml
```

Measure wall-clock time without logs:

```bash
time cargo run -- configs/simple.toml
```

DCQCN example (requires `dcqcn` and typically `l2_pfc`):

```bash
RUST_LOG=info cargo run --features dcqcn,l2_pfc -- configs/dcqcn_simple.toml
```

## Rust examples

List available example binaries:

```bash
ls examples
```

Run one:

```bash
RUST_LOG=info cargo run --example basic
```

The repo currently includes examples such as `basic`, `switch`, `wire`, `tcp`, and `ecn` (names match the `.rs` filenames in `examples/`).
