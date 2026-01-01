# LeanGuard: Use Guide

LeanGuard is a Lean 4 checker suite under `lean/` that validates protocol traces emitted by Days.
The workflow is:

1. Build Days with trace logging enabled (Cargo feature `lean`).
2. Run a simulation that emits `*_events.csv` into your `log_path`.
3. Run the corresponding Lean checker on that CSV.

## Prerequisites

- A working Lean 4 toolchain (via `elan`) and `lake`.
- A working Rust toolchain (to build and run Days).

Days pins Lean via `lean/lean-toolchain`.

## Emit traces from Days

Traces are written to your configured `log_path` (default: `./output/`).
See `docs/docs/configuration/logging.md` for `log_path`.

### DCQCN (`dcqcn_events.csv`)

Build and run with DCQCN + trace logging enabled:

```bash
RUST_LOG=info cargo run --features dcqcn,lean,l2_pfc -- configs/dcqcn_simple.toml
```

This writes `dcqcn_events.csv` under `log_path`.

### PFC (`pfc_events.csv`)

Build and run with PFC + trace logging enabled:

```bash
RUST_LOG=info cargo run --features l2_pfc,lean -- configs/pfc.toml
```

This writes `pfc_events.csv` under `log_path`.

### TCP CUBIC (`cubic_events.csv`)

CUBIC event tracing is enabled by the `lean` feature:

```bash
RUST_LOG=info cargo run --features lean -- configs/simple.toml
```

If your scenario includes TCP flows using CUBIC, Days writes `cubic_events.csv` under `log_path`.

### WFQ (`wfq_events.csv`)

WFQ event tracing is enabled by the `lean` feature:

```bash
RUST_LOG=info cargo run --features lean -- configs/simple.toml
```

If your scenario includes a WFQ scheduler, Days writes `wfq_events.csv` under `log_path`.

## Build and run the Lean checkers

All checkers live under `lean/` and are built with `lake`.

```bash
cd lean
lake build
```

Then run one of:

```bash
./.lake/build/bin/dcqcn_check  <path/to/dcqcn_events.csv>
./.lake/build/bin/pfc_check    <path/to/pfc_events.csv>
./.lake/build/bin/cubic_check  <path/to/cubic_events.csv>
./.lake/build/bin/wfq_check    <path/to/wfq_events.csv>
```

Exit codes:

- `0`: `ACCEPT`
- `1`: `REJECT: ...` (first violating row + message)
- `2`: usage / parse error

## What “REJECT” means (and how to debug)

LeanGuard treats the simulator and the CSV as **untrusted** inputs.
`REJECT` means at least one logged transition is not consistent with the executable Lean semantics.

Diagnostics are designed to be actionable:

- parsing errors point at the CSV line and field
- semantic errors point at the first failing row in canonical replay order (not necessarily the physical file order)

Typical causes:

- missing required fields for an event kind
- inconsistent constant parameters across rows for the same endpoint/flow
- violated protocol gates (cooldowns / refresh intervals)
- mismatched send/receive pairing (e.g., `*_recv` without prior `*_sent`)
- post-state snapshot not equal to the state computed by the Lean update equations
