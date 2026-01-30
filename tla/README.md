# TLC baseline (trace validation)

This directory holds a **TLA+/TLC trace-validation baseline** for Days, intended to be comparable to LeanGuard as described in `ideas/comparison.md`.

## Key idea

- Days emits `*_events.csv`.
- We export:
  - `*_events.csv` → `*.ndjson` (lossless; used for diagnostics and “first failing row” reporting)
  - `*_events.csv` → a generated `TraceData.tla` module that defines `Trace == << ... >>` for TLC
- TLC runs a protocol-specific trace harness against `TraceData.tla`.

Currently implemented trace validators:

- `tla/DcqcnTrace.tla` (DCQCN)
- `tla/AqmTrace.tla` (AQM drop/mark decision witnesses)
- `tla/PfcTrace.tla` (PFC pause/resume pairing + hysteresis)
- `tla/WfqTrace.tla` (WFQ schedule/depart replay in scaled integer time)
- `tla/DrrTrace.tla` (DRR schedule replay)
- `tla/CubicTrace.tla` (TCP CUBIC, slow-start + congestion-avoidance ACK growth via a fixed-point approximation)

### Why `TraceData.tla` (and why rescaling)?

In practice, the released `tla2tools.jar` (TLC) has two constraints that matter for Days traces:

- TLC integers are **32-bit** (`[-2^31, 2^31]`), so raw `rate_bps = 10_000_000_000` (10Gbps) does not fit.
- The JSON/NDJSON trace-ingestion modules used in trace-validation research (`Json` / `IOUtils` / `ndJsonDeserialize`) are **not reliably available in released jars**, so we avoid depending on them for the baseline.

To keep the baseline reproducible, `leanguard-run` generates `TraceData.tla` and **rescales** large numeric fields to fit TLC.

## Tooling

Run the baseline via the normal runner (recommended):

```bash
cargo run --bin leanguard-run -- \
  --config configs/dcqcn_simple.toml \
  --mode check-only \
  --checker-dir lean/.lake/build/bin \
  --tlc-check --tlc-jar /path/to/tla2tools.jar
```

This:

- runs Days (or reuses an existing `log_path` depending on `--mode`)
- exports `*.ndjson` next to each `*_events.csv` (lossless)
- creates a TLC workspace under `<log_path>/tlc/<SpecName>/spec/` containing:
  - the TLA module (e.g. `DcqcnTrace.tla`)
  - the `.cfg`
  - `TraceData.tla` (generated from the CSV, rescaled)

You can also export a standalone `TraceData.tla` for a single CSV:

```bash
cargo run --bin days-trace-export -- --format tla --input <log_path>/dcqcn_events.csv
```

### Rescaling rules (for `TraceData.tla` only)

`TraceData.tla` stores:

- `*_ns` fields in **microseconds** (`round(ns / 1_000)`)
- `*_bps` fields in **10 Mbps units** (`round(bps / 10_000_000)`)
- `*_ppb` fields in **permille units** (`round(ppb / 1_000_000)`)

The corresponding TLA specs use the same scaled units (e.g. `PPB == 1000` in `DcqcnTrace.tla`).

Because rescaling loses precision, `DcqcnTrace.tla` uses a small tolerance for snapshot equality on scaled `alpha_ppb` and `rate_bps` (currently `±1` in the scaled units).

`WfqTrace.tla` also uses a small tolerance (`±1` microsecond) when comparing the logged `vtime_ns` / `finish_time_ns` fields against the replay computation, because both the trace export and the scheduler logging round floating-point values.

### Note: `CubicTrace.tla` uses a fixed-point approximation

`CubicTrace.tla` currently validates:

- parameter stability + time monotonicity,
- slow-start ACK updates (`cwnd_bytes` increments),
- congestion-avoidance ACK updates via a scaled integer approximation of the RFC 8312 update,
- byte-level congestion/timeout reductions,

Because the trace export rescales times and the reference implementation uses floating-point arithmetic, `CubicTrace.tla` compares the computed `cwnd_bytes` to the logged value with a small tolerance (see `CWND_BYTES_TOL` in `tla/CubicTrace.tla`).

## Running TLC manually (optional)

If you want to run TLC directly, run it inside the generated TLC workspace so `TraceData.tla` is visible.
For example:

```bash
cd <log_path>/tlc/DcqcnTrace/spec
java -cp /path/to/tla2tools.jar tlc2.TLC -config DcqcnTrace.cfg DcqcnTrace.tla
```

## Notes

- For trace validation, DFS can help performance:
  - `java -Dtlc2.tool.queue.IStateQueue=StateDeque -cp ... tlc2.TLC ...`
- We disable deadlock checking (`CHECK_DEADLOCK FALSE`) and use TLC’s reported diameter/depth as the signal for how many trace steps were matched (see `leanguard-run`’s `matched_prefix` / `first_failure` fields).
