# TLA+/TLC experiments notes 

## 1) What’s in the current `merge` branch (Days repo) relevant to TLA+/TLC

In `/Users/bli/Playground/days`, the local branch `merge` (tracking `origin/merge`) contains a complete TLC baseline
implementation and experiment harness.

The main additions are:

### A. TLC baseline specs (`tla/`)

A new `tla/` directory with protocol-specific trace validators + configs:

- `tla/DcqcnTrace.tla` + `tla/DcqcnTrace.cfg`
- `tla/AqmTrace.tla` + `tla/AqmTrace.cfg`
- `tla/PfcTrace.tla` + `tla/PfcTrace.cfg`
- `tla/WfqTrace.tla` + `tla/WfqTrace.cfg`
- `tla/DrrTrace.tla` + `tla/DrrTrace.cfg`
- `tla/CubicTrace.tla` + `tla/CubicTrace.cfg`
- `tla/README.md` explains how the baseline works and how to run it.

Key design constraint (per `tla/README.md`): released TLC uses **32-bit integers**, so the baseline avoids raw
`*_bps` / `*_ns` values and instead generates a scaled `TraceData.tla`.

### B. Trace export pipeline (CSV → NDJSON + `TraceData.tla`)

New Rust utility code + a CLI tool:

- `src/utils/trace_export.rs` (core conversion logic)
- `src/bin/days-trace-export.rs` (standalone exporter)

Exports:

- `*_events.csv` → `*_events.ndjson` (lossless, canonicalized)
- `*_events.csv` → `TraceData.tla` (scaled for TLC)

Scaling rules (from `tla/README.md`):

- `*_ns` stored in microseconds (`ns / 1_000`)
- `*_bps` stored in 10 Mbps units (`bps / 10_000_000`)
- `*_ppb` stored in permille (`ppb / 1_000_000`)

### C. TLC integrated into the normal runner (`leanguard-run`)

`src/bin/leanguard-run.rs` includes TLC integration:

- Flags like `--tlc-check`, `--tlc-jar` (or `--tlc-bin`), `--tlc-spec-dir`, `--tlc-no-dfs`
- Optional RSS sampling via `--measure-rss`
- Results embedded into the same JSON summary:
  - `tlc_results[]` including status, argv, stdout/stderr, runtime, and first failure location heuristics

Conceptually, the baseline is “trace validation”: TLC checks the exported trace against the protocol spec/model.

### D. Benchmark + agreement harness

There are Python utilities under `utils/` for running systematic experiments and emitting CSV summaries (used by the
paper draft):

- `utils/bench_leanguard_vs_tlc.py`: runs Lean checkers + TLC baseline and summarizes runtime/acceptance.
- `utils/fault_injection_agreement.py` (or similar): fault-injection agreement checks between Lean and TLC.

(Exact filenames may vary slightly; check `utils/`.)

---

## 2) How to run TLC baseline (typical workflows)

### Via `leanguard-run`

```bash
# Check-only mode uses existing logs (no simulation)
leanguard-run \
  --mode check-only \
  --config <case.toml> \
  --checker-dir lean/.lake/build/bin \
  --tlc-check \
  --tlc-spec-dir tla \
  --tlc-jar /path/to/tla2tools.jar
```

### Direct TLC invocation (manual)

The baseline can also be run directly via Java:

```bash
java -cp /path/to/tla2tools.jar tlc2.TLC -config tla/DcqcnTrace.cfg tla/DcqcnTrace.tla
```

In practice, the repository’s `leanguard-run` integration handles staging the generated `TraceData.tla` for the specific
trace.

---

## 3) Notes / constraints

- **Integer scaling is required** due to TLC 32-bit integer limitations.
- NDJSON export is kept lossless for diagnostics, but the TLC model consumes a scaled representation.
- TLC is feasible as an ACCEPT/REJECT oracle baseline, but it’s not a realistic inner-loop coverage signal.

