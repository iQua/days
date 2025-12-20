# Lean DCQCN Trace Checker

Days can emit a per-transition DCQCN event log (`dcqcn_events.csv`) and a small Lean 4 CLI can check that the log is internally consistent with the DCQCN state machine used by the simulator.

## 1) Generate `dcqcn_events.csv`

Build/run with both features enabled:

`cargo run --features dcqcn,lean,l2_pfc -- configs/dcqcn_simple.toml`

The output file is written under the config’s `log_path` as:

`<log_path>/dcqcn_events.csv`

## 2) Build and run the Lean checker

From the repo root:

```bash
cd lean
lake build
lake exe dcqcn_check ../logs/dcqcn_simple/dcqcn_events.csv
```

Expected output:

- `ACCEPT` (checker accepted the trace)
- `REJECT: ...` (first failing row, with a line number)

## Notes / limitations

- The Lean CSV parser is intentionally minimal and assumes no quoted commas.
- The checker simulates the same floating-point DCQCN update logic as the Rust implementation and checks that the logged `*_ppb` / `*_bps` snapshots match the same rounding used in Rust logging.
