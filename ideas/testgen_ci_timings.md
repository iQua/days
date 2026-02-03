# Local “CI-style” test-generation timings (LeanPaper support)

This note records a local run of the **test-generation pipeline** for multiple protocols so the paper (in `lean-paper/`) can cite/track end-to-end costs without relying on GitHub Actions.

## What we are measuring

For each target protocol we run the `leanguard-testgen` pipeline end-to-end:

1. `leanguard-testgen seed-index` — index a seed TOML into a fresh corpus.
2. `leanguard-testgen campaign` — generate and execute `--budget` mutated cases.
   - `campaign` invokes `leanguard-run` to run **Days simulation + LeanGuard checker(s)**.
   - We use the default settings in `utils/ci_testgen_timings.py` (`--budget 1`, `--max-calibration-iters 0`).

The per-protocol runner writes one CSV row with:
- `seed_index_s`, `campaign_s`, `total_s` (wall-clock seconds)
- `accepted/rejected/errors` (from the campaign summary)

## Why this exists (relation to GitHub workflow)

The GitHub Actions workflow `.github/workflows/testgen-timings.yml` runs the same per-protocol script in CI and uploads CSVs as artifacts. For LeanPaper work, we run the same steps locally to obtain CSVs without GitHub.

## Commands (run locally)

From repo root:

1) Build LeanGuard checker binaries (all needed executables):

```bash
cd lean
lake build dcqcn_check pfc_check cubic_check drr_check wfq_check aqm_check aqm_dcqcn_check
cd ..
```

2) Build the Rust runners used by test generation:

```bash
cargo build --release --features lean,dcqcn,l2_pfc --bin leanguard-run --bin leanguard-testgen
```

3) Run all protocol “CI tests” locally (writes 1 CSV per protocol):

```bash
mkdir -p artifacts
for p in aqm dcqcn pfc wfq drr cubic; do
  python3 utils/ci_testgen_timings.py --protocol "$p" --out "artifacts/testgen_${p}.csv"
done
```

## This run (results + environment)

- Git commit: `09caa054bce58096eeef7ead80f87b7a74aa56df`
- Toolchain:
  - `rustc 1.92.0`, `cargo 1.92.0`
  - `Python 3.12.7`
  - `Lake 5.0.0` (Lean 4.26.0)

### Output CSVs

- `artifacts/testgen_aqm.csv`
- `artifacts/testgen_dcqcn.csv`
- `artifacts/testgen_pfc.csv`
- `artifacts/testgen_wfq.csv`
- `artifacts/testgen_drr.csv`
- `artifacts/testgen_cubic.csv`

### Summary (latest row per CSV)

| protocol | seed_config | seed_index_s | campaign_s | total_s | accepted | rejected | errors |
|---|---|---:|---:|---:|---:|---:|---:|
| aqm | `configs/simple.toml` | 0.16579 | 0.394072 | 0.559862 | 1 | 0 | 0 |
| dcqcn | `configs/dcqcn_simple.toml` | 0.003621 | 0.494983 | 0.498605 | 1 | 0 | 0 |
| pfc | `configs/pfc.toml` | 0.004261 | 0.295637 | 0.299898 | 1 | 0 | 0 |
| wfq | `configs/wfq_simple.toml` | 0.003482 | 0.432893 | 0.436374 | 1 | 0 | 0 |
| drr | `configs/drr_simple.toml` | 0.003481 | 0.264849 | 0.26833 | 1 | 0 | 0 |
| cubic | `configs/cubic_simple.toml` | 0.003608 | 0.241207 | 0.244815 | 1 | 0 | 0 |

