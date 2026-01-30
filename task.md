# Task Tracker: LeanGuard vs. TLC Baseline (Days)

Last updated: 2026-01-30

This file tracks ongoing work to implement the comparison plan in `ideas/comparison.md` and to realize the expected outcomes in `ideas/benefits-over-tla.md`.

## High-level goal

Build a **TLA+/TLC trace-validation baseline** that consumes the **same Days traces** as LeanGuard (after the same canonicalization) and integrates into the existing workflow (`leanguard-run`, `leanguard-testgen`) so we can measure:

- agreement/disagreement on ACCEPT/REJECT
- runtime + memory
- diagnostic quality (first failing step)
- engineering effort (spec + mapping + conversion)

## Status

### Done

- Cleaned `ideas/comparison.md` to remove garbled text, consolidate references, and clarify trace ingestion options (generated TLA module vs JSON/NDJSON + loader).
- Updated `ideas/benefits-over-tla.md` to account for canonicalization cost (`O(n log n)` + replay `O(n)`).
- Removed `utm_source=...` URL artifacts from `ideas/formal-verification.md`.
- Added TLC baseline scaffolding:
  - `src/utils/trace_export.rs`:
    - lossless `*_events.csv` → `*.ndjson` export (canonicalized by `(time_ns, event_id)`),
    - `*_events.csv` → generated `TraceData.tla` export (rescaled to fit TLC’s 32-bit integers),
    - unit test that enforces “NDJSON is lossless, TLA trace is scaled”.
  - `src/bin/days-trace-export.rs` exports either:
    - `--format ndjson` (lossless), or
    - `--format tla` (rescaled `TraceData.tla` module for TLC).
  - Added protocol-specific TLC trace validators under `tla/` (one spec per `*_events.csv`):
    - `tla/DcqcnTrace.tla` (DCQCN)
    - `tla/AqmTrace.tla` (AQM)
    - `tla/PfcTrace.tla` (PFC)
    - `tla/WfqTrace.tla` (WFQ)
    - `tla/DrrTrace.tla` (DRR)
    - `tla/CubicTrace.tla` (TCP CUBIC; congestion-avoidance ACK growth via fixed-point approximation + small `cwnd_bytes` tolerance)
- Integrated TLC baseline runs into `src/bin/leanguard-run.rs`:
  - Flags: `--tlc-check`, `--tlc-spec-dir`, `--tlc-bin`, `--tlc-jar`, `--tlc-no-dfs`.
  - Output: optional `tlc_results` and `tlc_accept` fields in the JSON summary.
  - Diagnostics: parses TLC output (`Diameter:` or “depth of the complete state graph search”) to estimate the longest matched prefix and reports the next failing NDJSON row (by `(time_ns,event_id,kind)` when present).
  - End-to-end verified: `configs/dcqcn_simple.toml` + TLC 2.19 (`/tmp/leanguard_refs/tla2tools_v1.7.4.jar`) produces `tlc_results[0].status = accept` and `matched_prefix == trace_len` on the shipped trace.

### In progress

- Agreement/fault-injection experiments:
  - multiple seeds/configs (not just `dcqcn_simple.toml`),
  - injected faults where both checkers should reject, and compare “first failing row” quality.
- Memory reporting:
  - extend `leanguard-run` to capture JVM max heap / peak RSS for TLC runs (for paper plots).

### Next (implementation work)

- Tighten/justify numeric tolerances (DCQCN `±1` scaled unit; CUBIC `CWND_BYTES_TOL`).
- Add a small “protocol baseline suite” doc section that explains which configs are representative for each protocol.

## References to read (and what we need from them)

### Core (TLA+/TLC trace validation)

- [x] TLA+ trace validation guide (workflow + DFS queue setting + positioning). (`ideas/comparison.md` ref: `tla-trace-validation`)
- [x] Merz trace-validation slides (practical guidance + harness sketch). (`ideas/comparison.md` ref: `merz-trace-validation-slides`)
- [x] “Validating Traces of Distributed Programs Against TLA+ Specifications” (NDJSON ingestion, TraceSpec pattern, TraceAccepted condition). (`ideas/comparison.md` ref: `traceval-arxiv`)
- [x] “Verifying Software Traces Against a Formal Specification with TLA+ and TLC” (classic “trace as constraint” pattern).

### Stretch / “SOTA+” positioning

- [x] TraceLink paper (automation/mapping + causal trace validation; what we do *not* claim to reimplement). (`ideas/comparison.md` ref: `tracelink`)

### Protocol / algorithm references (for step-semantics alignment)

- [x] DCQCN SIGCOMM 2015 (“Congestion Control for Large-Scale RDMA Deployments”): confirm the RP/NP update equations and the intended parameters (`g`, `K`, rate increase phases). (Paper key equations match the Rust implementation order: update `alpha`, then apply multiplicative decrease using the updated `alpha`.)
- [x] WFQ (Demers/Keshav/Shenker): virtual finish tags `F_i = max(F_{i-1}, R(t_i)) + P_i` and “pick smallest finish tag” scheduling rule.
- [x] DRR (Shreedhar/Varghese): per-queue deficit counters, per-round quantum, and eligibility (`size <= deficit`) / counter update semantics.
- [x] CUBIC (RFC 8312): the sender-side cubic window growth function and state machine, as a stable reference for the variant implemented in Days.
- [x] RED / RED-ECN (Floyd/Jacobson 1993): average queue computation, min/max thresholds, and probability regions.

### Additional references already mentioned in `ideas/` (may inform future phases)

- [x] Lean 4 two-phase-commit proof writeup (inductive invariants workflow; uses Apalache to check inductiveness).
- [x] IronFleet (refinement-based verification and “prove practical distributed systems correct” framing).
- [x] Verdi (Coq distributed systems verification; plus related Raft proof work).
- [x] QUICtester / Prognosis (automated blackbox noncompliance checking via learned models).
- [x] RFC 3168 (ECN).

## Open questions / decisions

- **Baseline definition:** For the “fair” baseline, do we constrain TLC to a replay-like mode (post-state equality on every step), or do we also run a partial-observation mode as a secondary experiment?
- **Trace ingestion:** released `tla2tools.jar` (TLC) is practically constrained (32-bit integers; NDJSON ingestion modules are not reliably available). Current baseline uses generated `TraceData.tla` (rescaled) for reproducibility; NDJSON is retained losslessly for diagnostics only.
- **Diagnostics parity:** How exactly to map TLC failures to “first failing row” in the same terms LeanGuard reports (file, canonical index, `(time_ns, event_id)`, `kind`)?

## Reference notes (actionable takeaways)

- TLC trace validation commonly uses a reusable TLA module (often called `TraceSpec`) with:
  - a trace constant/value `Trace`,
  - a line counter `l`,
  - and `Next` that reads `e == Trace[l]`, checks constraints, applies a step rule, and increments `l`.
- Deadlock checking is not helpful for trace validation; the baseline uses `CHECK_DEADLOCK FALSE`.
- Practical performance tip: run DFS for trace validation via:
  - `JVM_OPTIONS=-Dtlc2.tool.queue.IStateQueue=StateDeque`
  - (per the TLA+ trace-validation guide; `StateDeque` was added in Jan 2024).
- Trace validation supports partial observability (existential matching) but can blow up when traces are “thin”; for a fair LeanGuard baseline, we should primarily run a replay-like mode that constrains next-state values to match logged witness/snapshots.

### Notes on current baseline compromises

Because `TraceData.tla` is rescaled (to fit TLC’s integer limits), the DCQCN spec uses a small tolerance for snapshot equality on scaled `alpha_ppb` and `rate_bps` (currently `±1` in the scaled units). This is sufficient to make the TLC baseline agree on `dcqcn_simple.toml`; whether we can tighten this (or avoid it via different scaling) is an open question for the paper methodology.

## Benchmark results (LeanGuard vs. TLC baseline)

Timestamp: 2026-01-29 (local)

Notes:

- `checker_ms_mean±stdev` is the runtime of the LeanGuard checker process for the protocol (e.g., `dcqcn_check`, `wfq_check`).
- `tlc_total_ms_mean±stdev` is end-to-end TLC baseline time per run (export `TraceData.tla` + stage workspace + run TLC).
- `tlc_cmd_ms_mean±stdev` is just the TLC process runtime (excludes export/staging overhead).
- All runs below had `tlc_status = accept`.
- Raw per-run data (do not commit): `logs/bench_leanguard_vs_tlc_2026-01-29.json`
- To rerun: `python3 utils/bench_leanguard_vs_tlc.py --reps 5`

| Protocol | Config | Events | checker_ms (mean±stdev) | tlc_total_ms (mean±stdev) | tlc_cmd_ms (mean±stdev) | tlc_total / checker |
|---|---|---:|---:|---:|---:|---:|
| `aqm` | `configs/cubic_simple.toml` | 12 | 9.4 ± 2.1 | 601.2 ± 19.9 | 598.6 ± 19.6 | 67.3× |
| `aqm` | `configs/dcqcn_simple.toml` | 242 | 13.0 ± 12.6 | 684.8 ± 65.5 | 670.0 ± 63.7 | 82.5× |
| `aqm` | `configs/dcqcn_multi.toml` | 1,151 | 13.6 ± 3.8 | 830.6 ± 7.2 | 775.4 ± 7.2 | 65.7× |
| `aqm` | `configs/dcqcn_1s.toml` | 242 | 7.6 ± 1.8 | 664.0 ± 14.5 | 651.0 ± 14.6 | 91.9× |
| `aqm` | `configs/dcqcn_2s.toml` | 242 | 6.2 ± 1.1 | 663.2 ± 4.6 | 650.0 ± 4.6 | 109.4× |
| `aqm` | `configs/dcqcn_10s.toml` | 242 | 20.6 ± 8.7 | 668.4 ± 11.2 | 654.4 ± 10.8 | 48.2× |
| `aqm` | `configs/wfq_simple.toml` | 400 | 12.4 ± 0.9 | 718.6 ± 15.2 | 692.8 ± 14.5 | 58.2× |
| `aqm` | `configs/drr_simple.toml` | 400 | 10.4 ± 3.2 | 684.4 ± 8.4 | 657.8 ± 9.3 | 74.2× |
| `aqm` | `configs/pfc.toml` | 4,002 | 31.8 ± 6.6 | 1,304.4 ± 22.2 | 1,095.6 ± 15.3 | 42.5× |
| `dcqcn` | `configs/dcqcn_simple.toml` | 2,084 | 23.0 ± 10.8 | 1,181.6 ± 34.1 | 1,044.2 ± 35.4 | 59.0× |
| `dcqcn` | `configs/dcqcn_multi.toml` | 4,298 | 33.6 ± 3.0 | 1,759.0 ± 44.7 | 1,517.0 ± 48.3 | 52.8× |
| `dcqcn` | `configs/dcqcn_1s.toml` | 10,084 | 66.4 ± 5.1 | 2,625.6 ± 58.4 | 2,115.6 ± 59.2 | 39.7× |
| `dcqcn` | `configs/dcqcn_2s.toml` | 20,084 | 120.6 ± 3.8 | 4,590.2 ± 75.5 | 3,593.2 ± 72.9 | 38.1× |
| `dcqcn` | `configs/dcqcn_10s.toml` | 100,084 | 599.6 ± 15.3 | 32,450.8 ± 969.1 | 27,061.2 ± 1,161.9 | 54.2× |
| `pfc` | `configs/pfc.toml` | 104 | 13.4 ± 5.9 | 640.0 ± 12.5 | 628.2 ± 12.2 | 53.2× |
| `wfq` | `configs/wfq_simple.toml` | 1,200 | 18.8 ± 9.1 | 1,238.0 ± 23.7 | 1,164.6 ± 24.9 | 74.4× |
| `drr` | `configs/drr_simple.toml` | 800 | 15.0 ± 9.8 | 963.8 ± 33.5 | 896.4 ± 40.3 | 82.5× |
| `cubic` | `configs/cubic_simple.toml` | 6 | 10.6 ± 12.2 | 652.8 ± 16.5 | 645.4 ± 15.6 | 115.7× |
