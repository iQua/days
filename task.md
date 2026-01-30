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
  - Added optional memory sampling: `--measure-rss` records `peak_rss_kb` for each checker and TLC run (polls `ps`, so keep off for timing benchmarks).
  - Output: optional `tlc_results` and `tlc_accept` fields in the JSON summary.
  - Diagnostics: parses TLC output (`Diameter:` / “depth of the complete state graph search”) to estimate the longest matched prefix, and reports the next failing NDJSON row (by `(time_ns,event_id,kind)` when present).
  - Made TLC “reject” less heuristic: each baseline `.cfg` checks `INVARIANT ProgressOk` (“before end-of-trace, `Next` must be enabled”), so trace mismatch produces an explicit TLC counterexample/invariant violation (classified as `reject`).
  - End-to-end verified: `configs/dcqcn_simple.toml` + TLC 2.19 (`/tmp/leanguard_refs/tla2tools_v1.7.4.jar`) produces `tlc_results[0].status = accept` and `matched_prefix == trace_len` on the shipped trace.
- Added a minimal fault-injection agreement suite: `python3 utils/fault_injection_agreement.py` (generates small corruptions per protocol and compares Lean vs TLC REJECT + first-failure key).

### In progress

- Broaden memory reporting:
  - aggregate `peak_rss_kb` across protocols/configs for plots,
  - optionally add JVM heap/GC telemetry (beyond RSS) if reviewers ask for it.

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
- For principled failure signaling, the baseline also checks `INVARIANT ProgressOk` (a state predicate using `ENABLED Next`) so “cannot advance the trace” becomes a standard TLC safety violation with a counterexample.
- Practical performance tip: run DFS for trace validation via:
  - `JVM_OPTIONS=-Dtlc2.tool.queue.IStateQueue=StateDeque`
  - (per the TLA+ trace-validation guide; `StateDeque` was added in Jan 2024).
- Trace validation supports partial observability (existential matching) but can blow up when traces are “thin”; for a fair LeanGuard baseline, we should primarily run a replay-like mode that constrains next-state values to match logged witness/snapshots.

### Notes on current baseline compromises

Because `TraceData.tla` is rescaled (to fit TLC’s integer limits), the DCQCN spec uses a small tolerance for snapshot equality on scaled `alpha_ppb` and `rate_bps` (currently `±1` in the scaled units). This is sufficient to make the TLC baseline agree on `dcqcn_simple.toml`; whether we can tighten this (or avoid it via different scaling) is an open question for the paper methodology.

## Benchmark results (LeanGuard vs. TLC baseline)

Timestamp: 2026-01-30 (local)

Notes:

- `checker_ms_mean±stdev` is the runtime of the LeanGuard checker process for the protocol (e.g., `dcqcn_check`, `wfq_check`).
- `tlc_total_ms_mean±stdev` is end-to-end TLC baseline time per run (export `TraceData.tla` + stage workspace + run TLC).
- `tlc_cmd_ms_mean±stdev` is just the TLC process runtime (excludes export/staging overhead).
- TLC runs include `INVARIANT ProgressOk` (uses `ENABLED Next`) to get principled REJECT counterexamples; this can noticeably increase TLC runtime for some specs (notably DRR).
- All runs below had `tlc_status = accept`.
- Raw per-run data (do not commit): `logs/bench_leanguard_vs_tlc_2026-01-30.json`
- To rerun: `python3 utils/bench_leanguard_vs_tlc.py --reps 5`

| Protocol | Config | Events | checker_ms (mean±stdev) | tlc_total_ms (mean±stdev) | tlc_cmd_ms (mean±stdev) | tlc_total / checker |
|---|---|---:|---:|---:|---:|---:|
| `aqm` | `configs/cubic_simple.toml` | 12 | 9.0 ± 0.7 | 610.0 ± 18.9 | 606.2 ± 19.1 | 68.2× |
| `aqm` | `configs/dcqcn_simple.toml` | 242 | 14.6 ± 12.6 | 708.4 ± 80.3 | 692.4 ± 74.8 | 67.2× |
| `aqm` | `configs/dcqcn_multi.toml` | 1,151 | 10.4 ± 2.3 | 940.2 ± 155.1 | 882.6 ± 155.7 | 94.5× |
| `aqm` | `configs/dcqcn_1s.toml` | 242 | 8.0 ± 1.0 | 675.0 ± 23.0 | 661.8 ± 23.4 | 85.6× |
| `aqm` | `configs/dcqcn_2s.toml` | 242 | 6.4 ± 1.1 | 682.8 ± 29.3 | 669.8 ± 29.3 | 109.3× |
| `aqm` | `configs/dcqcn_10s.toml` | 242 | 23.4 ± 9.2 | 687.8 ± 10.2 | 674.4 ± 10.4 | 39.6× |
| `aqm` | `configs/wfq_simple.toml` | 400 | 12.0 ± 1.7 | 721.4 ± 13.2 | 696.6 ± 12.6 | 61.5× |
| `aqm` | `configs/drr_simple.toml` | 400 | 10.0 ± 1.6 | 701.8 ± 13.6 | 677.4 ± 13.4 | 71.7× |
| `aqm` | `configs/pfc.toml` | 4,002 | 32.8 ± 5.7 | 1,288.4 ± 20.1 | 1,108.4 ± 17.4 | 40.2× |
| `dcqcn` | `configs/dcqcn_simple.toml` | 2,084 | 27.4 ± 9.0 | 1,269.6 ± 43.7 | 1,140.0 ± 40.4 | 49.4× |
| `dcqcn` | `configs/dcqcn_multi.toml` | 4,298 | 34.4 ± 3.0 | 1,857.6 ± 50.2 | 1,617.6 ± 51.6 | 54.2× |
| `dcqcn` | `configs/dcqcn_1s.toml` | 10,084 | 69.4 ± 9.0 | 2,838.6 ± 21.9 | 2,322.0 ± 24.3 | 41.4× |
| `dcqcn` | `configs/dcqcn_2s.toml` | 20,084 | 123.0 ± 4.8 | 4,951.6 ± 197.3 | 3,957.8 ± 198.9 | 40.3× |
| `dcqcn` | `configs/dcqcn_10s.toml` | 100,084 | 610.6 ± 17.0 | 30,278.8 ± 3,723.6 | 25,383.4 ± 3,667.9 | 49.6× |
| `pfc` | `configs/pfc.toml` | 104 | 23.4 ± 27.8 | 646.4 ± 15.6 | 635.2 ± 14.3 | 49.4× |
| `wfq` | `configs/wfq_simple.toml` | 1,200 | 26.8 ± 27.5 | 1,634.0 ± 34.9 | 1,570.6 ± 37.5 | 95.0× |
| `drr` | `configs/drr_simple.toml` | 800 | 15.6 ± 9.7 | 4,532.4 ± 85.2 | 4,470.0 ± 83.4 | 351.6× |
| `cubic` | `configs/cubic_simple.toml` | 6 | 21.4 ± 27.7 | 710.2 ± 16.5 | 703.6 ± 15.9 | 65.7× |

## Fault-injection agreement (Lean vs. TLC)

Timestamp: 2026-01-30 (local)

We ran a small corruption suite across all 6 protocols using `python3 utils/fault_injection_agreement.py` (raw output: `logs/fault_injection_agreement_2026-01-30.json`).

Results:

- ACCEPT smoke tests: 6/6 agree on ACCEPT.
- Injected faults: 7/7 agree on REJECT, and the first-failure key `(time_ns,event_id)` matches for all cases.

| Protocol | Case | Lean | TLC | First-failure key |
|---|---|---|---|---|
| `aqm` | invalid ECN mark | reject | reject | `(0,0)` |
| `pfc` | recv sender mismatch | reject | reject | `(256000000,2)` |
| `dcqcn` | alpha mismatch (first row) | reject | reject | `(100000,0)` |
| `dcqcn` | CNP size mismatch | reject | reject | `(952000,9)` |
| `wfq` | finish-time mismatch | reject | reject | `(0,1)` |
| `drr` | deficit mismatch | reject | reject | `(0,1)` |
| `cubic` | cwnd mismatch | reject | reject | `(1000000000,0)` |

## Memory snapshots (peak RSS)

These are sampled via `leanguard-run --measure-rss` (polls `ps`, so it adds overhead; treat as approximate).

- `configs/dcqcn_simple.toml`:
  - `dcqcn_check` peak RSS ≈ 10,624 KB
  - `DcqcnTrace.tla` peak RSS ≈ 339,248 KB
- `configs/dcqcn_10s.toml`:
  - `dcqcn_check` peak RSS ≈ 56,416 KB
  - `DcqcnTrace.tla` peak RSS ≈ 3,142,112 KB
