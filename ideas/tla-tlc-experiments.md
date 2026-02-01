 1) What’s in the current merge branch (Days repo) relevant to TLA+/TLC

 In /Users/bli/Playground/days the local branch merge tracks origin/merge and contains a complete TLC baseline
 implementation and experiment harness. The main additions are:

 ### A. TLC baseline specs (tla/)

 A new tla/ directory with protocol-specific trace validators + configs:

 - tla/DcqcnTrace.tla + .cfg
 - tla/AqmTrace.tla + .cfg
 - tla/PfcTrace.tla + .cfg
 - tla/WfqTrace.tla + .cfg
 - tla/DrrTrace.tla + .cfg
 - tla/CubicTrace.tla + .cfg
 - tla/README.md explains how the baseline works and how to run it.

 Key design constraint (per tla/README.md): released TLC uses 32-bit integers, so the baseline avoids raw *_bps/*_ns
 values and instead generates a scaled TraceData.tla.

 ### B. Trace export pipeline (CSV → NDJSON + TraceData.tla)

 New Rust utility code + a CLI tool:

 - src/utils/trace_export.rs (core conversion logic)
 - src/bin/days-trace-export.rs (standalone exporter)

 Exports:
 - *_events.csv → *_events.ndjson (lossless, canonicalized)
 - *_events.csv → TraceData.tla (scaled for TLC)

 Scaling rules (from tla/README.md):
 - *_ns stored in microseconds (ns/1_000)
 - *_bps stored in 10 Mbps units (bps/10_000_000)
 - *_ppb stored in permille (ppb/1_000_000)

 ### C. TLC integrated into the normal runner

 src/bin/leanguard-run.rs gains TLC integration (flags like --tlc-check, --tlc-jar, optional DFS control, and RSS
 sampling). It stages a TLC workspace under:
 <log_path>/tlc/<SpecName>/spec/

 ### D. Experiment harness + result export scripts

 - utils/bench_leanguard_vs_tlc.py (benchmarks checker vs TLC across configs)
 - utils/fault_injection_agreement.py (inject faults; compare ACCEPT/REJECT + first-failure key)
 - utils/export_experiment_results.py (summaries to Markdown + CSV)
 - task.md (acts as the “oracle” checklist: what’s implemented, how to run, and what to report)

 2) “Consulting the oracle”: how to proceed to run the TLC experiments

 The branch itself spells out the intended workflow in two places:

 - tla/README.md = how the TLC baseline works + how to run it
 - task.md = experiment plan + what results to extract/report

 Following that plan, the reproducible pipeline is:

 ### Step 0: Get TLC (tla2tools.jar)

 I downloaded TLC v1.7.4 to match the branch assumptions:

 ```bash
   mkdir -p /tmp/leanguard_refs
   curl -L -o /tmp/leanguard_refs/tla2tools_v1.7.4.jar \
     https://github.com/tlaplus/tlaplus/releases/download/v1.7.4/tla2tools.jar
 ```

 ### Step 1: Build Days + Lean checkers

 ```bash
   cd /Users/bli/Playground/days
   cargo build --features lean,dcqcn,l2_pfc --bin days --bin leanguard-run --bin days-trace-export

   cd lean
   lake build
   # important: build these explicitly (lake build does not build all exes by default)
   lake build pfc_check drr_check cubic_check
 ```

 ### Step 2: Generate fresh logs for the experiment configs

 Because the CSV logger appends, delete the old log directories first (or change log_path), then rerun:

 ```bash
   cd /Users/bli/Playground/days
   rm -rf logs/dcqcn_simple logs/dcqcn_multi logs/dcqcn_1s logs/dcqcn_2s logs/dcqcn_10s \
          logs/pfc logs/wfq_simple logs/drr_simple logs/cubic_simple

   for c in \
     configs/dcqcn_simple.toml configs/dcqcn_multi.toml configs/dcqcn_1s.toml \
     configs/dcqcn_2s.toml configs/dcqcn_10s.toml configs/pfc.toml \
     configs/wfq_simple.toml configs/drr_simple.toml configs/cubic_simple.toml
   do
     cargo run --quiet --features lean,dcqcn,l2_pfc --bin days -- "$c"
   done
 ```

 ### Step 3: Run the benchmark suite (LeanGuard vs TLC)

 ```bash
   cd /Users/bli/Playground/days
   python3 utils/bench_leanguard_vs_tlc.py \
     --reps 5 \
     --checker-dir lean/.lake/build/bin \
     --tlc-jar /tmp/leanguard_refs/tla2tools_v1.7.4.jar
 ```

 ### Step 4: Run fault-injection agreement

 Important nuance: `utils/fault_injection_agreement.py` selects the smallest trace under `--logs-root`. If your `logs/`
 contains many old fixtures, it may pick the wrong one. I ran it against a controlled directory:

 ```bash
   cd /Users/bli/Playground/days
   rm -rf logs_paper
   mkdir -p logs_paper/{aqm_src,pfc_src,dcqcn_src,wfq_src,drr_src,cubic_src}
   cp logs/dcqcn_simple/aqm_events.csv logs_paper/aqm_src/
   cp logs/pfc/pfc_events.csv         logs_paper/pfc_src/
   cp logs/dcqcn_simple/dcqcn_events.csv logs_paper/dcqcn_src/
   cp logs/wfq_simple/wfq_events.csv  logs_paper/wfq_src/
   cp logs/drr_simple/drr_events.csv  logs_paper/drr_src/
   cp logs/cubic_simple/cubic_events.csv logs_paper/cubic_src/

   python3 utils/fault_injection_agreement.py \
     --logs-root logs_paper \
     --checker-dir lean/.lake/build/bin \
     --tlc-jar /tmp/leanguard_refs/tla2tools_v1.7.4.jar
 ```

 ### Step 5: Export results

 ```bash
   cd /Users/bli/Playground/days
   python3 utils/export_experiment_results.py --out-md /tmp/results_2026-01-31.md --out-dir logs
 ```

 3) The results I obtained (what to report)

 ### A. DCQCN: Lean vs TLC time (means over 5 runs)

 From the latest utils/bench_leanguard_vs_tlc.py run:

 - dcqcn_simple (2,084 rows): Lean 18 ms vs TLC 1,501 ms (~82×)
 - dcqcn_10s (100,084 rows): Lean 711 ms vs TLC 23,800 ms (~34×)

 (Full table is now embedded into the paper; see next section.)

 ### B. Agreement + diagnostics (fault injection)

 On our controlled suite (AQM/PFC/DCQCN/WFQ/DRR/CUBIC), Lean and TLC:

 - agree on ACCEPT for the unmodified traces,
 - agree on REJECT for 7 injected corruptions,
 - and identify the same first-failing key (time_ns, event_id).

 ### C. Memory (peak RSS snapshots; approximate)

 Using target/debug/leanguard-run --measure-rss:

 - dcqcn_simple: TLC peak RSS ~ 0.4 GB
 - dcqcn_10s (100k rows): TLC peak RSS ~ 7+ GB

 This matches the “oracle” expectation in task.md: TLC pays substantial overhead for explicit-state machinery even in
 “replay-like” trace validation.

4) Paper update: experimental results section written in `~/Playground/lean-paper`

 I updated:

 - `~/Playground/lean-paper/experiments.tex`

 by adding:

 - `\subsection{Comparison with TLA+/TLC trace validation}` (label sec:experiments:tlc)
 - a new table `\label{tab:tlc}` containing the DCQCN Lean-vs-TLC timing results
 - text describing:
     - how the TLC baseline is constructed (generated TraceData.tla, scaling to avoid 32-bit overflow),
     - deterministic replay + “must advance” invariant,
     - agreement and first-failure parity,
     - and memory overhead.

 I also ran make in `~/Playground/lean-paper` to ensure the draft builds successfully with the new section.

 If you want, I can also (a) wire the LaTeX table numbers directly from the generated CSV, or (b) tighten the wording to
 better match whatever camera-ready space constraints you’re targeting (e.g., compress the methodology paragraph and
 keep only the headline ratio + memory point).
