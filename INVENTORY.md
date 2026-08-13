# Measurement instrumentation removal inventory

This branch removes measurement-only instrumentation instead of feature-gating it. Git history is
the archive: commit `acd7f10cc1fdbefdbc88494de410ec7f69ba2153` is the restoration pointer where
every item below still exists. The isolated T32 merge is `c8374c4`; its first parent,
`1c1cadee80e5392c09631f275550ca5d42c44aa3`, was used to separate T32 additions from the
production implementation around them.

## Deleted

| Measurement site | Disposition |
| --- | --- |
| `executor/src/drain_profile.rs` | Deleted in full: T32 layouts, head-visit and lookup histograms, selected-event/remote-emission totals, checked decoding, and profile-only helper types. |
| `executor/src/{cuda,metal}_kernels.*` T32 drain kernels | Deleted the duplicate `days_round_drain_profile` entries, counter flags, head-visit accounting, remote-emission accounting, and binary-lookup/iteration accounting. The production `days_round` kernels are unchanged. |
| `executor/src/{cuda,metal}.rs` T32 drain runners | Deleted the opt-in run results/APIs, diagnostic layouts and buffers, lazy kernel/pipeline loading, graph/encoder substitutions, and counter decoding. |
| `executor/src/safe_horizon.rs` T32 root trace | Deleted `RootGroupTrace`, `ScalarT32RootTraceRun`, the opt-in runner, per-round observation construction hook, root/publication sets, and the duplicated observed round loop. The ordinary round loop and `RoundMetrics` remain. |
| `src/bin/t32_drain_profile.rs` | Deleted the T32 counter/root-trace reporter and its duplicate identity output. |
| `executor/tests/t32_drain_profile*.rs`, `executor/tests/t32_prepare_profile.rs` | Deleted tests whose only purpose was the removed T32 layouts, kernels, counters, and split/unsplit prepare surfaces. |
| `scripts/t32-instrumentation-*`, `scripts/tests/t32-instrumentation-protocol.sh` | Deleted the T32 capture protocol, fixtures, digest/analysis machinery, binary/counter hashes, cross-machine records, and tolerance enforcement. |
| `executor/build.rs` T32 CUDA build | Deleted the second diagnostic fatbin, `DAYS_T32_PROFILE` define, and host-only placeholder; the production fatbin build remains. |
| CUDA prepare/phase profiling in `executor/src/cuda.rs` and `executor/src/cuda_kernels.cu` | Deleted split prepare kernels, direct-dispatch timestamp plumbing, phase/prepare profile result types, split-vs-unsplit comparison API, and their unit/integration tests. Production CUDA graph timing (`graph_capture_ns`, `host_submit_ns`, `device_ns`, `wall_ns`) and `days_round_prepare` remain. |
| `src/bin/t17c_cuda_profile.rs` | Deleted the CUDA phase profiler, split/unsplit prepare comparison, inclusive five-percent tolerance check, and inline arithmetic tests. |
| Metal phase profiling in `executor/src/metal.rs` | Deleted dispatch-boundary counter sampling, phase result types, profiled runner APIs, sample buffers/decoding, and phase-only tests. Production command-buffer timing and ordinary encoding remain. |
| `executor/Cargo.toml` Metal counter features | Deleted the orphaned `objc2-metal` `MTLComputePass` and `MTLCounters` feature members used only by dispatch-boundary profiling. |
| Metal FEL/merge probes in `executor/src/metal.rs` and `executor/src/metal_kernels.metal` | Deleted matched-control/stress-probe/fan-in types and APIs, diagnostic resources and pipelines, `DAYS_T15E_DIAGNOSTICS`, probe kernels, decomposition logic, and probe-only tests. |
| `src/bin/t15b_round_profile.rs`, `src/bin/t15e_drain_benchmark.rs` | Deleted the binaries that existed only to exercise removed Metal phase/FEL/merge instrumentation. |
| `src/bin/t20a_lp_shape.rs` | Deleted the T20 per-round LP-shape/counter reporter. |
| `configs/benchmarks/p11/fattree_k32_load90_profile.toml` | Deleted the profile-only fixture introduced with the T20 reporter and unreferenced elsewhere. |
| `src/bin/t20e_plan_benchmark.rs` and `measure_{cuda,metal}_planner_for_testing` | Deleted the planner timing/probe binary and its orphaned timing hooks. Exact plan sizing and planner equality APIs/tests remain. |
| `src/bin/t21_horizon_trace.rs`, `tests/t21_horizon_trace.rs` | Deleted the horizon/round trace instrument, CSV/histogram output, and its binary tests. |
| CUDA/Metal readback word counters | Deleted `READBACK_WORDS`, `PLANE_WORDS`, their test-only APIs, all accounting calls, and the measurement-only size/ratio assertions. The production T20l live-region gather/readback mechanism remains. |
| Shared and legacy concurrency tracing | Deleted the two active/peak task counters, the tracing layer, wall-clock sampler thread, `tracing_active`/`tracing_interval` config and CLI/topology plumbing, 37 model `#[instrument]` spans, tests, direct dependencies, example keys, and documentation. Nexosim's generic optional tracing feature remains for independent upstream tracing support. |
| Nexosim `perf_stats` | Deleted both Cargo features, all 15 atomic counters, all gated accounting/reporting sites, `[perf_stats]` output, and documentation/research references. |
| `MetalPlan::new` production compilation | Restricted to `planner-test-hooks` after the planner-timing binary was deleted; its only remaining caller is the retained exact sizing helper `size_metal_plan_for_testing`. |
| Public exports, opt-in flags, test-gate audit rows, and Cargo feature members orphaned by the above | Deleted only after repository-wide reference searches and full builds proved they had no remaining consumer. |

## Kept because the surface is production or shared

| Candidate | Reason kept |
| --- | --- |
| `src/bin/t20a_round_timing.rs` | Production timing bin using the ordinary scalar/CPU clock surface. Its `instrumentation=off`, `end_to_end_ns`, and `backend_ns` output is preserved. |
| `src/bin/t20f_frontier.rs` | Production capability/timing bin using `run_ns` plus the executor-native device clocks; also emits the protected complete-state fingerprint. |
| `src/bin/t15a_round_benchmark.rs`, `src/bin/t15e_sustained_benchmark*`, `src/bin/t16_cuda_gate.rs`, `src/bin/t17c_cuda_geometry.rs`, `src/bin/t17c_wide_corpus.rs`, and `src/bin/t20b3_queue_bytes.rs` | These execute ordinary production paths and measure through production clocks. They do not select a removed diagnostic kernel or hook. |
| `RoundMetrics`, `CpuRoundMetrics`, scalar/CPU round vectors, and their window/replay variants | Shared production/gate surface used by `t20a_round_timing`, E5 round/transition anchors, CPU execution tests, and the real-image replay gate. These predate the T20-T32 probe hooks removed here. |
| Metal dominant-arena high-water hook (`DAYS_DOMINANT_ARENA_HIGH_WATER`, `ArenaOccupancyHighWater`, `DominantArenaHighWater`) | Ambiguous, so kept. It is diagnostic-only, but the protected E5 capped-Metal capacity gate consumes it to prove observed arena occupancy stays within authored production capacities. Removing it would weaken a capacity derivation/correctness gate that is explicitly out of scope. |
| `size_{cuda,metal}_plan_for_testing`, planner test-hook features, `DeviceSizingReport`, capacity caps/floors/retry/warm-start/high-water growth | Production capacity derivation, refusal/retry behavior, exact layout reporting, and planner equality depend on these surfaces. Only wall-clock planner measurement wrappers were removed. |
| `CapacityRetryRecord` and `capacity_retry_trace` | Deterministic production retry/refusal evidence used by production runners and correctness tests, not a measurement-only counter. |
| CUDA/Metal T20l live-region gather/readback | Production result recovery compacts live device regions before copying them to the host. Only its measurement bookkeeping was removed; gather functions, kernels, decode ordering, and retry behavior remain. |
| Structural safe-horizon tests in `executor/tests/safe_horizon_rounds.rs` | Restored the three executor invariants for horizon lookahead, monotonic round partitioning, and the equivalent-slot round bound. They do not depend on the deleted trace binary or CSV output. |
| Queue byte totals and `t20b3_queue_bytes` | Production byte-unit admission state and a production-run regression harness; no diagnostic selector is involved. |
| `aqm_trace`, `mechanism_trace`, `tcp_trace`, LeanGuard CSV traces, and trace manifests | Semantic protocol certificates and validator inputs, not performance measurement instrumentation. |
| Validator, checkpoints, `csv_logging`, topology/lowering, actor/executor mechanisms, protocol state, device compaction/scheduling, and TCP ledger/ring state | Explicitly protected production behavior. No deletion in this branch targets these mechanisms. |
| `RunResult`/`RunSummary` Debug shape, byte counts, FNV-1a64 helpers, rounds, and transitions | Protected identity/fingerprint contract. Field order and output lines remain unchanged. |
| Other P11 fixtures (`rq9_*`) | Still referenced by validator, planner equality, queue-byte, plan-memory, and parallel-lowering correctness gates. |

## Protected anchors and clocks

- E5 PRIMARY Summary identity: 50,572,617 Debug bytes, FNV-1a64
  `56f7b24157e2e852`, 664 rounds, and 212,378,014 transitions.
- CUDA native fields retained: `graph_capture_ns`, `host_submit_ns`, `device_ns`, and `wall_ns`.
- Metal native fields retained: `host_encode_submit_ns`, `device_ns`, and `wall_ns`.
- Legacy native timing output retained byte-for-byte:
  `Nexosim step_until wall-clock time: {:.9} seconds.`,
  `Nexosim total wall-clock time: {:.9} seconds.`, and
  `Elapsed wall-clock time: {:.3} seconds.`
- Benchmark output retained: `backend_ns` in `t20a_round_timing`; `run_ns`, `wall_ns`, and
  `device_ns` in `t20f_frontier`.

## Verification

All commands ran from the isolated cleanup worktree.

- `cargo build --workspace --locked`: PASS.
- `cargo test -p days-executor -- --show-output`: PASS, 329 passed, 2 ignored, 0 failed.
- `cargo test -p days --features test -- --show-output`: PASS, 178 passed, 26 ignored,
  0 failed.
- `cargo test -p days-legacy --features test -- --show-output`: PASS, 247 passed,
  4 ignored, 0 failed.
- `cargo test -p days-validation --features test -- --show-output`: PASS, 23 passed,
  2 ignored, 0 failed.
- `cargo test -p days --features cuda-planner-test --test t20e_planner_bit_equal --
  --show-output`: PASS, 5/5.
- `cargo test -p days --features test,metal-spike --test t20e_planner_bit_equal --
  --show-output`: PASS, 5/5.
- `cargo test --release -p days --features test,metal-spike --test t20b3_queue_bytes
  k32_byte_policy_strict_run_is_retry_free -- --show-output`: PASS.
- `cargo build -p days-legacy --all-features` and the all-feature legacy config-hardening suite:
  PASS, 22/22 config-hardening tests.
- Warnings denied: executor default, executor `metal-spike`, root `test,metal-spike`,
  `leanguard-run`, and legacy all-features all PASS.
- `cargo fmt --all -- --check`, `cargo xtask audit`, and `git diff --check`: PASS.
- Repository-wide deleted-symbol and deleted-binary reference searches: zero matches outside this
  inventory.
- A short isolated legacy run emitted the preserved 9-decimal `Nexosim step_until` and
  `Nexosim total` lines plus the 3-decimal `Elapsed` line; no `Concurrency:` or `[perf_stats]`
  output appeared.

E5 PRIMARY Summary anchors:

- Scalar full completion: PASS — 50,572,617 bytes, FNV-1a64 `56f7b24157e2e852`,
  664 rounds, 212,378,014 transitions.
- CPU W2 and W4 full completion: PASS on both — the same bytes, FNV, rounds, and transitions.
- Metal full completion: PASS — the same bytes, FNV, rounds, and transitions; zero capacity
  retries.
- Production-only `t20f_frontier` Metal run: PASS — preserved `wall_ns`, `device_ns`, and
  `run_ns` records and reproduced the same bytes/FNV/rounds/transitions.

The final cleanup diff is strongly net-negative: **85 files changed, 578 insertions, and 12,880
deletions** against base `acd7f10`.
