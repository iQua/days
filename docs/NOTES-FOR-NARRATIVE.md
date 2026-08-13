# Notes for the narrative-page refresh

The mechanical/reference pages were refreshed against `d38e3a7`. The pages
below were deliberately left for the narrative orchestrator. These bullets are
the code/history changes each page should reflect.

## `content/docs/index.mdx`

- Split the product description into two engines. The legacy engine is the
  Nexosim actor/coroutine implementation; the exact engine compiles a
  `SimulationImage` of LP state and canonical events and runs it on Scalar,
  CPU, Metal, or CUDA (`legacy/src/lib.rs`, `src/scenario/compile.rs`,
  `executor/src/lib.rs`). Do not call the exact engine an actor runtime.
- State the shipped exactness result precisely: the T24 corpus gate compares
  CPU and each compiled device backend with global Scalar. Device-omitted
  diagnostics are excluded; the remaining complete result must be byte-for-byte
  identical (`validation/tests/t24_tcp_corpora.rs`).
- Summarize the final P11 device arc: planner equality, persistent arenas,
  per-entity capacity and retry, ring/vector sizing, compact readback, and
  opt-in capacity warm start. Warm start changes sizing only; capacity is
  refuse-or-run (`executor/src/device_sizing.rs`,
  `executor/src/device_compaction.rs`, `src/bin/t20f_frontier.rs`).
- Mention the 262,144-flow frontier fixture and production fingerprint runner
  (`configs/benchmarks/p11/rq9_frontier_closed_k32.toml`). The supplied campaign
  evidence says the complete frontier was byte-identical on the two CUDA
  architectures used by the project; keep that as machine-qualified campaign
  evidence, not a universal hardware claim.
- Qualify device capabilities: Metal/CUDA accept all five exact schedulers,
  TailDrop/ECN-threshold, and TCP Reno/CUBIC, but reject RED, PFC, DCQCN/CNP,
  and collective generators before execution (`executor/src/validate.rs`).
- Add `csv_logging`: it defaults on; false performs no CSV/trace/manifest
  filesystem work or report-vector growth while preserving correctness
  reductions (`src/utils/logger.rs`, `legacy/tests/csv_logging.rs`).
- Do not reference the profiling-only binaries, counters, scripts, and legacy
  samplers removed by the instrumentation cleanup. Retained
  `RoundMetrics`/`CpuRoundMetrics` are production/gate surfaces
  (`INVENTORY.md`).

## `content/docs/research/index.mdx`

- Add navigation that separates legacy LeanGuard CSV replay, exact P10c
  certificates, and the `lean/DaysExecutor*` execution-model proofs.
- Add a P11/P12 evaluation entry only if the corresponding narrative pages
  clearly distinguish shipped code, committed fixtures, and external campaign
  evidence.
- Do not link to E6 as runnable until its divergent commits are intentionally
  imported into this branch.

## `content/docs/research/design.mdx`

- Replace the single universal canonicalization story with two lineages:
  legacy families generally sort `(time_ns,event_id)` and reject duplicates;
  exact TCP/P10c use full `EventKey` fields, and some P10c checkers require
  already-strict canonical order.
- The global `O(n log n)` replay statement applies to sorting legacy families,
  not to every strict-order exact checker.
- Explain the exact execution model mechanically: an event key is
  `(time_ns, phase, origin_node, origin_sequence)`; safe-horizon rounds drain
  each active LP below a half-open horizon, exchange remote events at a barrier,
  and merge them canonically (`executor/src/event.rs`,
  `executor/src/safe_horizon.rs`).
- Keep terminology aligned with code: “actor/model/mailbox” for legacy
  Nexosim; “LP/image/event/transition” for the exact executor.
- Include the separate `DaysExecutor` proof surface rather than presenting
  trace replay as the only mechanized verification story.

## `content/docs/research/paper-summary.mdx`

- Add the final P11 result described above, including the 262,144-flow frontier
  and architecture-qualified dual-CUDA evidence.
- P12 shipped work in this tree includes the T24 exact TCP corpus/device work,
  T27 CUDA upload provisioning, T28 strict legacy configuration plus exact E1
  expression, T30 `csv_logging`, and the E5 family/gates. The cleanup removed
  the later profiling-only additions; they are not current user or
  instrumentation surfaces.
- E5 committed fixtures are Reno primary (`q200`), CUBIC, and `q256` backup,
  each at 8,192 byte-terminated flows. Use them as runnable P12 examples.
- Attribute the user-supplied E5 performance figures as **indicative native
  run-clock medians on `boston` (Intel i7-13700K, RTX 4090)**: Scalar 126.2 s,
  CPU (22 workers) 5.007 s, CUDA 0.759 s. Do not generalize beyond that machine
  or turn them into a cross-platform guarantee.
- E6 is not committed in `d38e3a7`. Its CBR/load/quiet fixtures exist only on
  divergent commits (`6685e98`, `c186328`, `09a33ce`, with equivalent
  `feat/e6-fixture` commits). Either import them deliberately before calling
  E6 runnable or label all E6 results external/unshipped.
- T31 pinned staging (`3f636cd`) is unmerged. The `boston` A/B verdict was HOLD:
  exactness passed, but process wall regressed materially. Describe it as a
  measured negative, not a current CUDA feature.
- The CUDA SoA drain experiment is also unmerged. Its seven frozen anchors were
  byte-exact, but paired A/B results were slower on E5 and all E6 device clocks
  (E6 load 60 process wall was parity). State the negative result without
  implying the SoA layout ships.
- Remove or source any old test-generation evaluation percentages that have no
  committed result artifact in this tree.

## `content/docs/research/instantiations.mdx`

- Expand the checker inventory to match `lean/lakefile.lean`: legacy DCQCN,
  PFC, CUBIC, DRR, WFQ, AQM, exact TCP, SP, P10c mechanisms/AQM/DCQCN/
  collectives, and the AQM/DCQCN cross-check.
- Distinguish legacy `(time_ns,event_id)` schemas from exact full-`EventKey`
  schemas and strict-order P10c validation.
- Replace vague “depending on instrumentation” language with concrete protocol
  certificate schemas. The cleanup removed profiling instrumentation, not the
  retained certificate loggers/checkers.
- State backend capability boundaries where an instantiation cannot currently
  be generated by Metal/CUDA (PFC, DCQCN, RED, collectives).

## `content/docs/research/dcqcn-certificates.mdx`

- Label the existing page explicitly as the legacy floating-point DCQCN
  certificate (`lean/LeanGuard/DcqcnEventLog.lean`); its pairing, gate,
  freeze, and bound claims remain current.
- Link or add a separate exact P10c DCQCN section. It uses full `EventKey`
  order and is not interchangeable with the legacy `(time_ns,event_id)` CSV.
- State that exact DCQCN runs on Scalar/CPU and is rejected on Metal/CUDA in the
  current backend capability validator.

## `content/docs/research/test-generation.mdx`

- Correct corpus retention: fuzz/campaign persists accepted and rejected cases;
  accepted cases are marked `new` or `redundant`. It does not keep only
  coverage-expanding accepted cases (`src/utils/testgen.rs`).
- Supported campaign protocols are DCQCN, AQM, PFC, WFQ, DRR, and CUBIC.
- Remove or attach committed provenance for the exact coverpoint percentages,
  hit rates, and 97x reduction. This tree contains no pinned result artifact
  for those figures.
- The current CLI has no set-cover subcommand; present set cover as a paper
  method only if that boundary is explicit.
