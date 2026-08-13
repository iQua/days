# Documentation audit at `d38e3a7`

This is a page-by-page audit of the documentation as it existed after merging
`feat/instrumentation-cleanup` (`d38e3a7`) into `feat/docs-refresh`. It records
the pre-rewrite state. Classifications are exclusive:

- **CURRENT**: the page's factual claims match the code in scope.
- **STALE**: at least one factual claim is wrong or materially misleading.
- **MISSING-TOPIC**: no factual error was found, but the page omits a required
  current subsystem.

## Summary

| Classification | Pages |
| --- | ---: |
| CURRENT | 7 |
| STALE | 38 |
| MISSING-TOPIC | 1 |
| **Total** | **46** |

The worst offenders are `architecture/executor-scope.mdx`,
`architecture/time-concurrency.mdx`, `architecture/simulation-engine.mdx`,
`configuration/logging.mdx`, `configuration/topology.mdx`,
`reference/traces/schema.mdx`, and `verification/leanguard.mdx`.

No page directly names the cleanup's deleted `t17c_cuda_profile`,
`t21_horizon_trace`, T32 profiling counters, legacy `perf_stats`, or legacy
tracing sampler. Several pages do, however, omit the post-cleanup production
surface and should not reintroduce those names.

## Repository boundary found during the audit

The prescribed tree contains the E5 fixture family, T24/T27/T28/T30 work, and
the final P11 executor/device implementation. It does **not** contain E6
fixtures, the T31 experiment, or the SoA drain experiment. E6 and SoA live on
divergent `feat/soa-drain` history; T31 also is not an ancestor of `d38e3a7`.
Consequently E6 cannot truthfully be documented here as a runnable example.
Narrative notes can record those results as external, unshipped experiments.

## Required cross-page topics

The baseline site has no adequate home for these required topics:

- `csv_logging` defaults to `true`; `false` suppresses directory creation,
  CSV/trace/manifest writes, and report-buffer growth while correctness
  reductions still execute (`src/utils/logger.rs:525-562,608-661,1023-1029`,
  `legacy/tests/csv_logging.rs:88-109`).
- Exact execution has Scalar, CPU, Metal, and CUDA backends. The T24 TCP corpus
  comparison removes device-omitted diagnostic planes and then requires the
  complete non-diagnostic result to match the scalar oracle byte for byte
  (`executor/src/validate.rs:18-25`,
  `validation/tests/t24_tcp_corpora.rs:105-117,449-527`).
- Safe-horizon execution uses the minimum positive cross-LP lookahead, a
  half-open per-round horizon, whole-LP draining, and canonical barrier merge
  (`executor/src/safe_horizon.rs:328-397,478-620`).
- Metal/CUDA implement image planning, persistent arenas, per-entity capacity
  sizing and retry, ring/vector sizing, compact canonical readback, and
  capacity warm starts. Warm-start data is a sizing hint, not simulation state
  (`executor/src/device_sizing.rs:516-672`,
  `executor/src/device_compaction.rs:1-42`, `executor/src/metal.rs:617-890`,
  `src/bin/t20f_frontier.rs:383-528`).
- The 262,144-flow frontier is a committed fixture and production runner
  surface (`configs/benchmarks/p11/rq9_frontier_closed_k32.toml:9-16`,
  `src/bin/t20f_frontier.rs:383-528`).
- Metal/CUDA reject DCQCN/CNP, collective generators, PFC control/pause, and
  RED admission before execution, with messages that name the backend and
  suggest Scalar or CPU (`executor/src/validate.rs:405-469`).
- E5 is committed and runnable (`tests/t21_p12_e5.rs:951-1039,1224-1260` and
  `configs/benchmarks/p12/e5_wide_k32_q200*.toml`). E6 is absent from this
  tree and cannot be advertised as runnable.
- The supplied E5 medians are indicative only: scalar 126.2 s, CPU 5.007 s,
  and CUDA 0.759 s on `boston` (Intel i7-13700K, RTX 4090). They are not
  encoded as a committed benchmark result in this tree.

## Architecture

### `architecture/code-tour.mdx` — STALE

- It omits Dragonfly and says graph construction returns a graph plus a host
  vector. The builder supports FatTree, Torus, Dragonfly, and custom graphs and
  returns `HostAttachments` (`src/topos/config.rs:106-145`,
  `src/topos/build.rs:461-516`).
- It says `Wire` is outside the topology pipeline. Explicit propagation
  enables physical-link and host-attachment wire stages
  (`src/topos/config.rs:175-195`, `legacy/src/topos/topo.rs:962-998`).
- It presents a legacy-only tour as the whole codebase and omits exact
  lowering, image/event/model validation, all four backends, device sizing and
  compact readback (`src/scenario/compile.rs:491-526`,
  `executor/src/lib.rs:1-149`, `executor/src/device_compaction.rs:1-42`).

### `architecture/executor-scope.mdx` — STALE

- It calls the exact executor planned and unselectable. The compiler and
  Scalar/CPU/Metal/CUDA stack are implemented (`src/scenario/compile.rs:491-526`,
  `executor/src/lib.rs:1-149`).
- Its v1 matrix is obsolete. Exact lowering implements FIFO, SP, WFQ, DRR,
  WRR; TailDrop, RED, RED-ECN, ECN-threshold; PacketDistribution, TCP, DCQCN,
  PFC, and collectives, subject to backend validation
  (`src/scenario/compile.rs:575-765,895-1045`).
- “Open-loop UDP” is not the model name; the implemented mechanism is
  `PacketDistribution` (`src/scenario/compile.rs:60-72,895-903`).
- It describes one CPU process instead of the persistent whole-LP worker pool
  (`executor/src/cpu.rs:306-410`).
- It makes positive lookahead universal. Global scalar accepts zero-delay
  channels; round scalar, CPU, Metal, and CUDA require positive cross-LP
  lookahead (`executor/src/safe_horizon.rs:370-377`,
  `executor/tests/validate.rs:959-965`).
- It limits all backends to three schedulers. All four implement five; devices
  accept TailDrop/ECN-threshold/TCP and reject RED/PFC/DCQCN/collectives
  (`executor/src/model.rs:143-191`, `executor/src/validate.rs:405-469`).
- It says only `model_host_attachment` activates endpoint stages. Declaring
  scalar propagation also activates them (`src/topos/config.rs:175-195`).
- It promises identical full diagnostic vectors on devices. Device results
  intentionally omit scalar reference diagnostic planes
  (`executor/src/scalar.rs:99-167`,
  `validation/tests/t24_tcp_corpora.rs:105-117`).
- Its PFC/DCQCN/follow-on and future-rejection sections are implemented today;
  VirtualClock is the remaining unsupported exact discipline
  (`src/scenario/compile.rs:575-773`, `tests/t25_mechanism_lowering.rs:75-96`).
- It omits safe-horizon rounds, canonical exchange, device planning/arenas,
  capacity retry/warm start, compact readback, and the 262,144-flow frontier
  (`executor/src/safe_horizon.rs:478-620`,
  `executor/src/device_sizing.rs:516-672`, `src/bin/t20f_frontier.rs:383-528`).

### `architecture/overview.mdx` — STALE

- It presents the legacy actor/coroutine model as all of Days. The exact engine
  is an LP/image/event transition system (`executor/src/image.rs:16-24`,
  `executor/src/event.rs:51-127`).
- It says message timestamps normally replace the engine clock. Current legacy
  handlers read the exact event clock; packet `f64` time is a compatibility and
  reporting view (`legacy/src/utils/exact_time.rs:7-62`,
  `legacy/src/switches/switch.rs:75-105`).
- Its packet path omits optional host injection/delivery and propagation stages
  (`legacy/src/topos/topo.rs:962-998,1766-1874`).
- Its output list omits `csv_logging`, AQM traces, and opt-in `tcp_metrics.csv`
  (`src/utils/logger.rs:28-44,228-302,741-762`).

### `architecture/simulation-engine.mdx` — STALE

- It says Days generally builds on Nexosim. Nexosim is the legacy runtime; the
  exact executor is independent (`legacy/src/lib.rs:95-148`,
  `executor/src/lib.rs:1-40`).
- It presents message-local `f64` time as authoritative. Legacy now converts
  the Nexosim clock to exact integer nanoseconds, while exact images use
  integer nanoseconds directly (`legacy/src/utils/exact_time.rs:7-88`,
  `executor/src/event.rs:51-83`).
- It omits the global scalar lifecycle and safe-horizon round executors
  (`executor/src/safe_horizon.rs:166-184,328-620`).

### `architecture/time-concurrency.mdx` — STALE

- It says most handlers avoid `cx.time()` and treat packet `f64` time as now.
  Sources, sinks, switches, schedulers, and wires now read the exact engine
  clock (`legacy/src/utils/exact_time.rs:7-62`,
  `legacy/src/flows/source.rs:151-178`, `legacy/src/flows/sink.rs:286-325`).
- It says tests assert packet time equals engine time. Current compatibility
  checks generally allow packet time to be no later than the event clock
  (`legacy/src/switches/switch.rs:75-105`).
- It omits exact `EventKey` phase ordering and safe-horizon half-open rounds
  (`executor/src/event.rs:51-127`, `executor/src/safe_horizon.rs:328-620`).

### `architecture/topology-build.mdx` — STALE

- It says the builder returns `hosts: Vec<usize>` and omits Dragonfly; it
  returns `HostAttachments` and supports Dragonfly
  (`src/topos/build.rs:461-516`, `src/topos/config.rs:106-145`).
- It omits `num_threads`, `hosts_per_edge`, host attachment, scalar/tiered
  propagation, and `FatTreeEcmp` (`src/topos/config.rs:5-17,66-195`,
  `legacy/src/topos/topo.rs:71-125`).
- Its scheduler list needs engine scope: legacy supports VirtualClock; exact
  lowering rejects it and supports the other five disciplines
  (`src/scenario/compile.rs:575-640`).
- It describes only `Topology::new`, not separate exact compilation, stable LP
  IDs, LP ownership, exact routing/pairing/collectives, or capability checks
  (`src/scenario/compile.rs:491-526,797-822,1016-1045`,
  `src/scenario/ids.rs:24-57`, `executor/src/validate.rs:364-469`).

## Components

### `components/flows.mdx` — STALE

- It presents only legacy actor flows. Exact lowering has distinct supported
  subsets and rejects explicit flow IDs/dependencies/paths and unsupported
  transport combinations with specific compile errors
  (`src/scenario/compile.rs:637-773,895-1045`).
- It omits `flow_set.pairing` (`Random`, `SwitchOffsetHalf`,
  `SameSwitchNext`) and top-level `FatTreeEcmp`
  (`src/topos/build.rs:41-46`, `legacy/src/flows/flow.rs:404-588`).
- The ECMP description is incomplete: current P12 ECMP is a shared O(1)
  canonical fat-tree routing policy, not merely enumeration of all equal-cost
  paths (`legacy/src/topos/topo.rs:71-154`).

### `components/l2.mdx` — STALE

- “Zero overhead when disabled” is not a code-verifiable guarantee. The valid
  claim is that legacy L2/PFC is compile- and runtime-gated
  (`legacy/Cargo.toml:18-23`, `src/topos/config.rs:70-104`).
- Compiling `l2` does not by itself make all internal links use `LinkFrame`;
  the topology installs that pipeline for runtime PFC with `l2_pfc`
  (`legacy/src/topos/topo.rs:962-998`).
- The diagram routes reverse PFC through a serializer, but ingress PFC output
  connects directly to the upstream egress gate
  (`legacy/src/topos/topo.rs:1045-1056`).
- It describes PFC as legacy-only and omits exact scalar/CPU PFC lowering and
  the explicit Metal/CUDA PFC capability rejection
  (`src/scenario/compile.rs:702-765`, `executor/src/validate.rs:442-450`).
- It omits the incompatibility between legacy PFC and configured propagation
  (`legacy/src/config.rs:1080-1130`).

### `components/logging.mdx` — STALE

- It says Days always writes CSVs and buffers reports. With
  `csv_logging=false`, no directory/files/manifest are created and report rows
  are discarded before vector growth (`src/utils/logger.rs:525-562,647-661`,
  `legacy/tests/csv_logging.rs:88-109`).
- Its inventory omits `aqm_events.csv`, `tcp_events.csv` discovery, opt-in
  `tcp_metrics.csv`, and `traces.json` (`src/utils/logger.rs:228-302,741-762`,
  `src/bin/leanguard-run.rs:333-353`).
- It does not distinguish legacy on-disk logging from exact executor result and
  certificate serializers (`executor/src/tcp_trace.rs:27-93`,
  `executor/src/mechanism_trace.rs:168-220`).

### `components/packet.mdx` — STALE

- It calls `Packet.time` the current simulation time. In current legacy code it
  is a compatibility/report timestamp; the exact Nexosim clock is authoritative
  (`legacy/src/utils/exact_time.rs:7-62`,
  `legacy/src/switches/switch.rs:75-105`).
- It omits the exact executor's fixed-layout packet/event image, which does not
  use the legacy actor `Packet` as its execution representation
  (`executor/src/image.rs:16-24`, `executor/src/event.rs:51-127`).
- It generalizes `last_packet` completion. Basic/DCQCN sinks use the marker,
  while TCP completion is source-owned (`legacy/src/flows/basic_sink.rs:104`,
  `legacy/src/flows/dcqcn_sink.rs:208`,
  `legacy/src/topos/topo.rs:1876-1893`).

### `components/schedulers.mdx` — STALE

- It describes only legacy `f64` scheduler actors and presents that as the
  complete scheduler implementation. Exact Scalar/CPU/Metal/CUDA use the image
  model and five supported exact disciplines (`executor/src/model.rs:143-191`,
  `executor/src/device_scheduler.rs:1-40`).
- Its flow-control description uses packet timestamps as the idle test; current
  legacy departure paths use the exact engine clock
  (`legacy/src/schedulers/port.rs:334-361`).
- It does not state the capability split: VirtualClock is legacy-only; devices
  reject RED but support TailDrop and ECN-threshold
  (`src/scenario/compile.rs:575-640`, `executor/src/validate.rs:453-469`).

### `components/switch.mdx` — STALE

- It says switches update time from `packet.time` and tests require equality
  with `cx.time()`. The handler reads the exact event clock and only treats
  packet time as a compatibility value that may not be later than the event
  (`legacy/src/switches/switch.rs:75-105`).
- It omits the exact engine's LP-per-egress representation and stable semantic
  IDs (`src/scenario/ids.rs:24-57`, `executor/src/image.rs:16-24`).

## Configuration

### `configuration/collectives.mdx` — STALE

- It mixes two schemas without a usable boundary: the strict legacy loader
  accepts Broadcast, Gather, AllReduce, and RingAllReduce, while exact lowering
  accepts RingAllReduce and AllGather. `AllGather` is not accepted by the
  legacy `days` CLI (`legacy/src/config.rs:148-153`,
  `src/scenario/compile.rs:1016-1045`).
- Its final link points to logging for app-source configuration, and documents
  `chunk_size` elsewhere even though the strict legacy loader rejects that key
  (`legacy/src/config.rs:1008-1014`).
- It says `graph` is stored but unused. Legacy derives endpoints from graph
  edges when explicit endpoints are absent and validates graph consistency
  (`legacy/src/flows/collective.rs:332-359`,
  `legacy/src/config.rs:779-819`).
- It omits current lowerer's rejection of TCP/DCQCN collectives,
  `first_flow_id`, routing, paths, graph input, and duration termination
  (`src/scenario/compile.rs:1038-1081,1120-1130`).
- It omits that devices reject collective generators before execution
  (`executor/src/validate.rs:428-431`).

### `configuration/flows.mdx` — STALE

- It says flow-set endpoints are random only and omits accepted `pairing`
  policies (`src/topos/build.rs:41-46`, `legacy/src/flows/flow.rs:419-588`).
- Its routing list omits `FatTreeEcmp` and its constrained transport/termination
  domain (`src/topos/route.rs:106-122`, `legacy/src/flows/flow.rs:166-243`).
- It does not distinguish legacy TCP BBR/ECN/custom-CUBIC support from exact
  lowering, which accepts Reno/CUBIC but rejects TCP ECN, BBR, unsupported CUBIC
  parameters, nondeterministic distributions, and duration-terminated TCP/DCQCN
  (`src/scenario/compile.rs:637-773`).
- It says `path` overrides routing. Strict validation rejects paths combined
  with ShortestPath/ECMP, and `PathFromConfig` requires a path
  (`legacy/src/config.rs:373-396`). Generated-topology explicit paths use
  attachment-switch IDs rather than configured host IDs
  (`legacy/src/flows/flow.rs:446-465`).
- It says traffic needs either `size` or `duration`; the precise rule is
  exactly one (`legacy/src/config.rs:437-450`).
- It presents stochastic distributions as portable. Exact lowering requires a
  constant positive packet size and, for open-loop traffic, a constant positive
  arrival interval (`src/scenario/compile.rs:1203-1228,1545-1615`).

### `configuration/l2.mdx` — STALE

- It omits `link.propagation_ns` and `link.propagation_tiers`, and the mutual
  exclusion/engine restrictions around them (`src/topos/config.rs:90-104`,
  `src/scenario/compile.rs:812-822`, `legacy/src/config.rs:1080-1130`).
- It says only that PFC needs a feature; exact Scalar/CPU can execute lowered
  PFC while Metal/CUDA reject the image with a capability error
  (`src/scenario/compile.rs:702-765`, `executor/src/validate.rs:442-450`).
- It implies `[link.pfc]` has one optional/defaulted shape. Legacy defaults it;
  exact lowering requires it, requires eight-entry XOFF/XON/buffer arrays, and
  rejects nonzero refresh/drain timers (`legacy/src/topos/topo.rs:1065-1100`,
  `src/scenario/compile.rs:702-761,841-852`).

### `configuration/logging.mdx` — STALE

- It omits the accepted root key `csv_logging` and its default-true/no-files
  false behavior (`legacy/src/config.rs:18-40`,
  `src/utils/logger.rs:525-562,608-661`).
- It presents `time_quantum_ns` as a general engine knob. Legacy accepts it;
  exact lowering rejects nonzero values
  (`legacy/src/topos/topo.rs:872-875`, `src/scenario/compile.rs:769-773`).
- It documents `[app_source].chunk_size` as accepted and effective. The strict
  legacy loader explicitly rejects it (`legacy/src/config.rs:1008-1014`).
- Its app-source defaults are misleading because the present-table defaults and
  whole-struct defaults differ; document accepted keys without promising an
  unverified effective default (`src/topos/config.rs:147-153`,
  `legacy/src/flows/app_source.rs:17-35`).
- It omits that `[app_source]` is rejected unless an active TCP Broadcast or
  RingAllReduce collective uses it (`legacy/src/config.rs:1014-1035`).
- It presents seed zero as reproducible, but strict validation rejects zero
  because it selects nondeterministic entropy (`legacy/src/config.rs:922-927`).
- Its `ui_interval = duration / 100` default omits the one-nanosecond floor and
  independent absent-duration fallback (`legacy/src/utils/ui.rs:34-43`).
- Its output list omits opt-in `tcp_metrics.csv` and does not condition all
  files, including `traces.json`, on `csv_logging=true`
  (`src/utils/logger.rs:741-762,1023-1029`).

### `configuration/overview.mdx` — STALE

- It calls the legacy CLI schema “Days” without distinguishing exact lowering's
  narrower accepted model combinations (`legacy/src/config.rs:12-46`,
  `src/scenario/compile.rs:491-526`).
- It calls both `port_rate` and `rate_gbps` bits per second. `port_rate` is bps;
  `*_rate_gbps` values are Gbit/s and are scaled by one billion during lowering
  (`src/scenario/compile.rs:1323-1352`).
- Its feature-gated trace inventory omits AQM and TCP and omits
  `csv_logging=true` (`src/utils/logger.rs:570-586`,
  `src/bin/leanguard-run.rs:333-353`).
- It does not state that the strict legacy loader denies unknown fields
  (`legacy/src/config.rs:12-46`).

### `configuration/switches.mdx` — STALE

- Its supported table merges engines. VirtualClock is valid only in legacy;
  exact lowering supports FIFO/SP/WFQ/DRR/WRR and rejects VirtualClock
  (`src/topos/config.rs:31-39`, `src/scenario/compile.rs:575-640`).
- It calls VirtualClock `vticks` required. Legacy supplies `[1.0]` when absent
  in topology construction; strict validation only rejects an explicitly empty
  vector (`legacy/src/topos/topo.rs:1439-1457`,
  `legacy/src/config.rs:698-717`).
- It says only WFQ requires positive exact weights. DRR and WRR do as well
  (`src/scenario/compile.rs:854-873`).
- It presents all four drop policies without backend scope. Exact Scalar/CPU
  support them, but devices reject RED/RED-ECN and accept TailDrop and
  ECN-threshold (`executor/src/validate.rs:453-469`).
- It calls zero capacity universally unlimited. Exact ECN-threshold requires
  positive capacity and exact RED requires capacity of at least ten
  (`src/scenario/compile.rs:648-693,1802-1841`).
- Its ECN-threshold boundary is legacy-only. Legacy uses
  `post_depth > floor(fraction*capacity)`; exact uses
  `post_depth >= ceil(fraction*capacity)`
  (`legacy/src/schedulers/drop.rs:189-202`,
  `executor/src/scalar.rs:4980-4994`).
- It does not surface the exact validation errors for zero weights, class
  vectors, capacity, or unsupported mechanism/backend combinations
  (`executor/src/validate.rs:364-469,3307-3382`).

### `configuration/topology.mdx` — STALE

- It says there are three modes and omits Dragonfly
  (`src/topos/config.rs:106-145`, `src/topos/build.rs:461-516`).
- It says the builder returns a host node vector. It returns explicit
  `HostAttachments` (`src/topos/build.rs:461-516`).
- It says FatTree hosts are `0..k^2/2`; current `hosts_per_edge` controls host
  multiplicity and must be in `1..=k/2` (`src/topos/build.rs:329-346,530-538`).
- It omits Dragonfly's `routers_per_group`, `global_ports_per_router`, and
  `hosts_per_router`, plus host attachment, scalar/tiered propagation, and
  top-level routing policy (`src/topos/config.rs:126-195`,
  `src/scenario/compile.rs:797-822`).

## User-facing setup and examples

### `install.mdx` — STALE

- It presents `cargo build -p days-legacy` as the general build; that builds
  only the Nexosim engine. The workspace also contains current executor,
  shared/current, validation, and xtask packages (`Cargo.toml:19-21`).
- It omits the current `metal-spike`, `cuda`, `cuda-planner-test`, and device
  test-hook feature surfaces (`Cargo.toml:23-32`,
  `executor/Cargo.toml:10-25`).
- It describes `test` generically; at the shared package it enables executor
  planner test hooks (`Cargo.toml:23-24`).

### `quickstart.mdx` — STALE

- It gives only the legacy CLI while speaking for all engines
  (`executor/src/validate.rs:18-25`).
- Its default output list omits AQM/PFC traces and opt-in `tcp_metrics.csv`, and
  it fails to condition every output on `csv_logging=true`
  (`src/utils/logger.rs:570-586,741-762,1023-1029`).
- It omits current backend selection, safe-horizon execution, and capability
  rejection behavior.

### `testing.mdx` — STALE

- It locates tests only under `tests/`; the supported matrix spans
  `executor/tests`, `tests`, `legacy/tests`, and `validation/tests`.
- Its default four-package matrix is current, but the device section omits the
  planner equality/strict K32 gates, CUDA surfaces, T24 four-backend corpus
  campaign, and E5 completion/identity gates (`README.md:30-57`,
  `validation/tests/t24_tcp_corpora.rs:449-527`,
  `tests/t21_p12_e5.rs:951-1039,1224-1260`).

### `examples.mdx` — STALE

- It says there are only config-driven legacy runs and legacy Rust examples.
  Root exact examples and the `t20f_frontier` runner also ship
  (`examples/scalar_benchmark.rs:47-79`,
  `examples/round_benchmark.rs:15-68`, `src/bin/t20f_frontier.rs:35-61`).
- It omits the committed E5 and T24 fixture families. E6 cannot be added as a
  runnable example because it is absent from this tree
  (`configs/benchmarks/p12/e5_wide_k32_q200*.toml`,
  `configs/benchmarks/tcp/t24-corpus-manifest.md`).

## Reference

### `reference/cli/days.mdx` — CURRENT

The one-config positional interface, wrong-arity panic, error exit, and default
`RUST_LOG=info` match `legacy/src/main.rs:5-23`.

### `reference/cli/index.mdx` — STALE

- It implies three binaries are the complete shipped surface. The root package
  retains gate/benchmark binaries and the production `t20f_frontier` runner
  under `src/bin/`; deleted profiling-only binaries are correctly absent.
- It does not distinguish user-facing tools from retained validation binaries
  (`Cargo.toml`, `src/bin/t20f_frontier.rs:35-61`).

### `reference/cli/leanguard-run.mdx` — STALE

- It lists exit codes 0 and 1 only. Configuration/refusal/no-trace failures use
  exit 2 (`src/bin/leanguard-run.rs:140-172,259-265`).
- Its fallback discovery omits AQM and TCP; current discovery covers PFC, AQM,
  DCQCN, WFQ, DRR, CUBIC, and TCP (`src/bin/leanguard-run.rs:333-353`).
- Its output summary omits stable fields such as version, mode, config/log
  paths, determinism, and trace discovery (`src/bin/leanguard-run.rs:95-109`).
- It omits `tcp_check`, the AQM/DCQCN cross-check, and the fact that
  `csv_logging=false` leaves no certificates to discover
  (`src/bin/leanguard-run.rs:364-419`, `src/utils/logger.rs:1023-1029`).

### `reference/cli/leanguard-testgen.mdx` — CURRENT

Options, subcommands, corpus layout, artifacts, and coverage-union schema match
`src/bin/leanguard-testgen.rs:8-67` and `src/utils/testgen.rs:307-347,498-574`.

### `reference/index.mdx` — CURRENT

The page contains accurate navigation claims only.

### `reference/traces/index.mdx` — CURRENT

Its two legacy on-disk output categories are accurate. A rewrite should still
separate them from exact executor in-memory certificate serializers.

### `reference/traces/manifest.mdx` — STALE

- It says `traces.json` is always written at run end. It is written only when
  `csv_logging` is enabled; false also avoids creating `log_path`
  (`src/utils/logger.rs:535-562,1023-1029`).
- Its candidate list omits `aqm_events.csv`; the version-1 schema itself is
  correct (`src/utils/logger.rs:1054-1068`,
  `src/utils/trace_manifest.rs:5-30`).

### `reference/traces/schema.mdx` — STALE

- It universalizes `(time_ns,event_id)`. That is the legacy LeanGuard key;
  exact TCP/P10c certificates use full `(time_ns, event_phase,
  event_origin_node, event_origin_sequence)` keys
  (`executor/src/tcp_trace.rs:7,27-37`,
  `lean/LeanGuard/P10c/DcqcnEventLog.lean:45-55`).
- It says nanoseconds are rounded from internal `f64`; exact executor time is
  already integral.
- It omits TCP and exact P10c rate/PFC/DRR/WRR/DCQCN/collective certificate
  families (`lean/lakefile.lean:8-49`).

## Internals

### `internals/index.mdx` — MISSING-TOPIC

Its links are accurate, but it exposes only Nexosim internals. It needs the
exact compiler/image, canonical events, global scalar and safe-horizon paths,
CPU LP ownership, Metal/CUDA planning/capacity/readback/warm-start, and backend
validation (`src/scenario/compile.rs:491-526`, `executor/src/lib.rs:1-149`,
`executor/src/device_sizing.rs:516-672`).

### `internals/nexosim/index.mdx` — STALE

- It calls Nexosim the foundation of Days without saying legacy. Exact
  execution is separate (`legacy/src/lib.rs:95-148`, `executor/src/lib.rs:1-40`).
- The listed vendored paths and `SimInit` compatibility shims remain current
  (`crates/nexosim/src/simulation/sim_init.rs:129-151`).

### `internals/nexosim/ports.mdx` — STALE

- It says legacy models communicate only through ports. App sources also use
  Tachyonix channels, and scheduler/PFC plumbing shares atomic queue state
  (`legacy/src/flows/app_source.rs:1-15,126-207`,
  `legacy/src/schedulers/state.rs:1-54`).
- It needs explicit legacy scope; the exact engine does not use Nexosim ports.

### `internals/nexosim/simulation.mdx` — STALE

- It calls `Topology::run` the Days entrypoint; it is the legacy entrypoint
  (`legacy/src/topos/topo.rs:2030-2093`).
- `process_event`/`process_query` use registered `EventId`/`QueryId`; direct
  compatibility method injection is `process_event_fn`
  (`crates/nexosim/src/simulation.rs:368-450`).
- `run` does not simply run until halted: tickless mode returns when no events
  remain, while ticker mode waits for halt
  (`crates/nexosim/src/simulation.rs:170-208,323-365`).
- Its determinism discussion omits exact `EventKey` and canonical round merge
  (`executor/src/event.rs:51-62`, `executor/src/safe_horizon.rs:541-581`).

### `internals/nexosim/time.mdx` — STALE

- It presents seconds-as-`f64` as Days-wide time. Exact execution and current
  legacy event scheduling use integer nanoseconds; legacy `f64` remains a
  controller/report compatibility view (`legacy/src/utils/exact_time.rs:7-88`,
  `executor/src/event.rs:51-83`).
- It repeats the obsolete claim that hot paths normally avoid `cx.time()`
  (`legacy/src/flows/source.rs:151-178`,
  `legacy/src/schedulers/port.rs:334-361`).

## Verification

### `verification/design.mdx` — STALE

- It says the current canonical key is universally `(time_ns,event_id)`.
  Exact TCP and P10c use full `EventKey`; some exact checkers require already
  canonical input rather than sorting it
  (`executor/src/tcp_trace.rs:7,27-37`,
  `lean/LeanGuard/P10c/DcqcnEventLog.lean:130-135,190-194`).
- It presents one `*EventLog`/`Semantics` layout for every protocol, which does
  not describe the P10c mechanism family.
- Its executable list omits TCP, SP, P10c mechanisms/AQM/DCQCN/collectives,
  AQM, and the AQM/DCQCN cross-check (`lean/lakefile.lean:8-49`).
- It omits the `lean/DaysExecutor*` execution-model proof surface.

### `verification/index.mdx` — CURRENT

Its narrow description of trace-replay conformance is accurate. It should later
link the separate exact executor proof/P10c lineage.

### `verification/leanguard.mdx` — STALE

- It omits the `csv_logging=true` requirement for legacy CSV/manifest output
  (`src/utils/logger.rs:535-562,1023-1029`).
- Its CUBIC example uses PacketDistribution-only `configs/simple.toml`; the
  runnable fixture is `configs/ci/leanguard_cubic.toml`.
- Its DRR and WFQ examples also use FIFO `configs/simple.toml`; runnable
  fixtures are `configs/ci/leanguard_drr.toml` and
  `configs/ci/leanguard_wfq.toml`.
- Its checker list omits TCP, SP, and the P10c executables
  (`lean/lakefile.lean:12-49`).

## Narrative pages (audit only; do not edit in this refresh)

### `index.mdx` — STALE

- It presents the legacy actor/coroutine architecture as all of Days. The exact
  image executor has Scalar/CPU/Metal/CUDA backends
  (`executor/src/validate.rs:18-25`).
- It combines capabilities without backend qualification; devices reject
  DCQCN, collectives, PFC, and RED (`executor/src/validate.rs:405-469`).
- Its output list omits `csv_logging`, AQM and scheduler/CUBIC traces, and E5
  TCP metrics (`src/utils/logger.rs:228-302,741-762`).

### `research/dcqcn-certificates.mdx` — CURRENT

The legacy DCQCN canonical key, CNP matching, gates, parameter freezing, and
rate/state bounds match `lean/LeanGuard/DcqcnEventLog.lean:65-66,304-394` and
`lean/LeanGuard/Dcqcn/Semantics.lean:31-84`.

### `research/design.mdx` — STALE

- It universalizes sort-and-replay and `(time_ns,event_id)`. Exact P10c uses
  full `EventKey` and validates already-canonical order
  (`lean/LeanGuard/P10c/DcqcnEventLog.lean:130-135,190-194`).
- Its global `O(n log n)` claim therefore does not apply to linear strict-order
  P10c checks, and it omits the `DaysExecutor` mechanized proofs.

### `research/index.mdx` — CURRENT

The page contains accurate navigation claims only.

### `research/instantiations.mdx` — STALE

- It says all current checkers share one legacy canonicalization family. Exact
  executor checkers use full `EventKey`.
- It omits legacy DCQCN, exact TCP, SP, P10c rate/PFC/DRR/WRR/DCQCN/collective,
  and the AQM/DCQCN cross-layer checker (`lean/lakefile.lean:12-49`).

### `research/paper-summary.mdx` — STALE

- It presents one universal certificate shape; the code has legacy
  `(time_ns,event_id)` and exact full-`EventKey` lineages.
- It says generation keeps only coverage-expanding cases. The implementation
  persists all accepted cases and marks novelty new/redundant
  (`src/utils/testgen.rs:498-574,1234-1247`).
- Its evaluation claims have no committed result artifact in this tree.

### `research/test-generation.mdx` — STALE

- It repeats the incorrect keep-only-novel behavior; accepted and rejected
  cases are persisted and annotated (`src/utils/testgen.rs:498-574,823-900`).
- Its exact campaign percentages and counts have no committed result pin.
- It describes offline greedy set cover, but no set-cover command or
  implementation exists (`src/bin/leanguard-testgen.rs:31-67`).
