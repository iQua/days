# Code Tour

This page maps the main subsystems of Days to the source tree, to make it easier to navigate the codebase.

## Entry points

- `src/main.rs`: CLI entry point (expects a TOML config path).
- `src/lib.rs`: library root; shared globals (IDs, seed) and module exports.

## Topology construction and execution

- `src/topos/build.rs`: parses topology settings and builds a `petgraph` `UnGraph` (FatTree, Torus, or custom edges/hosts).
- `src/topos/topo.rs`: the main orchestration layer:
  - parses runtime config (threads, duration, mailbox capacity),
  - instantiates switches, schedulers, sources/sinks,
  - computes routes and installs switch forwarding tables,
  - runs the simulation and flushes logs,
  - conditionally wires the PFC link pipeline when enabled.

## Switching and forwarding

- `src/switches/mod.rs`: `SchedulingDiscipline` enum (selects which scheduler is built per edge).
- `src/switches/switch.rs`: `PacketSwitch` (demultiplexes packets using `fib` / `r_fib` and forwards via `Output<Packet>` ports).

## Queueing and scheduling (per edge)

- `src/schedulers/mod.rs`: `SchedulerReport` and `ReportStatistics` trait.
- `src/schedulers/drop.rs`: drop/mark policies (`TailDrop`, `RED`, `RED_ECN`, `ECN_THRESHOLD`).
- `src/schedulers/state.rs`: `QueueState` (shared occupancy tracking, used by PFC).
- `src/schedulers/port.rs`: FIFO scheduler (`Port`).
- `src/schedulers/drr.rs`: Deficit Round Robin.
- `src/schedulers/wrr.rs`: Weighted Round Robin.
- `src/schedulers/wfq.rs`: Weighted Fair Queueing.
- `src/schedulers/sp.rs`: Static Priority.
- `src/schedulers/vc.rs`: Virtual Clock.

## Flows (sources, sinks, routing, collectives)

- `src/flows/mod.rs`: shared data types (distributions, traffic config, feature-gated DCQCN config).
- `src/flows/packet.rs`: `Packet`, ECN field, TCP ACK metadata, control packet markers.
- `src/flows/flow.rs`: `Flow` config parsing and path computation.
- `src/flows/route.rs`: routing implementations (ShortestPath, ECMP, PathFromConfig).
- `src/flows/source.rs`: `PacketSource` enum (dispatch wrapper for different sources).
- `src/flows/dist_source.rs`: packet distribution source.
- `src/flows/tcp_source.rs`: TCP source (Reno/CUBIC/BBR congestion control, optional app-backed data).
- `src/flows/sink.rs`: `PacketSink` enum (dispatch wrapper for different sinks).
- `src/flows/basic_sink.rs`: basic sink (stats + completion notification).
- `src/flows/tcp_sink.rs`: TCP sink (ACK generation, ECN echo).
- `src/flows/cc.rs`: congestion control trait + algorithm selection.
- `src/flows/reno.rs`, `src/flows/cubic.rs`, `src/flows/bbr.rs`: congestion control algorithm implementations.
- `src/flows/collective.rs`: collective parsing and endpoint generation.
- `src/flows/app_source.rs`: application-level byte buffers for TCP collectives (actor-based).
- `src/flows/wire.rs`: optional propagation-delay element (not part of the default topology pipeline).
- `src/flows/dcqcn_source.rs`, `src/flows/dcqcn_sink.rs`: DCQCN models (feature `dcqcn`).

## Optional L2 / PFC

- `src/l2/frame.rs`: `LinkFrame` (data + optional PFC frames).
- `src/l2/link.rs`: `Link` serializer (transmits frames at a configured rate).
- `src/l2/pfc.rs`: `PfcIngressPort` + `PfcEgressGate` and reporting.

## Logging, UI, and tracing

- `src/utils/logger.rs`: CSV logger singleton, report buffering, optional DCQCN event traces.
- `src/utils/ui.rs`: progress bar UI model.
- `src/utils/tracing.rs`: concurrency tracking layer and sampler model.

## Tests and examples

- `tests/`: integration tests (often feature-gated via `--features test`).
- `examples/`: runnable Rust examples (`cargo run --example <name>`).
