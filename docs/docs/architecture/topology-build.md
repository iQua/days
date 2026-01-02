# Topology Build Pipeline

This page describes how a TOML config turns into a runnable simulation.

## 1) Build the network graph

`src/topos/build.rs` constructs an undirected `petgraph::graph::UnGraph<usize, ()>` plus a `hosts: Vec<usize>` list.

Supported topology modes:

- **FatTree** (`[topology] category="FatTree"` + `[topology.fat_tree] k = ...`)
- **Torus** (`[topology] category="Torus"` + `[topology.torus] dim = ... n = ...`)
- **Custom graph** (omit `[topology]` and provide `edges = [...]` + `hosts = [...]`)

## 2) Initialize `Topology`

`Topology::new` (`src/topos/topo.rs`) reads the config multiple times into smaller structs:

- `Config` (switch config, optional topology config, optional L2/app-source config, `time_quantum_ns`)
- `MailboxConfig` (`mailbox_capacity`)
- `UIConfig` (`duration`, `ui_interval`)
- `ConcurrencyConfig` (`threading`, `hot_workers`, `concurrency_level`)

It also sets global ID ranges so that switch IDs and endpoint IDs do not collide:

- switches are assigned IDs `[0..num_switches)`
- endpoints (sources/sinks) start at `ENDPOINT_ID = num_switches`

This matters because `PacketSwitch.outputs` can target either a neighboring switch ID or an endpoint ID.

## 3) Expand collectives into flows

Collectives (`[[collective]]` / `[[collective_set]]`) are high-level communication patterns used for ML-style workloads. Before routing, `Topology::process_collectives` produces concrete `Flow` entries for each collective.

Special case: `CollectiveType::RingAllReduce` expands into scatter + gather phases with explicit dependency ordering.

## 4) Connect switches (create per-edge schedulers)

`Topology::connect` iterates the graph edges and calls `connect_neighbours(upstream, downstream)` for each adjacency.

For every directed edge, Days instantiates a **scheduler model** based on `switch.discipline`:

- `FIFO` -> `Port`
- `DRR` -> `DRRServer`
- `WRR` -> `WRRServer`
- `WFQ` -> `WFQServer`
- `SP` -> `SPServer`
- `VirtualClock` -> `VirtualClockServer`

Each scheduler is registered into `SimInit` and connected so that:

```
PacketSwitch.outputs[downstream_id] -> Scheduler::packet_received
Scheduler.output -> (optional L2 pipeline) -> downstream PacketSwitch::packet_received
```

## 5) Attach flow endpoints to host switches

For each `Flow`, Days creates:

- a `PacketSource` (distribution, TCP, or DCQCN)
- a `PacketSink` (basic sink, TCP sink, or DCQCN sink)

and wires them to the host switches selected by `flow.graph` (or generated host pairs for `flow_set`).

Flow dependencies (`starts_after` / `starts_before`) are wired using `FlowFinishMsg`:

- for packet distribution flows: the sink notifies dependents on `last_packet`
- for TCP/DCQCN: the source notifies dependents when the flow completes

## 6) Route flows and install forwarding tables (FIB)

`Topology::route_flows`:

1. Computes each flow’s path (`Flow::compute_path`) using one of:
   - shortest path
   - ECMP
   - explicit `path = [...]`
2. Installs per-flow forwarding entries:
   - `fib[flow_id]` on intermediate switches for forward traffic
   - `r_fib[flow_id]` on intermediate switches for reverse traffic (ACK/control)

Once FIBs are installed, switches can forward packets purely based on `flow_id`.

## 7) Activate UI, tracing, and run

Finally, `Topology::run`:

- activates the progress UI and optional concurrency tracer,
- initializes the simulation and steps until `duration`,
- collects sink statistics and flushes CSV logs.
