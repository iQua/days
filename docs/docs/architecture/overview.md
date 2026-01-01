# Architecture Overview

Days is built around a small set of concepts:

- **Discrete-event simulation**: all progression happens by processing scheduled events in timestamp order.
- **Actor model**: each simulated component is an actor that owns its state and communicates via message passing.
- **Packets carry time**: most components avoid querying the global simulation clock by reading the timestamp stored in the message itself.

Days uses the `nexosim` crate as the simulation engine. Each actor implements `nexosim::model::Model` and receives messages through connected `Output<T>` ports and `Mailbox<T>` mailboxes.

## Main components

- **Topology builder** (`src/topos/`): parses the TOML config, builds the network graph, instantiates models, wires ports, and runs the simulation.
- **Switch** (`src/switches/`): `PacketSwitch` demultiplexes packets using a per-flow forwarding table (FIB).
- **Schedulers** (`src/schedulers/`): model per-port queueing, drop/ECN policies, and transmission delay.
- **Flows** (`src/flows/`): model sources/sinks (packet distributions, TCP, DCQCN) and collectives.
- **Optional L2** (`src/l2/`, feature-gated): link-layer frames and PFC (802.1Qbb-style pause).
- **Logging/UI** (`src/utils/`): progress UI, concurrency tracing, and CSV report logging.

## Message flow (L3/L4-only)

When L2 is disabled (default), each network hop looks like:

```
PacketSwitch (upstream) -> Scheduler (per edge) -> PacketSwitch (downstream)
```

For an end-to-end flow, the path is:

```
PacketSource -> (host) PacketSwitch -> [Scheduler+Switch]* -> PacketSink
```

Where:

- the switch chooses the next hop using `fib[flow_id]` for data packets and `r_fib[flow_id]` for reverse traffic (ACKs and control packets),
- the scheduler models queueing + serialization delay and forwards the packet with an updated timestamp.

## Optional L2/PFC pipeline

When `link.mode="Pfc"` and the binary is built with `--features l2_pfc`, each edge becomes a small pipeline:

```
Scheduler -> PfcEgressGate -> Link (serializer) -> PfcIngressPort -> PacketSwitch
```

This keeps the default path lean: when L2 is disabled, none of the extra models exist.

## Reports and observability

Days periodically emits CSV reports (configurable by `report_interval`) for:

- sources (`sources.csv`)
- sinks (`sinks.csv`)
- schedulers (`switches.csv`)
- optional PFC ports (`pfc.csv`)
- optional per-event protocol traces for the Lean checker:
  - `dcqcn_events.csv` (with `--features dcqcn,lean`)
  - `pfc_events.csv` (with `--features l2_pfc,lean`)
  - `cubic_events.csv` (with `--features lean`)
  - `drr_events.csv` (with `--features lean`)
  - `wfq_events.csv` (with `--features lean`)
