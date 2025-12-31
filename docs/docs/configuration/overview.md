# Configuration Overview

Days simulations are configured using a single TOML file. The CLI entry point (`src/main.rs`) expects the path to that file as its only argument:

```bash
RUST_LOG=info cargo run -- configs/simple.toml
```

## Units and types

The most common conventions in the TOML schema:

- **Time**: seconds as floating point (`f64`) unless otherwise noted.
- **Rates**: bits per second (`port_rate`, `rate_gbps`, …).
- **Sizes**: bytes for traffic sizes, packet sizes, and most buffers.
- **Topology node IDs**: integers (`usize`), used consistently across graph edges, hosts, paths, and endpoints.

## Minimal example

This is a minimal, custom-topology run: one link, one packet-distribution flow.

```toml
seed = 1
duration = 2.0
num_threads = 1
log_path = "./output/minimal"

edges = [[0, 1]]
hosts = [0, 1]

[switch]
port_rate = 1e9
capacity = 100
discipline = "FIFO"
drop = "TailDrop"

[[flow]]
flow_type = "PacketDistribution"
graph = [[0, 1]]
[flow.traffic]
  initial_delay = 0.0
  size = 10000
  arr_dist = { type = "Uniform", low = 0.001, high = 0.001 }
  pkt_size_dist = { type = "DiscreteUniform", low = 512, high = 512 }
```

## Sections at a glance

- General / runtime: `configuration/logging.md`
- Topology: `configuration/topology.md`
- Switches, schedulers, and drop/ECN: `configuration/switches.md`
- Flows and traffic: `configuration/flows.md`
- Collectives: `configuration/collectives.md`
- Optional L2/PFC: `configuration/l2.md`

## Feature-gated configuration

Some config values are only valid when the binary is built with the right Cargo features:

- `link.mode = "Pfc"` requires `--features l2_pfc`
- `flow_type = "DCQCN"` and `[traffic.dcqcn]` require `--features dcqcn`
- Protocol event traces for the Lean checker:
  - `dcqcn_events.csv` requires `--features dcqcn,lean`
  - `pfc_events.csv` requires `--features l2_pfc,lean`
  - `cubic_events.csv` requires `--features lean`
- `--features perf_stats` enables internal performance statistics from the `nexosim` simulation engine
