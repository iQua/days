# Autoresearch: fattree_k32_tcp_f32_mt simulation runtime

## Objective
Reduce the **in-simulator elapsed wall-clock time** of:

`cargo run --release --bin days configs/benchmarks/flow/fattree_k32_tcp_f32_mt.toml`

The primary metric is **not** total command wall time. Instead, it is the value reported by the simulator's final log line:

`Elapsed wall-clock time: <x> seconds.`

That excludes routing/setup time and focuses on the simulation core.

Workload details:
- topology: FatTree with `k = 32`
- 32 TCP flows using shortest-path routing
- duration: `10.0s` simulated time
- fixed 1024-byte packets every 800us
- 100 Mbps FIFO+TailDrop switch ports
- runtime knobs from config: `threading = "multiple"`, `time_quantum_ns = 20000`, `hot_workers = 2`, `mailbox_capacity = 512`, `concurrency_level = "accelerated"`

## Metrics
- **Primary**: `sim_wall_s` (seconds, lower is better), extracted from the simulator's final `Elapsed wall-clock time:` log line
- **Secondary**: `command_real_s` from `/usr/bin/time -p`, and any qualitative observations from logs

## How to Run
`./autoresearch.sh`

It runs the benchmark command, prints output, and emits:
- `METRIC sim_wall_s=<number>`
- `METRIC command_real_s=<number>`

## Files in Scope
- `configs/benchmarks/flow/fattree_k32_tcp_f32_mt.toml` — fixed benchmark workload reference; read-only unless harness maintenance is necessary
- `src/flows/route.rs` — shortest-path / ECMP routing implementation
- `src/flows/flow.rs` — flow path construction
- `src/topos/topo.rs` — routing setup and runtime configuration wiring
- `crates/nexosim/src/executor/mt_executor.rs` — MT executor park/linger/search behavior
- `crates/nexosim/src/simulation.rs` — simulation stepping hot path and task grouping
- `src/schedulers/port.rs`, `src/flows/wire.rs`, `src/flows/tcp_source.rs` — simulation hot-path scheduling if the new benchmark points there

## Off Limits
- Lean proofs under `lean/`
- Broad benchmark changes that make the workload easier instead of making the simulator faster
- Documentation-only churn unrelated to the active benchmark
- New dependencies unless absolutely required

## Constraints
- Keep the benchmark command/workload fixed: `cargo run --release --bin days configs/benchmarks/flow/fattree_k32_tcp_f32_mt.toml`
- Primary metric comes from the simulator's own elapsed-wall-clock log line
- Do not cheat by suppressing work, changing workload semantics, or biasing the benchmark harness
- Prefer low-risk runtime changes; correctness-sensitive routing changes need extra scrutiny
- No manual commits; experiment logging handles commits

## What's Been Tried
- Previous autoresearch target (`configs/exp_tcp_fattree.toml`) produced useful transferable ideas:
  - a fat-tree-specific shortest-path fast path can matter a lot
  - executor park/search/linger policy also matters materially
  - many small constant retunes are noise; structural changes win more often
- The current branch already contains the latest keeps from the previous target:
  - `38e94c2` fat-tree implicit A*-style routing traversal
  - `04802e5` 5us worker search-before-park
  - `63d432c` hot-worker-only 5us search policy
  - `2c04bbf` 200us hot-worker linger
  - `b109311` 1us cold-worker search window
- New target differs materially because the metric excludes setup/routing time and measures only the simulator-reported elapsed wall-clock time for the run itself.
- First task for this target: establish a clean baseline on the new benchmark using the log-line metric, then retune only if the new benchmark still benefits.
