# Autoresearch: exp_tcp_fattree runtime

## Objective
Reduce wall-clock runtime of the `configs/exp_tcp_fattree.toml` workload without changing the benchmark workload itself.

The workload is a 2.0s multi-threaded TCP fat-tree simulation:
- topology: FatTree with `k = 96`
- 500 TCP flows using shortest-path routing
- fixed 1024-byte packets every 800us
- 10 Mbps FIFO+RED switch ports
- current runtime knobs in the config: `threading = "multiple"`, `time_quantum_ns = 102400`, `hot_workers = 2`, `concurrency_level = "accelerated"`

This is a throughput-oriented performance target. The command should continue to complete successfully and produce the same kind of simulation outputs.

## Metrics
- **Primary**: wall-clock runtime (`wall_s`, seconds, lower is better)
- **Secondary**: simulation step count, avg/max groups per step, scheduled fast vs erased events, injected events, executor park/linger counters

## How to Run
`./autoresearch.sh` — builds the release binary if needed, runs the benchmark workload, prints simulator perf stats, and emits `METRIC` lines.

## Files in Scope
- `configs/exp_tcp_fattree.toml` — fixed benchmark workload reference; read-only unless benchmark harness maintenance is unavoidable
- `src/flows/tcp_source.rs` — TCP pacing / send scheduling
- `src/flows/wire.rs` — propagation-delay scheduling
- `src/schedulers/port.rs` — FIFO port serialization / batched scheduled sends
- `src/utils/time.rs` — Days-side time quantization helpers
- `src/topos/topo.rs` — runtime configuration wiring
- `crates/nexosim/src/simulation.rs` — simulation stepping hot path and perf counters
- `crates/nexosim/src/simulation/scheduler.rs` — scheduler queue / time quantization behavior
- `crates/nexosim/src/executor/mt_executor.rs` — MT executor park/linger/inject behavior
- `crates/nexosim/src/model/context.rs` — fast scheduling APIs if batching strategy changes

## Off Limits
- Lean proofs under `lean/`
- Documentation-only changes unrelated to the benchmark harness
- Broad workload changes that make the benchmark easier instead of making the simulator faster
- New dependencies unless absolutely required

## Constraints
- Keep the benchmark command/workload fixed: `target/release/days configs/exp_tcp_fattree.toml`
- Prioritize primary metric improvement over secondary metrics
- Keep edits focused on runtime behavior and low-overhead instrumentation
- Avoid correctness-risky semantic changes unless the performance win is clear and the change is tightly scoped
- No manual commits; experiment logging handles commits

## What's Been Tried
- Existing branch history already landed several performance-oriented changes before this autoresearch session:
  - fast-path clone avoidance in port/wire/scheduler scheduling
  - simulation-step hot-path tuning and injector draining changes in Nexosim
  - local flush queue overhead reduction
  - fast-only simulation step specialization
  - accelerated per-step group bundling
  - time quantization plumbing (`time_quantum_ns`) and hot workers
- Prior analysis notes in `ideas/concurrency-perf.md` and `ideas/step-quantization.md` point to two recurring themes:
  - MT speedups depend strongly on how many actions coalesce at the same timestamp
  - per-step executor/barrier overhead can dominate when step count remains very high
- Warm baseline after setup: `wall_s=10.89`, with logs showing roughly ~2s before flow attachment, ~4s in routing on 500 flows, and ~4.08s in the simulation core.
- Kept: avoid per-flow network-graph clones in `Flow::compute_path` / `Topology::route_flows`; this cut warm `wall_s` to `10.04` and reduced the routing phase from about 4s to about 2s.
- Discarded: replacing shortest-path lookup with a custom BFS made the benchmark much faster (`wall_s=7.59`) but materially changed packet totals and delay, so it is not a safe performance-only optimization.
- Next focus: keep routing semantics stable while reducing remaining setup/runtime overhead, especially flow attachment / switch activation and MT simulation barrier costs.
