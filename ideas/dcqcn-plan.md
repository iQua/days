# DCQCN Simulation Plan

This document lays out a concrete, repo-specific plan to add DCQCN (SIGCOMM 2015, Zhu et al.) to Days.

## Goals

- Implement DCQCN as a *rate-based* congestion control model (not TCP).
- Reuse existing ECN marking in switches and layer-2 PFC (lossless fabric) when enabled.
- Provide configurable parameters (with paper defaults) via TOML.
- Keep the default build lean via a `dcqcn` feature gate.
- Add tests and sample configs to validate correctness and reproducibility.

## High-Level Architecture Mapping

DCQCN consists of three logical roles that map cleanly to Days components:

- **Congestion Point (CP)**: switch egress queue that ECN-marks packets when queue exceeds threshold `K`.
- **Notification Point (NP)**: receiver that generates CNP packets upon receiving CE-marked data.
- **Reaction Point (RP)**: sender that adjusts sending rate based on CNPs and internal timers.

In Days terms:

- CP lives in `src/schedulers/` via a new ECN marking strategy.
- NP and RP live in new flow types within `src/flows/`.

## Feature Gating

Add a `dcqcn` feature (off by default). All DCQCN-specific code (flow type, source/sink, config structs, tests, examples) should be gated with `#[cfg(feature = "dcqcn")]`.

Rationale:

- Reduces default build size and complexity.
- Allows incremental integration without impacting existing TCP/ECN/PFC behavior.

## Data Model Extensions

### Packet

Add a generic control marker to `Packet` so DCQCN can carry CNP packets:

- `PacketKind::Data | PacketKind::Control`
- `Control::TcpAck | Control::DcqcnCnp`

Prefer keeping this *always compiled* (no cfg) to avoid cfg sprawl in core packet handling. DCQCN-specific usage is still behind the feature gate.

### Flow Type

Add `FlowType::DCQCN` under `#[cfg(feature = "dcqcn")]`.

### Traffic Config

Add DCQCN config struct:

- `DcqcnCharacteristics` in `src/flows/mod.rs` (gated)
- Include under `TomlTrafficCharacteristics` and `TrafficCharacteristics` (gated)

Suggested fields (paper defaults):

- `rate_gbps` (initial rate)
- `min_rate_gbps`
- `max_rate_gbps`
- `g` (alpha weight)
- `ai_rate_gbps`
- `hai_rate_gbps`
- `mi_factor` (multiplicative decrease factor)
- `rtt_ns` (if using fixed RTT)
- `cnp_interval_ns` (to rate-limit CNP generation)
- `pacing_interval_ns` (minimum send spacing)

## CP (Switch ECN Marking)

Add a new ECN marking policy in `src/schedulers/drop.rs`:

- `DropStrategy::EcnThreshold` (or similar)
- If queue exceeds threshold `K`, return `DropAction::MarkEcn` instead of drop.
- Support unit config in bytes or packets.

Plumbing:

- Update each scheduler constructor in `src/schedulers/*.rs` to accept the new strategy.
- Extend `SwitchConfig` in `src/topos/topo.rs` to include ECN threshold config.
- Reuse existing queue length tracking (`queue_length` and `QueueState`) to compute thresholds.

## NP (Receiver CNP Generation)

Create `src/flows/dcqcn_sink.rs` (gated):

- On receiving CE-marked data, generate a CNP `Packet` back to the sender.
- CNP size can be minimal (e.g., 64B), optionally set higher priority.
- Rate-limit CNP generation using `cnp_interval`.
- CNP packets should not be ECN-capable (NotEct).

## RP (Sender Rate Control)

Create `src/flows/dcqcn_source.rs` (gated):

State and behavior:

- Maintain `rate`, `target_rate`, `alpha`, `last_cnp_time`, `no_cnp_timer`.
- On CNP: update alpha, apply multiplicative decrease, set target rate.
- On periodic timer without CNPs: decay alpha, increase rate (AI/HAI).
- Schedule send events based on `packet_size / rate`.

Implementation details:

- Integrate with existing packet send loop in `src/flows/source.rs`.
- Use `Output<Packet>` like other sources.
- Track bytes sent and stop on traffic completion like TCP/Dist sources.

## Wiring in Topology

In `src/topos/topo.rs`:

- When `flow_type == DCQCN`, instantiate `DcqcnPacketSource` and `DcqcnPacketSink`.
- Wire source output to host switch and sink output back to the network (similar to TCP).
- For flow start dependencies, treat DCQCN like TCP: sender notifies flow completion.

## Logging

Option A (minimal): extend `PacketSourceReport` and `PacketSinkReport` with DCQCN fields (gated).

Option B (cleaner): add a `Report::DcqcnReport` variant and a separate CSV file `dcqcn.csv`.

Suggested fields:

- `rate`, `alpha`, `cnp_count`, `ecn_marked_count`, `throughput`.

## Tests

Add under `tests/` (gated with `#![cfg(feature = "dcqcn")]`):

1. **Unit tests** for DCQCN sender state:
   - Alpha update on CNP
   - Rate decrease (MI)
   - Rate increase (AI/HAI)

2. **Integration test** (new `tests/dcqcn.rs` + `tests/dcqcn.toml`):
   - Simple topology with ECN marking threshold.
   - Ensure CNP generation triggers sender rate reduction.

3. **PFC compatibility** (if `l2_pfc` enabled):
   - Check that ECN marking occurs without packet drops.

## Example Configs

Add one or two example configs in `configs/`:

- `configs/dcqcn_simple.toml`
- `configs/dcqcn_convergence.toml`

Update `docs/examples/index.md` with commands:

- `cargo run --features dcqcn,l2_pfc -- configs/dcqcn_simple.toml`

## Milestones

1. **Core data model**: Packet control type, FlowType gating, config structs.
2. **CP marking**: ECN threshold policy and switch config wiring.
3. **NP/RP implementation**: source/sink modules and topology wiring.
4. **Logging and examples**: CSV output + sample configs.
5. **Tests**: unit + integration + PFC combination.

## Open Questions / Decisions

- Should DCQCN run over a fixed RTT model or use sampled RTT from packet timing?
- Do we need to model per-queue thresholds for multi-queue schedulers (SP/WFQ/DRR), or keep one threshold per port initially?
- Should CNP packets be prioritized (e.g., PCP 7), and should that be configurable?

## Suggested File Touches

- `Cargo.toml` (feature gate)
- `src/flows/mod.rs` (gated DCQCN config)
- `src/flows/flow.rs` (FlowType::DCQCN)
- `src/flows/packet.rs` (control packet markers)
- `src/flows/dcqcn_source.rs` (new)
- `src/flows/dcqcn_sink.rs` (new)
- `src/flows/source.rs` / `src/flows/sink.rs` / `src/topos/topo.rs` (wiring)
- `src/schedulers/drop.rs` (ECN threshold marking)
- `src/utils/logger.rs` (DCQCN logging)
- `tests/dcqcn.rs` + `tests/dcqcn.toml`
- `configs/dcqcn_simple.toml`
- `docs/examples/index.md`
