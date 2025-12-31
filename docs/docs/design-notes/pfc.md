# Optional Layer-2 (L2) Support in Days (PFC/802.1Qbb-Ready)

Days currently simulates Layer 3/4 behavior by connecting `PacketSwitch` elements with per-port schedulers and passing `Packet` messages along the graph. Adding a link-layer protocol like Priority-based Flow Control (PFC, IEEE **802.1Qbb**) introduces:

- Link-local control frames flowing opposite to data (pause/resume).
- Per-priority behavior (8 priorities, 0–7).
- Receiver-side buffering and backpressure effects.

This document describes how to add L2 support **as an opt-in extension** so that when L2 is disabled, Days preserves **today’s simulation performance and event volume**.

## Goals

- **Zero additional runtime overhead when L2 is not used**
  - No extra models instantiated.
  - No extra scheduled events.
  - No per-packet branching in hot paths.
- **Opt-in L2 at topology construction time**
  - Choose L2 behavior once when building the model graph.
  - Keep existing L3/L4 elements unchanged.
- **Enable accurate PFC modeling when opted in**
  - Link-local control frames.
  - Per-priority pause gating and receiver-side triggers.
  - Standard-consistent pause timing (pause quanta).

## Non-goals (for the first iteration)

- Full Ethernet/VLAN header modeling (MAC addresses, VLAN tags, etc.).
- Switch management/control plane (LLDP, DCBX).
- Deadlock-avoidance mechanisms beyond base PFC (e.g., watchdog/recovery).

## Design Principle: Two Topology Build Paths

The key to keeping L2-off performance unchanged is to avoid “always-on” abstraction layers that add runtime checks.

### Fast path (default): L2 disabled

Build **exactly the same pipeline** as the current code:

`PacketSwitch -> Scheduler (e.g., Port/SP/…) -> PacketSwitch`

All links carry `Packet` only, and only existing schedulers generate transmission-time events.

### Extended path: L2 enabled (e.g., PFC)

Build a different per-edge pipeline that inserts L2 models **only when L2 is enabled**:

`Packet -> PfcEgressGate -> LinkFrame -> Link (serializer) -> LinkFrame -> PfcIngressPort -> Packet`

Additionally, for PFC:

- `PfcIngressPort` (receiver side) generates `PfcFrame` control frames upstream.
- `PfcEgressGate` (sender side) enforces per-priority pause gating.

When L2 is off, **none of these models exist**, so no extra messages or events are generated.

## Avoid Per-Packet Branching: Don’t Use a Global “Frame Enum” Everywhere

A common pitfall is switching the whole simulator to a single message type like:

`enum Frame { Data(Packet), Pfc(PfcFrame) }`

If this type is used everywhere, every hop pays a `match`/branch cost and likely schedules extra work even when L2 is not used.

Instead:

- L2-off builds links that carry `Packet` (as today).
- L2-on builds links that carry `LinkFrame` **only inside the L2 pipeline**.
- `PacketSwitch` and existing schedulers remain `Packet`-based.

## Runtime Opt-In: Topology-Time `LinkMode`

Add a small configuration knob (default off), evaluated only during topology construction:

```toml
[link]
mode = "None"   # or "Pfc"
```

In `src/topos/topo.rs`:

- `mode == None` → call `connect_neighbours_l3(...)` (existing behavior).
- `mode == Pfc` → call `connect_neighbours_pfc(...)` (builds L2 pipeline).

This keeps the L2-off path easy to audit: it is literally the code you already have today.

## Compile-Time Opt-In: Cargo Features to Compile L2 Out

To eliminate *any* L2 code footprint in baseline builds, gate L2 behind features:

- Feature `l2`: enables L2 modules and types.
- Feature `l2_pfc`: enables PFC-specific types and models.

Behavior:

- Default build: `cargo build` → no L2 code compiled.
- L2-capable build: `cargo build --features l2_pfc` → L2 is available, but still only instantiated when `link.mode="Pfc"`. (Note: `l2_pfc` implies `l2`, so specifying both is redundant.)

This gives two levels of opt-in:

1) **Compile-time**: remove L2 entirely for baseline performance experiments.
2) **Runtime config** (within an L2-capable binary): choose L2 per simulation config.

## Packet Priority: Keep Baseline Clean (Optional)

PFC needs packet priority (0–7). There are two approaches:

1) **Always include `priority: u8` in `Packet`**
   - Simplest.
   - Likely negligible overhead.
2) **Gate priority behind `#[cfg(feature = "l2")]`**
   - Keeps `Packet` layout unchanged in baseline builds.
   - Requires separate binaries for L2 vs non-L2 (often fine for research workflows).

For strict “no baseline impact”, prefer (2).

## PFC-Specific Notes (When `link.mode="Pfc"`)

When you opt in to L2+PFC, the L2 pipeline should model:

- **Receiver-side per-priority buffering** (bytes-based, not packet-count-based).
- **XOFF/XON threshold crossing** triggers:
  - Above XOFF → send pause for that priority.
  - Below XON → send pause time 0 (resume) for that priority.
- **Pause duration using standard units**
  - Pause time is encoded in *pause quanta*.
  - Convert to seconds using link rate: pause_quanta × 512 bit-times / rate.
- **Pause refresh**
  - If congestion persists, periodically refresh pause before it expires.

## Keeping “L2 Off == Baseline” Verifiable

Add a regression check so you don’t accidentally introduce extra models/events when L2 is disabled:

- Integration test builds a small topology with `link.mode="None"` and asserts:
  - No L2 models are instantiated.
  - (Optionally) summary stats match the baseline run under a fixed seed.

This is primarily an “audit guardrail” for performance.

## Suggested Refactor Shape (Topology Builder)

In `src/topos/topo.rs`:

- Keep the existing logic in a function like `connect_neighbours_l3(...)` (no changes).
- Add a new function `connect_neighbours_pfc(...)` behind `#[cfg(feature="l2_pfc")]`.
- Dispatch once based on `LinkMode`:
  - `None` → `connect_neighbours_l3`
  - `Pfc` → `connect_neighbours_pfc`

The “L2 disabled” build path remains as close as possible to today’s code for confidence and performance.

