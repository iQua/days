# LeanGuard: Design Sketch

LeanGuard follows a “trace certificate” methodology:

- the simulator emits a **per-transition** event trace (CSV), one row per state-machine transition
- the checker **canonicalizes** the trace and **replays** an executable Lean reference model
- the checker returns `ACCEPT` if every row is a legal step, otherwise `REJECT` with the first violating row

This narrows the trusted computing base: you do not need to verify the simulator runtime, scheduler, or concurrency model—only that the logged transitions are consistent with the spec-first reference semantics.

## Core pipeline

At a high level, each checker is:

1. **Parse** CSV → a typed `Row` list (reject malformed rows).
2. **Canonicalize** rows to a deterministic total order.
3. **Replay**: fold a partial `step` function over the canonicalized list.
4. **Report**: `ACCEPT` or `REJECT: ...` with the first failing row and a diagnostic.

Canonicalization is part of the specification: logs can be emitted by concurrently simulated components, and physical file order is not treated as semantically meaningful.

In Days’ current checkers, the canonical key is `(time_ns, event_id)` and duplicates are rejected.

## Event vocabulary and replay-oriented schemas

To make replay deterministic, a trace row should contain:

- **when and where**: `time_ns` plus stable identities (endpoint, flow, switch/port if applicable)
- **what happened**: an event kind aligned with the protocol’s transition structure (send/receive/timer/decision)
- **guards and parameters**: enough information to evaluate gates/thresholds and enforce parameter stability
- **post-state snapshot**: the state variables updated by that transition, logged in deterministic encodings

Days logs key quantities as integers with explicit units (e.g., `time_ns`, rates in `*_bps`, and fixed-point factors like `*_ppb`) so the checker can compare by exact equality.

## How this maps to the Days repository

- Lean code lives in `lean/LeanGuard/`.
- Each protocol has:
  - a `*EventLog.lean` that defines `Row`, parsing, canonical key, and `step`
  - a `*/Semantics.lean` file that defines executable update equations and helper invariants
  - a small `*Main.lean` CLI entrypoint that prints `ACCEPT` / `REJECT`

The current executables are defined in `lean/lakefile.lean`:

- `dcqcn_check` (DCQCN trace replay)
- `pfc_check` (PFC trace replay)
- `cubic_check` (TCP CUBIC trace replay)
- `drr_check` (DRR trace replay)
- `wfq_check` (WFQ trace replay)

On the simulator side, Days emits:

- `dcqcn_events.csv` when built with `--features dcqcn,lean`
- `pfc_events.csv` when built with `--features l2_pfc,lean`
- `cubic_events.csv` when built with `--features lean`
- `drr_events.csv` when built with `--features lean`
- `wfq_events.csv` when built with `--features lean`

## Extending LeanGuard to a new protocol

The recommended instantiation recipe is:

1. Choose an **event vocabulary** that matches the protocol’s transition points.
2. Define a replay-oriented CSV schema (one row per transition).
3. Add Days instrumentation at the transition points, logging post-state snapshots and stable IDs.
4. Implement a Lean `Row` parser + canonical key + `step` function that:
   - enforces required fields per kind
   - enforces parameter stability (where intended)
   - checks pairing rules and gates (cooldowns / eligibility windows)
   - recomputes the post-state and compares it to the logged snapshot
5. Add a `lean_exe` entry in `lean/lakefile.lean` and a CLI wrapper that prints `ACCEPT`/`REJECT`.
