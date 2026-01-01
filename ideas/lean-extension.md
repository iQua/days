• Concrete extension plan: add a new protocol class for switch queue management / ECN marking (AQM), then optionally
  compose it with DCQCN for end‑to‑end causality. This aligns with the LeanGuard design notes that treat “queueing/
  marking/scheduling” as distinct network‑element transitions and explicitly call out switch‑side ECN marking events for
  causal justification. See ~/Playground/lean-paper/design.tex.

  Phase 1 — Define a replayable AQM/ECN event vocabulary (new class)

  - Schema + log sink
      - Add AqmEventKind + AqmEventRow + NEXT_AQM_EVENT_ID and CSV output aqm_events.csv in src/utils/logger.rs.
      - Update Report, SharedState, and ElementType to include AQM events in src/utils/logger.rs.
  - Decision witness plumbing
      - Extend the drop strategy interface to return a witness, not just DropAction. For example: DropDecision { action,
        queue_len, byte_len, capacity, capacity_unit, min_thr, max_thr, avg_len, max_p, rand_u, threshold, ecn_before,
        ecn_after }.
      - Implement this in src/schedulers/drop.rs for TailDrop, EcnThreshold, and RED/RED_ECN. For RED, log the sampled
        rand_u so Lean can deterministically replay the probabilistic decision.
  - Instrumentation points
      - Emit AqmEventRow at the decision site in all queueing schedulers and ports:
          - src/schedulers/drr.rs
          - src/schedulers/wfq.rs
          - src/schedulers/wrr.rs
          - src/schedulers/sp.rs
          - src/schedulers/vc.rs
          - src/schedulers/port.rs
      - Fields should include (time_ns, event_id, scheduler_id/port_id, queue_id/class_id, packet_id, flow_id,
        size_bytes, decision witness…).

  Phase 2 — Lean checker for AQM/ECN

  - Add a new Lean namespace and executable:
      - Semantics: lean/LeanGuard/Aqm/Semantics.lean
      - Log parsing + replay: lean/LeanGuard/AqmEventLog.lean
      - CLI wrapper: lean/LeanGuard/AqmMain.lean
      - Register in lean/lakefile.lean as aqm_check
  - Semantics to enforce:
      - Deterministic decision rules for TailDrop and EcnThreshold.
      - RED/RED_ECN recompute probability from logged parameters and require rand_u <= p to justify mark/drop.
      - Reject “mark CE on non‑ECT” (match Packet::mark_ce behavior).
  - Follow the LeanGuard extension recipe in docs/docs/verification/design.md.

  Phase 3 — Cross‑layer causality (AQM → DCQCN)

  - Add a composite check that links aqm_events.csv and dcqcn_events.csv by (pkt_id, flow_id) and time ordering:
      - Each CnpSent with trigger_ecn=Ce must be justified by a prior AqmEventRow with action=MarkEcn for the same
        packet.
  - Implement as either:
      - A new combined checker under lean/LeanGuard/Composite/…, or
      - A “joined trace” mode that canonicalizes both streams into a single namespace.
  - This directly targets the paper’s “causal justification” gap. See ~/Playground/lean-paper/design.tex.

  Phase 4 — Tests, configs, docs

  - Configs: add a minimal reproducible config, e.g. configs/aqm_ecn.toml (use DropStrategy::EcnThreshold or RedEcn).
  - Tests:
      - Add a Rust integration test that runs a short sim and asserts aqm_events.csv exists and has expected columns,
        e.g. tests/aqm_trace.rs.
      - Optionally add a Lean checker run in CI later (not required now).
  - Docs:
      - Update logging list and LeanGuard use guide:
          - docs/docs/configuration/logging.md
          - docs/docs/verification/leanguard.md
          - docs/docs/verification/design.md

  Optional follow‑on tracks (same framework, not new classes)

  - Schedulers beyond WFQ/DRR (same class, broader coverage):
      - Implement Lean checkers for SP/WRR/Virtual‑Clock using existing enqueue/schedule/depart patterns.
      - Files: src/schedulers/sp.rs, src/schedulers/wrr.rs, src/schedulers/vc.rs, plus new lean/LeanGuard/Sp*, Wrr*, Vc*.
  - Additional CC algorithms (same class, broader coverage):
      - Add Lean checkers for Reno/BBR (already in Rust), modeled after CUBIC:
          - Rust logs from src/flows/reno.rs, src/flows/bbr.rs via src/utils/logger.rs.
          - Lean semantics under lean/LeanGuard/Reno/* and lean/LeanGuard/Bbr/*.
