DCQCN Campaign (Phase 4.1–4.3) Plan

  Goal & Success Criteria

  - Implement a DCQCN‑focused campaign that mutates DCQCN‑specific TOML knobs, analyzes DCQCN traces, and
    performs calibration loops to reach DCQCN coverpoints.
  - Done when:
      - leanguard-testgen campaign --protocol dcqcn --budget N generates targeted DCQCN mutations and records
        them in case metadata.
      - Calibration adjusts DCQCN knobs when goal coverpoints are missing and --max-calibration-iters > 0.
      - DCQCN trace analyzer extracts CNP/timer counts and alpha/rate extrema to guide calibration.

  Non-goals / Out of Scope

  - Implement other protocol campaigns (AQM/PFC/WFQ/DRR) or micro‑seed packs.
  - Extend checker coverage output (assumed already handled in Phase 4 prereqs).
  - Add new DCQCN Lean coverpoints beyond those listed in the plan.

  Assumptions

  - DCQCN configs use flow.traffic.dcqcn keys as in configs/dcqcn_simple.toml.
  - dcqcn_events.csv includes kind, endpoint_id, flow_id, alpha_ppb, rate_bps, cnp_interval_ns, and
    last_cnp_ns.
  - Calibration is only attempted for DCQCN campaigns and only when --max-calibration-iters is set.

  Proposed Solution

  - Add DCQCN targeted mutators and record protocol‑specific Mutation variants.
  - Add a lightweight DCQCN trace analyzer module and a shared CSV helper for future protocols.
  - Implement DCQCN calibration loop in campaign() that re-runs with adjusted knobs until goals met or max
    iterations reached.

  System Design

  - Campaign loop:
      - Base overrides → generic mutations → DCQCN targeted mutations.
      - Run Days + checkers.
      - If accepted but missing goal coverpoints, analyze dcqcn_events.csv and adjust one knob per iteration.
  - DCQCN analyzer:
      - Count CNP send/recv/timer ticks (overall + by endpoint/flow).
      - Track alpha/rate min/max.
      - Infer cnp_apply vs cnp_ignored_due_to_interval using last_cnp_ns and cnp_interval_ns.
  - Calibration knobs (priority):
      - CNP frequency (rate, ECN threshold).
      - Interval gating (cnp_interval_ns).
      - Alpha steering (g).
      - Rate clamp forcing (min/max rate).

  Interfaces & Data Contracts
      - src/utils/testgen/trace.rs — CSV utilities for analyzers.
        Mutation::TweakDcqcnRateGbps, Mutation::TweakDcqcnMinRateGbps,
        Mutation::TweakDcqcnMaxRateGbps, Mutation::TweakDcqcnAiRateGbps,
        Mutation::TweakDcqcnHaiRateGbps, Mutation::TweakSwitchEcnThreshold,
        Mutation::CalibrationStep.

  Execution Details

  - Implement DCQCN analyzer and CSV helper:
      - src/utils/testgen/dcqcn.rs
      - src/utils/testgen/trace.rs
  - Add DCQCN targeted mutators and calibration loop:
      - src/utils/testgen.rs
      - DCQCN config surface documented in‑code near mutate_dcqcn_targeted.
  - Add dry‑run test to ensure targeted mutations appear:
      - tests/testgen_campaign_dcqcn.rs

  Testing & Quality

  - Run:
      - cargo test --features test -- test_campaign_dcqcn_targeted_mutations_in_dry_run -- --show-output
  - Acceptance checks:
      - Campaign dry‑run includes at least one DCQCN‑specific mutation.
      - Calibration records Mutation::CalibrationStep when active.

  Rollout, Observability, and Ops

  - No rollout changes; behavior is behind the campaign path for DCQCN only.
  - Calibration steps and targeted mutations are recorded in per‑case metadata.

  Risks & Mitigations

  - Risk: Trace schema changes break analyzer parsing.
      - Mitigation: analyzer uses header‑indexed access; missing fields degrade gracefully.
  - Risk: Calibration overshoots and produces invalid configs.
      - Mitigation: clamp rates/thresholds and only adjust one knob per iteration.

  Open Questions

  - None.


› Implement the plan.


• Implemented the DCQCN campaign pieces (targeted mutators, trace analyzer, calibration loop scaffolding, and
  CSV helpers) plus a dry‑run test for DCQCN mutations.

  Changes:

  - Added DCQCN‑specific mutation variants and targeted mutators in src/utils/testgen.rs.
  - Added DCQCN trace analyzer and shared CSV utilities in src/utils/testgen/dcqcn.rs and src/utils/testgen/
    trace.rs.
  - Wired DCQCN calibration loop into campaign() for DCQCN campaigns with goal coverpoints and --max-
    calibration-iters.
  - Added test tests/testgen_campaign_dcqcn.rs to assert targeted DCQCN mutations appear in dry‑run.

  Tests run:

  - cargo test --features test -- test_campaign_dcqcn_targeted_mutations_in_dry_run -- --show-output

  If you want, I can now:

  1. Run a real DCQCN campaign with calibration enabled to sanity‑check traces.
  2. Add a micro DCQCN seed under configs/testgen/ to exercise boundaries faster.
