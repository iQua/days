## Phase 4 goal and “definition of done”

Phase 4 (“protocol‑targeted generation”) takes the Phase 3 MVP loop (mutate TOML → run Days → run Lean checkers → keep/minimize) and adds **protocol-aware generators** that deliberately hit semantic edge cases (gates, thresholds, rounding, ties, pairing obligations) described in `lean-paper/generation.tex`.

**Primary deliverables**

1. **Targeted generation campaigns** runnable via `leanguard-testgen` (new subcommand or new mode/flags) that focus on one protocol family at a time: **DCQCN, AQM, PFC, WFQ, DRR** (and optionally CUBIC).
2. **Calibration loops** (run → inspect trace/coverage → tweak 1–2 knobs → rerun) for each protocol to reliably reach rare coverpoints.
3. **Protocol-specific mutation operators + boundary synthesis operators** integrated into `days/src/utils/testgen.rs` (or a new submodule) and recorded in `Mutation` history.
4. **Coverage-aware “keep/discard” and dedup** for targeted campaigns (using checker-provided coverpoints once Phase 2 is wired through; otherwise a trace-signature fallback).
5. **A small library of micro/topology seed configs** (tiny topologies, 1–3 flows, explicit edges) that are purpose-built to hit corner cases cheaply.

---

## 0) Phase 4 prerequisites in the current codebase (must be addressed as part of Phase 4 work)

These are small but important fixes/bridges so Phase 4 can build on Phase 1–3 reliably.

### 0.1 Make `testgen` treat REJECT as a normal outcome (not an execution error)

Status: complete (2026-01-22).

**Where:** `days/src/utils/testgen.rs` → `run_leanguard(...)`

**Current issue to address**

* `run_leanguard` returns `Err(...)` when `leanguard-run` exits non-zero.
* But `leanguard-run` exits `1` on “not accepted” (normal REJECT), and Phase 4 *needs the JSON summary* for calibration and minimization.

**Tasks**

* Adjust `run_leanguard` to return stdout/stderr + exit_code even when exit code is `1`.
* Only treat “could not execute” / “no output” / “invalid JSON” / exit code `2` as *errors*.
* Update the fuzz/replay/minimize callers to:

  * parse JSON from stdout even when exit_code=1
  * classify reject as `accept=false` and continue.

**Subtasks**

* Update `RunOutput` to include `exit_code`, `stdout`, `stderr`.
* Update error counting in `fuzz`: count “execution failed” separately from “reject”.

### 0.2 Extend seed indexing tags beyond Cargo features

Status: complete (2026-01-22).

**Where:** `days/src/utils/testgen.rs` → `detect_required_features` and `SeedIndexEntry`

Phase 4 needs better seed selection than “dcqcn” / “l2_pfc”.

**Tasks**

* Add protocol tags (not build features) to seed index entries:

  * `has_dcqcn_flows`, `has_tcp_flows`, `link_mode_pfc`
  * `switch_discipline_wfq`, `switch_discipline_drr`, `switch_drop_red`, `switch_drop_ecn_threshold`, …
* Store these as `protocol_tags: Vec<String>` or `capabilities: { ... }` in `seeds_index.json`.

**Subtasks**

* Implement TOML scanning for:

  * `switch.discipline` (WFQ/DRR/etc.)
  * `switch.drop` (RED/ECN_THRESHOLD/TailDrop)
  * presence of flow types in `flow` and `flow_set`
* Keep existing `required_features` as-is (it’s still useful as a hint).

---

## 1) Phase 4 foundation: coverage + target selection plumbing (wired into existing tools)

The paper’s Phase 4 logic assumes you can *observe semantic coverpoints* and guide generation. Your Rust `CaseMetadataV1.coverage` is currently a stub, so Phase 4 needs a concrete plan to fill it.

### 1.1 Decide the “coverage contract” between `leanguard-run` and `leanguard-testgen`

Status: complete (2026-01-22).

**Where it plugs in today**

* `leanguard-run` produces `RunSummaryV1` JSON including per-checker stdout/stderr.
* `testgen.rs` parses accept + trace list from `run_summary`.

**Phase 4 plan**

* Add **optional coverage output** to checkers and plumb it into `RunSummaryV1`.
* Keep backwards compatibility: if a checker doesn’t support coverage yet, `RunSummaryV1` should still parse.

**Tasks**

1. Update `leanguard-run` (`days/src/bin/leanguard-run.rs`) to accept a new flag like:

   * `--coverage` (bool) and/or
   * `--coverage-dir <path>`
2. When running each checker (`run_checker`):

   * if coverage enabled, pass `--coverage-out <file>` (per-checker file under `<log_path>/coverage/`)
3. Extend `CheckerResult` to include:

   * `coverage_path: Option<String>`
   * `coverage: Option<Vec<String>>` (or structured object)
4. Add `coverage` summary in top-level `RunSummaryV1`:

   * union of all coverpoints across checkers
   * per-checker coverpoints

**Subtasks**

* Ensure coverage artifacts live under `log_path` so `check-only` can also read them.
* Ensure output is deterministic and can be used for dedup.

### 1.2 Consume coverage in `testgen` metadata and corpus scoring

Status: complete (2026-01-22).

**Where:** `days/src/utils/testgen.rs` → `ParsedRunSummary`, `build_case_metadata`, fuzz loop.

**Tasks**

* Extend `ParsedRunSummary` to parse:

  * coverpoints (per checker + union)
  * optional stats (rows processed, etc.) if checkers provide them
* Update `CoverageInfo` in metadata:

  * `observed: Vec<String>` becomes populated
  * `mode` becomes `"checker_coverpoints"` when present
  * `novelty` becomes `"new"` / `"redundant"` based on a persisted global coverage set

**Subtasks**

* Add a persistent `global_coverage.json` under `corpus_root/metadata/`:

  * maintained by fuzz/campaign runs
  * used to decide whether to keep accepted cases
* Add a “trace signature fallback” mode when coverage isn’t available:

  * signature options (cheap):

    * set of trace filenames from manifest (`traces.json`)
    * hash of `(trace kind sequence)` extracted from CSV header + `kind` column (AQM/DCQCN/WFQ/DRR already have `kind`)
  * This keeps Phase 4 usable even before coverage is fully implemented in Lean.

---

## 2) Add a Phase 4 “campaign” interface in `leanguard-testgen`

Random fuzzing (Phase 3) isn’t enough for Phase 4; you need protocol-specific goals and calibration.

### 2.1 Add a new CLI subcommand (recommended): `Campaign`

Status: complete (2026-01-22).

**Where:** `days/src/bin/leanguard-testgen.rs`

**Proposed shape**

* `leanguard-testgen campaign --protocol <dcqcn|aqm|pfc|wfq|drr> --budget N [--goal <coverpoint>] [--max-calibration-iters K] [--seed-filter ...]`

**Tasks**

* Add `Command::Campaign { protocol, budget, rng_seed, goal, max_calibration_iters, ... }`
* Thread through to `days::utils::testgen::campaign(...)` (new function)

**Subtasks**

* Add a “dry-run” mode that prints planned mutations without executing (useful for debugging generators).
* Add `--use-trace-signature` fallback flag for early bring-up.

### 2.2 Implement a campaign dispatcher in `utils/testgen.rs`

Status: complete (2026-01-22).

**Where:** `days/src/utils/testgen.rs`

**Tasks**

* Introduce:

  * `enum TargetProtocol { Dcqcn, Aqm, Pfc, Wfq, Drr, Cubic }`
  * `struct CampaignOptions { protocol, budget, rng_seed, goal_coverpoints, max_calibration_iters, ... }`
* Implement `campaign(opts, campaign_opts) -> CampaignSummary`

**Subtasks**

* Reuse corpus layout and finalization logic from `fuzz`:

  * use `_work/<case_id>` then move into `accepted/` or `rejected/`
* Store campaign parameters into per-case metadata (so you can reproduce the intent).

---

## 3) Protocol-targeted generator framework (common scaffolding)

### 3.1 Split mutation logic into two layers: generic + targeted

**Where:** `days/src/utils/testgen.rs` (or create `days/src/utils/testgen/targeted.rs`)

**Tasks**

* Keep existing generic mutators (duration, initial_delay, distributions, capacity, shuffle edges)
* Add targeted mutator entrypoints:

  * `mutate_dcqcn(config, rng, goal) -> Vec<Mutation>`
  * `mutate_aqm(...)`, `mutate_pfc(...)`, `mutate_wfq(...)`, `mutate_drr(...)`

**Subtasks**

* Extend `Mutation` enum with protocol-specific variants, e.g.:

  * `TweakDcqcnCnpInterval { from, to }`
  * `TweakDcqcnG { from_ppb, to_ppb }`
  * `TweakEcnThreshold { from, to }`
  * `TweakPfcXoff { prio, from, to }`
  * `TweakSwitchWeights { from, to }`
  * `SetFlowPriority { flow_id or selector, from, to }`
* Make sure each mutation records:

  * TOML path (for debugging)
  * old/new values (for minimization + reproducibility)

### 3.2 Add boundary-case synthesis operators (paper’s generator family #2)

These are not “random tweaks”; they explicitly solve small equalities/inequalities to hit a boundary.

**Where:** `days/src/utils/testgen.rs` new helpers

**Tasks**

* Implement a small “boundary synthesizer” per protocol that proposes parameter tuples:

  * For time gates: solve `last + interval = t` and `t±1`
  * For thresholds: solve occupancy `== xoff` and `== xon`
  * For DRR: solve `deficit == packet_size`
  * For WFQ: solve finish-tag ties (`finish_time_a == finish_time_b`)
* Represent these as `Mutation::SynthesizeBoundary { name, params... }`

**Subtasks**

* Start with rule-of-thumb synthesis (no need for a full solver):

  * choose packet sizes that align to threshold bytes
  * choose quantum/weights that create equal finish tags
* Later, allow the checkers to expose “boundary hints” (paper’s optional extension) and feed them here.

### 3.3 Add calibration loops (paper + your `ideas/test-generation.md`)

Each campaign attempt can do a few adaptive reruns, not just one shot.

**Where:** `campaign()` implementation in `days/src/utils/testgen.rs`

**Tasks**

* For each generated candidate config:

  1. Run `leanguard-run` (simulate-and-check)
  2. If accepted but doesn’t hit goal coverpoints → adjust *one controlling knob* and rerun (up to K times)
  3. If rejected → save as rejected (and optionally auto-minimize later)
  4. If accepted and meets novelty criteria → keep

**Subtasks**

* Define a per-protocol “control knob priority order” for calibration, e.g.:

  * DCQCN: traffic intensity → ECN threshold → cnp_interval → duration
  * PFC: traffic intensity → buffer_capacity → xoff/xon → refresh interval
  * DRR/WFQ: packet sizes → weights/quantum → initial_delay alignment
* Log each calibration step as `Mutation::CalibrationStep { iter, knob, from, to, reason }`.

---

## 4) DCQCN targeted generation campaign

### 4.1 Identify the config surface and parsing paths

**Where to ground in Days**

* Seeds: `configs/dcqcn_simple.toml`, `configs/dcqcn_*.toml`
* Flow parsing: `days/src/flows/flow.rs` already handles `flow_type = DCQCN` (feature-gated).
* Checkers and traces:

  * `dcqcn_events.csv` and `aqm_events.csv` show up in `log_path` per docs.
  * `leanguard-run` already selects `dcqcn_check` and `aqm_dcqcn_check` if both traces exist.

**Tasks**

* Document (in code comments or a small internal doc) the TOML keys you will mutate for DCQCN:

  * DCQCN endpoint params (`g`, `mi`, `cnp_interval`, min/max rate, ai/hai rates)
  * Switch AQM mode and thresholds (`switch.drop = ECN_THRESHOLD`, threshold)
  * Traffic knobs (arrival rate, packet sizes, start alignment)

*(Even if the exact key names are in existing configs, Phase 4 should explicitly encode them in the generator so mutations are robust.)*

### 4.2 Implement DCQCN targeted mutators

**Goal coverpoints (from the paper + `ideas/test-generation.md`)**

* `cnp_apply` vs `cnp_ignored_due_to_interval`
* `timer_with_cnp_seen` vs `timer_without_cnp_seen`
* `alpha_above_0p1` vs `alpha_below_0p1` (AI vs HAI region)
* `rate_clamped_min`, `rate_clamped_max`

**Mutator tasks**

1. **CNP-interval boundary generator**

   * Mutate `dcqcn.cnp_interval` to a small set of strategic values: `{0, 1µs, 10µs, 100µs, 1ms, 10ms}` (units matching the config schema).
   * Mutate traffic to create back-to-back CE marks.
2. **Alpha threshold steering**

   * Mutate gain `g` / MI factor to push alpha around 0.1.
   * Combine with congestion intensity changes to make alpha move.
3. **Rate clamp forcing**

   * For min clamp: shrink min_rate, increase congestion, shorten reaction interval.
   * For max clamp: reduce congestion, increase AI/HAI rates, extend duration.

**Subtasks**

* Add DCQCN-specific `Mutation` variants for each knob.
* Add a “seed selector” that prioritizes seed configs tagged with DCQCN and (optionally) AQM.

### 4.3 DCQCN calibration loop and trace inspection (fallback if coverage isn’t ready)

**Artifacts**

* `log_path/dcqcn_events.csv`
* possibly `aqm_events.csv`
* `log_path/traces.json` tells you if they exist.

**Tasks**

* Implement a lightweight DCQCN trace analyzer:

  * count `cnp_sent`, `cnp_recv`, `timer_tick` by flow/endpoint
  * compute time deltas between consecutive CNPs to infer “ignored due to interval” opportunities
  * observe min/max of `alpha_ppb` and `rate_bps` columns (the DCQCN trace schema in the paper includes these; your checker likely already uses them)
* Map analyzer findings to calibration actions:

  * if too few CNPs: increase offered load (arrival rate), reduce link rate, reduce ECN threshold
  * if alpha never crosses 0.1: adjust `g` and/or extend duration
  * if clamps never hit: shrink bounds or push congestion harder/softer

**Subtasks**

* Put analyzers in a separate Rust module so PFC/AQM/WFQ/DRR can reuse CSV reading utilities.

---

## 5) AQM targeted generation campaign

### 5.1 Establish supported AQM policies and knobs

**Where**

* Config seeds: `configs/simple.toml` (likely RED), `configs/dcqcn_simple.toml` (ECN threshold), etc.
* Switch config keys are mutated today in `mutate_switch_capacity` and `mutate_switch_port_rate`.

**Tasks**

* Add targeted mutations for:

  * policy selection: `switch.drop ∈ {TailDrop, RED, ECN_THRESHOLD}`
  * policy parameters: capacity, thresholds, RED min/max, etc. (whatever is present in existing configs)

### 5.2 Address the “non‑ECN traffic” branch gap (if you want the full AQM matrix)

The paper’s coverpoints mention ECN eligibility branches (mark vs drop for not-ECT). If Days currently can’t generate non‑ECT packets, Phase 4 should plan a minimal config extension.

**Tasks (planned schema extension)**

1. Add optional ECN capability knob:

   * per-flow: `ecn = "ect0" | "ect1" | "not_ect"` (or boolean `ecn_capable`)
   * and/or per-traffic block
2. Update TOML parsing:

   * `days/src/flows/flow.rs`: add fields in `TomlFlow` / `TomlFlowSet`, propagate into `FlowParams`
3. Propagate into packet generation:

   * `days/src/flows/packet.rs` (and/or source modules) set ECN bits accordingly
4. Ensure `aqm_events.csv` logs enough info for the checker:

   * packet ECN-capability field (if not already logged)

*(This is a Phase 4 plan item; implementation can be staged behind a feature flag if desired.)*

### 5.3 Implement AQM targeted mutators + boundary synthesis

**Goal coverpoints**

* TailDrop: enqueue vs overflow drop
* ECN_THRESHOLD: mark at/above threshold vs drop overflow vs pass below threshold
* RED: under_min, between, over_max; mark vs drop decisions; RNG draw boundaries

**Mutator tasks**

* **Overflow boundary generator**

  * Synthesize `switch.capacity` and `pkt_size_dist` so that `occupancy + pkt_size == capacity` and `== capacity+1`.
* **ECN threshold boundary generator**

  * Synthesize `ecn_threshold` to match a reachable occupancy boundary (multiples of packet sizes).
* **RED region steering**

  * Adjust RED min/max thresholds and traffic intensity so the queue spends time in each region.

**Calibration loop tasks**

* Parse `aqm_events.csv` for:

  * decision kind (enqueue/drop/mark)
  * occupancy snapshot at decision time (if present)
* Adjust controlling knobs to move occupancy into desired region.

---

## 6) PFC targeted generation campaign

### 6.1 Validate and leverage the existing PFC config surface

**Where**

* PFC mode detection: `leanguard-run` uses config’s `link.mode == "pfc"` to hint features; manifest includes `pfc_events.csv`.
* PFC config lives under `[link]`/`[link.pfc]` (per docs).

**Tasks**

* Enumerate the PFC keys you will mutate (based on existing configs like `configs/pfc.toml`):

  * `xoff[]`, `xon[]` thresholds (per priority)
  * `buffer_capacity`
  * `pause_quanta`
  * refresh/cooldown timers (if present)

### 6.2 Use flow priorities for multi-priority PFC cases

**Grounding:** `days/src/flows/flow.rs`

* `priority: Option<u8>` exists for both `[[flow]]` and `[[flow_set]]`.
* `checked_priority` enforces 0..=7 and sets default 0.
* Flow struct stores `priority: u8`.

**Tasks**

* Add targeted mutators that:

  * assign different priorities to different flows/flow_sets
  * ensure at least 2 priorities are active in the same run (to cover per-priority PFC behavior)
* Add “priority shaping” seed configs (see §8) that make this cheap.

### 6.3 Implement PFC boundary synthesis + calibration

**Goal coverpoints**

* `pause_assert` vs `pause_refresh` vs `resume`
* boundary hits:

  * `occ_eq_xoff`, `occ_gt_xoff`
  * `occ_eq_xon`, `occ_lt_xon`
* multi-priority: observe multiple prios in a single trace

**Mutator tasks**

* **Threshold equality synthesis**

  * Choose packet sizes so occupancy can hit XOFF/XON exactly.
  * Mutate `xoff`/`xon` arrays to multiples of typical packet sizes.
* **Pause refresh forcing**

  * Set a small refresh interval and ensure sustained offered load.
* **Resume forcing**

  * Reduce offered load or extend duration to allow drain below XON.

**Calibration tasks**

* Parse `pfc_events.csv` for pause/resume events and (if present) occupancy and priority.
* If occupancy never reaches XOFF:

  * increase offered load, lower XOFF, or lower link rate
* If never resumes:

  * reduce offered load, raise XON, or increase drain opportunity

---

## 7) WFQ + DRR targeted generation campaigns (multi-class scheduling)

These are the most “Phase 4-ish” because rare edges are about **ties, pointer movement, and boundary arithmetic**.

### 7.1 First: verify how classes are selected in the current simulator

**Grounding in current architecture**

* We know flows carry `priority` (0..7) in `Flow`.
* We need to confirm packets/scheduler use that priority to pick a class/queue.

**Tasks**

* Audit the scheduling pipeline:

  * likely `days/src/schedulers/*` and `days/src/switches/switch.rs`
  * confirm where packet “class” is derived
  * confirm WFQ/DRR state supports multiple classes and how many
* Document the mapping:

  * is it fixed 8 priorities?
  * does it depend on `switch.weights.len()` / `class_count`?
  * what happens if a packet arrives with priority ≥ class_count?

**Subtasks**

* If the mapping is incomplete today:

  * plan a minimal implementation to route `Flow.priority` → packet header → scheduler class
  * ensure trace schemas (`wfq_events.csv`, `drr_events.csv`) include `class_id` / priority consistently so checkers can replay

### 7.2 Seed configs purpose-built for scheduler corner cases

Phase 4 will be much easier if you add small scheduler seeds instead of only fat-tree benchmarks.

**Tasks**

* Add a directory like `configs/testgen/schedulers/` with:

  * `wfq_2class_tie.toml`
  * `wfq_idle_to_active.toml`
  * `drr_deficit_eq_size.toml`
  * `drr_scan_steps.toml`
* Use explicit tiny topology: 2 hosts + 1 switch + 2 links (fast, deterministic).
* Use 2–3 flows/flow_sets with different `priority`.

**Where it connects**

* These become inputs to `seed-index` and are selected by campaign seed filters.

### 7.3 WFQ targeted mutators and goals

**Goal coverpoints**

* idle-to-active transitions
* `finish_time_tie_observed`
* `queue_becomes_empty_after_depart`
* multi-class observed (`class_count_ge_2`)

**Mutator tasks**

* **Tie synthesis**

  * Mutate `switch.weights` and packet sizes so two queues compute equal finish tags.
  * Align arrival times (`traffic.initial_delay`) so enqueues happen at the same `time_ns` (to stress canonicalization and ties).
* **Idle-to-active**

  * Mutate flow start gaps so a queue empties then becomes active again.
* **Cost reduction**

  * Keep duration tiny while still producing `wfq_events.csv`.

**Calibration tasks**

* Parse `wfq_events.csv` fields (as logged today) to detect:

  * equal `finish_time_ns` among candidates
  * whether schedule events occurred when multiple queues non-empty
* If ties not observed:

  * adjust weights and packet sizes; increase concurrency (more same-time events)

### 7.4 DRR targeted mutators and goals

**Goal coverpoints**

* `size_eq_deficit` and `size_lt_deficit`
* `scanSteps_gt_0`
* `deficit_reset_on_empty`
* wrap-around / round-boundary behaviors (if represented in trace)

**Mutator tasks**

* **Deficit equality synthesis**

  * Choose packet size distribution + quantum to make `deficit == pkt_size` occur.
* **Scan steps**

  * Ensure some classes empty so pointer advances.
  * Adjust traffic rates per class so some classes starve.
* **Empty reset**

  * Create a burst in one class then stop it to empty the queue.

**Calibration tasks**

* Parse `drr_events.csv` for:

  * `scan_steps`, `deficit_bytes`, packet sizes per schedule event
* If scan_steps always 0:

  * reduce traffic in some classes or stagger starts

---

## 8) Micro-harness vs integration scenarios (paper’s “tiers”) — Phase 4 staging plan

The paper recommends two tiers: micro-harness tests (cheap, high semantic coverage) and integration tests (end-to-end interactions).

### 8.1 Tier 1 now: micro scenarios using existing TOML config surface

**Tasks**

* Create “micro” seeds for each protocol using existing topology/flow encoding:

  * DCQCN micro: 2 hosts, 1 bottleneck switch, ECN threshold
  * AQM micro: one congested output port with a single flow set
  * PFC micro: one congested link with explicit priorities
  * WFQ/DRR micro: one scheduler instance, 2–3 classes
* Add campaign seed filters to prioritize these micro seeds.

### 8.2 Tier 1 later (optional but high ROI): dedicated scheduler micro-harness mode

This is aligned with `ideas/test-generation.md` and the paper’s “micro-harness tests” section, but it requires simulator changes.

**Planned tasks (design-level)**

* Add a `[harness]` section to TOML that runs a scheduler in isolation:

  * `type = "WFQ" | "DRR"`
  * scripted `[[harness.event]]` stimuli (enqueue/schedule/depart) with `time_ns`, `class_id`, `size_bytes`
* Implement harness in a new module (e.g., `days/src/harness/`) that emits the same `*_events.csv` schemas.
* Update `leanguard-run` trace discovery and checker selection remains unchanged (it just sees non-empty traces).

*(This is “Phase 4.5” type work: not required for initial targeted campaigns, but it unlocks much better coverage-per-second.)*

---

## 9) Corpus policies for targeted campaigns

### 9.1 Keep rules (accepted tests)

**Tasks**

* Keep a candidate if:

  * accepted AND adds new coverpoints to `GlobalCov`, OR
  * accepted AND hits explicitly requested `--goal` coverpoints (even if not globally novel), OR
  * accepted AND reduces cost while preserving a target coverage signature (optional)

**Where**

* `days/src/utils/testgen.rs` campaign loop + `CoverageInfo.novelty`

### 9.2 Dedup rules (accepted tests)

**Tasks**

* Define a stable signature:

  * primary: sorted coverpoint list hash
  * fallback: (trace filenames + kind-sequence hashes)
* Store signature in metadata and skip saving duplicates unless it improves cost.

### 9.3 Handling REJECTs

**Tasks**

* Always save REJECT cases to `rejected/` with:

  * full `run_summary.json`
  * mutations + calibration steps
* Optionally integrate with existing `minimize` command:

  * allow `campaign --auto-minimize-rejects` (calls `minimize` on new rejects up to a cap)

---

## 10) Testing plan for Phase 4 additions

### 10.1 Unit tests for TOML mutation correctness

**Where:** new tests under `days/tests/`

**Tasks**

* For each new targeted mutator:

  * start from a small TOML value
  * apply mutator
  * assert the correct field path changed and values are within bounds
* Reuse existing test style from `tests/testgen_seed_index.rs`.

### 10.2 Integration tests with stub `leanguard-run` (like `tests/leanguard_run.rs`)

**Tasks**

* Add a new test that:

  * runs `leanguard-testgen campaign --protocol wfq --budget 1`
  * uses a stub `leanguard-run` executable that emits JSON including fake coverage
  * verifies:

    * case is filed under `accepted/` when coverage is “new”
    * metadata includes `coverage.observed` and campaign info
    * calibration steps are recorded when enabled

---

## 11) Documentation updates (so Phase 4 is usable)

### 11.1 Update LeanGuard docs to include targeted campaigns

**Where**

* `days/docs/docs/verification/leanguard.md`
* optionally add a new page under `docs/docs/verification/` like `testgen.md`

**Tasks**

* Document:

  * how to run `seed-index`, then `campaign`
  * how to restrict to protocol (`--protocol dcqcn`)
  * where to find saved configs/logs/metadata
  * how to replay a case and interpret coverpoints

### 11.2 Add a “Phase 4 seed pack”

**Where:** `configs/testgen/` or similar

**Tasks**

* Provide at least one tiny seed per protocol family with comments explaining what it targets.

---

## Suggested implementation order (minimizes churn, maximizes early value)

1. **Fix `run_leanguard` handling of exit code 1** + parse run summary reliably (0.1).
2. **Add seed protocol tags** to improve seed selection (0.2).
3. **Add campaign CLI + dispatcher** (2.1–2.2).
4. **Wire in coverage ingestion** (1.1–1.2), with trace-signature fallback.
5. **Implement DCQCN campaign** (4.x) + minimal DCQCN micro seeds.
6. **Implement PFC campaign** (6.x) leveraging existing `priority` in `flows/flow.rs`.
7. **Implement AQM campaign** (5.x); optionally stage non-ECN traffic support as a follow-on.
8. **Implement WFQ + DRR campaigns** (7.x) after confirming class mapping and adding micro scheduler seeds.
9. Add dedup/cost policies + tests + docs.

---

If you want, I can also include a concrete **“protocol → config keys to mutate” matrix** (DCQCN/AQM/PFC/WFQ/DRR) written in the same style as your existing `Mutation` enum so it can be dropped directly into a design doc for the Phase 4 PR series.
