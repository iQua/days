Below is a concrete, engineering-first plan to start implementing the “LeanGuard Test Generation” features using the **exact config surface you already have** in `configs/` (TOML), plus a few small, targeted extensions where today’s configs don’t let us reach important edge cases (notably multi-class scheduling and non‑ECN traffic).

I’m going to assume the intended workflow is:

1. Generate / mutate a TOML scenario config
2. Run Days → produce event traces (CSV)
3. Run Lean checkers on those traces → ACCEPT / REJECT (+ coverage)
4. Keep only configs that expand semantic coverage or expose bugs
5. Minimize and turn them into a stable regression corpus

---

## 1) What we can leverage immediately from the existing configs

From the configs you shared, Days already has a rich, testgen-friendly “knob surface”:

### Topology & network

* **Two topology encodings**

  * Explicit `hosts` + `edges = [{src,dst,delay,rate,jitter}, …]` (e.g., `simple.toml`, `pfc.toml`, `dcqcn_simple.toml`)
  * Parametric `topology = { type="FatTree"| "Torus", … }` (most `benchmarks/*`)
* Link mode includes `mode="Pfc"` with a detailed `[link.pfc]` block (thresholds, buffer capacity, pause quanta, etc.)

### Traffic & flows

* Two encodings again:

  * Explicit `[[flow]]` entries (small examples)
  * `[[flow_set]]` with `flow_count` + a structured `traffic` object (benchmarks)

Traffic models include `PacketDistribution` with `arr_dist` types like `Uniform` and `Exp`, `size_dist`, `initial_delay`, and protocol-specific blocks:

* `flow_type="TCP"` with `cc_algorithm="CUBIC"`
* `flow_type="DCQCN"` with DCQCN parameters
* Collectives exist (`[[collective]]`), but we can defer those for MVP testgen.

### Switch behavior

* `switch.drop` in {`TailDrop`, `RED`, `ECN_THRESHOLD`}
* `switch.discipline` in {`FIFO`, `WFQ`, `DRR`, `SP`, `VirtualClock`}
* `switch.weights`, `switch.run_batch_size`, etc.

This is enough to start generating meaningful AQM / PFC / DCQCN / CUBIC tests today.

The one big gap (for deeper WFQ/DRR coverage) is: **your existing configs don’t create multiple scheduling classes** (all `switch.weights=[1]`, and only `pfc.toml` uses explicit per-flow `priority`). We’ll handle that in the plan.

---

## 2) Phase 1: Build an end-to-end “single config → verified” runner

Before fuzzing/generation, make a robust runner that can take **any existing TOML** and return a structured result.

### 2.1 Deliverable: `leanguard-run` command (thin wrapper)

Implement as either:

* a Rust `xtask` (`cargo xtask leanguard-run …`), or
* a Python CLI in `scripts/`, if that’s faster initially.

**Responsibilities**

1. Create an isolated output directory per run:

   * `log_path = logs/leanguard/<case_id>/`
2. Run Days with deterministic settings (unless the test asks otherwise):

   * Prefer `threading="single"` and a fixed `seed`
3. Discover which traces were emitted (manifest, or scanning directory)
4. Run the correct Lean checker(s) for the scenario
5. Emit a single JSON summary with:

   * `accept: true/false`
   * `checker_results: {pfc:…, aqm:…, dcqcn:…, wfq:…, drr:…, cubic:…}`
   * `coverage_bits` per checker (once we add it)

### 2.2 Standardize trace discovery: add a “trace manifest”

Right now, a runner will otherwise need to guess filenames. Make Days write:

* `log_path/traces.json` (or `manifest.json`) containing a list like:

  * `"pfc_events.csv"`
  * `"aqm_events.csv"`
  * `"dcqcn_events.csv"`
  * `"wfq_events.csv"`
  * `"drr_events.csv"`
  * `"tcp_cubic_events.csv"`

This single change makes the whole pipeline vastly more robust.

### 2.3 Checker selection rules (based on config + manifest)

In `leanguard-run` implement simple rules:

* If manifest contains `pfc_events.csv` → run `pfc_check`
* If manifest contains `aqm_events.csv` → run `aqm_check`
* If manifest contains `dcqcn_events.csv` → run `dcqcn_check`
* If both `aqm_events.csv` and `dcqcn_events.csv` exist → run `aqm_dcqcn_check`
* If manifest contains `wfq_events.csv` → run `wfq_check`
* If manifest contains `drr_events.csv` → run `drr_check`
* If manifest contains `tcp_cubic_events.csv` → run `cubic_check`

This lets you use your current seeds immediately:

* `dcqcn_simple.toml` → DCQCN (+ likely AQM + PFC, depending on what Days emits)
* `pfc.toml` → PFC
* `simple.toml` → AQM (RED)
* `benchmarks/scheduling/*wfq*.toml` → WFQ
* `benchmarks/scheduling/*drr*.toml` → DRR
* `tcp_simple.toml` / `tcp_fattree.toml` → CUBIC

---

## 3) Phase 2: Add machine-readable semantic coverage to Lean checkers

Coverage guidance is the “engine” of test generation. Without it, you can still do parameter sweeps—but you’ll miss rare corners.

### 3.1 Output format: JSON coverage sidecar

Extend each checker executable to accept an optional argument/flag:

* `--coverage-out <path>`
  and write e.g.:

```json
{
  "checker": "dcqcn_check",
  "accept": true,
  "cover": ["cnp_apply", "timer_ai", "alpha_above_0p1", "rate_clamped_min"],
  "stats": {"rows": 1842}
}
```

Keep stdout as the human string (`ACCEPT`/`REJECT`) so you don’t break existing workflows.

### 3.2 Initial coverpoints worth adding (per checker)

These are chosen to align with the actual branch structure in your Lean semantics.

**DCQCN**

* `cnp_apply` vs `cnp_ignored_due_to_interval`
* `timer_with_cnp_seen` vs `timer_without_cnp_seen`
* `alpha_above_0p1` vs `alpha_below_0p1` (HAI vs AI region)
* `rate_clamped_min` and `rate_clamped_max`
* `alpha_updated_nontrivial` (alpha changed)

**PFC**

* `pause_assert` (new pause)
* `pause_refresh` (pause while already paused)
* `resume` (pause_quanta=0)
* boundary hits:

  * `occ_eq_xoff`, `occ_gt_xoff`
  * `occ_eq_xon`, `occ_lt_xon`
* multi-priority coverage:

  * `prio_0_seen`, … `prio_7_seen` (at least bucket them)

**AQM**

* per strategy:

  * `taildrop_overflow_drop`, `taildrop_enqueue`
  * `ecn_threshold_mark`, `ecn_threshold_drop_overflow`, `ecn_threshold_pass`
  * `red_under_min`, `red_over_max`, `red_between`
  * `red_should_drop`, `red_should_mark`
* ecn capability branch:

  * `mark_non_ecn_packet_drop` (only if Days can generate not_ect traffic)

**WFQ**

* `enqueue_when_empty` (weightSum=0 reset path)
* `schedule_nonempty`
* `queue_becomes_empty_after_depart`
* `finish_time_tie_observed` (when two min finish times equal)
* multi-class observed:

  * `class_count_ge_2`

**DRR**

* `wraparound_updates_deficits`
* `scanSteps_gt_0`
* `size_eq_deficit` vs `size_lt_deficit`
* `deficit_reset_on_empty`
* multi-class observed:

  * `class_count_ge_2`

### 3.3 Why do this now?

Because once this exists, test generation becomes a simple feedback loop:

* mutate → run → accept? → did coverage expand? → keep/discard

---

## 4) Phase 3: “MVP TestGen” using only today’s config schema

Start with the configs you already have as the **seed corpus**, and generate *new configs* by mutation.

### 4.1 Deliverable: `leanguard-testgen` CLI

Core commands:

* `leanguard-testgen seed-index configs/`
  Builds an index of seeds + the protocol features they exercise.

* `leanguard-testgen fuzz --budget <N>`
  Runs N generated cases, keeps those with new coverage.

* `leanguard-testgen replay <case_dir>`
  Reproduces a case (runs Days + checkers).

* `leanguard-testgen minimize <case_dir>`
  Delta-debug/minimize a failing config.

### 4.2 Corpus layout

Create a git-tracked corpus directory:

```
leanguard_corpus/
  seeds/                  # symlinks or copied from configs/
  accepted/               # configs that expand coverage
  rejected/               # configs that trigger bugs (sim or checker)
  metadata/
    <case_id>.json        # coverage, parents, mutation history
```

### 4.3 General-purpose config mutators (work on almost all configs)

These are “cheap wins” that increase diversity without special protocol knowledge.

**Structural**

* Switch between `threading="single"` and `"multi"` (but only keep multi-threaded if stable)
* Vary `concurrency_level` and `time_quantum_ns` to create more tie situations (same `time_ns` events)
* Perturb `duration` small/medium (for faster runs + longer runs)

**Traffic**

* Change `arr_dist` parameters (Uniform low/high; Exp rate)
* Change `size_dist` parameters to align with queue thresholds (important for PFC/AQM)
* Change `initial_delay` to align or misalign flows (to trigger bursty coincident events)
* Change `flow_count` in `flow_set` (downscale huge benchmark configs for speed)

**Network**

* Scale link `rate` and `delay` up/down
* Add small jitter to force reorderings (if Days models it)

**Metamorphic transforms (should preserve “accept”)**

* Permute host IDs and edges order (should not affect correctness)
* Add constant offset to all start times / delays
* Scale all rates and arrival rates by the same factor (dimensionless behavior often preserved)

These metamorphic transforms are very useful early: they can produce *lots* of accepted tests and shake out ordering/canonicalization issues in traces.

---

## 5) Phase 4: Protocol-targeted generation to hit semantic edge cases

Once the MVP loop works, add *targeted mutators* that aim for specific coverpoints and boundary conditions.

### 5.1 DCQCN targeted generation (seed: `dcqcn_simple.toml`)

Key config knobs available today:

* `dcqcn.cnp_interval`
* `dcqcn.g`
* `dcqcn.ai_rate`, `dcqcn.hai_rate`, `dcqcn.min_rate`, `dcqcn.max_rate`
* switch `drop="ECN_THRESHOLD"`, `ecn_threshold`
* traffic arrival rate and sizes

**Target recipes**

* **CNP interval boundary**

  * Try setting `cnp_interval` to {0, very small, typical, very large}
  * Then tune traffic rate so multiple CE marks happen close together
  * Goal: observe both `cnp_apply` and `cnp_ignored_due_to_interval`

* **Alpha threshold boundary (0.1)**

  * Mutate `g` and `mi` (if present in config) to push alpha above/below 0.1
  * Use sustained congestion to keep marking/CNPs active

* **Rate clamp**

  * For `rate_clamped_min`: aggressive congestion + high `g` / large `mi`
  * For `rate_clamped_max`: light congestion + many timer ticks without CNPs + high ai/hai

**Practical trick**: add a “calibration loop”

* Run a candidate config once
* Parse `dcqcn_events.csv` (and/or queue traces)
* If you didn’t hit the coverpoint, auto-adjust *one controlling knob* (arrival rate or duration) and retry a few times

This is far more effective than blind randomization.

### 5.2 AQM targeted generation (seed: `simple.toml` for RED; `dcqcn_simple.toml` for ECN_THRESHOLD)

Your configs already cover:

* `drop="RED"`
* `drop="ECN_THRESHOLD"` + `ecn_threshold`
* `drop="TailDrop"`

**Target recipes**

* Overflow boundary:

  * Adjust `switch.buffer_size` (or `buffer_capacity`) and packet size so that a single enqueue crosses the capacity exactly or barely
* ECN threshold boundary:

  * Make `ecn_threshold` hit values that produce “exactly threshold” scenarios in PPB rounding
  * Vary packet size so average queue crosses threshold due to one packet arrival

**Gap to close (recommended small extension)**
To cover `ecnMarkAllowed` branches, you need some traffic that’s `not_ect`. There’s no config knob for that today, so add one:

* `tcp.ecn = true/false` (or per flow_set)
* `dcqcn` packets probably always ECN-capable; for AQM we want both.

Then you can generate cases where AQM would mark ECN but must drop because the packet is not ECN-capable.

### 5.3 PFC targeted generation (seed: `pfc.toml` plus PFC-on DCQCN configs)

PFC edge cases are largely about **queue occupancy vs thresholds**.

Your PFC config already has:

* xoff/xon arrays
* pause_quanta
* buffer_capacity
* refresh/drain intervals
* per-flow `priority` (this is huge for multi-class testing elsewhere too)

**Target recipes**

* **Exact threshold hits**

  * Set `xoff` and `xon` to multiples of packet sizes to get `occ == xoff` and `occ == xon`
* **Pause refresh**

  * Choose a small `refresh_interval` and create sustained congestion so queue stays > xon
* **Resume**

  * Ensure the queue drains below xon; then you should see pause_quanta=0 frames

**Calibration loop** works great here too:

* Run once, parse `pfc_events.csv` occupancy at pause/resume events
* Adjust traffic rate or buffer thresholds to move occupancy toward the boundary you want

### 5.4 WFQ/DRR targeted generation (needs multi-class → small config/schema work)

Today, your WFQ/DRR configs are benchmark fat-trees with `weights=[1]` and no explicit per-class traffic. That’s fine for smoke tests but not for deep scheduling coverage.

**Two ways forward; do both in this order:**

#### (A) Use existing schema + introduce per-flow priority for TCP flow_sets (small extension)

You already use `priority` in `pfc.toml` flows. Extend parsing so TCP `[[flow]]` or `[[flow_set]]` can set:

* `priority = 0..7` (or `class_id`)
* and Days uses that for scheduler class selection

Then create *new small seed configs*:

* 2–3 priorities/classes
* `switch.weights = [w0, w1, w2]`
* A tiny topology (explicit edges) to keep runs fast

This unlocks:

* WFQ: ties, finish tag ordering, multi-class selection
* DRR: scanSteps skipping empty classes, deficit replenishment cycles, wrap-around

#### (B) Add a “scheduler micro-harness mode” (best long-term ROI)

Network-level stimulation is noisy for scheduler edge cases. A micro-harness lets you precisely create:

* enqueue/schedule/depart patterns
* sizes and inter-arrival times that create ties, boundary deficits, etc.

Minimal design:

* New config section:

  * `[harness] type="WFQ"|"DRR"`
  * `[[harness.event]] kind="enqueue" time_ns=… class_id=… size_bytes=…`
  * plus schedule/depart ticks
* Harness runs the scheduler in isolation and emits the same `wfq_events.csv`/`drr_events.csv` format.

This will make your testgen *much* faster and far more targeted.

---

## 6) Phase 5: Minimization and turning findings into stable regression tests

Once testgen starts finding:

* coverage-expanding accepted tests, and
* failing tests (simulator or checker)

you need a robust minimization story, or the corpus becomes unmanageable.

### 6.1 Config minimization (delta debugging on TOML)

Implement a shrinker that tries, in order:

1. Reduce `duration`
2. Reduce number of flows / flow_count
3. Reduce topology size (e.g., FatTree k down, Torus size down, or remove edges)
4. Reduce distribution ranges (tighten Uniform high/low)
5. Reduce “noise” parameters (jitter, extra hosts)

Stop when the property is preserved:

* For acceptance tests: preserve a target set of coverpoints
* For failure tests: preserve the failure signature (same checker rejects, or Days crashes)

### 6.2 Keep two corpora

* `accepted/` corpus: must be stable, always ACCEPT in CI
* `bug/` corpus: reproduces known failures (optional in CI; maybe run nightly)

---

## 7) Phase 6: CI integration (so this becomes engineering muscle, not a one-off)

### 7.1 Deterministic CI suite

Add a CI job that:

1. Builds Days with the required Lean protocol-trace features
2. Builds Lean checkers
3. Runs `leanguard-run` on every config in `leanguard_corpus/accepted/`
4. Requires all ACCEPT

This is your “semantic regression suite”.

### 7.2 Optional “small fuzz smoke test”

In CI, you can also run:

* `leanguard-testgen fuzz --budget 20 --from-seeds …`
  and require:
* no crashes, no hangs, and no REJECT unless it’s a known bug bucket

But keep this *small*; the main generator runs locally or nightly.

---

## 8) Concrete “start tomorrow” checklist

If you want the fastest path to traction with the configs you already have, do this in order:

1. **Implement `traces.json` manifest output in Days** (per run)
2. **Implement `leanguard-run`**

   * run Days
   * run checkers based on manifest
   * produce `result.json`
3. **Add `--coverage-out` JSON support to each Lean checker**
4. **Create an initial seed list** (copy/symlink from existing configs)

   * `configs/dcqcn_simple.toml`
   * `configs/pfc.toml`
   * `configs/simple.toml`
   * `configs/tcp_simple.toml`
   * `configs/benchmarks/scheduling/fattree_k32_tcp_f128_wfq_st.toml`
   * `configs/benchmarks/scheduling/fattree_k32_tcp_f128_drr_st.toml`
5. **Implement 5–10 simple mutators** (duration downscale, arrival rate tweak, packet size tweak, threshold tweak, seed tweak)
6. **Implement the corpus loop**

   * keep if coverage expands
7. **Add the first “schema extension”**

   * TCP `ecn = true/false` (to unlock AQM `not_ect` branch tests)
8. **Add the second “schema extension”**

   * TCP `priority`/`class_id` (to unlock multi-class WFQ/DRR)
9. Start generating a small accepted corpus (dozens, not thousands), then lock it into CI.

---

## 9) What you’ll get early vs later

### Early (with today’s configs + minimal glue)

* A working end-to-end pipeline that can:

  * replay configs,
  * run Lean checkers,
  * accumulate semantic coverage,
  * and build a regression suite.
* Rapid improvements for: **DCQCN, AQM, PFC, CUBIC**.

### Later (after tiny schema additions + optional harness)

* Deep, systematic coverage for: **WFQ, DRR**
* Much higher probability of hitting “rare but real” corner states (ties, wrap-around, boundary deficits, threshold equalities)

---

If you want, I can also propose an explicit first set of **~20 “hand-designed” seed configs** (tiny topologies, 1–3 flows, each crafted to target one coverpoint) derived from your current TOMLs—those become the seed corpus that the mutational generator expands.
