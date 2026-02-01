# Investigation: Coverage-guided test generation experiments feasible **now** (Phase 0 complete)

## Summary
With Phase 0 plumbing completed (Lean checker coverpoints → `leanguard-run --coverage` → `leanguard-testgen` metadata + `global_coverage.json`), you can run **coverage-measured** and partially **goal-directed** experiments today:

- Coverage growth and redundancy measurements under `fuzz` and `campaign`.
- A/B comparison of semantic coverpoints vs the `trace_signature` fallback (`--no-coverage`).
- DCQCN goal-directed campaigns with calibration loops (trace-analyzer-driven) to hit specific semantic coverpoints.
- Offline corpus minimization (select minimal subset of accepted cases preserving semantic coverage).
- Failing-case minimization (TOML shrinkers) evaluation across a set of rejects.

Current limitations: `fuzz`/`campaign` **do not yet discard** redundant accepted cases based on coverage novelty (they only record novelty), accepted-case *config* minimization is not implemented, and targeted campaign logic is currently DCQCN-only.

## Symptoms / Constraints
- Phase 0 fixed the silent failure mode where Lean JSON coverage blobs were interpreted as one “coverage point”, and ensured `leanguard-testgen` passes `--coverage` by default.
- Experiments must be runnable now (no new feature work).
- We want semantic coverage (`checker_coverpoints`) and corpus growth, plus automated minimization of failing cases.

## Investigation Log

### 2026-02-01 / Phase 1 — Initial assessment
**Hypothesis:** After Phase 0, semantic coverpoints should flow end-to-end and can drive experiments.
**Findings:** Confirmed that Phase 0 is implemented (see `ideas/coverage-plumbing.md`).
**Evidence:** `ideas/coverage-plumbing.md`.

### 2026-02-01 / Phase 2 — RepoPrompt `context_builder`
**Hypothesis:** Testgen already stores coverage and global novelty; identify experiments and limitations.
**Findings:** `leanguard-testgen` produces per-case metadata including `coverage.mode/observed/novelty` and maintains `metadata/global_coverage.json`. Trace-signature fallback exists.
**Evidence:** `src/utils/testgen.rs`, `src/bin/leanguard-testgen.rs`, `src/bin/leanguard-run.rs`, `lean/LeanGuard/Shared/Coverage.lean`, `lean/LeanGuard/*EventLog.lean`.

### 2026-02-01 / Phase 3 — Follow-up deep dives (chat_send)
**Hypothesis:** Maybe fuzz/campaign already discard redundant accepted cases.
**Findings:** No—today they keep all accepted cases; novelty is informational.
**Evidence:** `src/utils/testgen.rs` fuzz/campaign finalization paths; only `if accept && novelty=="new"` updates `global_coverage.json`.

### 2026-02-01 / Phase 4 — Evidence gathering (manual code reading)
**Findings:**
- `CoverageInfo`:
  - mode is `checker_coverpoints` if RunSummary contains coverage union, else `trace_signature` (hash of `kind` column sequence per trace file), else `stub`.
  - novelty is computed only for accepted cases and only when a mutable global set is passed.
- `trace_signature` is computed via `DefaultHasher` over the `kind` column sequence using naive `split(',')` CSV parsing.
- `campaign` calibration loops are DCQCN-only and use the DCQCN trace analyzer (not semantic coverage) to decide knob adjustments.
- `minimize` only accepts failing cases (`accept=false`) and preserves only “still failing”.

**Evidence:**
- `src/utils/testgen.rs`:
  - `build_coverage_info` and `trace_signature_entries`.
  - `hash_trace_kind_sequence`.
  - `campaign_with_options` + `calibrate_dcqcn`.
  - `minimize` shrinkers list.

## What we can run right now (experiments)

See the “Experiments” section below for commands and metrics.

## Limitations / gotchas (important for interpreting results)
1. **Not truly coverage-guided generation yet**: coverage is measured and recorded; generation isn’t actively steered by global coverage (except DCQCN goal/calibration).
2. **Retention is not coverage-based**: redundant accepted cases are retained; you must do offline minimization/selection if you want a small CI corpus.
3. **Minimization is failing-only**: accepted-case minimization (preserve coverage while shrinking config) is not implemented.
4. **`trace_signature` is coarse and potentially noisy**: hashes only `kind` sequence; sensitive to row order; uses `DefaultHasher`.
5. **Coverage depends on building the right checkers**: if a checker is missing or does not emit coverage, `CoverageInfo.mode` will degrade.

## Recommended experiments (detailed)

### Experiment A — Coverage growth curve under `fuzz`
- Run fuzz with a fixed budget/seed and compute coverage growth over accepted cases.
- Primary metrics: coverage-union size vs accepted index; novelty rate; plateau point.

### Experiment B — A/B: semantic coverpoints vs `trace_signature`
- Run identical fuzz budgets with and without `--coverage` (`--no-coverage` in testgen).
- Compare novelty rate and “coverage size” under the two modes.

### Experiment C — DCQCN goal campaigns with calibration (on/off)
- Use `leanguard-testgen campaign --protocol dcqcn --goal <coverpoint> --max-calibration-iters K`.
- Measure goal hit rate and calibration steps.

### Experiment D — Offline corpus minimization (set-cover selection)
- Given an accepted corpus with semantic coverpoints, greedily select minimal accepted cases that preserve the same coverpoint union.

### Experiment E — Failing-case minimization quality
- Sample rejected cases and run `leanguard-testgen minimize ...`.
- Metrics: config size reduction, iterations kept, and failure stability.

### Experiment F — Coverage plumbing integrity / determinism check
- Verify end-to-end consistency across three layers:
  - checker `--coverage-out` JSON
  - `leanguard-run` RunSummary `.coverage.*`
  - `leanguard-testgen` metadata `coverage.*`


---

## Experiments (commands + artifacts + metrics)

### Common setup

Build the Lean checkers:

```bash
cd lean
lake build
cd ..

# If some checker binaries are missing under lean/.lake/build/bin, build them explicitly:
# cd lean && lake build dcqcn_check aqm_check aqm_dcqcn_check pfc_check wfq_check drr_check cubic_check && cd ..
```

Build the CLIs with the features needed by the stock `configs/` seeds:

```bash
cargo build --features lean,dcqcn,l2_pfc --bin leanguard-run --bin leanguard-testgen

export LEANGUARD_RUN="$PWD/target/debug/leanguard-run"
export TG="$PWD/target/debug/leanguard-testgen"
```

Sanity check that `leanguard-run` produces coverage in its JSON summary:

```bash
$LEANGUARD_RUN --config configs/dcqcn_simple.toml --checker-dir lean/.lake/build/bin --coverage | head
```

Expected:
- Summary JSON contains `.coverage.union` and `.coverage.per_checker`.
- Under the case’s `log_path`, coverage files exist at `logs/coverage/*_coverage.json`.

Key artifact locations during testgen:
- Per-case logs: `<corpus_root>/{accepted|rejected}/<case_id>/logs/`
- Per-case coverage files: `.../logs/coverage/<checker>_coverage.json`
- Per-case RunSummary: `.../run_summary.json`
- Per-case metadata (includes CoverageInfo): `<corpus_root>/metadata/<case_id>.json`
- Global coverage set: `<corpus_root>/metadata/global_coverage.json`

---

### Experiment 1 — Coverage growth curve (semantic coverpoints) under `fuzz`

**Goal:** quantify semantic coverage growth vs case budget and measure redundancy.

```bash
rm -rf leanguard_corpus_exp1

$TG --corpus-root leanguard_corpus_exp1 --leanguard-run "$LEANGUARD_RUN" seed-index configs

$TG --corpus-root leanguard_corpus_exp1 --leanguard-run "$LEANGUARD_RUN" \
  fuzz --budget 200 --rng-seed 1
```

**Metrics to compute:**
- `|global_coverage|` (size of `metadata/global_coverage.json.observed`)
- novelty rate among accepted cases: fraction with `metadata.coverage.novelty == "new"`
- growth curve: cumulative union size vs accepted-case index

**Sanity checks:**
- In `metadata/<case_id>.json`, ensure `coverage.mode == "checker_coverpoints"` and `coverage.observed` contains real semantic names (e.g. `cnp_apply`, `red_under_min`, `pause_assert`, ...).

---

### Experiment 2 — A/B comparison: `checker_coverpoints` vs `trace_signature` fallback

**Goal:** quantify how the fallback behaves vs real semantic coverpoints.

```bash
rm -rf leanguard_corpus_cov leanguard_corpus_sig

$TG --corpus-root leanguard_corpus_cov --leanguard-run "$LEANGUARD_RUN" seed-index configs
$TG --corpus-root leanguard_corpus_sig --leanguard-run "$LEANGUARD_RUN" seed-index configs

# Semantic coverpoints (coverage enabled by default)
$TG --corpus-root leanguard_corpus_cov --leanguard-run "$LEANGUARD_RUN" \
  fuzz --budget 200 --rng-seed 1

# Force fallback: do NOT pass --coverage to leanguard-run
$TG --no-coverage --corpus-root leanguard_corpus_sig --leanguard-run "$LEANGUARD_RUN" \
  fuzz --budget 200 --rng-seed 1
```

**Metrics:**
- coverage mode distribution (how often `checker_coverpoints` vs `trace_signature` vs `stub` appears)
- novelty rate and final coverage size under each mode
- stability: rerun with same `--rng-seed` and compare `global_coverage.json` and the accepted/rejected split

**Notes / pitfalls:**
- `trace_signature` hashes only the CSV `kind` column sequence per trace file (`trace_kind_hash:<trace>:<hex>`), so it is coarse and sensitive to row ordering.

---

### Experiment 3 — DCQCN goal-directed campaigns: calibration OFF vs ON

**Goal:** quantify whether the DCQCN calibration loop improves hit-rate for rare semantic coverpoints.

Example goal set (from `lean/LeanGuard/DcqcnEventLog.lean`):
- `cnp_ignored_due_to_interval`
- `alpha_above_0p1`
- `alpha_below_0p1`
- `rate_clamped_min`
- `rate_clamped_max`

Commands:

```bash
rm -rf leanguard_corpus_dcqcn_cal0 leanguard_corpus_dcqcn_cal5

$TG --corpus-root leanguard_corpus_dcqcn_cal0 --leanguard-run "$LEANGUARD_RUN" seed-index configs
$TG --corpus-root leanguard_corpus_dcqcn_cal5 --leanguard-run "$LEANGUARD_RUN" seed-index configs

# Calibration OFF
$TG --corpus-root leanguard_corpus_dcqcn_cal0 --leanguard-run "$LEANGUARD_RUN" \
  campaign --protocol dcqcn --budget 50 --rng-seed 1 \
  --goal cnp_ignored_due_to_interval --max-calibration-iters 0

# Calibration ON
$TG --corpus-root leanguard_corpus_dcqcn_cal5 --leanguard-run "$LEANGUARD_RUN" \
  campaign --protocol dcqcn --budget 50 --rng-seed 1 \
  --goal cnp_ignored_due_to_interval --max-calibration-iters 5
```

**Metrics:**
- goal hit rate among accepted cases: fraction where `coverage.observed` contains the goal
- calibration effort: count of `Mutation::CalibrationStep` in `mutations.json` per case
- acceptance vs reject rate

**Notes:**
- Goal satisfaction prefers semantic coverpoints from RunSummary; if absent, DCQCN falls back to the DCQCN CSV analyzer (`src/utils/testgen/dcqcn.rs`).

---

### Experiment 4 — Offline corpus minimization (accepted cases), preserving semantic coverage

**Goal:** produce a small “CI-friendly” accepted subset that preserves the same semantic coverpoint union.

Prereq: run Experiment 1 or 3 to generate accepted cases with `checker_coverpoints`.

Greedy set cover script (prints chosen case IDs):

```bash
python3 - <<'PY'
import glob, json

corpus="leanguard_corpus_exp1"
meta_dir=f"{corpus}/metadata"
paths=[p for p in glob.glob(meta_dir+"/*.json") if not p.endswith(("seeds_index.json","global_coverage.json"))]

accepted=[]
universe=set()
for p in paths:
  m=json.load(open(p))
  if not m.get("result",{}).get("accept",False):
    continue
  if m.get("coverage",{}).get("mode") != "checker_coverpoints":
    continue
  pts=set(m["coverage"]["observed"])
  if not pts:
    continue
  accepted.append((m["case_id"], pts))
  universe |= pts

chosen=[]
covered=set()
remaining=accepted[:]
while covered != universe:
  best=None
  for cid, pts in remaining:
    gain=len(pts - covered)
    if best is None or gain > best[0]:
      best=(gain, cid, pts)
  if best is None or best[0]==0:
    break
  _, cid, pts = best
  chosen.append(cid)
  covered |= pts
  remaining=[x for x in remaining if x[0]!=cid]

print("universe_points", len(universe))
print("chosen_cases", len(chosen))
for cid in chosen:
  print(cid)
PY
```

**Metrics:**
- compression ratio: `chosen_cases / total_accepted`
- verify preserved coverage by replaying only chosen cases into an empty corpus and comparing `global_coverage.json`.

---

### Experiment 5 — Failing-case minimization (current shrinkers)

**Goal:** measure how well the built-in TOML shrinkers reduce failing configs while keeping them failing.

Workflow:

1) Generate a corpus with rejects:

```bash
rm -rf leanguard_corpus_fail
$TG --corpus-root leanguard_corpus_fail --leanguard-run "$LEANGUARD_RUN" seed-index configs
$TG --corpus-root leanguard_corpus_fail --leanguard-run "$LEANGUARD_RUN" fuzz --budget 1000 --rng-seed 1
```

2) Pick a rejected case directory:

```bash
ls leanguard_corpus_fail/rejected | head
```

3) Minimize it (works only for `accept=false` cases):

```bash
$TG --corpus-root leanguard_corpus_fail --leanguard-run "$LEANGUARD_RUN" \
  minimize leanguard_corpus_fail/rejected/<case_id> --max-iters 25

# Re-run to confirm it still fails
$TG --corpus-root leanguard_corpus_fail --leanguard-run "$LEANGUARD_RUN" \
  replay leanguard_corpus_fail/rejected/<case_id>
```

**Metrics:**
- byte-size reduction of `config.toml` vs `config.orig.toml`
- number of shrink iterations kept (`minimize` summary)
- failure stability: same `days_error` or same rejecting checker (not guaranteed)

**Notes / limitation:**
- `minimize` preserves only “still failing” (`accept=false`), not a specific failure signature.

---

### Experiment 6 — Coverage plumbing integrity (3-layer consistency)

**Goal:** ensure coverpoints are not silently garbage and are consistent across:
1) checker `--coverage-out` file
2) `leanguard-run` RunSummary JSON
3) `leanguard-testgen` metadata

```bash
rm -rf leanguard_corpus_plumb
$TG --corpus-root leanguard_corpus_plumb --leanguard-run "$LEANGUARD_RUN" seed-index configs
$TG --corpus-root leanguard_corpus_plumb --leanguard-run "$LEANGUARD_RUN" fuzz --budget 1 --rng-seed 123
```

Then inspect the generated case under `accepted/` or `rejected/`:
- `.../logs/coverage/<checker>_coverage.json` should be a JSON object with a `cover` array.
- `.../run_summary.json` should have `coverage.union`/`coverage.per_checker` reflecting those files.
- `metadata/<case_id>.json` should have `coverage.mode == "checker_coverpoints"` and a sorted/deduped `coverage.observed`.


---

### Experiment 7 — Per-protocol coverpoint reachability (campaign as protocol-filtered fuzz)

**Goal:** for each protocol, measure which semantic coverpoints are reachable with the current seeds + generic mutations.

Even though only DCQCN has targeted mutations/calibration today, `campaign --protocol <proto>` still:
- selects seeds tagged for that protocol,
- applies generic mutations,
- runs Days + checkers + coverage,
- records semantic coverpoints in metadata.

Commands:

```bash
for proto in aqm pfc wfq drr cubic; do
  rm -rf "leanguard_corpus_${proto}"
  $TG --corpus-root "leanguard_corpus_${proto}" --leanguard-run "$LEANGUARD_RUN" seed-index configs
  $TG --corpus-root "leanguard_corpus_${proto}" --leanguard-run "$LEANGUARD_RUN" \
    campaign --protocol "$proto" --budget 200 --rng-seed 1
done
```

Metrics:
- unique coverpoints per protocol corpus (union over accepted cases)
- coverage growth vs budget
- acceptance/reject/error rates

Interpretation:
- This tells you which coverpoints are “easy” vs “hard” to hit with today’s config surface.
- Hard-to-hit coverpoints are candidates for future targeted mutators/calibrators (Phase 1+ work), but the reachability data itself is an experiment you can run now.


---

### Experiment 8 — Cost of enabling coverage (overhead)

**Goal:** quantify runtime overhead of collecting semantic coverpoints (writing coverage sidecars + parsing them) vs disabling coverage.

Method:
- Run identical `fuzz` budgets with and without `--coverage` (`--no-coverage`), and compare total wall-clock time.

Example:

```bash
/usr/bin/time -l $TG --corpus-root leanguard_corpus_timing_cov --leanguard-run "$LEANGUARD_RUN" \
  fuzz --budget 200 --rng-seed 1

/usr/bin/time -l $TG --no-coverage --corpus-root leanguard_corpus_timing_nocov --leanguard-run "$LEANGUARD_RUN" \
  fuzz --budget 200 --rng-seed 1
```

Notes:
- This includes Days simulation time, checker time, and coverage file I/O. If you want just checker overhead, run `leanguard-run` directly on an existing log directory via `--mode check-only`.

