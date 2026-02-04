# Investigation: Follow-up experiments — semantic coverpoints + coverage-guided corpus + shrinking (LeanGuard vs TLA+/TLC)

## Summary

LeanGuard already *implements* semantic coverpoints across protocols (in the Lean event logs) and can emit them as JSON,
and the Rust `leanguard-testgen` tool already has corpus/coverage bookkeeping.

The main blocker to running the proposed “coverpoints + coverage-guided generation + shrinking” experiments was
*plumbing*:

1. `leanguard-run` could not parse the Lean coverage JSON shape.
2. `leanguard-testgen` did not request coverage.

Additionally, shrinking/minimization existed only for **failing configs** (and only at the TOML level), so
“CI-friendly corpus” (shrink **accepted** cases while preserving coverage) required a follow-up extension.

## Symptoms

- Semantic coverpoints exist in Lean, but they were not usable end-to-end by the generator.
- `leanguard-testgen` could not actually run in “semantic coverage guided” mode because it didn’t enable coverage.
- Existing minimization/shrinking is **failing-only**, and does **not** shrink accepted cases while preserving coverage
  (which is what “CI-friendly corpus” needs).

## Investigation Log

### 2026-02-01 / Phase 2 (context_builder) — Testgen + coverage plumbing

**Hypothesis:** Semantic coverpoints already exist; the generator is missing wiring.

**Findings:** Confirmed.

- Lean event logs already call `covHit` for protocol-specific coverpoints.
- Checkers accept `--coverage-out` and write a coverage report.
- Rust testgen already records/compares coverage, but couldn’t get correct points at the time.

**Evidence:**

- Lean coverage JSON format is an **object** (not `Vec<String>`):
  - `lean/LeanGuard/Shared/Coverage.lean` (`CoverageReport.toJson`)
- DCQCN coverpoints are implemented and named like the paper examples:
  - `lean/LeanGuard/DcqcnEventLog.lean` (`recordCover` hits e.g. `cnp_apply`, `cnp_ignored_due_to_interval`,
    `timer_with_cnp_seen`, `timer_without_cnp_seen`, `alpha_{below,above}_0p1`, `rate_clamped_{min,max}`,
    `alpha_updated_nontrivial`, ...)
- Testgen prefers semantic coverpoints when present, else falls back:
  - `src/utils/testgen.rs` (`build_coverage_info`, `goals_satisfied`)

### 2026-02-01 / Phase 3 (deep dive) — Coverage format mismatch

**Hypothesis:** `leanguard-run` can’t ingest Lean’s coverage JSON correctly.

**Findings:** Confirmed.

At the time:

- `leanguard-run` only parsed `Vec<String>` or line-split.
- Lean emits a single-line JSON **object**, so it was interpreted as one giant bogus “coverage point”.

**Evidence:**

- Lean JSON object schema:
  - `lean/LeanGuard/Shared/Coverage.lean` (`CoverageReport.toJson` includes fields like `checker`, `accept`,
    `cover:[...]`, `stats:{rows, processed_rows}`, optional `error`)
- `leanguard-run` parsing (pre-fix):
  - `src/bin/leanguard-run.rs` `read_coverage_points` parsed `Vec<String>` else `content.lines()`

## Phase 0: required to unlock follow-up experiments

Phase 0 was defined as:

1. Parse the Lean coverage report JSON correctly in `leanguard-run`.
2. Make `leanguard-testgen` actually request coverage (`--coverage`) so coverpoints flow into:
   - `leanguard-run` JSON summaries (`coverage.union` / `coverage.per_checker`)
   - corpus metadata (`leanguard_corpus/metadata/*.json`)
   - global novelty tracking (`global_coverage.json`)

A dedicated regression test is important because the failure mode is silent (coverage “works” but is garbage).

## Follow-up experiment ideas (post-Phase-0)

Once semantic coverpoints flow end-to-end, the experiments become practical:

1. **Coverage growth curves**
   - Run `leanguard-testgen fuzz` for a fixed budget.
   - Track |coverage| over accepted cases.
   - Compare:
     - `checker_coverpoints` vs `trace_signature` fallback

2. **Corpus quality / CI-friendly corpus**
   - Define a target coverage set (union over a run).
   - Try to minimize the *set of configs* while preserving that coverage.
   - This requires an “accepted-case minimizer” (distinct from failing-only shrinker).

3. **Goal-directed campaigns**
   - Use `leanguard-testgen campaign --protocol dcqcn --goal ...` (goal coverpoints)
   - Measure success rate/time-to-hit goal points.

4. **TLC-in-the-loop baseline (oracle cost comparison, not coverage-guided)**
   - TLC is viable as an ACCEPT/REJECT oracle for trace validation.
   - But TLC is not a practical inner-loop *coverage sensor* (too expensive, no semantic coverpoints).

## Known limitation: shrinking

Current `minimize` assumes a failing case:

- `"Case is already accepted; minimize expects a failing case."`

For “CI-friendly corpus”, a different workflow is needed:

- Shrink accepted configs while preserving a desired coverage set, and/or while keeping acceptance.
