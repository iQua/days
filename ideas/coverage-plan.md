# coverage-plan.md (reconstructed)

> NOTE: This file was **reconstructed after accidental deletion**. It is a best-effort recovery based on the console snippet
> we still had, the conversation summary, and the current repo state.
> It may not match the original byte-for-byte, but it should capture the same intent and details.

Phase 0 = (1) fix coverage ingestion in `leanguard-run`, and (2) ensure `leanguard-testgen` actually enables coverage so
semantic coverpoints flow into the corpus metadata.

You currently have all the semantic coverpoints implemented in Lean, and all the coverage bookkeeping implemented in
Rust, but there are two plumbing breaks:

- `leanguard-run` reads coverage as `Vec<String>` or line-split, but Lean writes a JSON object with a `cover` field.
- `leanguard-testgen` never passes `--coverage` to `leanguard-run`.

At the time this plan was written, plan mode was active, so the intent was to specify the exact edits required to
complete Phase 0 and add a regression test.

---

## Phase 0.1 — Fix coverage ingestion in `leanguard-run`

### File: `src/bin/leanguard-run.rs`

#### Change A: import `Deserialize`

Currently:

```rust
use serde::Serialize;
```

Change to:

```rust
use serde::{Deserialize, Serialize};
```

#### Change B: parse Lean’s JSON object format in `read_coverage_points`

At the bottom of the file, update coverage parsing to:

1. Try parsing a Lean-style object:

```json
{"checker":"dcqcn_check","accept":true,"cover":["cp_a","cp_b"],"stats":{"rows":123,"processed_rows":123}}
```

2. Fall back to a bare JSON array of strings.
3. Fall back to newline-separated points.
4. **Do not** treat `{...}` / `[...]` blobs as a single “coverage point” in the fallback.

Canonical implementation sketch:

```rust
fn read_coverage_points(path: &Path) -> Option<Vec<String>> {
    let content = fs::read_to_string(path).ok()?;

    #[derive(Debug, Deserialize)]
    struct CoverageReportJson {
        cover: Vec<String>,
    }

    if let Ok(mut report) = serde_json::from_str::<CoverageReportJson>(&content) {
        report.cover.sort();
        report.cover.dedup();
        return Some(report.cover);
    }

    if let Ok(mut points) = serde_json::from_str::<Vec<String>>(&content) {
        points.sort();
        points.dedup();
        return Some(points);
    }

    let trimmed = content.trim_start();
    if trimmed.starts_with('{') || trimmed.starts_with('[') {
        return None;
    }

    let mut points = content
        .lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
        .map(|line| line.to_string())
        .collect::<Vec<_>>();
    if points.is_empty() {
        return None;
    }
    points.sort();
    points.dedup();
    Some(points)
}
```

---

## Phase 0.2 — Ensure `leanguard-testgen` enables coverage

### File: `src/utils/testgen.rs`

#### Change A: add a switch to the options struct

```rust
pub struct TestGenOptions {
    // ...
    pub coverage: bool,
    // ...
}
```

#### Change B: thread coverage into `run_leanguard(...)`

- Add a `coverage: bool` argument to `run_leanguard(...)`.
- When `coverage == true`, include `--coverage` when invoking `leanguard-run`.

```rust
if coverage {
    cmd.arg("--coverage");
}
```

- Update all call sites (fuzz, campaign loop, replay, minimize, etc.) to pass `opts.coverage`.

### File: `src/bin/leanguard-testgen.rs`

#### Change A: add a CLI flag to disable coverage

Default should be **coverage enabled**, with an escape hatch.

Example:

```rust
#[arg(long, default_value_t = false)]
no_coverage: bool,
```

Then:

```rust
coverage: !cli.no_coverage,
```

---

## Phase 0.3 — Add a regression test

### File: `tests/leanguard_run.rs`

Add an integration test that:

- Creates a stub checker script that accepts and writes a Lean-style JSON coverage object to the `--coverage-out` path.
- Runs `leanguard-run --mode check-only --coverage ...`.
- Asserts coverage is correctly surfaced:
  - `coverage.union` contains `cp_a`, `cp_b`.
  - `coverage.per_checker["dcqcn_check"]` contains the same.
  - `checker_results[0].coverage` contains the same.

---

## Phase 0.4 — Update existing testgen tests

Any test constructing `TestGenOptions { ... }` must be updated to initialize the new `coverage` field.

---

## After Phase 0

Once end-to-end coverpoints are flowing:

- Run short fuzz/campaign trials and verify corpus metadata uses:
  - `coverage.mode == "checker_coverpoints"`
  - `coverage.observed` grows over time
- Only then proceed to Phase 1/2 experiments (coverage-guided generation quality, corpus minimization while preserving
  coverage, etc.).
