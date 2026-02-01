 Implemented Phase 0 end-to-end semantic coverage plumbing in ~/Playground/days:

 1) `leanguard-run` now parses Lean coverage JSON correctly

 File: src/bin/leanguard-run.rs

 - read_coverage_points() now tries (in order):
     1. Lean CoverageReport JSON object: { "cover": ["cp1", ...], ... }
     2. Legacy JSON array: ["cp1", "cp2"]
     3. Fallback newline-separated points, but avoids treating {...} / [...] blobs as a single “point”
 - Coverage points are sorted + deduped.

 2) leanguard-testgen now actually enables coverage (by default)

 Files: src/utils/testgen.rs, src/bin/leanguard-testgen.rs

 - Added coverage: bool to TestGenOptions.
 - run_leanguard(...) now takes a coverage: bool and passes --coverage to leanguard-run when enabled.
 - All call sites now thread opts.coverage.
 - leanguard-testgen CLI now has:
     - --no-coverage (disables passing --coverage to leanguard-run)
     - default behavior is coverage enabled (coverage: !cli.no_coverage)

 3) Regression test added (prevents coverage JSON format regressions)

 File: tests/leanguard_run.rs

 - New integration test: leanguard_run_parses_lean_coverage_report_json
     - Creates a stub checker that writes a Lean-style coverage JSON object to the --coverage-out path.
     - Runs leanguard-run --coverage and asserts coverpoints appear in:
           - coverage.union
           - coverage.per_checker["dcqcn_check"]
           - checker_results[0].coverage

 Also updated the existing testgen tests to include the new coverage field:
 - tests/testgen_seed_index.rs
 - tests/testgen_campaign_dcqcn.rs

 Verification

 Ran successfully:
 - cargo fmt --all
 - cargo test --features test -- --show-output

 How to use

 - Coverage-enabled by default:
     - leanguard-testgen seed-index configs/
     - leanguard-testgen fuzz --budget 5
 - Disable if needed:
     - leanguard-testgen --no-coverage fuzz --budget 5

 Coverage should now flow Lean checker → coverage JSON file → leanguard-run summary JSON → testgen metadata/global
 coverage tracking, enabling the follow-up coverage-guided experiments.
