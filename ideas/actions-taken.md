# Actions Taken

- 2026-01-22: Section 0.1 updated `leanguard-run` handling in testgen (accept exit code 1, treat invalid JSON/no output as errors, propagate stdout/stderr/exit code) and aligned fuzz/replay/minimize parsing; edited `src/utils/testgen.rs`.
- 2026-01-22: Section 0.2 added protocol tag detection (flow types, switch discipline/drop, link mode) and value-based required feature detection for seed indexing; edited `src/utils/testgen.rs`.
- 2026-01-22: Section 1.1 added coverage plumbing in `leanguard-run` (CLI flags, per-checker coverage output, summary union/per-checker coverage); edited `src/bin/leanguard-run.rs`.
- 2026-01-22: Section 1.2 consumed coverage in testgen metadata, added global coverage tracking, and trace-signature fallback; edited `src/utils/testgen.rs`.
- 2026-01-22: Marked Section 0 and 1 tasks complete in `ideas/phase-4-plan.md`.
- 2026-01-22: Cleaned up unused coverage parsing fields/functions and trimmed RunOutput to stdout only; re-ran tests; edited `src/utils/testgen.rs`.
- 2026-01-22: Section 2.1 added `campaign` subcommand with protocol/budget/goal/calibration/seed-filter/dry-run/trace-signature flags; edited `src/bin/leanguard-testgen.rs`.
- 2026-01-22: Section 2.2 implemented campaign dispatcher, protocol parsing, seed filtering, and campaign metadata wiring with trace-signature option; edited `src/utils/testgen.rs`.
- 2026-01-22: Marked Section 2 tasks complete in `ideas/phase-4-plan.md`.
- 2026-01-22: Fixed `CampaignPlannedCase` visibility warning by making `Mutation` public and ran `leanguard-testgen seed-index configs` plus a dry-run campaign for WFQ; edited `src/utils/testgen.rs`.
- 2026-01-22: Ran `leanguard-testgen campaign --protocol wfq --budget 1` (real run) and recorded one accepted case under `leanguard_corpus/accepted/`; updated no source files.
- 2026-01-22: Ran `leanguard-testgen campaign --protocol wfq --budget 5` (real run) and recorded five accepted cases under `leanguard_corpus/accepted/`; updated no source files.
