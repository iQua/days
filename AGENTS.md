# Description

In this project, a new discrete-event network simulator, called Days, has been implemented. It uses process-based simulation, and models each process to be simulated as a coroutine in Rust.

# Repository Guidelines

## Project Structure & Module Organization
- `src/` contains shared/current lowering, topology, utility, and harness code; `executor/` contains the current engine; `legacy/` contains the Nexosim process models, schedulers, switches, and optional layer-2 protocols.
- `lean/` contains Lean code that checks several protocols, including DCQCN and PFC, for conformance to their protocol specifications.
- `legacy/src/main.rs` is the legacy CLI entry point; `src/lib.rs` exposes shared/current APIs.
- `tests/`, `legacy/tests/`, and `validation/tests/` hold current, legacy, and cross-engine integration tests respectively.
- `configs/` stores example simulation configs; `legacy/examples/` and the documentation show runnable scenarios.
- `docs/` contains the MkDocs site (`docs/mkdocs.yml`, content under `docs/docs/`, e.g. `docs/docs/design-notes/l2.md`).
- `logs/` and `target/` are generated artifacts and should stay uncommitted.

## Build, Test, and Development Commands
- `cargo build -p days-legacy` builds the legacy simulator.
- `cargo build -p days-legacy --features l2,l2_pfc` enables legacy L2/PFC support (still controlled by config at runtime).
- `cargo run --release -p days-legacy --bin days -- configs/simple.toml` runs a sample simulation from the repo.
- `RUST_LOG=debug days configs/simple.toml` runs the installed binary with verbose logging.
- `cargo fmt --all` formats Rust code. Run Clippy with warnings denied for executor default (`cargo clippy -p days-executor -- -D warnings`), executor Metal (`cargo clippy -p days-executor --features metal-spike -- -D warnings`), the LeanGuard runner (`cargo clippy --bin leanguard-run -- -D warnings`), and the shared/current Metal surface (`cargo clippy --features test,metal-spike -- -D warnings`).
- The four-package default matrix in `README.md` runs executor, shared/current, legacy, and validation tests.
- `cargo nextest run --workspace --all-features --no-capture` is the preferred faster test runner if installed.

## Coding Style & Naming Conventions
- Follow `rustfmt` (style edition 2024 per `rustfmt.toml`); use 4-space indentation and no tabs.
- Rust naming: `snake_case` for files/modules/functions, `PascalCase` for types, `SCREAMING_SNAKE_CASE` for constants.
- Keep config examples in TOML under `configs/` or `tests/*.toml` when they back a test.

## Testing Guidelines
- New behavior should include a `tests/*.rs` integration test and any required TOML fixtures.
- Name tests after the feature they validate (e.g., `tests/wfq.rs`, `tests/drop_red.rs`).
- Run the four-package default matrix in `README.md` before submitting changes.

## Commit & Pull Request Guidelines
- Recent commits use short, imperative, capitalized subjects, often with backticks for commands and optional PR refs like `(#77)`.
- Keep commit subjects focused; include related issue/PR references when applicable.
- PRs should describe the change, list test commands run, and note any config files used to reproduce results or output changes.

## Configuration & Logging Tips
- Simulation behavior is driven by TOML configs; prefer adding new examples under `configs/` and referencing them in docs/tests.
- Use `log_path` in configs to keep output contained, and avoid committing large logs.
