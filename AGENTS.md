# Description

Days is an exact, parallel discrete-event network simulator. Its executor,
Days AGO, compiles a TOML scenario into a simulation image, splits the network
into logical processes, and advances them concurrently in rounds bounded by a
safe horizon; Lean proofs (`lean/DaysExecutor/`) show round execution reaches
the serial result. Four backends (Scalar, CPU,
Metal, CUDA) must produce byte-identical complete state for every scenario
they accept. The original actor-model simulator (Nexosim coroutines) is frozen
in `legacy/`.

Standing invariants for any change:
- Byte identity with the Scalar oracle on every backend that runs a model;
  a backend that cannot run a model rejects it at validation, never falls back
  silently.
- No shared mutable state between logical processes: communication is by
  events (messages); no mutexes in simulation paths.
- Evidence and measurement records live in the separate days-gpu repository.

# Repository Guidelines

## Project Structure & Module Organization
- `src/` contains shared/current lowering, topology, utility, and harness code; `executor/` contains the current engine; `legacy/` contains the Nexosim process models, schedulers, switches, and optional layer-2 protocols.
- `lean/` contains Lean code that checks several protocols, including DCQCN and PFC, for conformance to their protocol specifications.
- `legacy/src/main.rs` is the legacy CLI entry point; `src/lib.rs` exposes shared/current APIs.
- `tests/`, `legacy/tests/`, and `validation/tests/` hold current, legacy, and cross-engine integration tests respectively.
- `configs/` stores example simulation configs; `legacy/examples/` and the documentation show runnable scenarios.
- `docs/` contains the Fumadocs site (Vite + bun; content under `docs/content/docs/`).
- `logs/` and `target/` are generated artifacts and should stay uncommitted.

## Build, Test, and Development Commands
- `cargo run --release --example scalar_benchmark -- <config.toml>` runs the Scalar oracle; `--example round_benchmark -- <config.toml> --workers N` runs the CPU executor; `cargo run --release --features metal-spike --bin t20f_frontier -- <config.toml> --engine device` runs on Metal (`--features cuda` on NVIDIA). See `README.md`.
- `cargo run --release -p days-legacy --bin days-legacy -- configs/simple.toml` runs the frozen legacy simulator (`--features l2,l2_pfc` for its L2/PFC support).
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
