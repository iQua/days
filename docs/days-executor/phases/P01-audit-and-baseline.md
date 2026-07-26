# P01: Audit and baseline

## Purpose

P01 begins the Days Executor program with a machine-enforced audit and evidence
contract. Task T0 adds the Rust `xtask` audit, versioned TOML schemas,
deterministic reproduction, mutation tests, checked-in evidence manifests, and
CI enforcement. The contract makes later phase claims reviewable and
reproducible before an executor implementation exists.

The phase metadata is additive. This change declares only T0; a later change
will append T1 to the same `P01-audit-and-baseline.toml` file.

## T0 audit and evidence contract

`cargo xtask phase-audit P01` validates the complete phase metadata set,
dependency closure and cycles, red-test declarations, source purity, generated
trees, budgets, evidence checksums and archive immutability, backend
selectability, test-matrix coverage, dependency drift, resolved licenses,
proof-evidence declarations, and the design note. It collects all faults and
emits stable diagnostic lines.

`cargo xtask reproduce --phase P01` verifies versioned evidence and executes
the declared commands for the host platform in declaration order without a
shell. The contract and all schema fields are specified in
`docs/days-executor/audit-contract.md`.

T0's red tests are the programmatically generated synthetic repositories in
`xtask/tests/phase_audit_mutations.rs`. They mutate one contract property at a
time and assert the exact diagnostic code. The corpus covers malformed and
misidentified metadata, dependency closure and cycles, foreign source and
toolchain rejection, generated-tree cleanliness, budget immutability, evidence
integrity, backend and matrix gates, dependency drift, proof tags, design
notes, red-test declarations, and reproduce outcomes. The real P01 metadata
also audits itself as a positive case.

## Deterministic commands

P01 declares the same two deterministic commands for Linux x86-64 and macOS
Arm64:

```text
cargo test --workspace
cargo test --features test
```

The first command exercises the complete workspace, including `xtask`. The
second preserves and exercises the existing Days `test` feature command. The
executor audit CI additionally runs formatting, clippy, the phase audit, and
phase reproduction on both operating systems.

## Scope

T0 does not perform the frozen baseline characterization assigned to T1. It
does not create `executor/`, invoke the Lean toolchain, or implement or select
GPU backends. No scheduler, backend, migration, or performance result is
claimed by this task.

The trust boundary is unchanged. T0 adds declaration checks for future
proof-changing phases but neither changes proofs nor expands trusted runtime
components.

There is no semantic, public API, public configuration, CLI, or default
behavior change to Days. The existing simulator remains the root package, and
the `xtask` package is development tooling with no dependency edge to or from
the Days package.

## Evidence

T0 checks in a golden diagnostic registry and a golden mutation-corpus map.
Their content hashes are recorded in
`docs/days-executor/evidence/P01/t0-evidence.toml`. T0 makes no performance
claim and declares no budget manifest.
