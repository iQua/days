# P01: Audit and baseline

## Purpose

P01 begins the Days Executor program with a machine-enforced audit and evidence
contract. Task T0 adds the Rust `xtask` audit, versioned TOML schemas,
deterministic reproduction, mutation tests, checked-in evidence manifests, and
CI enforcement. Task T1 freezes the premeasurement Nexosim retirement corpus,
adds migration-only ledgers and terminal digests, and characterizes the legacy
absolute-deadline time policy. Together they make later phase claims reviewable
and reproducible before an executor implementation exists.

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

P01 declares the same three deterministic commands for Linux x86-64 and macOS
Arm64:

```text
cargo test --workspace
cargo test --features test
cargo test --features test,migration_ledger
```

The first command exercises the complete workspace, including `xtask`. The
second preserves and exercises the existing Days `test` feature command. The
third covers the diagnostic ledger without enabling it by default. The executor
audit CI additionally runs formatting, clippy, the phase audit, and phase
reproduction on both operating systems.

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

## T1 frozen Nexosim baseline

T1 adds the off-by-default `migration_ledger` Cargo feature. The feature records
open-loop UDP transitions at source emission, switch forwarding, FIFO egress
enqueue/drop/dequeue/departure, and sink reception. Every time field is an
integer nanosecond. Rows use topology switch IDs, directed port endpoints,
flow IDs, packet IDs, and model-local sequence numbers; process-global
endpoint, switch, and scheduler counters and logger atomics are excluded.
`flush_reports` sorts the rows canonically before writing
`migration_ledger.csv`.

The same feature writes `migration_terminal_digest.csv`. Its canonical rows
summarize each source, switch, directed FIFO port, and sink. Source and sink
rows include packets, bytes, and final time. Port rows include enqueued,
dropped, forwarded, and final packet/byte occupancy. These are stable terminal
observations, not cryptographic hashes; `xtask` owns SHA-256.

The feature does not schedule an event, alter a timestamp, or change a queue
decision. It uses `try_log_report`, so model tests that do not initialize the
logger still run. The only non-instrumentation change sorts the existing
`HashMap` of switches before `SimInit::add_model`. The ordering audit found no
second `HashMap` or `HashSet` traversal in `topo.rs` that reaches model
registration, connection, or event scheduling. The keyed mailbox insertion,
per-key successor normalization, and keyed `PacketSwitch::outputs` construction
remain unchanged.

### Budget schema repair and stage boundary

T0's budget schema could not represent the machine-checkable inputs required by
the plan. T1 upgrades only the budget schema to version 2, adding reusable
platform, method, corpus, threshold, and waiver sections. It validates corpus
paths and hashes against repository bytes and rejects missing platform fields,
zero repetitions, empty corpus or thresholds, malformed or mismatched hashes,
and an empty waiver authority. Phase, evidence, dependency-baseline, and trace
schemas remain at version 1.

`docs/days-executor/budgets/retirement-budget.toml` freezes the developer's
Apple M5 Max, macOS build `26A5388g`, Rust 1.96.0 toolchain, three warmups,
fifteen repetitions, the paired-bootstrap confidence rule, the no-regression
geometric-mean threshold, the 20 percent per-workload limit, and maintainer
waiver authority. It contains no measured value.

This is stage 1. The budget file is parsed and golden-checked but is
intentionally absent from the phase metadata's `[[budgets]]` array. No
`frozen_at_commit`, `run_commit`, or `days_gpu_commit` exists yet. Stage 2 adds
those bindings after the stage-1 commit exists, then reruns measurements from
that descendant commit. This preserves T0's fail-closed ancestry checks without
inventing self-referential provenance.

### Corpus and comparison boundaries

The three exact-ledger fixtures under `configs/migration/` use single-threaded
Nexosim, FIFO/TailDrop, one-nanosecond quantization, fixed seed 1000, constant
100-microsecond arrivals, constant 1000-byte packets, 1 Gbit/s ports, and an
explicit two-millisecond simulation duration:

- `explicit.toml`: a three-node explicit topology with two flows;
- `torus.toml`: a bounded 3-by-3 torus with eighteen flows;
- `fattree.toml`: a bounded `k = 4` Fat-Tree with eight flows.

Equal-endpoint `Uniform` declarations are the configuration API's constant
form; `DistPacketSource` takes its existing no-sample fast path. Torus and
Fat-Tree endpoint selection consumes the fixed seed once during construction.
No RED decision appears in the exact corpus.

`configs/tcp_simple.toml` and `configs/ci/leanguard_dcqcn.toml` are explicitly
`terminal-observation`. Their full transition ordering is outside v1 lowering,
but their source/port/sink terminal state exercises the digest mechanism. The
complete paths, content hashes, feature sets, commands, and boundaries are in
`docs/days-executor/evidence/P01/retirement-corpus.toml`.

### Legacy `quantize_after` boundary

The characterized operation remains:

```text
next = quantize_time(base + 8 * packet_bytes / rate_bits_per_second)
```

It is an absolute deadline, not an independently rounded duration. The concrete
aligned exact domain frozen for the three exact fixtures is:

```text
quantum = 1 ns
packet size = 1000 bytes
rate = 1,000,000,000 bit/s
raw serialization = 8000 ns
base = every integer nanosecond in 0..=2,000,000 ns
```

The characterization test exhaustively checks all 2,000,001 base values. Each
legacy departure equals `base + 8000 ns`, so these fixtures may use
`exact-ledger`.

Everything explicitly labeled `semantic-migration` falls outside that claim.
The table includes 11-byte packets at 16 Gbit/s (5.5 ns), sub-nanosecond
serialization that rounds to zero with a one-nanosecond quantum, and
`10^18`-nanosecond bases where the `f64` mantissa loses a one-nanosecond
increment. A rounded-zero serialization is recorded as zero lookahead and is
not admitted silently. The full exact edges, base residues, enabled/disabled
quantum cases, and large-time values are frozen in
`quantize-after-characterization.toml` and explained in its companion Markdown
note.

For the default disabled quantum, repeated 5.5-nanosecond serialization reaches
110 ns after 20 services and 550 ns after 100. Independently rounding each
duration to 6 ns reaches 120 ns and 600 ns. The divergences are therefore 10 ns
and 50 ns. With a one-nanosecond quantum, both policies happen to reach 120 ns
and 600 ns for this base-aligned case; the residue-class table demonstrates why
that coincidence is not a general duration-rounding equivalence.

### Red tests and evidence

The T1 red tests are:

- `supported_nexosim_st_model_exposes_complete_ledger_and_terminal_digest`;
- `retirement_budget_edited_after_measurement_is_rejected`;
- `repeated_fixture_runs_have_identical_ledger_output`.

An additional unit fixture creates two FIFO ports in one process after the
process-global scheduler counter advances and compares byte-identical canonical
ledger serialization. This directly proves that migration rows do not inherit
the global mutable ID leak. A topology unit fixture independently guards the
switch-registration helper by serializing two differently inserted switch maps
in one process and requiring the same sorted registration ledger. The
`migration_ledger_preserves_default_aggregate_output` integration test compares
feature-on source and sink reports with feature-off checked-in goldens and
requires both feature-on and feature-off `switches.csv` files to remain empty
for this fixture.

Small characterization tables, a ledger sample, a complete small terminal
digest, feature-off aggregate goldens, corpus inventory, and budget live under
`docs/days-executor/evidence/P01/` and are checksummed by
`t1-evidence.toml`. Stage-1 raw ledgers and digests are present in
`days-gpu/evidence/P01/` with content hashes but no commit citation. Stage 2
will add the formal archive and measurement manifests after both repositories
contain immutable commits.

### Migration and exclusions

`migration_ledger` is a diagnostic build feature only. It is not a selectable
runtime backend and is absent from default features. Removing the feature
restores the prior output-file set and instrumentation-free model layouts.

T1 deliberately does not reset or replace process-global IDs, introduce
scenario tuple IDs, sort nodes/edges/flows/routes, add `executor/`, change time
arithmetic, change scheduler behavior, or edit `crates/nexosim`. Those items
belong to later tasks.
