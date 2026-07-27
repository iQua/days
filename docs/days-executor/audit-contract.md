# Days Executor audit and evidence contract

This document defines the audit and evidence contract for the Days Executor
program. The contract is enforced by the Rust `xtask` package without changing
the Days simulator, its configuration, its command-line interface, or its
default behavior.

## Commands

Run the audit for one phase:

```text
cargo xtask phase-audit P01
cargo xtask phase-audit P01 --repo-root /path/to/days
cargo xtask phase-audit P01 --allow-network
```

Reproduce the test commands declared for the host platform:

```text
cargo xtask reproduce --phase P01
cargo xtask reproduce --phase P01 --repo-root /path/to/days
```

Discover every phase metadata file, then audit and reproduce every phase:

```text
cargo xtask all-phases
cargo xtask all-phases --repo-root /path/to/days
cargo xtask all-phases --allow-network
```

`all-phases` fails with `DAYS-AUDIT-0001` when the phase directory is missing
or contains no phase metadata; an empty discovery set never succeeds.

The phase CI runs:

```text
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo xtask all-phases
```

P01 reproduction declares these test commands for each supported platform:

```text
cargo test --workspace
cargo test --features test
```

Print the direct-dependency baseline derived from the workspace manifests, or
rewrite the checked-in baseline:

```text
cargo xtask dependency-baseline
cargo xtask dependency-baseline --write
```

The `.cargo/config.toml` alias expands `cargo xtask` to
`cargo run --package xtask --`. It intentionally uses the debug profile:
auditing is I/O bound, and avoiding a release build keeps local and CI bootstrap
times short.

Unless `--repo-root` is supplied, the command walks upward from the current
directory to find the workspace root `Cargo.toml`. Repository-relative paths in
metadata are resolved from that root.

`phase-audit` loads the complete metadata set, runs every applicable check,
sorts its diagnostics, and reports every fault instead of stopping after the
first. It exits 0 only when there are no error diagnostics. Each output line has
the stable form:

```text
<CODE> <slug> <subject>: <message>
```

For example:

```text
DAYS-AUDIT-0008 forbidden-source executor/kernel.cu: hand-authored `.cu` source in an executor-owned path
```

Information diagnostics use the same format and never affect the exit status.

## Diagnostic registry

| Code | Slug | Severity | Summary |
| --- | --- | --- | --- |
| `DAYS-AUDIT-0001` | `phase-metadata-missing` | error | No `docs/days-executor/phases/<PHASE>-*.toml`, or more than one match. |
| `DAYS-AUDIT-0002` | `phase-metadata-malformed` | error | Metadata is not valid TOML, has an unknown field, a missing required field, or a malformed value. |
| `DAYS-AUDIT-0003` | `phase-metadata-schema-version` | error | Unsupported `schema_version` in any versioned schema. |
| `DAYS-AUDIT-0004` | `phase-metadata-identity` | error | `phase` or `name` disagrees with the file stem, or a duplicate phase or task ID exists. |
| `DAYS-AUDIT-0005` | `unrecorded-dependency` | error | A dependency is absent, has an unknown external gate, or lacks phase closure. |
| `DAYS-AUDIT-0006` | `dependency-cycle` | error | The phase/task dependency graph contains a cycle. |
| `DAYS-AUDIT-0007` | `red-test-missing` | error | A task declares no red test, its path does not exist, or the file contains no declared function. |
| `DAYS-AUDIT-0008` | `forbidden-source` | error | Hand-authored or checked-in forbidden source or interpreter exists in the audited tree. |
| `DAYS-AUDIT-0009` | `forbidden-toolchain` | error | An executor-owned workflow or declared command invokes a forbidden tool. |
| `DAYS-AUDIT-0010` | `generated-tree-dirty` | error | A generated or inspection directory is not ignored, has tracked files, or is dirty. |
| `DAYS-AUDIT-0011` | `budget-hash-mismatch` | error | A declared budget manifest is missing, or its phase-metadata hash does not match the file. |
| `DAYS-AUDIT-0012` | `post-measurement-budget-change` | error | An evidence record's `budget_hash` does not match the current budget hash. |
| `DAYS-AUDIT-0013` | `evidence-manifest-invalid` | error | An evidence manifest is missing, unparseable, or schema-invalid. |
| `DAYS-AUDIT-0014` | `evidence-checksum-mismatch` | error | A checked-in golden artifact is missing or its content hash does not match. |
| `DAYS-AUDIT-0015` | `evidence-link-not-immutable` | error | A `days-gpu` archive artifact omits provenance, is absent from its recorded commit, or has a mismatched hash. |
| `DAYS-AUDIT-0016` | `incomplete-backend-selectable` | error | A backend marked incomplete is selectable or lacks a feature gate. |
| `DAYS-AUDIT-0017` | `matrix-command-missing` | error | A declared feature or platform has no corresponding declared test command. |
| `DAYS-AUDIT-0018` | `design-note-missing` | error | The declared design note does not exist. |
| `DAYS-AUDIT-0019` | `dependency-baseline-drift` | error | The workspace's direct dependencies differ from `dependency-baseline.toml`. |
| `DAYS-AUDIT-0020` | `license-not-allowed` | error | A package in the resolved dependency set has a license outside `allowed_licenses`. |
| `DAYS-AUDIT-0021` | `proof-evidence-missing` | error | `proof_changing = true` but a required proof-evidence tag is absent. |
| `DAYS-AUDIT-0022` | `optional-check-skipped` | info | An optional check that needs the network or an absent tool was skipped. |
| `DAYS-AUDIT-0023` | `reproduce-command-failed` | error | A declared reproduce test command exited non-zero. |
| `DAYS-AUDIT-0024` | `reproduce-no-host-command` | error | No declared reproduce test command matches the host platform. |
| `DAYS-AUDIT-0025` | `audit-internal-error` | error | An audit check attempted to emit an unknown diagnostic code. |
| `DAYS-AUDIT-0026` | `budget-freeze-invalid` | error | A budget freeze is missing or unverifiable, has different content, lacks a linear first-parent path to the run, or cites a measurement artifact path without an adding commit after the freeze. |
| `DAYS-AUDIT-0027` | `measurement-evidence-invalid` | error | Measurement evidence omits its required binding, a declared budget has no citing measurement, or a required consumer does not reuse the exact frozen budget identity. |
| `DAYS-AUDIT-0028` | `archive-check-skipped` | info | Archive verification was skipped because the `days-gpu` repository is unavailable. |

The machine-readable registry golden is
`docs/days-executor/evidence/P01/diagnostics.toml`.

## Common validation rules

All versioned manifests are TOML. Each document kind accepts exactly the
version in this table:

| Document kind | Supported `schema_version` |
| --- | --- |
| Phase metadata | 1 |
| Evidence manifest | 1 |
| Dependency baseline | 1 |
| Budget manifest | 4 |

Version 0 and every version not listed for that document kind are rejected
separately from malformed TOML. Schema records deny unknown fields, so a
misspelled key cannot be silently ignored.

Every hash field is a string with the exact form
`sha256:<64 lowercase hexadecimal characters>`. Hashes are computed over the
file bytes without text normalization.

Phase identifiers match `P[0-9]{2}`. Task identifiers match
`T[0-9]+[A-Z]*`. Repository paths are relative to the workspace root.
Declared paths are canonicalized after resolution; a symlink that escapes the
repository is rejected.

## Phase metadata schema

Phase metadata lives at
`docs/days-executor/phases/<PHASE>-<name>.toml`, beside the design note with the
same stem. Another `[[tasks]]` block can be appended without changing existing
records.

### Phase fields

| Field | Type | Presence | Rule |
| --- | --- | --- | --- |
| `schema_version` | integer | required | Must equal 1. |
| `phase` | string | required | Must match `P[0-9]{2}` and the filename prefix. |
| `name` | string | required | Must equal the filename stem after `<PHASE>-`. |
| `title` | string | required | Human-readable phase title. |
| `design_note` | string path | required | Must name an existing repository file. |
| `depends_on` | array of strings | required | Phase identifiers present in the metadata set. |
| `external_depends_on` | array of strings | required | External gate IDs from the curated set; T0 recognizes `G23_RELEASE`. |
| `features` | array of strings | required | Cargo features introduced or exercised by the phase. |
| `platforms` | array of strings | required | Platform identifiers covered by declared commands. |
| `semantic_changes` | array of strings | required | Empty means no semantic change. |
| `api_changes` | array of strings | required | Empty means no public API or configuration change. |
| `trust_boundary_change` | boolean | required | Whether the phase changes the trust boundary. |
| `proof_changing` | boolean | required | Enables the proof-evidence tag gate when true. |
| `out_of_scope` | array of strings | required | Explicit exclusions for the phase. |
| `selectable_backends` | array of strings | required | Backends selectable at runtime after the phase. |
| `tasks` | array of task tables | required | At least the tasks delivered by this change. |
| `test_commands` | array of test-command tables | required | Deterministic commands for the declared matrix. |
| `backends` | array of backend tables | optional | Absence is equivalent to an empty array. |
| `budgets` | array of budget-reference tables | optional | Absence is equivalent to an empty array. |

### `tasks` fields

| Field | Type | Presence | Rule |
| --- | --- | --- | --- |
| `id` | string | required | Must match `T[0-9]+[A-Z]*` and be unique within the phase. |
| `title` | string | required | Human-readable task title. |
| `depends_on` | array of strings | required | Bare task IDs resolve in the same phase; cross-phase tasks use `PXX/TN`. |
| `evidence` | array of string paths | required | Evidence manifests attributed to the task. |
| `red_test` | table | optional in TOML, required by audit | Omitting it produces `DAYS-AUDIT-0007`. |

The `red_test` table requires string fields `path` and `name`. `path` must
exist, and its text must contain `fn <name>`.

### `test_commands` fields

| Field | Type | Presence | Rule |
| --- | --- | --- | --- |
| `platform` | string | required | A platform ID or `any`. |
| `features` | array of strings | required | Features exercised by this command. |
| `argv` | array of strings | required | Executable followed by arguments; never a shell string. |
| `deterministic` | boolean | required | Must be true. |

When a phase declares specific platforms, every platform must occur explicitly
in at least one command; `any` does not satisfy a specifically declared
platform. An `any` command remains valid for a phase that declares no specific
platform. Every declared feature must occur in at least one command's
`features`.

### `backends` fields

| Field | Type | Presence | Rule |
| --- | --- | --- | --- |
| `name` | string | required | Backend identifier. |
| `complete` | boolean | required | Whether the backend implementation is complete. |
| `selectable` | boolean | required | Whether it can be selected at runtime. |
| `feature` | string | optional | Required and non-empty for an incomplete backend. |

An incomplete backend must not be selectable or appear in
`selectable_backends`. Every selectable backend must be declared and complete.

### `budgets` fields

| Field | Type | Presence | Rule |
| --- | --- | --- | --- |
| `id` | string | required | Stable identifier matching the budget manifest's inner `id`. |
| `owner_phase` | phase ID | required | Owning phase matching the budget manifest's inner `phase`. |
| `required_consumers` | array of phase IDs | required | Phases that must reuse this exact frozen identity when their metadata exists. |
| `path` | string path | required | Budget manifest path. |
| `content_hash` | SHA-256 string | required | Must match the current file bytes. |
| `frozen_at_commit` | string | required | Full 40-lowercase-hex Git commit SHA containing the frozen budget bytes. |

The audit verifies that the working-tree budget hash matches `content_hash`,
that the budget path at `frozen_at_commit` exists and hashes to the same value,
and that the inner budget identity agrees with `id` and `owner_phase`. Duplicate
budget IDs within a phase are rejected. If metadata for a named
`required_consumers` phase exists, that phase must cite the identical
`id`/`owner_phase`/`path`/`content_hash`/`frozen_at_commit` tuple.

For every measurement citation, `frozen_at_commit` must be a strict ancestor
of `run_commit`, the range must contain no merge commit, and following first
parents from `run_commit` must reach the freeze. Equality is rejected because
it means the measurement was committed together with the budget. Every
measurement artifact path must have an adding commit within
`(frozen_at_commit, run_commit]`; an identical blob at the freeze is rejected.
The adding-commit check proves pathname appearance, not content origination.
A rename into the declared pathname or a delete followed by a re-add can
satisfy it. Independently, the audit checks the declared blob hash at
`run_commit`.
An uncommitted budget, unknown or unreachable commit, missing path, ambiguous
history, or different budget bytes fails closed.

## Evidence manifest schema

Task evidence manifests conventionally live at
`docs/days-executor/evidence/<PHASE>/`. Their fields are:

| Field | Type | Presence | Rule |
| --- | --- | --- | --- |
| `schema_version` | integer | required | Must equal 1. |
| `id` | string | required | Stable evidence identifier. |
| `phase` | string | required | Phase identifier. |
| `task` | string | required | Task identifier. |
| `kind` | string enum | required | `golden`, `archive`, or `measurement`. |
| `description` | string | required | Human-readable evidence description. |
| `command` | array of strings | required | Producing command as argv. |
| `tool_version` | string | required | Producing tool and version. |
| `schema` | string | required | Evidence payload schema. |
| `tags` | array of strings | required | Machine-readable proof and evidence classifications. |
| `budget` | string path | optional | Budget governing a measurement. |
| `budget_hash` | SHA-256 string | conditionally required | Required exactly when `budget` is present. |
| `run_commit` | string | measurement only, required | Full 40-lowercase-hex Git commit SHA of the measured code. |
| `artifacts` | array of artifact tables | required | One or more checked-in or `days-gpu` artifact records. |

Each `artifacts` table uses these fields:

| Field | Type | Presence | Rule |
| --- | --- | --- | --- |
| `path` | string path | required | A `days` repository path for golden or measurement evidence; a `days-gpu` path under `evidence/<PHASE>/` for archive evidence. |
| `content_hash` | SHA-256 string | required | Must match the checked-in golden, the measurement blob at `run_commit`, or the recorded `days-gpu` blob. |
| `days_gpu_commit` | string | archive only, required | Exact `days-gpu` Git commit SHA containing the artifact. |
| `tool_version` | string | archive only, required | Non-empty producing tool and version. |
| `command` | array of strings | archive only, required | Exact non-empty producing argv. |
| `schema` | string | archive only, required | Non-empty payload schema version. |

A golden artifact's hash must match its checked-in file. A measurement
artifact must be readable from `run_commit` with `git cat-file`, its hash must
match that committed blob, and its pathname must have an adding commit within
the audited post-freeze range. Golden and measurement artifacts must not carry
archive-only provenance. An archive artifact must carry every archive-only
field. URL fields are not part of the version 1 archive contract.

For archive evidence, the audit reads the artifact blob from the recorded
`days-gpu` commit and verifies its hash. The path must exist in that commit;
an artifact that exists only as an uncommitted working-tree file is rejected.
The `days-gpu` checkout is consulted only when a phase actually declares
archive evidence. P01 declares no archive evidence and does not depend on a
`days-gpu` checkout, including in CI. When archive evidence is declared, the
audit uses the repository named by `DAYS_GPU_ROOT`, or the `days-gpu` sibling
of the `days` repository when the environment variable is unset. If that
repository is unavailable or unreachable, the audit emits an explicit
`DAYS-AUDIT-0028` information diagnostic and skips archive verification.
Archive verification is intentionally a local-only gate: plan section 4.4
names one local measurement platform, so this verification runs where those
measurements run rather than requiring the separate repository in general CI.

A present `budget_hash` without `budget` is malformed metadata for golden and
archive evidence. For measurement evidence, any missing member of the required
`budget`, `budget_hash`, and `run_commit` binding is
`DAYS-AUDIT-0027`. When evidence declares a budget, its hash must still match
the budget at audit and reproduce time. Every budget declared by a phase must
be cited by at least one measurement manifest. Every evidence kind, including
`measurement`, must contain at least one artifact.

The budget binding is temporal as well as content-addressed. The audit compares
the current budget bytes with `content_hash`, compares the budget blob at the
phase metadata's `frozen_at_commit` with that same hash, and requires a strict,
merge-free, first-parent history to the measurement's `run_commit`. Equal
commits are rejected as a measurement committed together with its budget.
Every measurement pathname must have an adding commit in that range and must
not contain the same bytes at the freeze.

This guarantee is tamper-evident against published history, not tamper-proof
against an author before publication. A coherent local rewrite can construct a
history indistinguishable from an honest one using repository content alone.
Closing that gap requires an external anchor, such as a published immutable
ref, a signed freeze tag held outside the mutable repository, or a third party
retaining the earlier history. The audit also cannot prove that a measurement
was executed; a reproducible command and reviewed commit sequence establish
provenance without claiming physical execution attestation.

## Budget manifest schema

Budget manifests conventionally live at `docs/days-executor/budgets/`. A budget
must be frozen before it controls admission, performance claims, or default
selection.

| Field | Type | Presence | Rule |
| --- | --- | --- | --- |
| `schema_version` | integer | required | Must equal 4. Version 3 and all unknown versions are rejected. |
| `id` | string | required | Stable budget identifier. |
| `phase` | string | required | Owning phase identifier. |
| `frozen_at` | string | required | ISO-8601 calendar date in `YYYY-MM-DD` form. |
| `description` | string | required | Human-readable purpose and scope. |
| `platform` | table | required | Named hardware, operating system, toolchain, and expected platform probe. |
| `method` | table | required | Frozen build, sampling, timing, observation, ordering, and resampling method. |
| `admission` | table | required | P23 statistic, confidence rule, and non-empty threshold set. |
| `corpus` | array of corpus tables | required | Non-empty hashed workload set with independent purpose and comparison fields. |
| `waiver` | table | required | Public approving role and mandatory pre-cutover review policy. |

The `platform` table requires the existing non-empty `name`, `cpu`, `os_build`,
and `toolchain` strings. It also requires an integer `expected_num_cpus` of at
least 1 and a non-empty `mt_thread_count_source`. The runner records each
sample's effective thread count and rejects an MT sample whose observed value
differs from `expected_num_cpus`.

The `method` table requires non-negative integer `warmups` and integer
`repetitions` of at least 1. It pins a non-empty build profile and Cargo flag
array; primary `sim_execution` and secondary `end_to_end` boundaries; a
positive finite sample wall-time floor; and a positive finite effective
simulation duration. Non-empty rules identify the duration source, require
each sample to record its simulated end time and effective thread count, and
define run and pairing order. The method also pins the resampling algorithm,
PRNG and seed, ST and MT configurations, and the rule that selects the best
exact Nexosim configuration. Its non-empty `resolved_defaults` array names each
implicit input, exact resolved value, and production source. Duplicate default
names are rejected. Schema validation checks the declared floor's shape and
value. The later measurement runner checks observed samples against it.

The `admission` table requires `evaluated_at = "P23"` and non-empty `statistic`
and `confidence_rule` strings. Its non-empty `thresholds` array contains
`name`, `metric`, `metric_kind`, `applies_to`, `timing_boundary`,
`comparison`, `value`, and `unit`. `metric_kind` is `wall-time`,
`throughput`, `bytes`, or `count`. Wall-time and throughput metrics require a
`timing_boundary` of `sim_execution` or `end_to_end`; byte and count metrics
must omit it. `applies_to` is `corpus` or a workload identity present in the
corpus. `comparison` is one of `<`, `<=`, `>`, `>=`, or `==`. P01 records
absolute Nexosim references and evaluates no admission threshold.

Each `corpus` table requires a repository-relative `path`, its canonical
`sha256:<64 lowercase hexadecimal characters>` `content_hash`, non-empty
`workload` and `mode` strings, and a `role` equal to `correctness` or
`performance`. `comparison_boundary` independently equals `exact-ledger`,
`terminal-observation`, or `semantic-migration`; validation imposes no
cross-field rule between role and boundary. Parsing resolves every corpus path
inside the repository, requires a regular file, and verifies its exact bytes
against `content_hash`.

The `waiver` table requires a non-empty `approving_role`. Its `policy` must be
exactly `A waiver must be a reviewed manifest change made before the cutover
decision.` This makes the authority and timing rule part of the frozen,
machine-checked budget rather than post-measurement prose.

The budget manifest's `frozen_at` field remains a descriptive ISO date. The
content and temporal Git binding deliberately lives outside that blob in the
phase metadata's `frozen_at_commit` reference.

## Dependency baseline schema

`docs/days-executor/dependency-baseline.toml` is a versioned offline control
file:

| Field | Type | Presence | Rule |
| --- | --- | --- | --- |
| `schema_version` | integer | required | Must equal 1. |
| `allowed_licenses` | array of strings | required | Exact accepted license expressions for resolved packages. |
| `direct_dependencies` | array of dependency tables | required | Exact workspace direct-dependency set. |

Each `direct_dependencies` table requires `package`, `name`, `version`,
`source`, and `kind` string fields. `package` is the declaring workspace
member, `name` is the dependency key, `version` is the declared version
requirement, `source` identifies the registry, path, or Git source, and `kind`
is `normal`, `dev`, or `build`.

The offline drift check reads `[dependencies]`, `[dev-dependencies]`, and
`[build-dependencies]` directly from each workspace member manifest. It
intentionally ignores target-specific `[target.*]` dependency sections in
version 1. `cargo xtask dependency-baseline --write` is the supported way to
refresh the direct package/version/source records after an intentional
dependency review. The accepted license expressions are a hand-curated
constant in the audit implementation. Baseline generation and `--write` copy
that curated set and never derive or widen it from the currently resolved
packages.

The resolved-set license check runs `cargo metadata --format-version 1
--offline`. A package with a missing or unallowlisted license is an error. If
offline metadata cannot run, the audit emits one `DAYS-AUDIT-0022` information
line. `--allow-network` permits one retry without `--offline`; failure still
degrades loudly. T0 does not implement an advisory scanner, so every audit also
emits an explicit `DAYS-AUDIT-0022` information line stating that no advisory
scan ran. The security check is never silently absent.

## Dependency graph

The audit builds one graph over phase IDs and fully qualified task IDs such as
`P01/T0`. A bare task dependency such as `T0` resolves within the containing
phase; cross-phase task dependencies use `P02/T3`. Every target must exist in
the complete metadata set. When a task in phase X depends on a task in phase Y,
Y must also be reachable through X's phase-level `depends_on` closure. External
dependencies are checked separately against the curated external-gate set.
Cycle detection is iterative, and reported cycle members are sorted for
deterministic output.

## Source purity and CPU-only bootstrap

The audit examines tracked files and untracked, non-ignored files. Extension and
filename matching is case-insensitive.

Forbidden extensions are:

```text
py pyi pyx cu cuh metal msl wgsl cpp cc cxx hpp hh hxx c h cmake sh bash zsh ps1 bat
```

Forbidden exact filenames are:

```text
CMakeLists.txt setup.py conanfile.txt Makefile meson.build requirements.txt environment.yml
```

The `.py`, `.cu`, `.cuh`, `.metal`, `.msl`, `.wgsl`, `.pyx`, `.cpp`, `.cc`,
`.cxx`, and `.hpp` extensions and the exact filename `CMakeLists.txt` are
forbidden repository-wide. The exact, case-sensitive file allowlist contains
one entry and takes precedence over the subtree allowlist below:

| Exact path | Reason |
| --- | --- |
| `utils/count_loc.py` | Pre-existing developer utility outside the Days Executor path. |

Plan section 12.1 scopes source purity to “forbidden hand-authored source in the
Days Executor path,” while section 4.2 says, “Existing Days packaging outside
this executor stays out of this source-purity claim.” The pre-existing
`utils/count_loc.py` developer utility is therefore legitimately allowed by
the plan.

The repository-wide default deny is deliberately stricter than that minimum
scope. It closes the concrete bypasses in which generator or foreign GPU source
could be hidden inside a blanket-allowlisted subtree, such as
`docs/generate.py`, `src/executor_codegen.py`, or `src/kernel.cu`. A short,
explicit, reviewable exception list closes that hole while honoring the plan;
an implicit subtree exemption would not. Another exception is justified only
for necessary pre-existing source outside the executor that the plan explicitly
leaves out of the source-purity claim, and requires the same explicit review
and documentation. Merely residing in an allowlisted subtree is not sufficient.

### Executor-owned paths

Executor ownership takes precedence over every allowlist rule:

- `executor/`
- `xtask/`
- `docs/days-executor/`
- every `.github/workflows/days-executor-*.yml` file

The executor-owned workflow list intentionally excludes the three workflows
that predate this contract.

### Source allowlist

| Path | Reason |
| --- | --- |
| `lean/` | Existing LeanGuard proof sources and fixture runners predate the executor contract. |
| `utils/` | Existing repository maintenance helpers are outside the executor package. |
| `docs/` | Existing documentation-site content and build tooling are outside executor ownership; `docs/days-executor/` overrides this rule. |
| `crates/nexosim/` | The vendored Nexosim fork is existing simulator infrastructure. |
| `src/` | Existing Days runtime and binaries are outside the new executor implementation. |
| `tests/` | Existing simulator integration tests and fixtures are outside executor ownership. |
| `configs/` | Existing simulator configuration examples are outside executor ownership. |
| `examples/` | Existing runnable simulator examples are outside executor ownership. |
| `ideas/` | Existing research notes and prototypes are outside executor ownership. |
| `.github/workflows/` | Existing workflows are grandfathered; the executor-owned workflow overrides this rule. |
| `pyproject.toml` | Existing maturin packaging metadata publishes the current simulator binary. |
| `autoresearch.sh` | Existing repository automation is explicitly outside executor ownership. |

A forbidden file produces `DAYS-AUDIT-0008` when it is under an executor-owned
path or when no allowlist entry covers it. The repository-wide foreign-source
set above overrides these path allowlists. An extensionless file under an
executor-owned path also produces 0008 when its first line is a shebang naming
a forbidden shell or Python interpreter; no broader content classification is
performed.

Every executor-owned workflow, declared test-command argv, and evidence
manifest command argv is token-scanned with the full forbidden-tool set. After
skipping a workflow line whose first non-whitespace character is `#`, the scan
splits on every character other than an ASCII letter, digit, or underscore and
lowercases each token, which gives word-boundary matching. The full tokens are:

```text
sh bash zsh python python2 python3 pip pip3 nvcc metal msl wgsl cuda xcrun cmake ninja make curl conda nvidia nvidia_smi
```

Every other `.github/workflows/*.yml` file is scanned only for the
foreign-toolchain subset:

```text
nvcc metal msl wgsl cuda nvidia cmake ninja xcrun conda
```

This split follows plan section 4.2: existing Days packaging, including
LeanGuard's elan, shell, and curl use and maturin's Python use, is outside the
executor source-purity claim. Consequently `python`, `bash`, `sh`, and `curl`
are not rejected in non-executor workflows; they remain forbidden in
executor-owned workflows and declared executor commands.

The audit implementation itself invokes no shell or foreign toolchain: its only
subprocesses are `git` and the optional `cargo metadata` checks. This
mechanically establishes a CPU-only bootstrap without requiring a Python
interpreter or GPU toolchain.

Transient generated GPU source is permitted only under the ignored
`target/gpu-inspect/` inspection directory. Because ignored `target/` content
is excluded from the untracked source scan and tracked content is rejected by
the generated-tree checks, this exception cannot admit checked-in source.

## Generated and inspection directories

The controlled directories are:

- `target/gpu-inspect/`
- `target/days-executor-generated/`

Each must be ignored by Git, contain no tracked files, and have an empty
path-scoped Git status. The root `.gitignore` records both paths explicitly even
though the broader `target/` rule also covers them.

## Immutable archive evidence

Large evidence artifacts are committed to the separate `days-gpu` repository
under `evidence/<PHASE>/`. The manifest in this repository records the
repository-relative path, SHA-256 content hash, and exact `days-gpu` commit SHA,
plus the producing tool version, argv command, and payload schema version.

Immutability is established by the content hash and recorded Git commit. The
audit reads the path from that commit when the `days-gpu` repository is
available and rejects a missing path, a hash mismatch, or an artifact that has
not been committed. URL-based archives, public-release host allowlists, and
mutable-link heuristics are not part of the contract.

## Artifact policy

Small golden fixtures, manifests, and checksums are checked into the
`days` repository. Measurement manifests likewise checksum a small result
record committed at `run_commit` that binds the budget to the measured run.
Large traces, benchmark tables, profiles, and plots are committed to
`days-gpu` under `evidence/<PHASE>/` and cited by a separate archive manifest.
The checked-in archive manifest records the artifact path, content hash,
`days-gpu` commit SHA, producing tool version, exact argv command, and payload
schema version.

Performance claims require benchmark evidence. Whenever performance gates
admission or default selection, the budget manifest must be versioned and
frozen before measurement. Phase metadata records both the budget content hash
and the commit containing those exact bytes. Every measurement record embeds
the budget path, its exact hash, and the measured code's `run_commit`. The
audit verifies that the freeze commit contains the recorded bytes, reaches the
run commit through a strict merge-free first-parent range, and that every
measurement artifact pathname has an adding commit in that range. Separately,
the blob at `run_commit` must match the declared hash, and identical bytes at
the freeze are rejected. The adding-commit check does not prove where the
content originated.
Equality fails because the measurement would have been committed together with
the budget. The result is tamper-evident against published history. Preventing
a coherent pre-publication rewrite requires an external published or signed
anchor; repository content alone cannot supply one.

Public API or configuration changes require migration notes in phase metadata
and the design note. Incomplete implementation remains feature-gated and may
not become a selectable backend.

## Proof-changing gate

When `proof_changing = true`, the union of `tags` across the phase's evidence
manifests must contain all of:

- `lean-toolchain`
- `lake-build-command`
- `theorem-inventory`
- `axiom-report`
- `schema-roundtrip-vectors`
- `proof-mutation-fixtures`
- `trust-boundary-diff`

The audit emits one `DAYS-AUDIT-0021` fault for each missing tag. It verifies
declarations only and does not invoke Lean.

## Reproduction

`cargo xtask reproduce --phase <PHASE>` reuses metadata validation, checks
budget bindings and checked-in golden or measurement evidence, then maps the
host to `<os>-<arch>` using Rust's `OS` and `ARCH` constants. Architectures use
`x86_64` or `aarch64`.

Commands whose platform equals the host or is `any` run in declaration order
from the repository root with inherited standard I/O. Other platform commands
produce explicit `DAYS-AUDIT-0022` skip lines. Execution uses the argv array
directly and never invokes a shell. The first non-zero command produces
`DAYS-AUDIT-0023` and stops reproduction. No matching host command produces
`DAYS-AUDIT-0024`.

After command execution, reproduction reruns the checked-in evidence checksum,
budget binding, generated-tree cleanliness, and source-purity checks. A command
that exits successfully but corrupts or creates audited artifacts therefore
fails with the same stable diagnostic code as the preflight audit.

## Limitations

- `reproduce --phase` does not verify `days-gpu` archive evidence; archive
  verification is a local-only `phase-audit` gate.
- Direct-dependency drift does not compare dependency feature selections or
  target-specific `[target.*]` sections.
- Security advisory scanning is not implemented and remains an explicit
  information-level skip rather than a scanner.
- Feature-matrix coverage compares declared feature labels only and does not
  verify that command argv contains corresponding feature arguments.
- Source purity does not inspect foreign source embedded in Rust string
  literals or files with non-standard extensions such as
  `executor/kernel.inc`.
- Source purity does not detect a symlink placed at the exact allowlisted path
  `utils/count_loc.py`.
