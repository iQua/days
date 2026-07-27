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
the plan. T1 upgrades only the budget schema to version 4. The schema separates
the common measurement method from the P23 admission rule and adds structured
platform, corpus-role, workload, and mode fields. Version 4 also binds each
threshold to a metric class, workload or corpus scope, and, for timing metrics,
either the `sim_execution` or `end_to_end` boundary. It records the production
source and resolved value of load-bearing defaults. The schema validates corpus
paths and hashes against repository bytes and rejects incomplete methods, empty
corpora or thresholds, malformed hashes, and an empty waiver authority. Phase,
evidence, dependency-baseline, and trace schemas remain at version 1.

`docs/days-executor/budgets/retirement-budget.toml` freezes the developer's
Apple M5 Max, macOS build `26A5388g`, Rust 1.96.0 toolchain, three warmups,
fifteen repetitions, the unpaired-bootstrap confidence rule, the no-regression
geometric-mean threshold, the 20 percent per-workload limit, and maintainer
waiver authority. The method applies to P01 reference collection and P23
candidate runs. P01 records absolute Nexosim measurements and evaluates no
threshold. The method freezes the per-workload selected-mode rule before
measurement. The A2 result artifact, not the premeasurement budget, records the
selected mode and event-rate outcome after samples exist. This avoids putting a
measured outcome inside the blob that must be frozen first. Plan section
12.1.3 currently reads as if the selected outcome belongs in that
premeasurement blob and needs an owner amendment to state this rule/outcome
split. P01 does not edit the plan repository.

The F3/A evidence remains in the repository as an honest historical record,
but its measurement manifest is no longer live task evidence. It is superseded
because review required budget schema and method changes and because its
recorded producer, `/private/tmp/t1_f3_runner`, was not reproducible from the
repository. The A2 records under `baselines-v2` are authoritative for the F4
budget. New paths let the audit prove each artifact was introduced after the
freeze. B2 will bind the result after the owner commits A2; P01 does not guess
that commit SHA.

The checked-in `cargo xtask nexosim-baseline collect` runner is the only
producer for A2. It builds timed Days with the exact frozen Cargo flags, asks
`rustc --print cfg` for the feature set, and rejects `migration_ledger`. It
records the binary SHA-256 and toolchain in a build record. It executes the
frozen warmups, repetitions, manifest order, duration guard, thread-count
guard, and wall-time floor. A separate untimed `perf_stats` build obtains the
Nexosim event count, so event counting does not contaminate either timed
boundary. The v2 sample and summary schemas carry the event count, both
events-per-second figures, and the selected-mode outcome.

The first committed runner self-test exercised hardcoded output strings but
never opened a corpus config. It stayed green while the collector failed on
the leading comment in the first fixture, which gave a false signal that the
real input path worked. A2 parses complete TOML documents with `toml::Table`.
Its self-test loads the frozen budget and resolves `log_path` through the
production parser for all thirteen corpus entries.

The collector now performs cheap validation before it starts a simulator
process. It parses the budget, validates the new output path, resolves every
corpus config and `log_path`, rejects feature-bearing timed Cargo flags, and
runs the timed feature probe. It creates the output directory after those
checks pass. The remaining order is correctness collection, a clean timed
build, untimed event counting, three warmup rounds, fifteen recorded rounds,
and artifact emission. The timed build follows correctness because the
correctness build enables `migration_ledger` in the shared release target.

P01 names a second runner failure class: declared-but-unenforced metadata. The
corpus carried `mode = "default"` for the original TCP entry, but no tool
enforced the ST characterization requirement. The corpus later declared the
DCQCN feature set, while the runner built every correctness fixture with
`migration_ledger` alone. In both cases the declaration was correct and the
consumer ignored it. Preflight now joins the retirement-corpus entries to the
frozen budget in order, checks their duplicated identity and hash fields,
validates their commands, verifies each required feature against
`Cargo.toml`, and rejects a featured performance entry.

Some declarations still have limited enforcement. `model_scope` remains a
nonempty description. The runner checks that `comparison_boundary` matches the
budget enum, but no audit proves that a produced artifact supports that
boundary. The runner validates each optional `ledger_path` and `ledger_hash`
as a pair, while `tests/migration_ledger.rs` still hardcodes the three golden
paths with `include_bytes!`. The ledger comparisons are real, but the test
does not consume those manifest fields. P01 records that coupling as later
audit work because changing the golden test is separate from the A2 runner
repair.

The budget reference also has a stable `id`, an owning phase, and required
consumer phases. A present P23 phase must cite the same frozen tuple rather
than substitute a relaxed budget. Measurement history must be a strict,
merge-free first-parent sequence, and every measurement artifact must be added
after the freeze. These checks defeat empty-child and sibling-merge histories.
They are tamper-evident against published history, not tamper-proof before
publication. A published immutable ref, externally held signed tag, or third
party retaining the earlier history is needed to detect a coherent local
rewrite.

### Corpus and comparison boundaries

The corpus role and comparison boundary are independent axes. `role` records
why a fixture exists: `correctness` or `performance`. `comparison_boundary`
records the strongest comparison the fixture admits. A collapsed enum would
permit `role = "ledger-equality"` beside
`comparison_boundary = "terminal-observation"`, a contradictory state that
would need a cross-field rule. The orthogonal encoding cannot express that
contradiction.

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

Their complete canonical ledgers are frozen under
`docs/days-executor/evidence/P01/ledgers-v1/`: 141 rows for explicit, 1,621 for
torus, and 1,361 for fat-tree. Each file was reproduced byte-for-byte by two
separate processes before acceptance. The migration-ledger integration test
compares fresh output with these complete goldens, not merely one current run
with another.

`configs/migration/tcp_simple_st.toml` is a mechanical copy of
`configs/tcp_simple.toml`. The copy adds only `threading = "single"` and
`num_threads = 1`. The original config omits both fields, so `SimInit::new()`
uses the platform's 18-thread default. Plan sections 5.4 and 15 exclude that
mode from ST characterization evidence because thread scheduling affects it.

The derived TCP fixture and `configs/ci/leanguard_dcqcn.toml` are
`terminal-observation`. Their full transition ordering is outside v1 lowering,
but their source/port/sink terminal state exercises the digest mechanism. The
five entries have `role = "correctness"`.

The performance role uses eight tracked configs from the existing benchmark
suite. The fixed order is:

- `fattree_k4_f8_st.toml`, then `fattree_k4_f8_mt.toml`;
- `fattree_k8_f64_st.toml`, then `fattree_k8_f64_mt.toml`;
- `fattree_k16_f512_st.toml`, then `fattree_k16_f512_mt.toml`;
- `fattree_k32_f4096_st.toml`, then `fattree_k32_f4096_mt.toml`.

All eight configs use fixed seed 1000, FIFO/TailDrop, and open-loop packet
distributions. Their strongest frozen boundary is `terminal-observation`; a
performance-length run does not claim full-key ledger equality. Each ST config
sets `threading = "single"` and omits `concurrency_level` and `hot_workers`.
Each MT config sets `threading = "multiple"`,
`concurrency_level = "accelerated"`, and `hot_workers = 2`. The labels compare
two differently tuned configurations, not threading alone. The best-mode rule
states that distinction explicitly. The files remain byte-identical to commit
`cfdcfcb`.

The benchmark configs omit top-level `duration`.
`simulation_duration` is called at `src/topos/topo.rs:416`; its constant and
resolver at lines 1554-1557 supply an effective duration of 1500.0 seconds.
`default_simulation_duration_is_pinned_to_1500_seconds` executes that resolution
path with the key absent. Each later measurement record captures the logged
simulated end time, and the runner rejects a value other than 1500.0 seconds.

The MT configs also omit `num_threads`. Nexosim therefore takes
`num_cpus::get()` at `src/topos/topo.rs:447`; the named platform declares an
expected value of 18. Each sample records the effective thread count already
reported by Days, and the runner rejects an MT sample whose count differs from
18. The method records implicit inputs and enforces them instead of editing
hashed benchmark artifacts. The other inherited inputs are mailbox capacity
16, quantization disabled, UI duration 1.0 second with 0.01-second scheduled
updates, periodic reports disabled with interval `f64::MAX`, no-link mode,
FIFO batch size 1, shortest-path routing, and flow priority 0. Each value and
production `file:line` is structured under `method.resolved_defaults`.

The instrumentation added in this amendment captures `timer.elapsed()`
immediately after `sim.step_until` returns. This `sim_execution` boundary
excludes concurrency-sampler shutdown, statistics collection, and logger
flush. The existing elapsed log remains unchanged. The runner measures
`end_to_end` around the complete child process and records both values.

Calibration was authorized but not used for this committed benchmark corpus.
No timing selected corpus membership or changed a config value. The F3 runner
enforced the frozen 5 ms sample floor. The smallest recorded primary sample was
20.440958 ms, so no sample failed the floor. The complete paths, hashes,
commands, roles, modes, and boundaries are in
`docs/days-executor/evidence/P01/retirement-corpus.toml`.

### A2 Nexosim reference

The repository command
`cargo xtask nexosim-baseline collect --output-dir
docs/days-executor/evidence/P01/baselines-v2` produced the authoritative A2
records. The build record captures the same argv. It also records
`cargo rustc --locked --release --bin days -- --print cfg`, the complete probe
output, `verified_features = []`, and `migration_ledger_enabled = false`.
The probe output contains no Cargo `feature="..."` line. The timed build used
`cargo build --locked --release --bin days` and produced binary hash
`sha256:a9717b7028e8aff1e75f05388810b36dac46ee37299b536af84054058e4b0e93`.

The runner recorded 120 unique samples, fifteen for each workload and mode.
Each sample reached `1500000000000` simulated nanoseconds. ST samples used one
thread and MT samples used eighteen. The shortest primary sample was
20.595125 ms, above the frozen 5 ms floor. Each `end_to_end` value was at least
its corresponding `sim_execution` value.

The table reports seconds as median `[minimum, maximum]`:

| Workload | Mode | `sim_execution` | `end_to_end` |
| --- | --- | ---: | ---: |
| k4/f8 | ST | 0.020691875 [0.020595125, 0.021001833] | 0.023660583 [0.023467000, 0.024032500] |
| k4/f8 | MT | 0.152690417 [0.151515375, 0.153556000] | 0.155797583 [0.154507542, 0.156479750] |
| k8/f64 | ST | 0.196256958 [0.195395625, 0.197306791] | 0.199970834 [0.199019583, 0.201502792] |
| k8/f64 | MT | 0.426733583 [0.424715916, 0.436690833] | 0.431526583 [0.429198209, 0.441502167] |
| k16/f512 | ST | 1.746260375 [1.733728041, 1.775687875] | 1.768118458 [1.755817334, 1.797556750] |
| k16/f512 | MT | 1.936089708 [1.918802292, 1.993304958] | 1.964281458 [1.946003875, 2.021970542] |
| k32/f4096 | ST | 14.452573792 [14.312242417, 15.750409917] | 15.622496417 [15.489812959, 16.998490417] |
| k32/f4096 | MT | 7.150468958 [7.094004667, 7.436040125] | 8.368827041 [8.291108584, 8.645161708] |

Applying the frozen rule selects:

| Workload | Mode | Events | Median primary events/s |
| --- | --- | ---: | ---: |
| k4/f8 | ST | 54,072 | 2,613,199.625457 |
| k8/f64 | ST | 467,852 | 2,383,874.715922 |
| k16/f512 | ST | 3,751,102 | 2,148,077.144567 |
| k32/f4096 | MT | 21,600,888 | 3,020,905.080055 |

The medians changed from the superseded record as follows. Negative percentages
mean the A2 run completed faster:

| Workload | Mode | `sim_execution` old → A2 | Delta | `end_to_end` old → A2 | Delta |
| --- | --- | ---: | ---: | ---: | ---: |
| k4/f8 | ST | 0.020637416 → 0.020691875 | +0.264% | 0.024702500 → 0.023660583 | -4.218% |
| k4/f8 | MT | 0.151459833 → 0.152690417 | +0.812% | 0.155970250 → 0.155797583 | -0.111% |
| k8/f64 | ST | 0.202448375 → 0.196256958 | -3.058% | 0.208347959 → 0.199970834 | -4.021% |
| k8/f64 | MT | 0.434011542 → 0.426733583 | -1.677% | 0.440528500 → 0.431526583 | -2.043% |
| k16/f512 | ST | 1.880578541 → 1.746260375 | -7.142% | 1.905630292 → 1.768118458 | -7.216% |
| k16/f512 | MT | 1.945811083 → 1.936089708 | -0.500% | 1.976358750 → 1.964281458 | -0.611% |
| k32/f4096 | ST | 17.386321625 → 14.452573792 | -16.874% | 18.615065084 → 15.622496417 | -16.076% |
| k32/f4096 | MT | 7.994751958 → 7.150468958 | -10.560% | 9.262936375 → 8.368827041 | -9.653% |

The crossover remains between k16/f512 and k32/f4096, a shift of zero measured
workload steps. The MT/ST primary ratio at k16 rose from `1.034687486` to
`1.108706202`; ST remains selected. At k32 it rose from `0.459829982` to
`0.494754018`; MT remains selected, with the ST/MT speed ratio decreasing from
`2.174716828` to `2.021206425`.

### Superseded F3/A Nexosim reference

The following F3/A record is retained unchanged for traceability and is not the
authoritative result for the forthcoming F4 budget. P01 ran three unrecorded
warmup rounds and fifteen recorded rounds in manifest
order. Timed runs used `cargo build --locked --release --bin days` without
`migration_ledger`. Correctness runs used the diagnostic feature and recorded
no wall-time result. Every performance sample reached 1500.0 simulated seconds
and recorded one effective thread for ST or eighteen for MT.

The table reports seconds as median `[minimum, maximum]` across the fifteen
recorded samples:

| Workload | Mode | `sim_execution` | `end_to_end` |
| --- | --- | ---: | ---: |
| k4/f8 | ST | 0.020637416 [0.020440958, 0.021007375] | 0.024702500 [0.024295000, 0.025419042] |
| k4/f8 | MT | 0.151459833 [0.149931250, 0.160485875] | 0.155970250 [0.154498250, 0.164683541] |
| k8/f64 | ST | 0.202448375 [0.196561625, 0.204402375] | 0.208347959 [0.200876084, 0.210197792] |
| k8/f64 | MT | 0.434011542 [0.425276541, 0.474826250] | 0.440528500 [0.431730334, 0.481116500] |
| k16/f512 | ST | 1.880578541 [1.755074542, 2.010002584] | 1.905630292 [1.778508125, 2.035526250] |
| k16/f512 | MT | 1.945811083 [1.933370875, 2.242102334] | 1.976358750 [1.963363459, 2.273613208] |
| k32/f4096 | ST | 17.386321625 [15.331194875, 19.189513625] | 18.615065084 [16.517648833, 20.418935791] |
| k32/f4096 | MT | 7.994751958 [7.333259875, 9.334476458] | 9.262936375 [8.557429875, 10.604367791] |

The primary medians put MT 7.3 times behind ST at k4/f8 and 2.1 times behind
at k8/f64. The modes are close at k16/f512, where ST has the lower median. MT
becomes 2.2 times faster at k32/f4096. The crossover therefore lies between
k16/f512 and k32/f4096.

Applying the then-frozen rule to these reference medians selects ST for k4/f8,
k8/f64, and k16/f512, and MT for k32/f4096. The result confirms why the rule
selects a mode per workload: assuming MT wins would choose the slower reference
for three of four workloads. P01 records this implication but evaluates no
admission threshold. The comparison is between the ST and accelerated-MT
configurations described above, not a pure threading contrast.

The checked-in samples, summary, and five terminal digests total less than
24 KB. The generated transition ledgers range from 4.5 KB to 78.6 KB, and the
captured process output is 155 KB. None warrants a separate `days-gpu` archive,
so P01 declares no archive evidence and keeps one content-addressed source for
the measured records.

### RED prerequisite and coverage boundary

The corrected RED implementation lands separately as a prerequisite; P01
consumes it rather than implementing it. Diagnostic runs before the split
summed `packets_dropped` across canonical terminal-digest port rows. They were
not baseline measurements:

| Config | Drops before | Drops after | Maximum final queue occupancy | Capacity | `min_abs` |
| --- | ---: | ---: | ---: | ---: | ---: |
| `configs/simple.toml` | 0 | 0 | 0 | 100 | 70 |
| `configs/torus.toml` | 0 | 0 | 0 | 100 | 70 |
| `configs/fattree.toml` | 0 | 0 | 2 | 100 | 70 |
| `configs/tcp_simple.toml` | 0 | 0 | 0 | 100 | 70 |

Zero to zero is not evidence that the RED fix is correct. It is evidence that
these four configs are not RED workloads at all: none approaches the minimum
RED threshold. The only P01 corpus entry within RED's nominal blast radius is
the derived TCP fixture, and it also never enters a RED region. The corrected
RED therefore has no observable effect on any P01 baseline, and P01 evidence
provides no RED coverage.

Landing the fix before the next freeze turned out to be unnecessary in
hindsight. It was still the right decision under uncertainty because the
absence of RED-region traffic could not be known before the diagnostic runs. A
later phase, including P19, must not treat P01 evidence as covering RED
behavior.

The TCP ST digest is nonempty, byte-stable across two child processes, and
unchanged across the old and corrected RED implementations. It records one
source row with 5 packets and 2560 bytes, two port rows totaling 16 enqueues, 0
drops, and 16 forwards, and one sink row with 8 packets and 4096 bytes.
Aggregate endpoint reports and generic port hooks cover this terminal state.
The transition ledger still does not claim complete TCP source/sink
transitions; that remains P19 work. The fixture therefore keeps the
`terminal-observation` boundary. A digest stable only because it was empty
would not be evidence.

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
digest, feature-off aggregate goldens, the complete 13-entry corpus inventory,
and the budget live under `docs/days-executor/evidence/P01/` and are
checksummed by `t1-evidence.toml`. The measurement manifest cites the seven
baseline artifacts at commit A and the budget frozen at F3. P01 has no archive
manifest because the raw outputs did not warrant a separate repository.

### Migration and exclusions

`migration_ledger` is a diagnostic build feature only. It is not a selectable
runtime backend and is absent from default features. Removing the feature
restores the prior output-file set and instrumentation-free model layouts.

T1 does not implement RED corrections, reset or replace process-global IDs,
introduce scenario tuple IDs, sort nodes/edges/flows/routes, add `executor/`,
change time arithmetic, or edit `crates/nexosim`. Those items belong to
separate or later changes.
