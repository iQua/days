# `days-legacy` — FROZEN

**Status: FROZEN as of August 8, 2026 (P12 / T22).**
This crate is the Nexosim-based engine that the safe-horizon executor in `../executor/`
superseded. It is retired from development and retained, permanently, as a **running
baseline**. Frozen does **not** mean unbuilt: CI keeps building, testing, and running it,
and the gates in [Standing gates](#standing-gates) must stay green forever.

**E5 amendment (August 9, 2026 / T22a):** the user-authorized
`LEGACY-ON-E5 ORDERED — FREEZE CONTRACT AMENDED` registry entry in
`days-gpu/plans/p12-opening-plan.md` permits only the minimal correctness changes required
to express and run E5 with legacy's existing Reno implementation. The published E3 and
other frozen legacy baseline rows remain tied to annotated tag `p12-legacy-pre-e5`
(`3295ad08f345c4f57c0fa6c30d0749b6a80e0a95`; legacy tree
`a6a99594ed0334bc0864c6315af339850a6846eb`), never to a post-amendment revision.

If you are looking for the current simulator, it is `../executor/` (crate `days-executor`).
Nothing outside this directory may depend on `days-legacy`; `cargo xtask audit` enforces
that direction mechanically.

---

## 1. What is frozen

The whole crate:

| Path | Contents |
|---|---|
| `legacy/src/` | the Nexosim engine — flow process models, schedulers, switches, `l2/`, topology assembly |
| `legacy/examples/` | `basic`, `ecn`, `switch`, `tcp`, `tcp_test`, `wire` — all six run in CI |
| `legacy/tests/` | the crate's own integration suite |
| `legacy/Cargo.toml` | exact (`=x.y.z`) dependency pins — see [§4](#4-dependency-pinning-and-the-lockfile) |

Frozen **by reference**, not copied here:

- **`crates/nexosim/`** — the simulation kernel is vendored in-tree and reached through the
  workspace's `[patch.crates-io]` entry in the root `Cargo.toml`. `days-legacy` therefore
  builds against a *source-frozen* kernel, not a registry download. Upstream provenance is
  recorded in `crates/nexosim/.cargo_vcs_info.json` (git sha1 `4800578`, crate version
  `1.0.0`).
- **`/Cargo.lock`** — the workspace-root lockfile, now tracked in git, is the resolved
  dependency set for every published legacy number.

**Not** frozen, and this is the freeze's one real exposure:

- `days` (the root crate) is a **live** dependency of `days-legacy`. It supplies configuration
  parsing, topology construction, routing tables, and the CSV logger. Freezing `legacy/src/`
  therefore does not by itself freeze legacy's *observable behaviour*: a change to shared
  routing or logging code can move what this engine outputs without any diff under `legacy/`.
  That is also why change class **(a)** below has to exist.

Where each shared surface is actually covered — read this before assuming a green gate means a
stable baseline:

| shared surface | covered by | not covered by |
|---|---|---|
| configuration parsing | `days-validation` (`days::scenario::compile_config`, 5 of 6 binaries; incl. a TOML-key-ordering differential test) | — |
| topology construction | `days-validation` (`days::topos::build::build_graph`, 4 binaries, consumed by legacy via `Flow::flows_from_config_with_attachments`) | — |
| routing tables | this crate's own suite — `tests/scenario_lowering.rs::assert_legacy_physical_routes` walks the routes legacy installs against the lowered image on real fat trees | **`days-validation` — zero references to `days::topos::route`** |
| CSV logger | this crate's own suite — `tests/{scenario_lowering,host_attachment,ring_allreduce_coverage,dcqcn_event_id,pfc_event_id}.rs` read the CSVs back; root crate `tests/trace_manifest.rs` | **`days-validation` — zero references to the logger or its CSVs** |

`days-validation` (`validation/tests/`: 24 tests — 22 executed, 2 `#[ignore]`d — across
`service_start_selection` (7), `t24_tcp_corpora` (6), `t17c_wide_corpus` (4),
`t13f_width_via_load_full` (3), `tcp_executor_legacy` (3), `t15e_sustained` (1)) is the only
allow-listed dependant of this crate (`xtask/src/main.rs`) and is a **standing gate**, listed in
[§5](#standing-gates). What it guards is **legacy↔executor agreement** — that and no more.

**No automated check catches common-mode drift, and none can be differential.** Legacy's
`physical_flow_paths` (`src/topos/topo.rs:68`) and executor lowering both call
`compute_shortest_path_route_table`. A change under `days`'s `src/topos/` therefore moves **both
sides together**, so every assertion of the form "legacy agrees with the executor" stays green
through precisely the change that moves a published legacy baseline. Only an *absolute* anchor — a
pin of this fixture's counts against the frozen 41,932,800 — would catch it, and **no such pin
exists in CI today**.

The mitigation is therefore not a test. It is the freeze policy in [§3](#3-freeze-policy), its
changelog in [§8](#8-freeze-changelog), review, and one manual step: anyone changing shared `days`
code that legacy consumes runs the gates in [§5](#standing-gates) **and re-runs the E3 count check
of [§6.6](#66-mt-nondeterminism-disclosure-protocol-counts-and-delays) against the frozen anchor** before any
legacy number is republished. Automating that check is an open recommendation, not a shipped one.

## 2. Why it is frozen

1. **It is the paper's ST/MT baseline.** The Days AGO paper's supersession claim is measured
   against this engine, its own ancestor. A baseline that drifts is not a baseline. The P12
   arm-1 numbers must be reproducible from this tree alone.
2. **It is the differential oracle.** Executor semantics are cross-checked against legacy
   behaviour, and the LeanGuard trace-certificate campaign in `.github/workflows/leanguard.yml`
   runs the legacy binary as its `--legacy-runner` for six protocol matrices.
3. **The supersession is complete.** The executor covers the semantics that matter; new work
   goes there. Adding to legacy would make it a second system to maintain and would invalidate
   every baseline already published against it.

## 3. Freeze policy

**No changes. Four narrow exceptions, each requiring a changelog row in
[§8](#8-freeze-changelog).**

| # | Allowed change | Why it must be allowed |
|---|---|---|
| **(a)** | **Exhaustive-match arms forced by a workspace-shared enum growing a variant.** The new arm must not implement behaviour; it panics (or otherwise refuses) with a comment stating why legacy can never legitimately reach it. | `days-legacy` matches exhaustively on enums owned by the live `days` crate. When live work adds a variant, the frozen crate stops compiling. Refusing the arm would mean refusing all live development. |
| **(b)** | **Toolchain compatibility.** Edition/rustc/clippy-lint churn that breaks the build with no semantic change. | Required by the retirement policy (`days-executor-plan.md:605`): "changes only for toolchain compatibility". |
| **(c)** | **Dependency pin bumps.** Editing a `=x.y.z` pin in `legacy/Cargo.toml`. | Exact pins on crates that the *live* root crate also uses (see [§4](#4-dependency-pinning-and-the-lockfile)) mean a live-side patch bump inside the same semver range requires touching this manifest. That coupling is deliberate; it is also the only way to move a frozen dependency, so it is auditable. |
| **(d)** | **The ordered E5 expressibility repair.** Minimal fixes for exact event scheduling, strict `FatTreeEcmp`/structural-pairing handling, byte-budget TCP with configurable MSS, sound cumulative ACK/loss recovery using the existing Reno controller, propagation activation, and correctness observability. | Explicit user authority in `days-gpu/plans/p12-opening-plan.md`, registry entry `LEGACY-ON-E5 ORDERED — FREEZE CONTRACT AMENDED` (August 9, 2026). This is bounded to the reviewed T0–T7 plan; it does not authorize a new controller or general legacy development. |

**Never, under exceptions (a)–(c), and outside the exact bounds of exception (d):**

- no new protocols, mechanisms, schedulers, queue disciplines, or congestion-control variants;
- no new configuration keys, no new output fields, no new features in `[features]`;
- no performance work, no refactors, no "while I'm here" cleanups;
- no behaviour change of any kind. If a change would alter what any existing fixture
  produces, it is out of policy — stop and escalate rather than proceed.

**Every** change under (a), (b), or (c) adds a row to [§8](#8-freeze-changelog) in the same
commit. A change to `legacy/` without a changelog row is a policy violation regardless of how
small it is.

**This rule is enforced by review, not by CI.** Nothing mechanically checks that a diff touching
`legacy/` also touches the changelog table — `cargo xtask audit` enforces the dependency
*direction* only and never reads this file. Treat the requirement as a reviewer's checklist item on
every diff that touches `legacy/`. A CI check that would mechanise it is specified as an open
recommendation in `days-gpu/evidence/P12/t22-legacy-freeze.md` §7; until it lands, a green gate set
is **not** evidence that the changelog was updated.

## 4. Dependency pinning and the lockfile

Every dependency and external dev-dependency in `legacy/Cargo.toml` is an **exact** pin
(`=x.y.z`), not a caret range. The pins were introduced at the versions the workspace had
already resolved, so `cargo metadata` reproduced the pre-existing `Cargo.lock`
byte-for-byte — nothing moved and no build output changed.

Two mechanisms, deliberately redundant:

1. **`/Cargo.lock` is tracked** (`.gitignore` keeps `**/Cargo.lock` but negates `!/Cargo.lock`;
   nested and worktree lockfiles stay ignored). Build with `--locked` and the resolved graph is
   exactly the recorded one.
2. **Manifest pins** survive lockfile deletion and, more importantly, survive `cargo update`.
   Without them, `cargo update` could move the differential oracle's dependencies with no diff
   anywhere under `legacy/`.

**Known consequence — the pins are workspace-visible.** Cargo unifies a dependency to one
version per semver-compatible range across the workspace. Most of legacy's pins are on crates a
live crate also uses. **Consult this split before touching any pin:**

- **Shared — a live crate depends on these, so legacy's pin is the version the whole workspace
  gets:** `csv`, `env_logger`, `log`, `tracing`, `tracing-subscriber`, `parking_lot`, `petgraph`,
  `rand`, `serde`, `serde_json`, `thiserror`, `toml`, `tempfile`, `assert_cmd`, `predicates`, and
  **`nexosim`** — `days-validation` carries `nexosim = { version = "1.0.0", … }`
  (`validation/Cargo.toml:18`). The `[patch.crates-io]` entry in the root manifest redirects every
  consumer to the vendored `crates/nexosim` at 1.0.0, so the effect is muted today, but it is not
  nil: legacy's `=1.0.0` would **block** a vendored-kernel version bump that validation's `^1.0.0`
  would accept. That is the freeze working as intended, and it is why `nexosim` belongs here rather
  than on the list below.
- **Legacy-exclusive — no other crate is affected by these:** `indicatif`,
  `indicatif-log-bridge`, `rand_distr`, `tachyonix`, `futures`, `futures-executor`, `once_cell`,
  `num_cpus`.

While the live crate stays inside the same range, legacy's pin is the version everyone gets. This
is intended: it makes silent movement of a baseline dependency impossible. When the live crate
crosses a semver-major boundary (e.g. `rand` 0.10 → 0.11), the two versions coexist and legacy
simply keeps its frozen one. When a live-side bump *inside* the range is genuinely needed, use
change class **(c)** and re-run [§5](#standing-gates).

## 5. Standing gates

<a id="standing-gates"></a>
These must stay green. They are not optional, and a green root-crate gate set is **not** a
substitute for them: the root manifest is itself a package, so a bare `cargo build` or
`cargo test` never touches this workspace member. That gap is exactly how `b270b98` shipped a
broken workspace build and two broken CI jobs, caught only at T21 (see the changelog's first
row and `days-gpu/evidence/P12/t21-fixture-authoring.md` §4.6).

```bash
# 1. The workspace must build. This is the gate that catches change class (a).
cargo build --workspace --locked

# 2. The crate's own suite, verbatim what .github/workflows/examples.yml runs.
cargo test -p days-legacy --features test -- --show-output

# 2b. The differential suite that guards the live-`days` exposure described in §1.
#     Also verbatim from .github/workflows/examples.yml.
cargo test -p days-validation --features test -- --show-output

# 3. All six examples, verbatim what .github/workflows/examples.yml runs.
for example in legacy/examples/*; do
  cargo run --release -p days-legacy --example "$(basename "$example" .rs)"
done

# 4. The LeanGuard legacy runner builds under every protocol feature set
#    (.github/workflows/leanguard.yml drives these three combinations).
cargo build -p days-legacy --features lean --bin days
cargo build -p days-legacy --features l2_pfc,lean --bin days
cargo build -p days-legacy --features dcqcn,l2_pfc,lean --bin days

# 5. Nothing outside legacy/ may depend on days-legacy (one test-only exception,
#    days-validation, is allow-listed in xtask/src/main.rs).
cargo xtask audit
```

Reproducibility check to re-run whenever [§4](#4-dependency-pinning-and-the-lockfile) changes —
from a **clean clone**, not the working tree:

```bash
git clone --no-hardlinks --branch feat/days-executor <repo> /tmp/legacy-repro
cd /tmp/legacy-repro
cargo build --locked --release -p days-legacy --bin days
cargo test  --locked -p days-legacy --features test -- --show-output
git status --porcelain    # must be empty: --locked must not have rewritten Cargo.lock
```

## 6. Baseline-run harness contract (E3)

<a id="harness-contract"></a>
This section is normative for anyone producing legacy arm-1 numbers. Follow it exactly; a
measurer who does should reproduce another measurer's fields without further instruction.

### 6.1 Fixture

The P12 legacy-comparability fixture, `E3`, is the only fixture the paper's legacy ST/MT arms
run:

- `configs/benchmarks/p12/e3_legacy_rack_local_st.toml` — single-threaded arm
- `configs/benchmarks/p12/e3_legacy_rack_local_mt.toml` — multi-threaded arm

Both are **generated** by `configs/benchmarks/p12/gen_e3_rack_local.py` and must not be
hand-edited. k = 32 fat-tree, 16 hosts per edge switch (8,192 hosts, 1,280 switches), 16,896
explicit **byte-terminated** flows in three permutation components (8,192 intra-rack, 8,192
intra-pod cross-rack, 512 cross-pod), 1,000 B packets, `port_rate` 3,200,000, FIFO/TailDrop
capacity 100, `duration = 18`, `seed = 1000`, default shortest-path routing.

Three design constraints are load-bearing and must be restated in any table built from these
runs:

1. **Byte-terminated flows are mandatory.** Legacy emits one extra packet per
   *duration*-terminated flow; a duration-terminated fixture would show a spurious legacy/AGO
   count divergence.
2. **The traffic matrix is explicitly rack-local-dominant.** The shared canonical fat-tree
   route table funnels cross-pod traffic through a small minority of aggregation and core
   switches, capping active width at ~hops × k without such a matrix.
3. **E3 uses default shortest-path routing while E1/E2 use `FatTreeEcmp`.** Legacy has no
   equal-cost multipath and is frozen against new features, so an ECMP row here would not be
   running legacy's fabric. Any table placing E3 beside E1/E2 must say so.

### 6.2 Build

```bash
cargo build --release --locked -p days-legacy --bin days
# binary: target/release/days   (the root crate has no src/main.rs; this name is legacy's)
```

Record `rustc --version`, `cargo --version`, and the git commit for every machine.

### 6.3 Invocations

Timed samples, per arm, under the standing quiet-machine gate:

```bash
# ST
/usr/bin/time -p -o wall-st-<n>.txt \
  env RUST_LOG=info ./target/release/days \
  configs/benchmarks/p12/e3_legacy_rack_local_st.toml > run-st-<n>.log 2>&1

# MT
/usr/bin/time -p -o wall-mt-<n>.txt \
  env RUST_LOG=info ./target/release/days \
  configs/benchmarks/p12/e3_legacy_rack_local_mt.toml > run-mt-<n>.log 2>&1
```

Four requirements on the invocation, each with a reason:

- **`RUST_LOG=info`, not `error`.** The legacy binary reports its own in-process clocks and its
  effective concurrency configuration at `info`; `error` throws both away and leaves a 0-byte log.
  The whole log is small and fixed: measured **ST 12 lines / 1,294 B, MT 14 lines / 1,470 B** (the
  MT arm's two extra lines are its `hot_workers` and `concurrency_level` reports). The line set is
  startup-and-shutdown only — no per-event logging, because this fixture sets no `report_interval` —
  so it does not grow with fixture size. Keep the level identical across
  every arm, sample, and machine so it cannot bias a comparison, and **record the log's byte
  size per sample as a guard** — if it grows past a few KB the assumption above has broken and
  the round should stop and re-brief. (Note: P01 and the P11 slide preview used `RUST_LOG=error`.
  Neither is a diff target for P12, so the change costs nothing and buys the clock pair.)
- **Redirect both streams to a file.** `indicatif` draws a progress bar on a tty and not on a
  pipe; redirecting removes that work and makes samples comparable across shells.
- **Copy the CSVs out after every sample.** `log_path` is
  `logs/p12/e3_legacy_rack_local_{st,mt}` and the logger **truncates** `sources.csv`,
  `switches.csv`, and `sinks.csv` at startup (`fs::File::create` in `CsvLogger::init_output_files`).
  Sample *n*'s counts are destroyed the moment sample *n+1* starts, and the per-sample counts are
  exactly what [§6.6](#66-mt-nondeterminism-disclosure-protocol-counts-and-delays) requires. `logs/` is
  gitignored, so archive the extracted fields, not the raw CSVs (1.1 MB + 1.7 MB per sample).
- **One warmup, untimed, per arm per machine**, then the timed samples (the P01 protocol).

Sample count: the legacy arms cost tens of seconds per sample, not the tens of minutes that force
the slow-arm `n = 3` budget on the external arms. Run them at the **headline `n`** from the outset
(the statistical-hardening item: `n ≥ 10` with confidence intervals) rather than at `n = 3` and
again later. Whatever `n` is declared, the extraction step must **refuse to emit a statistic whose
sample count disagrees with it**, and every median must be published with its range.

### 6.4 Fields the measurer round MUST retain

Per **sample** — not per arm, not per median:

| Field | Where it comes from |
|---|---|
| `process_wall_s` | `real` from `/usr/bin/time -p` |
| `elapsed_wall_s` | `Elapsed wall-clock time: X seconds.` in the run log |
| `nexosim_total_wall_s` | `Nexosim total wall-clock time: X seconds.` |
| `nexosim_step_until_wall_s` | `Nexosim step_until wall-clock time: X seconds.` |
| `threading`, `effective_num_threads` | `Starting simulation with <mode> threading (N thread(s)).` — both arms |
| `hot_workers` | `Using N hot standby worker(s).` — **MT only**; the line is absent on ST, record `n/a` |
| `concurrency_level` | `Using <default\|accelerated> concurrency level.` — **MT only**; absent on ST, record `n/a` |
| `sent_packets`, `sent_bytes` | **sum** of `sent_packets` / `packet_sizes` over `sources.csv` |
| `received_packets`, `received_bytes` | **sum** of `received_packets` / `received_sizes` over `sinks.csv` |
| `derived_dropped_packets` | `sent_packets − received_packets` (see below) |
| `max_source_end_time`, `duration` | max `end_time` over `sources.csv`; `duration` from the fixture |
| `sink_rows`, `source_rows` | row counts of each CSV |
| `global_one_way_delay_mean_s` | `Average one-way delay: X seconds.` — **observe-only, NOT citable** (see [§6.6](#66-mt-nondeterminism-disclosure-protocol-counts-and-delays)); retained so the MT delay nondeterminism is visible per sample rather than merely warned about |
| `log_bytes` | size of the run log (the guard above) |
| machine, commit, `rustc`/`cargo` versions, quiet-gate record | the standing protocol |

Notes that will otherwise be got wrong:

- **`switches.csv` is empty.** These fixtures set no `report_interval`, so scheduler reports are
  never emitted and **drops are not directly reported**. Derive them as `sent − received`. That
  derivation is exact only when nothing is in flight at the stop boundary — which is why
  `max_source_end_time` is a retained field: it must be strictly less than `duration`. Do **not**
  add `report_interval` to get `switches.csv`; that edits a frozen fixture and adds per-interval
  logging work to a timed run.
- **`sources.csv` has two rows per flow.** The second is a terminal zero-count report. Sum the
  columns; never infer flow count from row count. Expect 33,792 source rows and 16,896 sink rows.
- **Clock pairs are mandatory** (the same discipline the external arms carry). Report
  `process_wall_s` **and** `elapsed_wall_s` together, always. They differ: process wall includes
  process start, topology construction, and the final CSV flush.
- **Never publish a median without its range**, and never emit a statistic whose sample count
  disagrees with the arm's declared `n`.
- **Every agreement claim on this fixture covers integer counts ONLY.** The delay columns do not
  agree — not between ST and MT, and not between two MT runs. This is a real reordering effect, not
  a rounding residue; see [§6.6](#66-mt-nondeterminism-disclosure-protocol-counts-and-delays).
  **No delay quantity from a legacy MT run may be cited.**

### 6.5 The MT configuration is machine-dependent — disclose it

The MT fixture sets `threading = "multiple"`, `concurrency_level = "accelerated"`,
`hot_workers = 2`, and **deliberately does not set `num_threads`**. Consequently:

- `num_threads` defaults to `num_cpus::get()` — a different value on every machine;
- `concurrency_level = "accelerated"` sets `max_groups_per_step_task = num_threads × 10`, so the
  batching factor scales with the core count too.

The MT arm is therefore a *machine-best* configuration, not a fixed one. Every MT table row
prints its `effective_num_threads`. Do not compare MT wall times across machines without that
column, and do not silently pin `num_threads` — pinning changes the arm's meaning and would
need its own recorded decision.

### 6.6 MT nondeterminism disclosure protocol (counts and delays)

Legacy MT is **not** a semantic oracle. It is nondeterministic on this fixture in the delay columns
and deterministic in the integer counts, and the two halves need different rules. Read
[§6.6.2](#662-delays-mt-is-nondeterministic-and-no-mt-delay-is-citable) before reporting anything
that is not an integer.

#### 6.6.1 Counts

Two facts are on record:

- **P01** (`days-gpu/evidence/P01/nexosim-baseline.md`): on duration-terminated fixtures, MT
  received-packet counts varied between samples (2 packets at k8/k16, 145 at k32), attributed to
  Nexosim MT interleaving **at the simulation stop boundary**. ST counts agreed exactly.
- **P11 slide preview** (`days-gpu/evidence/P11/slide-preview/slide-fourarm-preview.md`): the
  count nondeterminism did not manifest on the drop-free E3-class workload; the drop-heavy
  negative control is where legacy and Days AGO diverged (9.4%, from TailDrop packet-vs-byte
  admission semantics). **Refined here:** that observation is true of *counts* and false of
  *delays* — see [§6.6.2](#662-delays-mt-is-nondeterministic-and-no-mt-delay-is-citable). Drops are
  not required to observe legacy MT nondeterminism; they are only required to observe it *in the
  counts*.

E3 is built so the **count** boundary mechanism cannot fire: every flow is byte-terminated and
completes by ~16.0 s, two seconds before the 18 s horizon, so no packet is in flight when the
simulation stops and no count can be truncated. Note precisely what this does and does not say —
the MT interleaving still reorders execution extensively (§6.6.2); what the drained horizon buys is
that the reordering cannot change a *total*. **This is an expectation to be tested per machine, not
an assumption to be asserted.**

**ADDED 2026-08-08, from the P12 E1 spine-legacy round
(`days-gpu/evidence/P12/spine-legacy-e1.md` §5.2). This is an ADDITION, not a correction: every
sentence above stands, and E3 remains count-invariant on both machines it has been run on. What the
new evidence adds is the scope of the word "drained".**

**Count invariance is a property of the DRAINED HORIZON, not of the crate, not of the arm, and not
of drop-freeness.** Until now the drained-horizon expectation had been tested on exactly one
fixture, E3, where it held; E1 is the worked counter-instance where it fails, and the two together
say what the property actually attaches to.

| | E3 (18 s, byte-terminated) | **E1 spine (20 µs, horizon-cut)** |
|---|---|---|
| drain state at the stop boundary | **drained** — every flow completes by ~16.0 s | **undrained** — **90,990** packets still in the fabric at load 0.10, rising to 995,117 at load 0.90 |
| MT delivered counts | all **20** samples delivered 41,932,800 | **7–9 distinct values per 10 samples**, at every load, in both measurement blocks |
| MT `sinks.csv` digest | invariant | **10 distinct digests out of 10 samples**, on every MT arm of both blocks — no two MT samples produced the same sink state at all |
| ST counts | invariant | invariant (so step 5's stop condition did **not** trigger; the reference holds and the nondeterminism is MT's) |
| §6.6.1 step 3 `legacy-MT-nondeterministic` label | did not apply | **APPLIES, on the counts** |

Two consequences, and a boundary on what may be inferred.

- **The expectation is to be tested per FIXTURE as well as per machine.** The sentence above —
  "an expectation to be tested per machine, not an assumption to be asserted" — is hereby read as
  covering the fixture axis too. A new fixture does not inherit E3's count invariance, however
  drop-free it looks; it earns it by being drained and by being measured.
- **A fixture that cuts a live fabric cannot carry a legacy MT count at all** without the step-3
  label and the full per-sample spread. On such a fixture the delivered count is not a property of
  the workload, it is whatever had landed when the clock stopped.
- **What is NOT established.** The natural reading — that the P01 formulation generalises from
  *drops* to *any* count-affecting tie-break, drops on E3 and an undrained horizon on E1 — is a
  reading, **not isolated by measurement**: E1 differs from E3 in drain state, routing, traffic
  matrix, horizon and termination mode all at once, and separating those would need its own
  experiment. What is measured is that the label applies on E1 and did not on E3.

The protocol:

1. Extract and record the count fields for **every** MT sample individually. Never report only
   the median, and never report counts once "for the arm".
2. State explicitly whether all MT samples agreed. If they agreed, say so with the sample count:
   "all n = X MT samples delivered 41,932,800 packets / 41,932,800,000 bytes".
3. If any sample disagrees, publish the full per-sample spread, publish
   `max_source_end_time` for each, and label the arm's counts as
   **legacy-MT-nondeterministic**. Do not median them away, and do not attribute the divergence
   to Days AGO — the ST arm and the Days AGO scalar anchor are the references.
4. The cross-arm count target is the frozen Days AGO anchor for this fixture:
   **41,932,800 packets / 41,932,800,000 bytes, 0 drops** (`days-gpu/evidence/P12/t21-fixture-authoring.md`
   §4.8). Any legacy deviation from it is reported as a legacy-side finding with its mechanism
   named, never as a silent footnote.
5. The ST arm's counts are the legacy-side reference. If ST itself varies between samples, stop:
   that is a new finding, not a disclosure item.
6. **(ADDED 2026-08-08.)** Before citing any legacy MT count on a fixture other than E3, establish
   the fixture's **drain state at the stop boundary** and record it beside the count: how many
   packets are in the fabric when the simulation stops, and how that was determined. If the fixture
   does not drain, expect step 3 to apply and run enough samples to publish the spread rather than a
   value. On E1 the figure was obtained from a diagnostic copy that adds `report_interval` equal to
   the horizon — one report, at the cut — which was shown not to change the simulation by
   reproducing the timed arms' delivered counts exactly at all four loads.

#### 6.6.2 Delays: MT is nondeterministic, and no MT delay is citable

<a id="662-delays-mt-is-nondeterministic-and-no-mt-delay-is-citable"></a>
The count invariance above does **not** extend to the delay columns. Measured at the freeze on this
fixture, by diffing `sinks.csv` per flow (one ST run, three MT runs):

| comparison | `queueing_delay_mean` differing | `one_way_delay_mean` differing | integer counts / bytes / ids / times differing |
|---|---:|---:|---:|
| ST vs MT | **8,704 of 16,896** | **8,704** | **0** |
| MT vs MT (three pairs) | **8,704** every time | 8,683 – 8,693 | **0** |

Three things follow, and none of them is summation order:

1. **The per-flow delays genuinely differ, at large magnitude.** Over the 8,704 differing sinks
   (ST vs MT): ratio median **2.08×**, p90 **5.15×**, max **8.45×**; 99.8% differ by more than 1%
   and 53% differ by more than 2×. Example, sink `id 17665` (flow 8192): ST `one_way_delay_mean`
   0.047499999999985 vs one MT run's 0.013141666666652. This is not a last-bits residue.
2. **The divergence is exactly the non-rack-local traffic.** 8,704 = 8,192 intra-pod cross-rack
   + 512 cross-pod — i.e. **every flow that leaves its top-of-rack switch diverges, and all 8,192
   rack-local flows agree exactly.** That is execution-order contention at the aggregation and core
   switches, which is precisely what MT changes. ~~`queueing_delay_mean` hits all 8,704 in *every*
   comparison; `one_way_delay_mean` falls a few short between MT runs only because a handful of
   flows coincidentally land on the same value.~~

   **Corrected by the P12 local baseline round (August 8, 2026).** The struck sentence was written
   from **one ST and three MT samples**; it was a sound inference at that `n` and is superseded by
   **`n = 10` per arm in each of two independent blocks** — 20 ST-vs-MT and 18 MT-vs-MT comparisons
   (`days-gpu/evidence/P12/legacy-baselines-local.md` §4.4). What that larger `n` shows:

   - `queueing_delay_mean` does hit all 8,704 in **every ST-vs-MT comparison** — 20 of 20. That half
     stands, and the set is exactly components B + C: the differing `flow_id` set is `[8192, 16895]`
     in all 20, and **no rack-local flow differs in any comparison, in either delay column** (0 of 76
     differing sets contains a `flow_id` < 8192).
   - **Between two MT runs it does not.** `queueing_delay_mean` reaches 8,704 in only **6 of 18**
     MT-vs-MT pairs and lands on 8,701 / 8,702 / 8,703 in the other 12; `one_way_delay_mean` between
     MT runs spans 8,680–8,695. Every short set is a strict *subset* of B + C, never a different set.
   - So the coincidental-equality mechanism reaches **both** delay columns, not `one_way_delay_mean`
     alone. **8,704 remains the structural set**; "in *every* comparison" is true only of the
     ST-vs-MT direction.
   - The same round newly establishes the other side of the pair: **ST vs ST differs in nothing, in
     any column, in 18 of 18 comparisons** — the strongest support this contract has for "the ST arm
     is the only delay-bearing legacy arm".

   **No protocol consequence follows.** The absolute rule below is unchanged and, if anything,
   reinforced: no legacy MT delay quantity may be cited. A measurer who read the struck sentence and
   obeyed that rule did nothing that needs revisiting.
3. **The logged aggregate is a faithful summary, not an artifact.** Recomputing the global mean
   from the per-sink means and counts gives ST 0.001517160 and MT 0.001518399 / 0.001518479,
   reproducing the logged `0.001517` / `0.001518` exactly. The four samples logged ST 0.001517 and
   MT 0.001518, 0.001518, 0.001519 — the MT arm is nondeterministic run to run.

**Protocol consequence, and it is absolute: no delay quantity from a legacy MT run may be cited,
compared, plotted, or averaged.** That covers `one_way_delay_mean`, `queueing_delay_mean`, and the
logged `Average one-way delay`. **The ST arm is the only delay-bearing legacy arm.** Retain
`global_one_way_delay_mean_s` per sample anyway ([§6.4](#64-fields-the-measurer-round-must-retain))
so the nondeterminism is *observable* in the record rather than merely warned about; label the
column observe-only wherever it appears. If a future fixture needs legacy MT delays, that is a new
scoping decision with its own evidence, not something this contract permits.

### 6.7 Comparability statement to carry into every legacy/AGO table

- TCP scenarios are comparable **modulo the documented `f64` divergences**; legacy timing uses
  `f64`, the executor uses exact integer arithmetic.
- **Legacy MT delay quantities are not comparable to anything** — not to legacy ST, not to the
  Days AGO anchor, not to another legacy MT sample
  ([§6.6.2](#662-delays-mt-is-nondeterministic-and-no-mt-delay-is-citable)). Legacy/AGO agreement on
  this fixture is an **integer-count** statement. Any table with a delay column sourced from legacy
  must take it from the ST arm and say so.
- Legacy emits **one extra packet per duration-terminated flow**. E3 avoids this by construction;
  any other fixture must state how it does.
- The parity table's intentional exclusions (virtual clock, BBR) keep their recorded rationale.
- The P11 slide-preview four-arm table is **method, not evidence** — its own header says so. Its
  numbers are not carried forward; P12 re-measures at final HEAD.
- The P01 baselines are a historical record at commit `b4e5f07`, explicitly **not** a diff target.

### 6.8 Verified once, locally, at the freeze

The legacy side of E3 was smoke-run once per arm on the freeze machine to prove runnability —
**not** to measure it. Single sample, machine not under the quiet gate, `n = 1`:

| arm | outcome | wall |
|---|---|---|
| ST | completed; 16,896 sink rows; 41,932,800 packets / 41,932,800,000 bytes; sent = received; max source `end_time` 16.0000000000015 < 18 | completed in ~46 s — **not a measurement** |
| MT | completed; identical counts and identical max source `end_time`; 18 effective threads | completed in ~11 s — **not a measurement** |

Both arms reproduced the frozen Days AGO anchor's **integer counts** exactly. They did **not** agree
on the delay columns — 8,704 of 16,896 sinks differ, and MT differs run to run
([§6.6.2](#662-delays-mt-is-nondeterministic-and-no-mt-delay-is-citable)). No number in this table
may be cited, compared, or plotted.

## 7. Provenance

| Item | Value |
|---|---|
| Repository / branch | `iqua/days`, `feat/days-executor` (the freeze is in-tree, not a separate repo) |
| Freeze declaration | this file, P12 / T22, August 8, 2026. Its commit is the one that adds it: `git log --diff-filter=A --format=%H -- legacy/README.md` |
| Commit that froze the dependency state | `5cdb775` — *T22: Pin days-legacy dependencies and track the workspace lockfile* |
| Last content-bearing commit to `legacy/src/` | `14a0e60` — *T21: Fix the workspace build the routing enum broke* (a change-class-(a) arm; see §8) |
| Crate partition (creation of `legacy/`) | `76b7235`, August 1, 2026 — *Legacy partition: Separate engine and enforce boundaries* |
| Nexosim kernel | vendored at `crates/nexosim`, version `1.0.0`, upstream git sha1 `4800578` |
| Freeze-verification machine | MacBook Pro `Mac17,6`, Apple M5 Max, 18 cores, 128 GB, macOS 27.0 (`26A5388g`), arm64 |
| Toolchain at the freeze | `rustc 1.96.0 (ac68faa20 2026-05-25)`, `cargo 1.96.0 (30a34c682 2026-05-25)` |
| Policy source | `days-gpu/plans/days-executor-plan.md:605`; procedure in `days-gpu/plans/p12-opening-plan.md` §2 |

Per-machine toolchain identities for the measured baselines are recorded with the runs, in the
P01 format (`days-gpu/evidence/P01/nexosim-baseline.md`, "Revision, build, and machine").

## 8. Freeze changelog

Every post-freeze change to `legacy/` appears here, with its class from
[§3](#3-freeze-policy). Changes made *at* the freeze are recorded for completeness. Edits to this
README itself (adding a row, correcting a reference) are not policy changes and need no row; edits
to anything else under `legacy/` always do.

| Date | Commit | Class | Change | Forced by |
|---|---|---|---|---|
| 2026-08-08 | `14a0e60` | (a) | Added the `RouteTableError::UnsupportedTopology` arm in `src/topos/topo.rs`; it panics with the reason legacy can never legitimately reach it (legacy selects only the topology-agnostic shortest-path table). No behaviour change. | `b270b98` added the variant to the shared `days` routing enum, breaking `cargo build --workspace` and both legacy CI jobs. Recorded pre-freeze; kept here as the worked precedent for class (a). |
| 2026-08-08 | `5cdb775` | (c) | Converted every dependency and external dev-dependency in `Cargo.toml` from a caret range to an exact `=x.y.z` pin, at the versions already resolved. `cargo metadata` reproduced `Cargo.lock` byte-for-byte; no version moved. | The freeze itself ([§4](#4-dependency-pinning-and-the-lockfile)). |
| 2026-08-09 | `T22a: Amend the legacy freeze contract for E5` | (d) | Recorded the bounded E5 exception and pinned all pre-amendment E3/baseline provenance to annotated tag `p12-legacy-pre-e5` at `3295ad0` (legacy tree `a6a99594`). | The `LEGACY-ON-E5 ORDERED — FREEZE CONTRACT AMENDED` registry entry in `days-gpu/plans/p12-opening-plan.md`. |
| 2026-08-09 | `T22a: Make legacy serialization exact and failures fatal` | (d) | Replaced FIFO's f64 serialization deadline with checked integer-ceiling nanoseconds and propagated Nexosim initialization/step errors through the library and CLI. | Ordered E5 plan T1: prevent positive sub-nanosecond work from becoming an invalid zero-duration event or a false successful run. |
| 2026-08-09 | `T22a: Move legacy scheduling boundaries to integer nanoseconds` | (d) | Made starts, sampled delays/pacing, TCP polling and RTO deadlines, FIFO service starts, configured propagation, reporting/UI events, and the simulation horizon integer-nanosecond authoritative while retaining f64 controller/packet/report views. | Ordered E5 plan T2: exact event-clock boundaries without replacing the legacy controllers or report schema. |
| 2026-08-09 | `T22a: Honor E5 routing and structural pairing` | (d) | Added strict root routing/pairing parsing, shared structural flow pairs, and compiler-identical semantic hashes feeding the shared O(1) canonical fat-tree ECMP table; unsupported/conflicting configurations now fail instead of falling through. | Ordered E5 plan T3: express `FatTreeEcmp` plus `SwitchOffsetHalf` without changing the standing shortest-path or legacy per-flow ECMP policies. |
| 2026-08-09 | `T22a: Make byte-limited TCP independent of arrivals` | (d) | Made byte-sized TCP sources refill directly from their byte budget, derived a positive fixed integral MSS from configuration, plumbed it through Reno/CUBIC/BBR, and emitted an exact final short segment; duration traffic keeps the legacy synthetic source. | Ordered E5 plan T4: E5's 1,460-byte MSS and one-MiB byte budget must not depend on the ignored arrival distribution or the old hardcoded 512-byte segment size. |
| 2026-08-09 | `T22a: Repair legacy TCP loss recovery integration` | (d) | Made cumulative ACK advancement hole-safe, implemented Reno's missing send hook with byte-consistent flight/sequence accounting, and pinned deterministic fast and exact-RTO retransmission behavior through final completion. | Ordered E5 plan T5: minimally repair the existing Reno integration so loss cannot be falsely acknowledged; the controller itself remains legacy Reno and was not replaced. |
| 2026-08-09 | `T22a: Add E5 propagation and metrics preflight` | (d) | Added an E5-form ordered-stage preflight, opt-in final TCP correctness counters in a separate CSV, and keyed cancellation of completed sources' periodic timeout events. Historical no-key propagation and default log artifacts remain unchanged. | Ordered E5 plan T6: make the E5 compatibility mode and its completion/drop/retransmission evidence explicit without moving E3 defaults. |
| 2026-08-09 | `T22a: Add the lossy E5 analogue gate` | (d) | Added the k=4 simultaneous-start lossy correctness fixture and its exact demand/tail/drop/retransmission/drain assertions, plus a final-segment metric. Full E5 was not run: the ordered T7 E3 gate stopped the round because T2 exact scheduling changes published E3 timing/delay fields despite identical counts. | Ordered E5 plan T7 up to its binding E3 stop condition; see `days-gpu/evidence/P12/legacy-e5-fixes.md`. |
