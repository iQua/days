# `days-legacy` — FROZEN

**Status: FROZEN as of August 8, 2026 (P12 / T22).**
This crate is the Nexosim-based engine that the safe-horizon executor in `../executor/`
superseded. It is retired from development and retained, permanently, as a **running
baseline**. Frozen does **not** mean unbuilt: CI keeps building, testing, and running it,
and the gates in [Standing gates](#standing-gates) must stay green forever.

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
- The guard is the `days-validation` differential suite (`validation/tests/`: 24 tests — 22
  executed, 2 `#[ignore]`d — across `service_start_selection` (7), `t24_tcp_corpora` (6),
  `t17c_wide_corpus` (4), `t13f_width_via_load_full` (3), `tcp_executor_legacy` (3),
  `t15e_sustained` (1)), which runs legacy against the executor
  and is the only allow-listed dependant of this crate (`xtask/src/main.rs`). It is a **standing
  gate**, listed in [§5](#standing-gates), not an optional extra. Anyone changing shared `days`
  code that legacy consumes runs it, and re-checks the E3 counts in
  [§6.6](#66-mt-count-nondeterminism-disclosure-protocol) before republishing a legacy number.

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

**No changes. Three narrow exceptions, each requiring a changelog row in
[§8](#8-freeze-changelog).**

| # | Allowed change | Why it must be allowed |
|---|---|---|
| **(a)** | **Exhaustive-match arms forced by a workspace-shared enum growing a variant.** The new arm must not implement behaviour; it panics (or otherwise refuses) with a comment stating why legacy can never legitimately reach it. | `days-legacy` matches exhaustively on enums owned by the live `days` crate. When live work adds a variant, the frozen crate stops compiling. Refusing the arm would mean refusing all live development. |
| **(b)** | **Toolchain compatibility.** Edition/rustc/clippy-lint churn that breaks the build with no semantic change. | Required by the retirement policy (`days-executor-plan.md:605`): "changes only for toolchain compatibility". |
| **(c)** | **Dependency pin bumps.** Editing a `=x.y.z` pin in `legacy/Cargo.toml`. | Exact pins on crates that the *live* root crate also uses (see [§4](#4-dependency-pinning-and-the-lockfile)) mean a live-side patch bump inside the same semver range requires touching this manifest. That coupling is deliberate; it is also the only way to move a frozen dependency, so it is auditable. |

**Never, under any exception:**

- no new protocols, mechanisms, schedulers, queue disciplines, or congestion-control variants;
- no new configuration keys, no new output fields, no new features in `[features]`;
- no performance work, no refactors, no "while I'm here" cleanups;
- no behaviour change of any kind. If a change would alter what any existing fixture
  produces, it is out of policy — stop and escalate rather than proceed.

**Every** change under (a), (b), or (c) adds a row to [§8](#8-freeze-changelog) in the same
commit. A change to `legacy/` without a changelog row is a policy violation regardless of how
small it is.

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
version per semver-compatible range across the workspace. Most of legacy's pins are on crates
the live root crate also uses (`csv`, `env_logger`, `log`, `tracing`, `tracing-subscriber`,
`parking_lot`, `petgraph`, `rand`, `serde`, `serde_json`, `thiserror`, `toml`, `tempfile`,
`assert_cmd`, `predicates`). While the live crate stays inside the same range, legacy's pin is
the version everyone gets. This is intended: it makes silent movement of a baseline dependency
impossible. When the live crate crosses a semver-major boundary (e.g. `rand` 0.10 → 0.11), the
two versions coexist and legacy simply keeps its frozen one. When a live-side bump *inside* the
range is genuinely needed, use change class **(c)** and re-run [§5](#standing-gates).

Legacy-exclusive pins — no live crate is affected by these: `indicatif`,
`indicatif-log-bridge`, `nexosim`, `rand_distr`, `tachyonix`, `futures`, `futures-executor`,
`once_cell`, `num_cpus`.

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
  effective concurrency configuration at `info`; `error` throws both away. The whole log for
  this fixture is 14 lines / ~1.4 KB, constant in fixture size. Keep the level identical across
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
  exactly what [§6.6](#66-mt-count-nondeterminism-disclosure-protocol) requires. `logs/` is
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
| `threading`, `effective_num_threads` | `Starting simulation with <mode> threading (N thread(s)).` |
| `hot_workers` | `Using N hot standby worker(s).` |
| `concurrency_level` | `Using <default\|accelerated> concurrency level.` |
| `sent_packets`, `sent_bytes` | **sum** of `sent_packets` / `packet_sizes` over `sources.csv` |
| `received_packets`, `received_bytes` | **sum** of `received_packets` / `received_sizes` over `sinks.csv` |
| `derived_dropped_packets` | `sent_packets − received_packets` (see below) |
| `max_source_end_time`, `duration` | max `end_time` over `sources.csv`; `duration` from the fixture |
| `sink_rows`, `source_rows` | row counts of each CSV |
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

### 6.6 MT count-nondeterminism disclosure protocol

Legacy MT is **not** a semantic oracle. Two facts are on record:

- **P01** (`days-gpu/evidence/P01/nexosim-baseline.md`): on duration-terminated fixtures, MT
  received-packet counts varied between samples (2 packets at k8/k16, 145 at k32), attributed to
  Nexosim MT interleaving **at the simulation stop boundary**. ST counts agreed exactly.
- **P11 slide preview** (`days-gpu/evidence/P11/slide-preview/slide-fourarm-preview.md`): the
  count nondeterminism did not manifest on the drop-free E3-class workload; the drop-heavy
  negative control is where legacy and Days AGO diverged (9.4%, from TailDrop packet-vs-byte
  admission semantics).

E3 is built so the boundary mechanism cannot fire: every flow is byte-terminated and completes
by ~16.0 s, two seconds before the 18 s horizon, so no packet is in flight when the simulation
stops. **This is an expectation to be tested per machine, not an assumption to be asserted.**

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

### 6.7 Comparability statement to carry into every legacy/AGO table

- TCP scenarios are comparable **modulo the documented `f64` divergences**; legacy timing uses
  `f64`, the executor uses exact integer arithmetic.
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

Both arms reproduced the frozen Days AGO anchor exactly. No number in this table may be cited,
compared, or plotted.

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
