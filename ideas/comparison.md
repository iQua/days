##### What is the baseline algorithm (SOTA) that this paper should compare LeanGuard with, for a fair comparison study?

--

Below is a concrete “apples-to-apples” comparison plan for **LeanGuard vs a TLA+-based trace-validation baseline**, using the **same Days traces** and (as much as possible) the **same notion of correctness**.

I’m going to treat the baseline as:

* **TLC-based trace validation** (the established TLA+ workflow), as described in the TLA+ trace-validation guidance and in recent research on trace validation with TLC. ([TLA+ trace validation guide][tla-trace-validation])
* And I’ll call out **TraceLink** as the “SOTA+” version of this line of work (automated mapping + causal-trace support), even if you don’t fully reimplement TraceLink for Days. ([TraceLink][tracelink])

---

## 1) What “fair” should mean in this comparison

To keep the study fair, you want to control two big confounders:

### A. Equal information given to both checkers

LeanGuard’s checkers typically consume a trace row that already contains **the “witness” post-state snapshot** (e.g., alpha/rate/etc. in DCQCN). A lot of TLA+ trace-validation work can operate on *partial* traces and let TLC infer missing values (which changes both logging cost and checker power). ([Merz 2024 trace-validation slides][merz-trace-validation-slides])

So, define two modes, but make **Mode 1** the core “fair” one:

* **Mode 1 (Equal-information / Deterministic replay):** Feed the TLA+ validator the *same per-event post-state fields* that LeanGuard gets, and require equality checks against the spec’s computed next-state.
* **Mode 2 (Optional / TLA+ advantage mode):** Reduce logging and let TLC infer missing state (if you want to demonstrate the tradeoff space, but don’t mix it into the main “who’s faster / who’s easier” claim).

### B. Equal trace ordering assumptions

LeanGuard already has a notion of **canonicalization** (e.g., sorting by `(time_ns, event_id)` and requiring uniqueness) to linearize the trace for deterministic checking.

TLC trace validation also consumes a **sequence** of steps, so you should:

* use the *same canonicalization rule* for both pipelines, or
* explicitly evaluate robustness to reordering as a separate experiment.

---

## 2) Baseline definition you should implement

### Baseline (recommended): **Manual TLC trace validation**

This is “classic” trace validation with TLA+/TLC: the trace is ingested, and TLC checks that each recorded step corresponds to a valid spec step (and optionally checks state equality against logged snapshots). This is exactly the “manual mapping” the TraceLink paper describes as the status quo in TLA+ trace validation.

It’s also aligned with the general “trace validation with TLC” workflow described on the TLA+ trace-validation guidance page.

### SOTA baseline (optional, if you want to claim “best known”): **TraceLink-style trace validation**

TraceLink is explicitly positioned as a **push-button** approach that “automatically maps a trace … to a formal model,” and supports causal tracing / multiple interpretations. ([TraceLink][tracelink])

But: TraceLink’s automation relies heavily on the PGo/MPCal toolchain integration, so for Days you’d likely be re-implementing *ideas* rather than using their tooling directly.
For a LeanGuard paper comparison, I’d phrase it as:

* **Primary baseline:** manual TLC trace validation (fair + portable)
* **Stretch baseline:** TraceLink-style automation (fairness caveat: it adds automation LeanGuard may not aim to provide)

---

## 3) What you need to build for an apples-to-apples evaluation

### Step 3.1 — A shared trace artifact and canonicalization pipeline

**Input:** Days already produces `*_events.csv` (one per component/algorithm).
**Goal:** produce a canonical representation that both LeanGuard and the TLC-based baseline will consume.

Concretely:

1. **Read each `*_events.csv`**
2. **Canonicalize order** (exactly the same as LeanGuard’s expectation):

   * Sort by `(time_ns, event_id)`
   * Check uniqueness (reject duplicates)
3. **Convert to a TLC-friendly representation**

   Some TLC builds used in trace-validation research can internalize JSON/NDJSON traces into **TLA+ values** (e.g., `Trace == ndJsonDeserialize(...)` via the `Json`/`IOUtils` modules). However, released `tla2tools.jar` versions do not reliably ship these modules, and TLC’s integer arithmetic is practically limited to 32-bit signed integers. In practice, a reproducible baseline often needs a generated trace module and (for Days) a rescaling step for large fields like `*_bps` and `*_ns`.

   * **Option A (recommended for reproducibility):** export a generated `.tla` module (or a `.cfg` constant) that defines `Trace` as a sequence of records.
   * **Option B (if your TLC build supports it):** export JSON/NDJSON (one object per row) and let TLC deserialize it (as in trace-validation research). ([trace validation paper][traceval-arxiv])

Example NDJSON line (Option B):

```json
{"time_ns":123456,"event_id":17,"kind":"cnp_recv","endpoint_id":0,"flow_id":1,"alpha_ppb":12000,"rate_bps":5000000000,"cnp_seen":true,"last_cnp_ns":123400}
```

> Fairness note: This conversion is “infrastructure,” not advantage to either side, because both can consume the same canonicalized trace.

---

### Step 3.2 — A generic TLA+ trace-validation harness (reused across protocols)

Create a small reusable TLA+ module that:

* loads a trace into a constant/variable `Trace`
* maintains an index `l` (line number / step index)
* at each step, reads `e == Trace[l]` and applies a spec action based on `e.kind`
* increments `l`

This is the “generic setup” style demonstrated in practical TLA+ trace-validation material. ([TLA+ trace validation guide][tla-trace-validation])

Pseudo-skeleton (illustrative):

```tla
VARIABLES state, l

Init ==
  /\ l = 1
  /\ state = InitState

Next ==
  /\ l <= Len(Trace)
  /\ LET e == Trace[l] IN
       CASE e.kind = "CnpRecv"   -> CnpRecv(e)
          [] e.kind = "TimerTick"-> TimerTick(e)
          [] OTHER               -> FALSE
  /\ l' = l + 1
```

**Key point for fairness:** In Mode 1 (equal-information), `CnpRecv(e)` does **two** things:

1. compute the spec’s next-state from the previous state and event inputs
2. assert that the resulting state matches the **post-state fields logged in `e`**

That’s exactly analogous to LeanGuard’s “post-state snapshot equality” checks.

---

### Step 3.3 — Protocol-specific TLA+ “step” operators that mirror LeanGuard semantics

You need a TLA+ spec per algorithm/protocol you evaluate, but for a fair paper study you can do it in tiers:

#### Tier 1 (must-have): Implement **DCQCN** trace validation

Because LeanGuard’s paper and trace schema examples emphasize DCQCN-like control logic, it’s the most defensible single baseline target.

For DCQCN, define TLA+ state variables that correspond to what your trace logs as post-state, e.g.:

* per flow or per endpoint:

  * `alpha_ppb`
  * `rate_bps`
  * `cnp_seen`
  * `last_cnp_ns`
  * frozen params: `g_ppb, mi_ppb, ai_rate_bps, hai_rate_bps, init_rate_bps, min_rate_bps, max_rate_bps`

Then implement step operators:

* `CnpSent(e)` — if you log gating/interval constraints at sinks, enforce them
* `CnpRecv(e)` — update alpha/rate according to the DCQCN math and compare
* `TimerTick(e)` — update periodic behavior and compare

Also include the same “trace hygiene” invariants LeanGuard enforces:

* monotone time per endpoint/flow
* frozen params don’t change
* optional: packet invariants, etc.

#### Tier 2 (nice-to-have): One scheduler (WFQ or DRR)

Pick **one** scheduler algorithm that LeanGuard checks and implement a TLA+ validator for it. This shows the baseline generalizes beyond one protocol.

#### Tier 3 (full suite): PFC, AQM, CUBIC, etc.

Do this only if the paper claims broad generality and you can afford the spec engineering.

---

### Step 3.4 — Make TLC execution “fair” and stable (don’t accidentally benchmark TLC badly)

A common pitfall is benchmarking TLC in a mode that’s not representative.

If your trace-check harness is deterministic (Mode 1), TLC’s explored state-space is essentially O(length(trace)).

To keep TLC overhead reasonable, use recommended configuration tricks for trace validation (e.g., DFS queue optimizations) as documented in the TLA+ trace validation guidance. ([TLA+ trace validation guide][tla-trace-validation])

This matters because otherwise you’re measuring “TLC default queue behavior” more than “TLA+ trace validation as practiced.”

---

## 4) Tooling integration with the Days/LeanGuard workflow

To make this a clean study, integrate baseline runs where LeanGuard already runs.

### A. One command that produces traces and runs both checkers

You want something like:

* run Days once → produce `log_path/*_events.csv`
* run LeanGuard checkers (existing)
* run TLC validator(s) on the exact same traces

### B. Results format parity

LeanGuard already emits a JSON summary (accept/reject, errors, etc.). The TLC baseline should emit a similarly shaped JSON blob:

* `accept: true/false`
* `first_failure: { trace_file, index=l, time_ns, event_id, kind }`
* `runtime_ms`, `peak_mem_mb` (if you can measure)
* `stdout/stderr` excerpts for diagnosis

This is essential to compare:

* speed
* diagnostic quality
* agreement/disagreement

---

## 5) What to report (metrics) so the comparison is actually meaningful

A fair comparison paper section usually needs **at least**:

### 1) Correctness agreement

* For each scenario, do LeanGuard and the TLC baseline both accept?
* For each injected bug, do they both reject?
* Do they pinpoint the same first bad step?

### 2) Checker performance

* time to validate trace
* memory usage (or at least JVM max heap)

### 3) Logging burden

* trace size on disk
* number of logged fields / rows
* runtime overhead of instrumentation (run Days with tracing off vs on)

### 4) Engineering effort

You can’t ignore this in a tool paper:

* LoC of spec + mapping + trace conversion for TLC baseline
* LoC of Lean checkers (or number of invariants/operators)
* how many places need instrumentation changes

### 5) Robustness to ordering / concurrency artifacts

If Days can produce nondeterministic ordering, measure:

* without canonicalization, how fragile is each approach?
* with canonicalization, do both become stable?

If you want to bring in TraceLink ideas here, TraceLink explicitly addresses causal tracing / multiple allowed interpretations. ([TraceLink][tracelink])
But again: treat that as a “SOTA+” discussion unless you truly implement it.

---

## 6) Suggested “minimal but credible” experiment plan

If you want a baseline that’s publishable-quality without exploding effort:

1. **DCQCN only**, but across many configs/seeds
2. Evaluate:

   * runtime of LeanGuard checker vs TLC validator on identical traces
   * trace size
   * injected-fault detection and counterexample quality

Then add 1 extra protocol/scheduler if you need breadth.

---

## 7) The key design choice that makes it apples-to-apples

If you do only one thing to keep it fair, do this:

**Force the TLC baseline to validate the same witness fields LeanGuard validates**, rather than letting TLC “fill in” state you didn’t log.

That directly compares:

* LeanGuard’s executable checker approach
* vs. TLC’s spec-based trace replay approach

…and avoids “TLA+ got an advantage because it didn’t require logging X” arguments. (You can still report that as a separate experiment, but don’t mix it into the main head-to-head.) ([Merz 2024 trace-validation slides][merz-trace-validation-slides])

---

If you want, I can turn the above into a concrete checklist keyed to your repo layout (e.g., “add `tla/` directory with `TraceHarness.tla`, add a `trace_convert` tool, add a `--baseline` flag to `leanguard-run` / `leanguard-testgen`, and define the JSON schema for baseline results”), but the core outline above is the conceptual minimum that makes the study fair and defensible.


---

What is the classic trace validation with TLA+/TLC, how should it be implemented in the Days simulator?

---

“TLA+/TLC trace validation” is the classic *model-based* way to check that a concrete execution produced by an implementation is consistent with a TLA+ spec—without proving the whole implementation correct.

It’s a good baseline to compare LeanGuard against because it’s the best-known “trusted checker over untrusted execution” workflow in the formal methods community, and it’s used in serious systems (e.g., Microsoft’s CCF). ([TLA+ trace validation guide][tla-trace-validation])

Below is what it is (algorithmically), and how I’d implement it **specifically for Days** given your current trace + CLI architecture.

---

## What “trace validation” with TLA+/TLC is

### The core idea

You have:

* A **TLA+ specification** `Spec` with `Init` and `Next` (and maybe invariants).
* A **finite execution trace** `tr` from the implementation.

You then build a *trace-constrained* TLA+ model (often called a **trace spec**) that says:

> “There exists a behavior of the spec whose steps match the observed trace.”

TLC then does *constrained exploration*: it attempts to find *some* behavior consistent with the trace. If it can, you accept the trace; if it cannot, it rejects (and can give a counterexample prefix / last consistent state).

This is exactly the philosophy you summarized in the paper’s generation section: checker is an oracle + diagnostics, and traces are certificates—except TLC can allow partial observation (missing variables, unknown internal actions), which turns checking into search. ([trace validation paper][traceval-arxiv])

### What’s “classic” about it

Two “classic” patterns exist:

1. **Fully observed step checking (deterministic-ish)**
   The trace records enough to determine the next state (or at least the updated portion), so TLC mostly just checks that each logged transition is allowed by `Next`. This looks closer to LeanGuard’s replay, but expressed as TLA+ actions.

2. **Partially observed trace validation (search-based)**
   The trace records only *some* variables and/or event labels. TLC must search over:

   * unobserved variables,
   * unobserved internal actions / stuttering steps,
   * and sometimes different action interleavings.

This is powerful but can explode combinatorially if the trace is too “thin”; the literature explicitly warns that “less information in the trace increases nondeterminism and may lead to combinatorial explosion,” and recommends strategies like **DFS bounded by the trace length** to find a matching behavior quickly. ([trace validation paper][traceval-arxiv])

### How TLC is used in trace validation

There are two main operational setups:

* **Encode the trace as a constant / sequence** in the TLC model, then model check a “TraceSpec” that walks through it (works but can be awkward/large).
* **Read the trace from a file** and internalize it into TLA+ values (often NDJSON via `ndJsonDeserialize` in some TLC builds), or fall back to generating a `TraceData.tla` module that defines `Trace == << ... >>`. ([trace validation paper][traceval-arxiv])

---

## How this maps to Days today

Days already has most of what a TLC trace-validation pipeline needs:

* Deterministic **canonical ordering** keys in traces (`time_ns`, `event_id`) and a philosophy of canonicalize+check.
* Per-protocol **event traces** (`*_events.csv`) already designed around “one row per semantic transition.”
* A **trace manifest** (`traces.json`) for discovery and a unified runner (`leanguard-run`) that selects checkers based on emitted traces.
  (So you already have “trace discovery + driver” infrastructure that a TLC baseline can reuse.)

That puts Days in an unusually good position to implement trace validation in a clean, comparable way.

---

## What to implement in Days: a concrete design

### High-level architecture

Add a parallel “baseline checker” path next to LeanGuard:

```
Days run → *_events.csv (+ traces.json)
        → (export) → TLC trace input (NDJSON)
        → TLC validates against TLA+ spec
        → JSON summary alongside LeanGuard results
```

This mirrors your existing pipeline:

* `leanguard-run`: discovers traces and dispatches checkers
* `leanguard-testgen`: generates configs and runs `leanguard-run`

You’d extend those to optionally also run TLC validation.

---

## Step 1: Decide what the TLC baseline will validate

You have two viable baselines; pick based on “fairness” vs “engineering cost.”

### Baseline A (closest to LeanGuard): **fully observed / replay-like**

Use the existing `*_events.csv` as “witness snapshots” and check:

* For each row, the corresponding TLA+ action is enabled,
* and the *post-state snapshot* in the row matches the TLA+ action’s computed post-state.

This makes TLC behave more like a step checker (less search), which is likely the fairest “apples-to-apples” against LeanGuard.

### Baseline B (more “classic TLC trace validation”): **partially observed**

Only constrain TLC using:

* event names (`kind`), ids (`endpoint_id`, `flow_id`, …), and maybe a few key state vars,
* let the rest of spec variables be inferred by TLC search.

This is “closer” to the general trace validation literature, but can become infeasible quickly unless traces are very informative (and for fairness you’d need to argue why you chose a given observation level). ([trace validation paper][traceval-arxiv])

**Recommendation for Days:** Start with Baseline A for a fair comparison and practical runtimes; optionally report Baseline B as a secondary experiment showing the tradeoff between trace richness and validator complexity.

---

## Step 2: Define the trace format TLC will consume

TLC needs the trace represented as **TLA+ values** (records/sequences). NDJSON deserialization exists in some TLC builds used by trace-validation work, but it is not reliably available in released `tla2tools.jar` distributions, and TLC is practically limited to 32-bit integers. So pick either:

* **Option A (recommended for reproducibility):** export a generated TLA module/config that defines `Trace == << ... >>` as a sequence of records.
* **Option B (if supported in your TLC build):** export JSON/NDJSON (one object per step) and let TLC deserialize it (e.g., via `ndJsonDeserialize`). ([trace validation paper][traceval-arxiv])

If you choose Option B, for each Days trace row emit something like:

```json
{
  "time_ns": 12345,
  "event_id": 77,
  "kind": "cnp_recv",
  "scope": {"endpoint_id": 2, "flow_id": 9},
  "params": {... stable protocol params ...},
  "post": {... post-state snapshot fields ...}
}
```

Notes:

* If you use a released TLC jar, you may need to **rescale** large numeric fields (e.g., `*_bps`, `*_ns`) to fit TLC’s integer limits, and (depending on your scaling) allow small tolerances for snapshot equality.
* Preserve canonical order (sort by `(time_ns, event_id)` before exporting), or export in that order directly.

### Where to implement the exporter

You have two choices:

1. **Offline conversion tool**: a new Rust binary in `src/bin/`, e.g. `days-trace-export`, that converts `*_events.csv` → `*.ndjson`.
   Pro: doesn’t touch simulation; easy to iterate.
   Con: adds a conversion step.

2. **Emit JSON/NDJSON directly** from the logger path when a `tlc_trace = true` config flag is set.
   Pro: simpler pipeline, fewer moving parts.
   Con: touches logger & feature gates.

Given your existing CSV-based ecosystem (LeanGuard, plotting, etc.), I’d do **(1) first** and only switch to (2) if it’s worth it.

---

## Step 3: Write TLA+ specs that correspond to the Days traces

You need a TLA+ module per trace family:

* `DcqcnTrace.tla` (DCQCN baseline; in this repo it includes both the trace harness and the per-step reference semantics)
* `PFC.tla`
* `WFQ.tla`
* `DRR.tla`
* `AQM.tla`
* (optional) a cross-layer `AQM_DCQCN.tla`

These specs are the baseline’s “reference semantics,” analogous to your Lean semantics.

### Key modeling choice for Days: state as maps keyed by IDs

Your traces are naturally per-component:

* DCQCN: `(endpoint_id, flow_id)` updates at sources/sinks
* PFC: `(switch_port_id, prio)` or `(link_id, prio)`
* schedulers: `(switch_id, class_id)` or similar

In TLA+, represent global state as functions/maps:

* `SrcState ∈ [EndpointId × FlowId -> SrcRec]`
* `SchedState ∈ [SwitchId -> SchedRec]`
* etc.

Then each trace step updates only a small slice via `EXCEPT`.

This is exactly the kind of “component-local state + global replay loop” structure your LeanGuard design describes too; it translates cleanly.

---

## Step 4: Write the *trace wrapper* module (the heart of “trace validation”)

For each protocol, create a trace-validation module that:

1. Loads the trace (`Trace`) via a generated TLA module/constant (and optionally via NDJSON ingestion if your TLC build supports it).
2. Introduces a trace pointer variable `i`.
3. Defines `Next` to enforce that the `i`‑th trace entry corresponds to a valid step of the spec.

In this repo, we currently keep this as **one module per protocol** (e.g., `tla/DcqcnTrace.tla`) plus a `.cfg` file
that sets `SPECIFICATION TraceSpec` and disables deadlock checking (`CHECK_DEADLOCK FALSE`). Acceptance is determined by whether TLC finds a path that advances `i` through the full trace (practically: via TLC’s reported depth/diameter for the explored state graph).

### Skeleton (DCQCN example)

At a high level:

* `Init`: sets `i = 1`, initializes any needed state (or leaves some unconstrained, depending on baseline).

* `Next`:

  * reads `e = Trace[i]`,
  * dispatches on `e.kind`,
  * enforces corresponding action,
  * enforces snapshot equality constraints based on `e.post`,
  * increments `i`.

* `TraceAccepted`: `i = Len(Trace) + 1` (or equivalent), meaning all steps were validated.

This is the standard “walk the trace and constrain `Next`” approach described in trace validation literature and practice. ([TLA+ trace validation guide][tla-trace-validation])

### Handling “partial observation” if you want Baseline B

Instead of equating the entire post-state, you only equate observed fields and let TLC infer the rest. But that’s where state explosion can happen, and you’ll want:

* strong constraints (log more),
* and/or DFS bounded by trace length. ([trace validation paper][traceval-arxiv])

---

## Step 5: Integrate TLC into the existing Days CLI workflow

### Option 1: Extend `leanguard-run`

Add flags like:

* `--tlc-check`
* `--tlc-spec-dir tla/` (where the `.tla` files live)
* `--tlc-bin <path>` (optional; run a wrapper executable directly)
* `--tlc-jar <path>` (path to `tla2tools.jar`, if not using `--tlc-bin`)
* `--tlc-no-dfs` (disable the DFS queue optimization used for trace validation)

Flow:

1. Existing `discover_traces(log_path)` gives you the list of trace CSVs (via manifest or scan).
2. For each trace type:

   * export its CSV → a generated `TraceData.tla` module (and optionally also export a lossless `*.ndjson` for diagnostics)
   * invoke TLC with the correct `*.tla` module + `.cfg` model config in a workspace where `TraceData.tla` is visible
3. Record results in the same JSON summary schema you already output for LeanGuard checkers (Accept/Reject/Error + stdout/stderr).

This keeps the “one entrypoint per scenario” design you already have with `leanguard-run`.

### Option 2: New binary `tlc-run`

Mirror `leanguard-run` but dispatch to TLC. Then optionally have `leanguard-run` invoke it.

Either is fine; Option 1 is less plumbing.

### How to run TLC in practice

CCF’s docs show a practical pattern: a wrapper script (`tlc.py`) that runs TLC with a specific trace file as input for validation. ([trace validation paper][traceval-arxiv])

For Days, you can do the equivalent:

* Have `leanguard-run` call something like (Option A / generated trace module):

  * `java -Dtlc2.tool.queue.IStateQueue=StateDeque -cp path/to/tla2tools.jar tlc2.TLC -config <workspace>/DcqcnTrace.cfg <workspace>/DcqcnTrace.tla`
  * where `<workspace>/` contains `DcqcnTrace.tla`, `DcqcnTrace.cfg`, and a generated `TraceData.tla`.
* Parse TLC’s exit code / output to decide Accept vs Reject.

---

## Step 6: Make it comparable to LeanGuard

To be a fair comparison study, align the interfaces:

* **Same trace source:** the exact `*_events.csv` Days already emits (canonicalized), with a single conversion step for the baseline (e.g., generate `TraceData.tla` for TLC, and optionally also export lossless NDJSON for diagnostics).
* **Same canonicalization order:** use `(time_ns, event_id)` ordering before feeding to TLC (since LeanGuard canonicalizes too).
* **Same notion of “acceptance”:** for Baseline A, acceptance means “every logged step matches the reference semantics and snapshot.”
* **Same determinism discipline:** run with `threading="single"` for both baselines by default (you already enforce this in `leanguard-run` unless `--allow-nondeterministic`).

---

## Step 7: What you’ll learn (and what will likely happen)

### Expected strengths of TLC trace validation baseline

* It’s a recognized methodology and gives you a credible “SOTA baseline” narrative.
* It can naturally handle:

  * missing/unobserved variables,
  * stuttering/internal actions,
  * nondeterminism due to partial logging,
  * some degrees of concurrency/interleaving (especially when trace is an observation, not full state). ([TLA+ trace validation guide][tla-trace-validation])

### Expected pain points (where LeanGuard will likely look better)

* If you log less than full state, TLC can blow up quickly; the literature explicitly documents state explosion as trace precision decreases. ([trace validation paper][traceval-arxiv])
* Getting good diagnostics can be harder than a purpose-built “first failing row” replay checker, because TLC’s counterexamples are model-checking counterexamples (though you can structure them to point to the offending trace index).
* Implementing protocol arithmetic carefully in TLA+ (fixed-point) is work you already did in Lean.

---

## Minimal “Days implementation recipe” in 10 concrete bullets

1. Add `tla/` directory to the repo with:

   * `DcqcnTrace.tla` + `DcqcnTrace.cfg` (and additional modules/configs for other protocols).
2. Add `src/bin/days-trace-export.rs` that:

   * reads `*_events.csv`,
   * sorts rows by `(time_ns, event_id)` (if needed),
   * writes `*.ndjson`.
3. In `leanguard-run`, add `--tlc-check` flag.
4. In `leanguard-run::select_checkers`, add a parallel `select_tlc_specs(traces)` mapping trace files → TLA modules.
5. Implement `run_tlc(spec, trace_ndjson)` similar to `run_checker` (captures stdout/stderr, exit code).
6. Extend the JSON `RunSummaryV1` to include `tlc_results` (mirroring `checker_results`).
7. Add a test like `days/tests/leanguard_run.rs` but stubbing a TLC runner, ensuring manifest-based selection works.
8. Start with DCQCN only (lowest surface area; already trace-rich).
9. Add WFQ/DRR next (they’re local and map well to TLA+ actions).
10. Only then attempt AQM or cross-layer AQM+DCQCN (more coupling, more schema work).

---

If you want, I can also sketch what the **DCQCN trace wrapper** should look like in TLA+ in more detail (variables, action definitions, and exactly how to use the post-state snapshot fields you already log), but the blueprint above is the “classic TLC trace validation” story and the most direct way to implement it cleanly in Days.

[tla-trace-validation]: https://docs.tlapl.us/using%3Atlc%3Atrace_validation "using:tlc:trace_validation - TLA+ Wiki"
[tracelink]: https://fhackett.com/files/oopsla25-tracelink.pdf "TraceLinking Implementations with Their Verified Designs"
[merz-trace-validation-slides]: https://conf.tlapl.us/2024-fm/slides-merz.pdf "Validating Traces of Distributed Systems Against [+]Specifications"
[traceval-arxiv]: https://arxiv.org/html/2404.16075v2 "Validating Traces of Distributed Programs Against TLA+ Specifications"
