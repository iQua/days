##### What is the baseline algorithm (SOTA) that this paper should compare LeanGuard with, for a fair comparison study?

--

Below is a concrete “apples-to-apples” comparison plan for **LeanGuard vs a TLA+-based trace-validation baseline**, using the **same Days traces** and (as much as possible) the **same notion of correctness**.

I’m going to treat the baseline as:

* **TLC-based trace validation** (the established TLA+ workflow), as described in the TLA+ trace-validation guidance and in recent research on trace validation with TLC. ([docs.tlapl.us][1])
* And I’ll call out **TraceLink** as the “SOTA+” version of this line of work (automated mapping + causal-trace support), even if you don’t fully reimplement TraceLink for Days. ([fhackett.com][2])

---

## 1) What “fair” should mean in this comparison

To keep the study fair, you want to control two big confounders:

### A. Equal information given to both checkers

LeanGuard’s checkers typically consume a trace row that already contains **the “witness” post-state snapshot** (e.g., alpha/rate/etc. in DCQCN). A lot of TLA+ trace-validation work can operate on *partial* traces and let TLC infer missing values (which changes both logging cost and checker power). ([conf.tlapl.us][3])

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

This is “classic” trace validation with TLA+/TLC: the trace is ingested, and TLC checks that each recorded step corresponds to a valid spec step (and optionally checks state equality against logged snapshots). This is exactly the “manual mapping” the TraceLink paper describes as the status quo in TLA+ trace validation priorandroid approachproduction.stüt

[1]: https://docs.tlapl.us/using%3Atlc%3Atrace_validation?utm_source=chatgpt.com "using:tlc:trace_validation - TLA+ Wiki"
[2]: https://fhackett.com/files/oopsla25-tracelink.pdf "TraceLinking Implementations with Their Verified Designs"
[3]: https://conf.tlapl.us/2024-fm/slides-merz.pdf "Validating Traces of Distributed Systems Against [+]Specifications"


It’s also aligned with the general “trace validation with TLC” workflow described on the TLA+ trace-validation guidance page.

### SOTA baseline (optional, if you want to claim “best known”): **TraceLink-style trace validation**

TraceLink is explicitly positioned as a **push-button** approach that “automatically maps a trace … to a formal model,” supports causal tracing / multiple in([docs.tlapl.us][1]) 2025.

But: TraceLink’s automation relies heavily on the PGo/MPCal toolchain integration, so for Days you’d likely be re-implementing *ideas* rather than using Tr([fhackett.com][2])
For a LeanGuard paper comparison, I’d phrase it as:

* **Primary baseline:** manual TLC trace validation (fair + portable)
* **Stretch baseline:** TraceLink-style automation (fairness caveat: it adds automation LeanGuard may not aim to provide)

---

## 3) What you need to build for an apples-to-apples evaluation

### Step 3.1 — A shared trace artifact and canonicalization pipeline

**Input:** Days already produces `*_events.csv` (one per component/algorithm).
**Goa([conf.tlapl.us][3])canonical representation that both LeanGuard and TLC-based baseline will consume.

Concretely:

1. **Read each `*_events.csv`**
2. **Canonicalize order** (exactly the same as LeanGuard’s expectation):

   * Sort by `(time_ns, event_id)`
   * Check uniqueness (reject duplicates)
3. **Convert to TLC-friendly format**

   * The “standard” recent setup is **NDJSON** (one JSON object per line), because it plays well with TLC’s IO/JSON utilities shown in practical trace-validation setups. ([docs.tlapl.us][4])

Example output line (NDJSON):

```json
{"time_ns":123456,"event_id":17,"kind":"CnpRecv","endpoint_id":0,"flow_id":1,"alpha_ppb":12000,"rate_bps":5000000000,"cnp_seen":true,"last_cnp_ns":123400,...}
```

Why NDJSON? Because many TLC trace-validation “load trace” harnesses are built around deserializing JSON/NDJSON into a sequence of records. ([docs.tlapl.us][4])

> Fairness note: This conversion is “infrastructure,” not advantage to either side, because both can consume the same canonicalized trace.

---

### Step 3.2 — A generic TLA+ trace-validation harness (reused across protocols)

Create a small reusable TLA+ module that:

* loads a trace into a constant/variable `Trace`
* maintains an index `l` (line number / step index)
* at each step, reads `e == Trace[l]` and applies a spec action based on `e.kind`
* increments `l`

This is the “generic setup” style demonstrated in practical TLA+ trace-validation material. ([docs.tlapl.us][4])

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

To keep TLC overhead reasonable, use recommended configuration tricks for trace validation (e.g., DFS queue optimizations) as documented in the TLA+ trace validation guidance. ([docs.tlapl.us][1])

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

If you want to bring in TraceLink ideas here, TraceLink explicitly addresses causal tracing / multiple allowed interpretations. ([fhackett.com][2])
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

…and avoids “TLA+ got an advantage because it didn’t require logging X” arguments. (You can still report that as a separate experiment, but don’t mix it into the main head-to-head.) ([conf.tlapl.us][3])

---

If you want, I can turn the above into a concrete checklist keyed to your repo layout (e.g., “add `tla/` directory with `TraceHarness.tla`, add a `trace_convert` tool, add a `--baseline` flag to `leanguard-run` / `leanguard-testgen`, and define the JSON schema for baseline results”), but the core outline above is the conceptual minimum that makes the study fair and defensible.

[1]: https://docs.tlapl.us/using%3Atlc%3Atrace_validation?utm_source=chatgpt.com "using:tlc:trace_validation - TLA+ Wiki"
[2]: https://fhackett.com/files/oopsla25-tracelink.pdf "TraceLinking Implementations with Their Verified Designs"
[3]: https://conf.tlapl.us/2024-fm/slides-merz.pdf "Validating Traces of Distributed Systems Against [+]Specifications"
[4]: https://docs.tlapl.us/using%3Atlc%3Atrace_validation "using:tlc:trace_validation - TLA+ Wiki"
