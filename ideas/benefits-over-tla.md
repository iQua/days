### What should I expect to see as benefits for LeanGuard over the alternative TLA+/TLC approach?

---

If you build a “classic” TLA+/TLC trace-validation baseline for Days, you should still expect LeanGuard to look better on several axes that matter for *CI-grade, protocol-level conformance checking*—especially given the way Days already emits per-transition, replay-oriented traces.

Here are the main benefits you should reasonably expect to see, and why they follow directly from LeanGuard’s design (canonicalize → deterministic replay → first counterexample) versus “trace validation with TLC” (a model checker constrained by a trace, often existential / search-based).

## 1) Predictable performance: replay is linear-time; TLC can become search

**LeanGuard expectation**

* Runtime is **O(n log n)** due to canonicalization (sorting by `(time_ns, event_id)`), plus **O(n)** deterministic replay with low constant factors (including snapshot equality).
* This is exactly the “trace-certificate” design: the trace is intended to carry enough information (witnesses + post-state snapshots) so the checker doesn’t need to guess anything.

**TLA+/TLC baseline expectation**

* If you force the TLC baseline into a *replay-like* mode (i.e., you fully constrain the next state using logged post-state fields), TLC can also behave quasi-linearly—but you’re still paying:

  * TLC’s state representation overhead,
  * Java/JVM overhead,
  * and the general-purpose model-checking machinery.
* If you let TLC do “classic” partially observed trace validation (infer missing state, allow internal actions), you risk **combinatorial blowups** (search over unobserved values/actions/interleavings). This is a known tradeoff: TLC trace validation gets harder as traces become less constraining.

**What you should see in measurements**

* LeanGuard is likely to have **lower time/event** and **lower memory** for the same trace length when both are given the same information, especially on longer traces or many tests in CI.

## 2) Better failure localization: first bad row vs model-checker counterexample

**LeanGuard expectation**

* When a trace is rejected, LeanGuard is explicitly built to produce the **first violating row in canonical replay order**, with:

  * the exact kind,
  * the exact required-field violation / gate violation / mismatch,
  * and usually “expected vs observed” for snapshot equality.
* That “first counterexample” property is central to your paper’s debugging pitch.

**TLA+/TLC baseline expectation**

* TLC will produce a counterexample *behavior* (state/action sequence) showing where the spec can’t follow the trace constraints.
* You can engineer it to point to trace index `i`, but in practice:

  * TLC counterexamples are often **less directly aligned to “the offending row”** unless you build a lot of scaffolding.
  * When the baseline allows internal steps or inference, TLC may fail “later” or in a more global way (“no behavior satisfies all constraints”), which can be harder to map back to one logged transition.

**What you should see**

* In injected-fault studies, LeanGuard should give **more actionable diagnostics with less extra work**, and failures should be easier to interpret as “this specific logged transition is inconsistent with the reference semantics.”

## 3) Stronger “certificate” meaning: LeanGuard ACCEPT is constructive and deterministic

**LeanGuard expectation**

* ACCEPT means: “after canonicalization, every logged transition is a legal step of the executable reference semantics, and post-state snapshots match exactly.”
* This is *constructive*: the trace itself is the witness, and replay is deterministic.

**TLA+/TLC baseline expectation**

* Classic trace validation is often **existential**:

  * “there exists some spec behavior consistent with the trace observations.”
* If you fully constrain state (replay-like), you can make it constructive. But that’s no longer what many people mean by “classic TLC trace validation”; it’s closer to using TLC as a heavy interpreter for a deterministic step relation.

**Why this matters**

* LeanGuard’s certificate story is cleaner: you can store accepted traces as regression artifacts and treat them as stable, deterministic evidence.
* In TLC’s more “classic” partially observed mode, acceptance can depend on inference choices/search and may be less transparent.

## 4) Smaller and cleaner trust boundary for conformance checking

**LeanGuard expectation**

* The trusted core is:

  * Lean kernel + your checker executable(s) + small parsing/canonicalization code.
* The checker is “the spec,” and you can (in principle) prove meta-theorems about it inside Lean.

**TLA+/TLC baseline expectation**

* The trusted core includes:

  * TLC implementation + JVM + your trace harness + any trace parsing layer.
* TLC is widely used and robust, but it is not a small kernel, and it’s not designed as a proof-producing checker.

**What you should claim carefully**

* Don’t oversell “TLC is untrusted so it’s bad.” It’s a very credible baseline.
* The realistic LeanGuard advantage is: **smaller, more auditable trusted base** for “ACCEPT means conformance.”

## 5) Cleaner handling of numeric determinism and rounding

**LeanGuard expectation**

* LeanGuard’s whole methodology pushes you toward:

  * integer encodings (ns, bps, ppb),
  * explicit rounding/encoding semantics,
  * exact equality checks.
* This tends to make numeric mismatches extremely crisp: you get the failing row and the differing integer values.

**TLA+/TLC baseline expectation**

* TLA+ *can* model integer encodings well, but:

  * implementing fixed-point arithmetic and matching the logging conventions can be more awkward,
  * practical TLC runs may require rescaling to fit TLC’s integer limits (and can therefore force small equality tolerances),
  * and TLC’s performance can suffer with heavy arithmetic on large integers/records if you’re not careful.

**What you should see**

* LeanGuard should be easier to keep “bit-for-bit aligned” with your logged encoding rules, and numeric bugs should be easier to diagnose as local mismatches.

## 6) Built-in semantic coverage hooks that plug into your Phase 4+ testgen loop

This is a big one **given your roadmap**.

**LeanGuard expectation**

* Your paper’s Phase 4 testgen pitch relies on the checker as a *coverage sensor* (“coverpoints” from replay).
* LeanGuard checkers are executable semantics, so adding deterministic coverage accumulation (bitsets/strings) is straightforward and cheap.
* Your existing `leanguard-testgen` already has a place to store coverage in metadata (it’s currently stubbed in `CoverageInfo`), so LeanGuard has a clear path to become coverage-guided in practice.

**TLA+/TLC baseline expectation**

* TLC doesn’t give you semantic-branch coverage “for free.”
* You can instrument the spec to emit coverage signals, but:

  * it’s not a standard workflow,
  * and it’s usually harder to structure into the exact “coverpoint set per row” feedback loop your generator wants.

**What you should see**

* LeanGuard should integrate much more naturally with *coverage-guided corpus growth*, especially for protocol-targeted generation (Phase 4) and micro-harnesses.

## 7) Better ergonomics for “many small tests” in CI

**LeanGuard expectation**

* Native checker binaries, consistent exit codes (0/1/2), and a runner (`leanguard-run`) that already:

  * discovers traces via `traces.json`,
  * selects relevant checkers automatically,
  * produces a single machine-readable JSON summary.
* This is exactly what you want for “run thousands of small tests routinely in CI.”

**TLA+/TLC baseline expectation**

* TLC-based validation can absolutely be CI’d, but it typically brings:

  * JVM dependencies,
  * model config management (`.cfg`, module constants),
  * and often more tuning to keep runtime stable.

**What you should see**

* Lower operational friction and lower per-test overhead with LeanGuard when scaling to large corpora.

## 8) An incremental path to “ACCEPT implies X” theorems is more direct in Lean

**LeanGuard expectation**

* Because the checker is in Lean, you can:

  * state invariants over the checker state,
  * prove preservation lemmas,
  * and tie acceptance to reusable theorem templates (pairing/gates/accounting/bounded state).
* That’s aligned with your paper’s “executable checker → theorems” storyline.

**TLA+/TLC baseline expectation**

* TLA+ has TLAPS and proof workflows, but TLC trace validation itself is not a proof system.
* Bridging “TLC accepted this trace” to a theorem-backed contract is possible, but it’s not the default mode and usually not as tight as “the checked program is the spec inside the proof assistant.”

**What you should see**

* LeanGuard will look stronger if you emphasize “certificate meaning” and “proof-backed consequences of ACCEPT.”

---

# Where TLC might actually look better (important to acknowledge)

To keep your comparison credible, explicitly acknowledge cases where TLC has *real* advantages:

* **Partial observability:** If you want to log *less* and allow the checker to infer missing state or tolerate unlogged internal actions, TLC can be more natural—at the cost of search.
* **High-level concurrency/spec abstraction:** TLA+ is excellent for specifying concurrency at a high level. LeanGuard’s approach avoids exploring interleavings by *defining canonicalization as part of the semantics* and by insisting on unique tie-breakers.

This makes the comparison sharper: LeanGuard is “fast, deterministic, certificate-grade conformance checking for fully logged semantic transitions,” while TLC trace validation is “more flexible about what is observed, but can pay with complexity.”

---

# The punchline you should expect your evaluation to support

If you implement a fair TLC baseline (same traces, same canonicalization, same post-state snapshot checks), LeanGuard’s likely headline advantages are:

* **faster and more predictable checking** at scale,
* **more actionable, row-level counterexamples**,
* **cleaner certificate meaning and smaller trusted base**,
* **much easier integration with semantic coverage and coverage-guided test generation**.

Those are exactly the dimensions your codebase (Days + `leanguard-run` + `leanguard-testgen`) is already structured to exploit.
