---
In `~/Playground/lean-paper/`, this is a new paper on using Lean to formally verify that a protocol implementation in a discrete-event network simulation, Days, strictly adheres to the protocol specification. It uses DCQCN + ECN as a case study. Identify potential space for improvements in this paper and devise a concrete plan.
---

Below are the biggest “upgrade levers” I see in the current draft, plus a concrete, step-by-step plan to address them. I’m focusing on changes that (a) materially strengthen the technical story and (b) make the claims crisper and harder to misread as stronger-than-supported “full verification.”

---

## 1) Tighten the central claim: “strict adherence” vs “execution validation”

### What’s currently strong

Your paper is internally consistent about the method: *Days emits a trace; Lean replays a model; output is ACCEPT/REJECT*. That’s solid.

### Improvement opportunity

A reader could easily interpret the title/abstract framing as “formal verification of the simulator’s protocol implementation,” but what you actually have is closer to **translation validation / runtime trace checking** of *a specific execution*.

You already say this in places (“LeanDays verifies executions”), but the paper should **make it impossible to misinterpret**:

* Explicitly define two properties:

  1. **Per-run trace consistency** (what you do now): “Given trace `tr`, checker says ACCEPT iff `tr` is a valid execution of the Lean transition system.”
  2. **Implementation refinement** (what you do *not* prove): “All traces emitted by Days are in `Accepts`.”

Right now you mention refinement as “desired correctness statement” but don’t emphasize the gap.

### Concrete edits

* In **Abstract + Introduction**: replace phrases that sound like end-to-end verification (“strictly adheres”) with “mechanized execution validation,” “trace-checked conformance,” or “certified executions.”
* Add a short paragraph early:
  “LeanDays does not prove that Days can only produce accepted traces; instead it makes deviations observable and actionable per run.”
* In **Design (acceptance)**: make the “trace refinement” statement a named definition + a clear disclaimer.

**Deliverable:** one “Claims & Non-claims” box (or bullet list) that reviewers can’t miss.

---

## 2) Make the “protocol specification” explicit and auditable

### Improvement opportunity

Right now, the Lean executable semantics *is effectively the spec*. That’s okay, but if you claim “adheres to protocol specification,” a reviewer will ask:

* What exactly is the DCQCN spec you’re adhering to?
* Where are the ambiguities resolved?
* How do we know the Lean model isn’t just mirroring the Rust implementation?

You already acknowledge “shared misinterpretation.” The paper can go further by making the spec boundary explicit.

### Concrete plan

1. Add a short **DCQCN spec excerpt** in the paper:

   * Provide pseudocode of the update rules and gating logic you implement (source CNP reaction, timer tick, sink CNP interval gate).
   * Point out which details are *assumptions/choices* (e.g., rounding, exact α threshold for switching AI/H-AI).
2. Add a “Spec-to-Trace mapping” subsection:

   * “Spec transition X corresponds to trace kind Y”
   * “State variables in spec map to columns …”
3. Add a micro-table: “Ambiguities and resolutions”

   * Example: “floating point rounding” → PPB encoding + round-to-nearest (specify exact rounding mode).

**Deliverable:** a “Spec mapping” table and a short pseudocode listing that defines the intended semantics independently of both Rust and Lean codebases.

---

## 3) Address ordering/interleaving rigorously (this is a big one)

### Why it matters

You explicitly say Days can run many coroutines concurrently and logs may interleave. But the checker is a **single left-to-right fold**. That means the checker’s acceptance currently depends on **file order**, not just timestamps.

This becomes a reviewer trap:

* If `cnp_recv` appears before the corresponding `cnp_sent` in the CSV due to I/O interleaving, your checker will reject, even if the execution is “correct.”
* You claim you *don’t rely on a global total order*, but your fold *does* rely on a particular order (the one in the file).

### Two viable fixes (pick one and be explicit)

#### Option A: Engineer a canonical total order for the trace

Make the trace inherently sortable / mergeable:

* Add a globally monotonic `event_id` from the simulator runtime, or
* Emit per-endpoint logs and merge deterministically offline, or
* Guarantee centralized logging that orders events by simulation time + tie-breakers.

Then the checker either:

* Sorts by `(time_ns, event_id)` (and documents this), or
* Requires the file to already be in canonical order.

**Pros:** simplest checker, simplest story.
**Cons:** you introduce and must justify a global ordering mechanism.

#### Option B: Define acceptance over partial orders (existence of a valid interleaving)

Keep “no total order” as a principle, but make it formal:

* Treat each endpoint’s events as a sequence with monotone time.
* Add cross-endpoint dependency edges (e.g., `cnp_sent(flow,pkt)` must precede `cnp_recv(flow,pkt)`).
* Define acceptance as: **there exists a topological order** consistent with these constraints such that folding `step` succeeds.

Implementation-wise, you can do an “online topological replay”:

* Maintain per-endpoint cursors.
* Maintain a ready set of next events whose prerequisites are satisfied.
* Repeatedly pick a ready event and step it; if stuck → reject with an explanation.

**Pros:** aligns with your narrative about not needing a global order; robust to file interleavings.
**Cons:** more complex checker and proof story.

**Deliverable:** one clear choice + a “Trace ordering semantics” paragraph that states what order is assumed/constructed and why that matches Days.

---

## 4) Strengthen the formal core: prove something *about* ACCEPT

### Improvement opportunity

Right now, the Lean checker is executable, but the paper only sketches a roadmap toward “executable model with proofs.”

You can substantially improve reviewer confidence by proving at least one nontrivial theorem now.

### Concrete proof targets that are realistic and valuable

1. **Soundness of the step function w.r.t. a declarative relation**

   * Define an inductive `StepRel : S → Row → S → Prop`.
   * Prove: `step s r = some s' → StepRel s r s'`.
2. **Invariants over accepted traces**

   * Examples that matter for DCQCN:

     * `0 ≤ alpha ≤ 1` (in PPB encoding bounds).
     * `min_rate ≤ rate ≤ max_rate`.
     * Per-endpoint parameter stability.
     * Monotone time per endpoint.
3. **Rounding determinism lemma**

   * State precisely the rounding function used for PPB and rate snapshots.
   * Prove basic properties (“range bounds,” “idempotence on already-rounded values,” etc.).

These proofs don’t require verifying Days; they certify that **ACCEPT implies a meaningful, stated safety contract**.

**Deliverable:** a new “Theorems” subsection with 2–4 lemmas and short proof sketches, plus (optionally) an appendix with statements.

---

## 5) Close the ECN/DCQCN loop: right now the network is “offstage”

### Current limitation

You explicitly say you don’t validate causes of congestion signals end-to-end (no switch marking events), so “DCQCN + ECN” is only partially checked: the end-host logic is checked; the *causal provenance* of CE marks is not.

### Concrete upgrade path

Add minimal extra vocabulary that makes ECN causality checkable without “verifying the whole network”:

1. **Switch-side ECN marking trace rows**

   * Log per packet: `(pkt_id, flow_id, time_ns, switch_id, queue_len, threshold, marked?)`.
2. **Packet receive at sink rows** (or enrich `cnp_sent`)

   * Ensure each `cnp_sent` references the triggering data packet ID and its CE bit.
3. Checker enforces:

   * `cnp_sent` implies there exists a prior sink data-packet receive with `CE=1`.
   * That CE was produced by a switch marking event consistent with configured threshold.

You can still keep the TCB narrow: you’re not verifying switch forwarding, just checking that *if Days claims ECN happened*, it’s justified by a logged marking decision.

**Deliverable:** one end-to-end property that connects switch marking → CE at sink → CNP generation.

---

## 6) Trace schema robustness and evolution

### Improvement opportunities

* Minimal CSV parser “assumes no quoted commas.” This is fine as an engineering shortcut, but reviewers will flag it unless you **either** (a) lock down the format as “no quoting by construction,” or (b) implement a more robust parser.
* Parameter repetition on every row inflates logs; you mention trace volume concerns.

### Concrete plan

1. Add `schema_version` and `checker_version` fields (header metadata or first row).
2. Add a formal “well-formed schema” definition:

   * required columns, allowed empties by `kind`, allowed enums.
3. Optional: split the trace into:

   * A small `params.json` (or CSV header extension) per endpoint/flow, and
   * A compact per-transition event file.
   * Or keep repetition but support compression transparently (gzip) and document it.

**Deliverable:** schema versioning + a forward-compat story (“older checkers reject newer schema explicitly”).

---

## 7) Evaluation: make it harder to dismiss as “toy”

Your current experiments demonstrate feasibility and basic scaling up to ~100k rows. That’s a good start. To make it reviewer-proof, add:

### A) Realistic topologies and workloads

* Multiple flows, multiple RTTs, multiple endpoints (you have `dcqcn_multi` but it’s still short).
* A run that looks like what people actually simulate with Days (even if you cap it for trace size).

### B) Overhead breakdown

Report:

* Simulation time (no trace)
* Simulation time (trace)
* Trace size on disk
* Checker time
* Peak memory (checker)
* Optional: gzip size (since traces compress well)

### C) Bug-finding beyond synthetic corruptions

Synthetic corruption is useful, but reviewers prefer:

* One real bug caught during development, or
* A “mutation testing” style evaluation:

  * Apply a small set of realistic code mutations (remove gate, flip inequality, off-by-one pkt_id, rounding change)
  * Show which mutations are caught and with what diagnostic.

### D) False positives/negatives story

* Under what conditions can a correct run be rejected? (ordering, missing rows)
* Under what conditions can an incorrect implementation still be accepted? (shared misinterpretation)

**Deliverable:** a “mutation study” table + overhead breakdown + at least one larger, realistic config.

---

## 8) Related work: add the “closest neighbors” explicitly

You already cite PCC, TLA+, runtime verification generally, and networking formal methods. What’s missing is explicitly naming the closest conceptual buckets so reviewers don’t do it for you:

* **Translation validation** (your own phrasing hints at it; make it a first-class framing).
* **Runtime verification / monitor synthesis** (automata monitors, trace checking frameworks).
* “Executable spec as oracle” style testing / differential testing.

**Deliverable:** 1–2 paragraphs + citations that make your positioning unambiguous.

---

# A concrete execution plan (prioritized, with deliverables)

Below is a practical plan you can follow without ballooning scope into “verify all of Days.”

## Step 1 — Paper-level clarity upgrades

* Add a “Claims & Non-claims” box (per-run validation vs full refinement).
* Add a DCQCN spec pseudocode listing + an “assumptions” bullet list.
* Add a “Spec ↔ Trace” mapping table.

**Output:** clearer scope, fewer reviewer misunderstandings.

## Step 2 — Ordering/interleaving decision + implementation

Pick Option A (canonical order) or Option B (partial-order acceptance), then:

* Update trace schema accordingly (`event_id` or dependency-friendly fields).
* Update checker and document the ordering semantics.

**Output:** correctness is defined independent of accidental file interleavings.

## Step 3 — First Lean theorems

* Define a declarative `StepRel`.
* Prove `step` soundness (and optionally completeness).
* Prove 2–4 invariants that you can advertise as the meaning of `ACCEPT`.

**Output:** `ACCEPT` becomes a *certificate of stated safety properties*, not just “the checker ran.”

## Step 4 — Add minimal ECN causality events

* Instrument switches to log ECN marking decisions (or the minimal evidence).
* Link `cnp_sent` to a concrete triggering CE-marked packet event.
* Add one end-to-end check: “CNPs are justified by CE marks that are justified by switch marking events.”

**Output:** “DCQCN + ECN” becomes real end-to-end trace-checked conformance, still without whole-system verification.

## Step 5 — Evaluation strengthening

* Add: overhead breakdown, memory, compression.
* Add: mutation study (realistic bug patterns).
* Add: at least one “realistic-ish” larger scenario.

**Output:** stronger empirical case that this belongs in a systems venue and is useful in practice.

## Step 6 — Usability/artifact polish

* Versioned schema, explicit error codes, better diagnostics (show expected vs got, include endpoint/flow).
* CI recipe: how to gate PRs on ACCEPT and store failing traces.

**Output:** “this is a tool people can actually use,” not just a prototype.

---

## If you want one “highest ROI” change

If you do only one major technical improvement: **fix/define trace ordering semantics (Step 3 above)**. It directly affects soundness, usability, and the credibility of the “interleavings” discussion—and reviewers will notice it immediately.

---

If you tell me whether you’d rather adopt **canonical ordering (Option A)** or **partial-order acceptance (Option B)**, I can sketch the exact checker algorithm and the minimal schema changes to support it, tailored to your current `pending CNP` mechanism and per-endpoint monotone-time rule.

---
Proceed to adopt Option A (the simulator has a globally shared simulation timestamp, which is in floating point 64-bit and can easily be used to sort the events), and sketch the exact checker algorithm and changes to support it.
---

Got it — Option A can be made clean and **fully deterministic** if we (1) define a **canonical total order** from the globally shared simulation time and (2) add a **tie-breaker** so that “same-time” events still have a well-defined order.

Below is (A) the exact checker algorithm (including canonicalization/sort) and (B) the concrete trace + implementation changes needed in both Days and the Lean checker.

---

## A. Canonical total order: what we will assume and enforce

### Canonical key

Define a canonical order key for each trace row:

[
\textsf{key}(r) := (\texttt{time_ns}(r), \texttt{event_id}(r)).
]

* `time_ns`: derived from Days’ global floating-point simulation timestamp (you already log this).
* `event_id`: a **globally unique, monotone** integer assigned at log emission (or, better, taken from the scheduler if Days has a sequence/tie-break counter).

**Why you still want `event_id`:**
Even with a global timestamp, it is common to have multiple transitions at exactly the same simulation time (timers, immediate reactions, same-time deliveries). Sorting by time alone leaves those events unordered and can cause spurious `cnp_recv without prior cnp_sent` just because the sort interleaved equal-time events arbitrarily.

So Option A should be: “Sort by global time, break ties by `event_id`.”

---

## B. Exact checker algorithm (two-phase)

### Phase 0: Parse + Canonicalize (sort)

**Inputs:** CSV file
**Output:** `rows_sorted : Array Row` or a parse error

**Algorithm (precise):**

1. **Parse header**

* Read CSV header row.
* Build `col : String → Nat` mapping column name to index.

2. **Parse each data line**
   For each line number `ℓ` (starting at 2 for first data row):

* Split by commas preserving empty fields (your existing parser behavior).
* Parse required common fields:

  * `time_ns : Nat`
  * `event_id : Nat` (**new required field**)
  * `kind : Kind`
  * `endpoint_id : Nat`
  * `flow_id : Nat`
  * plus the existing optional/required kind-specific fields.
* Construct a `Row` record with:

  * all parsed fields
  * `srcLine := ℓ` (original file line number for diagnostics)
* If any required parse fails: `REJECT: line ℓ: <parse error>`.

3. **Sort**
   Sort all rows by `key = (time_ns, event_id)` ascending.

4. **Validate key uniqueness and monotonicity**
   Scan the sorted array once:

* For i from 1..n-1:

  * require `key(rows[i-1]) < key(rows[i])` (strict)
  * if equal: `REJECT: duplicate key at lines L1 and L2: (time_ns=..., event_id=...)`

This guarantees:

* The canonical order is deterministic.
* There is no ambiguity among equal-time events.

> If you absolutely don’t want strictness, you *can* allow equal keys and rely on stable sort, but it reintroduces “depends on input file order” for ties. I strongly recommend strictness.

---

### Phase 1: Replay-check in canonical order

**Inputs:** `rows_sorted`
**Output:** `ACCEPT` or `REJECT: line …`

Maintain checker state:

* `sources : Map endpoint_id → SourceState`
* `sinks   : Map endpoint_id → SinkState`
* `pending : Map (flow_id, pkt_id) → PendingInfo`

Where `PendingInfo` should store at least:

* `sentTimeNs : Nat`
* `sentEventId : Nat`
* `sentLine : Nat` (original line number)
* (optionally) `sinkEndpointId : Nat` and CNP header fields if you want richer diagnostics.

**Processing loop (exact):**
Let `prevKey := none`.

For each row `r` in `rows_sorted`:

1. **Global monotone sanity**

* If `prevKey` exists: require `prevKey < key(r)`. (Should always hold after Phase 0; keeping it is a cheap “belt-and-suspenders”.)
* Set `prevKey := key(r)`.

2. **Kind-specific required-field check**

* As you already do per kind (Table 2 in your draft): reject if required fields missing.

3. **Dispatch on `r.kind`**

#### Case: `cnp_sent`

Checks:

* Validate CNP packet fields (size 64B, ECN NotEct, flags false, etc.) — unchanged.
* Validate trigger ECN is CE — unchanged.
* Enforce sink interval gate:

  * Look up `sinkState` for `r.endpoint_id` (init if first time).
  * Require `sink.lastCnpNs + r.cnp_interval_ns ≤ r.time_ns`.
  * Require row logs last_cnp_ns consistent with send time (as you already do).
* Pending insertion:

  * Let `k := (r.flow_id, r.pkt_id)`; require `pkt_id` present.
  * Require `pending` does **not** contain `k` (avoid duplicates).
  * Insert `pending[k] := { sentTimeNs=r.time_ns, sentEventId=r.event_id, sentLine=r.srcLine }`.

#### Case: `cnp_recv`

Checks:

* Validate CNP packet fields — unchanged.
* Require matching pending CNP exists:

  * Let `k := (r.flow_id, r.pkt_id)`; require `pkt_id` present.
  * If `pending[k]` missing → `REJECT: line r.srcLine: cnp_recv without prior cnp_sent`.
* Optional but highly recommended causal-time check:

  * Let `p := pending[k]`.
  * Require `(p.sentTimeNs, p.sentEventId) < (r.time_ns, r.event_id)`.

    * If violated: `REJECT: line r.srcLine: cnp_recv precedes cnp_sent (sent at line p.sentLine)`.
* Apply your existing source parameter consistency + time checks (now interpreted in canonical order).
* Compute expected post-state according to DCQCN update (including interval gate on the source side) and compare to logged snapshot.
* Remove `pending[k]`.

#### Case: `timer_tick`

Checks:

* Validate required snapshot fields exist — unchanged.
* Parameter consistency — unchanged.
* Apply expected timer update and compare snapshots — unchanged.

4. Continue; if all rows pass → `ACCEPT`.

**End-of-trace behavior**

* Leaving entries in `pending` is still allowed (models loss/delay), as your current paper states — unchanged.

---

## C. Trace schema changes (CSV)

### 1) Add `event_id` column (required)

Add a globally unique integer field:

* **New column:** `event_id`
* Type: unsigned 64-bit integer (or Nat in Lean)
* Requirement: unique per row; monotone with emission

**New header prefix:**

```text
time_ns,event_id,kind,endpoint_id,flow_id,...
```

### 2) (Optional, but useful) Add `time_f64_bits` for exact ordering from float64

Since your “ground truth time” is float64, you can preserve it exactly:

* `time_f64_bits : u64` = IEEE754 bits of the f64 timestamp (via `to_bits()` in Rust).
* Canonical key could become `(time_f64_bits, event_id)`.

This avoids any collision caused by rounding `time_ns`. You can keep `time_ns` for readability and keep sorting on bits for strict fidelity.

**However:** if you’re confident `time_ns` is already computed deterministically and collisions aren’t problematic with `event_id`, you can skip this. The minimal change is just `event_id`.

---

## D. Changes in Days (Rust instrumentation)

### 1) Assign `event_id` at the point of logging

You want `event_id` to reflect the order in which transitions are *emitted as trace events*. The simplest implementation:

* In the logging module (where CSV rows are constructed), add:

  * `static EVENT_ID: AtomicU64 = AtomicU64::new(0);`
* When emitting a row:

  * `let event_id = EVENT_ID.fetch_add(1, Ordering::Relaxed);`

Write `event_id` into the CSV row.

### 2) Prefer scheduler-provided tie-break if available

If Days has an internal event queue with a natural tie-breaker (many DES schedulers maintain a `(time, seqno)` pair), it is better to log that `seqno` than a “logger atomic counter”, because it more directly corresponds to simulation execution order.

Either is acceptable for Option A; scheduler seqno is the gold standard.

### 3) Update all three instrumentation sites

Where you currently log:

* sink: `cnp_sent`
* source: `cnp_recv`
* source timer: `timer_tick`

Ensure each uses the same `event_id` generation mechanism and includes it in the emitted row.

---

## E. Changes in the Lean checker implementation

### 1) Extend the row type

Add:

```lean
structure Row where
  timeNs   : Nat
  eventId  : Nat
  kind     : Kind
  endpointId : Nat
  flowId   : Nat
  -- ...
  srcLine  : Nat   -- original CSV line number (for diagnostics)
```

Make `eventId` required for all kinds.

### 2) Parse all rows before checking

Because you need sorting, you can no longer do a single streaming fold (unless you implement an external sort).

So:

* Parse into `Array Row`.
* Sort.
* Then fold.

### 3) Sorting comparator

Define:

* `key r := (r.timeNs, r.eventId)`
* comparator: lexicographic on the pair

Then:

* `rowsSorted := rows.qsort (fun a b => key(a) < key(b))` (exact function name depends on your Lean utilities, but this is the intent)

After sorting:

* linear scan to ensure strict monotonicity of keys (reject duplicates).

### 4) Diagnostics must use `srcLine`

When rejecting, always report the original file line:

* `REJECT: line {r.srcLine}: ...`

Optionally include the canonical key in the message for debugging:

* `... (time_ns=..., event_id=...)`

---

## F. Paper updates (design section edits you should make)

### 1) Update “Event ordering” paragraph

Replace the current “we do not rely on a global total order” text with:

* LeanDays **canonicalizes** traces by sorting events by `(time_ns, event_id)`.
* This makes acceptance independent of file interleavings and logging concurrency.
* The trace semantics are therefore “multiset + canonicalization” rather than “raw file order”.

### 2) Update the acceptance definition

Change from:

[
\textsc{foldlM}(\textsc{step}, s_0, tr)
]

to:

[
\textsc{foldlM}(\textsc{step}, s_0, \textsc{canonicalize}(tr))
]

Where `canonicalize`:

* parses and rejects malformed rows
* sorts by the canonical key
* rejects duplicate keys (or defines a stable tie-break if you choose that route)

### 3) Mention complexity and memory

Sorting costs:

* time: (O(n \log n))
* memory: (O(n))

For your current 100k-row example, this should still be small, but it’s good to state it.

---

## G. One subtle but important edge case (and how Option A fixes it)

**Problem without tie-breaker:**
If `cnp_sent` and `cnp_recv` occur at the same `time_ns`, sorting by time alone can reorder them arbitrarily, yielding a false rejection.

**Solution:**
The `event_id` tie-break defines a deterministic total order even among equal timestamps. The checker then enforces causal ordering naturally via the pending-set mechanism.

---

## Summary of required concrete changes

1. **CSV schema:** add `event_id` column (required)
2. **Days instrumentation:** generate and log `event_id` for every row
3. **Lean checker:**

   * parse all rows into memory
   * sort by `(time_ns, event_id)`
   * reject duplicate keys
   * replay-check in sorted order
   * report original `srcLine` in errors
4. **Paper:** redefine acceptance over `canonicalize(tr)` and update ordering discussion accordingly

---

If you want, I can also draft the exact LaTeX edits for:

* the schema table (`Field` / `Meaning`) to include `event_id`
* the acceptance equation to include `canonicalize`
* a revised “One subtle point is event ordering” paragraph that matches Option A precisely.


