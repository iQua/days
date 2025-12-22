That outcome (B1 no change; A2/B4 slightly worse) is a really strong signal that the *main* limiter in your workload is **not**:

* reallocs during `flush_local` / heap growth (B1), nor
* the global scheduler merge itself (B4), nor
* raw per-message mailbox CAS cost (A2).

What’s left (and what matches Nexosim’s execution model) is usually:

1. **Not enough exploitable parallelism per simulation timestamp**, so MT just pays overhead.
2. **MT executor overhead dominates** because you hit the “barrier per timestamp” loop a huge number of times (park/unpark, injector mutex, stealing/search bookkeeping).
3. **Timestamp fragmentation** (often from `f64` arithmetic + `Duration::from_secs_f64`) makes events that “should” coincide land at *slightly different* `MonotonicTime`s, which kills coalescing and therefore kills concurrency.

Below is how I’d proceed, with concrete, code-backed hypotheses + patches.

---

## Why B4 & A2 plausibly got slower

### B4 (per-worker local PQs) often loses because it shifts work from “O(1) append + later heapify/push” to “O(log n) per schedule”

Baseline fast-path scheduling in Nexosim is already cheap: worker threads append `LocalScheduleItem` into a `Vec`. That’s extremely fast.

B4 replaces that with per-schedule heap insert (`log n`), and also adds “k-way peek/pull” scans in the stepper. If your flush wasn’t dominating, B4 tends to lose (small but consistent slowdown is exactly what you saw).

### A2 (mailbox batch push) often loses when:

* mailboxes aren’t the primary bottleneck, or
* you add batching overhead but still frequently block on capacity/backpressure, or
* the system is dominated by executor + step barriers, not queue CAS.

So the results are consistent: you optimized the wrong component.

---

## The most likely real cause: Nexosim only runs concurrently **within the same exact timestamp**

Your core loop is:

* pick next simulation time `t`
* spawn all actions scheduled at time `t`
* **wait until the worker pool is idle**
* advance to `t_next`

So MT only helps when “actions at time `t`” is large (many origins scheduled at exactly the same `MonotonicTime`).

If almost every event gets a unique timestamp, you end up doing roughly:

> one tiny task → executor barrier → one tiny task → executor barrier → …

…and MT will lose to ST.

### Why you can have “too many unique timestamps” even when math *looks* aligned

In Days, lots of scheduling times are computed using `f64` local time + repeated `+=` and then turned into `Duration::from_secs_f64(...)`.

Even if the *ideal* times are multiples of 800µs / 819.2µs (which are exact integer nanoseconds), `f64` accumulation can introduce tiny errors, and the conversion can round/truncate differently across chains. That creates nanosecond-level differences, which prevents “same-time” batching in `step_to_next`.

This is **the single most common reason** conservative synchronous engines don’t scale with threads.

---

## First: measure whether you actually have concurrency to exploit

Don’t guess; add a tiny metric to `Simulation::step_to_next()`:

* number of actions processed at this timestamp
* number of origin-groups at this timestamp (i.e., number of spawned tasks at this timestamp)
* steps count (timestamps visited)

If `avg(origin_groups_per_step)` is ~1–2, MT cannot win.

### Patch: add step stats in Nexosim (minimal, compile-time gated)

**`crates/nexosim/src/simulation.rs`** (sketch-level but close to drop-in)

```rust
// near top of file
#[cfg(feature = "perf_stats")]
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

#[cfg(feature = "perf_stats")]
static STEPS: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "perf_stats")]
static ACTIONS: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "perf_stats")]
static ORIGIN_GROUPS: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "perf_stats")]
static MAX_ACTIONS_PER_STEP: AtomicU64 = AtomicU64::new(0);
#[cfg(feature = "perf_stats")]
static MAX_GROUPS_PER_STEP: AtomicU64 = AtomicU64::new(0);

#[cfg(feature = "perf_stats")]
fn bump_max(dst: &AtomicU64, v: u64) {
    let mut cur = dst.load(AtomicOrdering::Relaxed);
    while v > cur {
        match dst.compare_exchange_weak(cur, v, AtomicOrdering::Relaxed, AtomicOrdering::Relaxed) {
            Ok(_) => break,
            Err(next) => cur = next,
        }
    }
}
```

Then inside `step_to_next()`:

* initialize counters at the time you set `current_key`
* increment `actions_this_step` every time you pull an action (including those in a SeqFuture)
* increment `groups_this_step` whenever you spawn a task (either single action or seq future)
* at the end of the timestep, update atomics

Sketch inside your existing loop (exact placement depends on your current code):

```rust
#[cfg(feature = "perf_stats")]
let mut actions_this_step: u64 = 0;
#[cfg(feature = "perf_stats")]
let mut groups_this_step: u64 = 0;

// ... in the loop where you pull actions:
actions_this_step += 1;

// when you spawn a single action or a seq future:
groups_this_step += 1;

// ... right before returning Ok(Some(current_time)):
#[cfg(feature = "perf_stats")]
{
    STEPS.fetch_add(1, AtomicOrdering::Relaxed);
    ACTIONS.fetch_add(actions_this_step, AtomicOrdering::Relaxed);
    ORIGIN_GROUPS.fetch_add(groups_this_step, AtomicOrdering::Relaxed);
    bump_max(&MAX_ACTIONS_PER_STEP, actions_this_step);
    bump_max(&MAX_GROUPS_PER_STEP, groups_this_step);
}
```

And print once somewhere convenient (e.g., in `Simulation::step_until_unchecked` when returning `Ok(())`, or in `Drop` for `Simulation`):

```rust
#[cfg(feature = "perf_stats")]
{
    let steps = STEPS.load(AtomicOrdering::Relaxed).max(1);
    let actions = ACTIONS.load(AtomicOrdering::Relaxed);
    let groups = ORIGIN_GROUPS.load(AtomicOrdering::Relaxed);
    eprintln!(
        "[perf_stats] steps={} actions={} groups={} avg_actions/step={:.2} avg_groups/step={:.2} max_actions/step={} max_groups/step={}",
        steps,
        actions,
        groups,
        actions as f64 / steps as f64,
        groups as f64 / steps as f64,
        MAX_ACTIONS_PER_STEP.load(AtomicOrdering::Relaxed),
        MAX_GROUPS_PER_STEP.load(AtomicOrdering::Relaxed),
    );
}
```

**Interpretation:**

* If `avg_groups/step` ≈ 1, MT basically can’t win (barrier dominates).
* If `avg_groups/step` is large (say 50–500+) and MT is still slower, then the executor/mailbox/runtime overhead is your culprit.

---

## If concurrency is low: the real fix is “make timestamps collide” (or accept ST)

This is where I’d focus next because it’s the most plausible given your results.

### Fix direction 1: stop using `f64` for scheduling-critical time math

You’ll get far better alignment (and deterministic coalescing) if you represent local time as:

* `MonotonicTime`, or
* integer nanoseconds (`u64`), or
* “ticks” with a defined quantum.

For example, Port batching should compute transmit time in **integer nanoseconds** from `rate_bps: u64`:

```rust
tx_ns = (packet_bytes * 8 * 1_000_000_000) / rate_bps
```

(no float accumulation, no rounding drift)

Even if you keep `Packet.time` as `f64` for reporting, you can maintain scheduling in integer time.

### Fix direction 2: introduce a configurable scheduling quantum (optional semantics change)

If you’re okay with “micro-quantization” for performance, round every deadline to e.g. 10ns or 100ns. That increases timestamp collisions and increases work per timestep (better MT amortization).

This is often the easiest path to MT speedups in conservative DES engines.

---

## If concurrency is high but MT is still slower: target the MT executor (next most likely)

Given B1/B4/A2 didn’t help, the next prime suspect is:

### The **main-thread injection path** into the MT executor is mutex-heavy

`Executor::spawn_and_forget` on MT always ends in `injector.insert_task(runnable)` which locks a `Mutex<Vec<Bucket<...>>>` **per spawn**.

If your timestep spawns a lot of origin groups, you’re hammering that mutex.

A high-ROI experiment is to add:

* `Executor::spawn_and_forget_many(Vec<Future>)` **or**
* a Nexosim-internal hook for `Simulation::step_to_next()` to push *buckets of runnables* into the injector using `Injector::push_bucket`, amortizing injector lock acquisitions to O(#buckets) instead of O(#tasks).

This doesn’t require changing simulation semantics and often matters a lot in MT.

(If you want, I can sketch the concrete diffs for bucketed spawning; it’s a tighter change than B4 and tends to be more “obviously right” for this executor design.)

---

## Why you should revert B4 and A2

Given your measurements:

* B4 made scheduling slower (expected if flush wasn’t dominant)
* A2 added overhead without relieving the real bottleneck

I’d keep the codebase close to baseline and iterate with **measurement first**, then pick the next target.

---

## Practical “next 2 things” that will very likely explain your results

1. Add the `perf_stats` counters above and check `avg_groups/step`.
2. If it’s low, focus on **timestamp alignment** (integer/quantized time) rather than more queue/mutex work.

Once you have that `avg_groups/step` number, the path forward becomes much less guessy:

* low groups/step → quantize / integer time (or accept ST)
* high groups/step → injector batching + keep workers hot + allocator/logging contention

If you’d like, I can also sketch:

* a small “round-to-ns” helper to replace all `Duration::from_secs_f64` call sites in Days with consistent rounding, as a minimal first step before a full integer-time refactor.

