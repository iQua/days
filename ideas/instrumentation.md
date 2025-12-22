Below is a **minimal, low-intrusion instrumentation patch outline** that will tell you (with hard numbers):

1. **How much time is spent waiting on / holding Nexosim’s global scheduler mutex**
2. **How often mailboxes hit “full” (capacity pressure / backpressure churn)**
3. **Whether the MT executor’s “remote” spin-loop actually triggers, and how costly it is**

I’m keeping this “surgical”: a small `perf` module with a handful of `Atomic*` counters, plus a few `Instant`-based timers placed exactly at the contention points.

---

## 0) Add a `perf` feature flag (so you can turn this on/off)

### `crates/nexosim/Cargo.toml`

Add a feature:

```toml
[features]
perf = []
```

### `days/Cargo.toml` (optional convenience)

Forward it:

```toml
[features]
perf = ["nexosim/perf"]
```

Run with:

```bash
cargo run --features perf -- configs/exp_tcp_fattree.toml
```

If you don’t want to touch Days features, you can still run with `--features nexosim/perf` depending on how your workspace features are wired.

---

## 1) Add a tiny global metrics module

### New file: `crates/nexosim/src/util/perf.rs`

```rust
//! Minimal perf counters (feature-gated).
#![allow(dead_code)]

#[cfg(feature = "perf")]
mod enabled {
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    // ---------- Scheduler queue lock metrics ----------
    pub static SCHED_LOCK_ACQS: AtomicU64 = AtomicU64::new(0);
    pub static SCHED_LOCK_WAIT_NS: AtomicU64 = AtomicU64::new(0);
    pub static SCHED_LOCK_HOLD_NS: AtomicU64 = AtomicU64::new(0);

    // Same lock, but from the stepping thread (Simulation::step_to_next).
    pub static STEP_LOCK_ACQS: AtomicU64 = AtomicU64::new(0);
    pub static STEP_LOCK_WAIT_NS: AtomicU64 = AtomicU64::new(0);
    pub static STEP_LOCK_HOLD_NS: AtomicU64 = AtomicU64::new(0);

    pub static STEP_COUNT: AtomicU64 = AtomicU64::new(0);
    pub static ACTIONS_PULLED: AtomicU64 = AtomicU64::new(0);

    pub static SCHED_BATCH_CALLS: AtomicU64 = AtomicU64::new(0);
    pub static SCHED_BATCH_EVENTS: AtomicU64 = AtomicU64::new(0);
    pub static SCHED_BATCH_MAX_SIZE: AtomicUsize = AtomicUsize::new(0);

    pub static SCHED_QUEUE_MAX_LEN: AtomicUsize = AtomicUsize::new(0);

    // ---------- Mailbox pressure ----------
    pub static MBOX_PUSH_FULL: AtomicU64 = AtomicU64::new(0);
    pub static MBOX_PUSH_CLOSED: AtomicU64 = AtomicU64::new(0);

    // ---------- MT executor “remote spin” ----------
    pub static EXEC_SPIN_EVENTS: AtomicU64 = AtomicU64::new(0);
    pub static EXEC_SPIN_NS: AtomicU64 = AtomicU64::new(0);

    #[inline]
    pub fn add_sched_lock(wait_ns: u64, hold_ns: u64) {
        SCHED_LOCK_ACQS.fetch_add(1, Ordering::Relaxed);
        SCHED_LOCK_WAIT_NS.fetch_add(wait_ns, Ordering::Relaxed);
        SCHED_LOCK_HOLD_NS.fetch_add(hold_ns, Ordering::Relaxed);
    }

    #[inline]
    pub fn add_step_lock(wait_ns: u64, hold_ns: u64) {
        STEP_LOCK_ACQS.fetch_add(1, Ordering::Relaxed);
        STEP_LOCK_WAIT_NS.fetch_add(wait_ns, Ordering::Relaxed);
        STEP_LOCK_HOLD_NS.fetch_add(hold_ns, Ordering::Relaxed);
    }

    #[inline]
    pub fn note_batch(batch_size: usize, queue_len: usize) {
        SCHED_BATCH_CALLS.fetch_add(1, Ordering::Relaxed);
        SCHED_BATCH_EVENTS.fetch_add(batch_size as u64, Ordering::Relaxed);
        SCHED_BATCH_MAX_SIZE.fetch_max(batch_size, Ordering::Relaxed);
        SCHED_QUEUE_MAX_LEN.fetch_max(queue_len, Ordering::Relaxed);
    }

    pub fn dump_to_log() {
        use log::info;

        let sched_acq = SCHED_LOCK_ACQS.load(Ordering::Relaxed);
        let sched_wait = SCHED_LOCK_WAIT_NS.load(Ordering::Relaxed);
        let sched_hold = SCHED_LOCK_HOLD_NS.load(Ordering::Relaxed);

        let step_acq = STEP_LOCK_ACQS.load(Ordering::Relaxed);
        let step_wait = STEP_LOCK_WAIT_NS.load(Ordering::Relaxed);
        let step_hold = STEP_LOCK_HOLD_NS.load(Ordering::Relaxed);

        let steps = STEP_COUNT.load(Ordering::Relaxed);
        let pulled = ACTIONS_PULLED.load(Ordering::Relaxed);

        let batch_calls = SCHED_BATCH_CALLS.load(Ordering::Relaxed);
        let batch_events = SCHED_BATCH_EVENTS.load(Ordering::Relaxed);
        let batch_max = SCHED_BATCH_MAX_SIZE.load(Ordering::Relaxed);
        let qmax = SCHED_QUEUE_MAX_LEN.load(Ordering::Relaxed);

        let m_full = MBOX_PUSH_FULL.load(Ordering::Relaxed);
        let m_closed = MBOX_PUSH_CLOSED.load(Ordering::Relaxed);

        let spin_events = EXEC_SPIN_EVENTS.load(Ordering::Relaxed);
        let spin_ns = EXEC_SPIN_NS.load(Ordering::Relaxed);

        let ns_to_ms = |ns: u64| (ns as f64) / 1e6;
        let avg_ns = |total: u64, n: u64| if n == 0 { 0.0 } else { (total as f64) / (n as f64) };

        info!("=== nexosim perf ===");
        info!(
            "scheduler_queue lock (scheduling): acq={} wait_total={:.3}ms hold_total={:.3}ms avg_wait={:.1}ns avg_hold={:.1}ns",
            sched_acq,
            ns_to_ms(sched_wait),
            ns_to_ms(sched_hold),
            avg_ns(sched_wait, sched_acq),
            avg_ns(sched_hold, sched_acq),
        );
        info!(
            "scheduler_queue lock (stepper):    acq={} wait_total={:.3}ms hold_total={:.3}ms avg_wait={:.1}ns avg_hold={:.1}ns",
            step_acq,
            ns_to_ms(step_wait),
            ns_to_ms(step_hold),
            avg_ns(step_wait, step_acq),
            avg_ns(step_hold, step_acq),
        );
        info!(
            "steps={} actions_pulled={} (avg {:.2} actions/step)",
            steps,
            pulled,
            if steps == 0 { 0.0 } else { (pulled as f64) / (steps as f64) }
        );
        info!(
            "schedule_event_batch: calls={} events={} avg_batch={:.1} max_batch={} sched_queue_max_len={}",
            batch_calls,
            batch_events,
            if batch_calls == 0 { 0.0 } else { (batch_events as f64) / (batch_calls as f64) },
            batch_max,
            qmax
        );
        info!("mailbox push: full={} closed={}", m_full, m_closed);
        info!(
            "executor spin: events={} total_spin={:.3}ms avg_spin={:.1}ns",
            spin_events,
            ns_to_ms(spin_ns),
            avg_ns(spin_ns, spin_events)
        );
        info!("====================");
    }
}

#[cfg(not(feature = "perf"))]
mod enabled {
    // No-op stubs so callers don’t need cfg gates.
    #[inline] pub fn add_sched_lock(_: u64, _: u64) {}
    #[inline] pub fn add_step_lock(_: u64, _: u64) {}
    #[inline] pub fn note_batch(_: usize, _: usize) {}
    #[inline] pub fn dump_to_log() {}
    // These names may be referenced behind cfg in other files; keep minimal API.
}

pub use enabled::*;
```

### Wire it into Nexosim’s util module

Add a module line where Nexosim collects its util submodules (likely `crates/nexosim/src/util.rs`):

```rust
pub(crate) mod perf;
```

(or `pub mod perf;` if you want to call it from Days as `nexosim::util::perf::dump_to_log()`).

---

## 2) Add a `len()` method to the scheduler queue wrapper (tiny, used for max queue size)

### `crates/nexosim/src/util/priority_queue.rs`

Add:

```rust
impl<K: Copy + Ord, V> PriorityQueue<K, V> {
    pub(crate) fn len(&self) -> usize {
        self.heap.len()
    }
}
```

This lets you record `SCHED_QUEUE_MAX_LEN` while you already hold the mutex.

---

## 3) Instrument scheduler mutex contention from **model threads** (scheduling path)

### `crates/nexosim/src/simulation/scheduler.rs`

At top:

```rust
#[cfg(feature = "perf")]
use std::time::Instant;
use crate::util::perf;
```

Now patch **at least** `GlobalScheduler::schedule_event_batch_from` (and ideally `schedule_event_from` too).

#### Patch `schedule_event_batch_from` (core)

Right around the mutex lock:

```rust
let sender = address.into().0;

#[cfg(feature = "perf")]
let wait_start = Instant::now();

let mut scheduler_queue = self.scheduler_queue.lock().unwrap();

#[cfg(feature = "perf")]
let wait_ns: u64 = wait_start
    .elapsed()
    .as_nanos()
    .try_into()
    .unwrap_or(u64::MAX);

#[cfg(feature = "perf")]
let hold_start = Instant::now();
```

Then after you finish inserting (and still holding the lock), record batch stats:

```rust
// ... validation ...

let mut inserted: usize = 0;
for (deadline, arg) in deadlines_and_args {
    let time = deadline.into_time(now);
    let action = Action::new(OnceAction::new(process_event(
        func.clone(),
        arg,
        sender.clone(),
    )));
    scheduler_queue.insert((time, origin_id), action);
    inserted += 1;
}

#[cfg(feature = "perf")]
{
    let qlen = scheduler_queue.len();
    perf::note_batch(inserted, qlen);
}
```

Finally, immediately before returning (still in scope, so mutex still held), measure hold time and add lock metrics:

```rust
#[cfg(feature = "perf")]
{
    let hold_ns: u64 = hold_start
        .elapsed()
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX);
    perf::add_sched_lock(wait_ns, hold_ns);
}
```

**Important:** also do the same “record hold time” on early error returns (invalid time). The truly minimal way is to just wrap the entire body so all returns go through a single “epilogue”, but if you want to keep edits small, just duplicate the little `#[cfg(feature="perf")] { ... }` block on the error return path.

#### Patch `schedule_event_from` similarly

Even simpler: record wait/hold and also `perf::note_batch(1, scheduler_queue.len())` (or add another counter, but not necessary).

This catches non-batched sources (likely a big factor).

---

## 4) Instrument scheduler mutex contention from the **stepper thread** (`Simulation::step_to_next`)

### `crates/nexosim/src/simulation.rs`

At top of file (or near `Simulation::step_to_next`):

```rust
#[cfg(feature = "perf")]
use std::time::Instant;
use crate::util::perf;
```

Then in `Simulation::step_to_next(...)`, right before:

```rust
let mut scheduler_queue = self.scheduler_queue.lock().unwrap();
```

add:

```rust
#[cfg(feature = "perf")]
let wait_start = Instant::now();

let mut scheduler_queue = self.scheduler_queue.lock().unwrap();

#[cfg(feature = "perf")]
let wait_ns: u64 = wait_start.elapsed().as_nanos().try_into().unwrap_or(u64::MAX);

#[cfg(feature = "perf")]
let hold_start = Instant::now();

#[cfg(feature = "perf")]
perf::STEP_COUNT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
```

Now record “actions pulled” inside the local helper `pull_next_action` (minimal and very informative):

Find:

```rust
fn pull_next_action(scheduler_queue: &mut MutexGuard<SchedulerQueue>) -> Action {
    let ((time, channel_id), action) = scheduler_queue.pull().unwrap();
    ...
    action
}
```

Add:

```rust
#[cfg(feature = "perf")]
perf::ACTIONS_PULLED.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
```

Finally, near the point where `step_to_next` is about to drop the lock (there is an explicit `drop(scheduler_queue);` already in the code), record hold time just before that:

```rust
// right before: drop(scheduler_queue);

#[cfg(feature = "perf")]
{
    let hold_ns: u64 = hold_start.elapsed().as_nanos().try_into().unwrap_or(u64::MAX);
    perf::add_step_lock(wait_ns, hold_ns);
}
drop(scheduler_queue);
```

This will tell you whether the **stepper** is holding the lock for long windows, starving schedulers, which often makes MT slower.

---

## 5) Instrument mailbox backpressure: count “push full” events

### `crates/nexosim/src/channel/queue.rs`

At top:

```rust
use crate::util::perf;
```

In `Queue::push`, find the “queue full” return:

```rust
cmp::Ordering::Less => {
    return Err(PushError::Full(msg_fn));
}
```

Patch to:

```rust
cmp::Ordering::Less => {
    #[cfg(feature = "perf")]
    perf::MBOX_PUSH_FULL.fetch_add(1, Ordering::Relaxed);
    return Err(PushError::Full(msg_fn));
}
```

Also count closed pushes (optional but basically free):

Where it returns `PushError::Closed`:

```rust
if enqueue_pos & self.closed_channel_mask != 0 {
    #[cfg(feature = "perf")]
    perf::MBOX_PUSH_CLOSED.fetch_add(1, Ordering::Relaxed);
    return Err(PushError::Closed);
}
```

This counter is *extremely* useful for confirming whether `mailbox_capacity=1` is killing throughput (you’ll typically see this number explode).

---

## 6) Instrument the MT executor’s spin-loop (and time spent spinning)

### `crates/nexosim/src/executor/mt_executor.rs`

At top:

```rust
#[cfg(feature = "perf")]
use std::time::Instant;
use crate::util::perf;
```

Locate:

```rust
while local_queue.spare_capacity() < bucket_iter.len() {}
```

Replace with the minimally-instrumented version:

```rust
let bucket_len = bucket_iter.len();
if local_queue.spare_capacity() < bucket_len {
    #[cfg(feature = "perf")]
    {
        perf::EXEC_SPIN_EVENTS.fetch_add(1, Ordering::Relaxed);
        let spin_start = Instant::now();
        while local_queue.spare_capacity() < bucket_len {
            std::hint::spin_loop();
        }
        let spin_ns: u64 = spin_start
            .elapsed()
            .as_nanos()
            .try_into()
            .unwrap_or(u64::MAX);
        perf::EXEC_SPIN_NS.fetch_add(spin_ns, Ordering::Relaxed);
    }

    #[cfg(not(feature = "perf"))]
    while local_queue.spare_capacity() < bucket_len {}
} else {
    // no spin
}
```

Even if you don’t change behavior, just **counting** how often this triggers is valuable. If `EXEC_SPIN_EVENTS` > ~0 on this workload, it’s no longer “remote.”

---

## 7) Dump metrics once per run (best place: Days `Topology::run`)

You want a single summary at end.

### `days/src/topos/topo.rs`

After the simulation finishes (right after your `sim.step_until(...)` call returns), add:

```rust
// Always safe: no-op when perf feature off.
nexosim::util::perf::dump_to_log();
```

If module visibility is `pub(crate)` in Nexosim, change it to `pub` or call from within Nexosim instead. (I recommend making `perf` public while it’s experimental.)

---

## What you should expect to see

After one run, you’ll get log lines like:

* **scheduler_queue lock (scheduling)**: high wait/hold → global scheduler mutex contention
* **scheduler_queue lock (stepper)**: large hold → stepper hogging the mutex, blocking model scheduling
* **sched_queue_max_len**: if it’s huge, batching lookahead is likely inflating the heap and making every op slower
* **mailbox push full**: if it’s enormous, `mailbox_capacity=1` is likely a major throughput killer
* **executor spin**: if nonzero and/or large total spin time, executor’s rare path is biting you

That’s enough to answer “where did the 50% go?” with numbers, and it’s minimal enough that you can revert cleanly.

---

If you want one extra (still minimal) metric that often clarifies things: I’d add **separate counters for `schedule_event_from` vs `schedule_event_batch_from`** (calls + events). That tells you immediately whether a non-batched component (like flow sources) is still dominating scheduler traffic.

