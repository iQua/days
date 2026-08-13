Nice — if quantization is already helping but MT is still (slightly) slower than ST, you’re almost certainly now dominated by **per-step synchronization costs** rather than “real work”:

* main thread: `Executor::run()` loops + `parker.park()` / unpark handshake
* workers: the “go idle” protocol (`try_set_worker_inactive` / `set_all_workers_inactive`) + parking
* repeated **hundreds of thousands** of times

A “hot pool” helps specifically by avoiding expensive **park/unpark and state-transition churn** when the simulation repeatedly goes idle for very short gaps between steps.

Below is a comprehensive, low-regret plan that preserves correctness constraints you care about (especially: **flush_local is only safe when no workers are executing model tasks**; and executor’s contract that tasks shouldn’t run “out of band”).

---

## Guiding constraints you must preserve

1. **No task execution outside `Executor::run()` boundaries**
   Your executor docs say “spawned tasks are not executed until `run()` is called.”
   A naive “worker keeps polling injector and runs tasks immediately” breaks this.

2. **`SchedulerState::flush_local()` is only safe when workers are quiescent**
   Your code explicitly says local buffers are “drained only when the executor is quiescent.”
   So between steps, workers may be awake, but must not execute tasks (and therefore must not write local buffers).

3. **`msg_count` / deadlock detection must remain meaningful at run boundaries**
   Today it’s updated before workers park; your “quiescence” definition must still imply msg_count consistency.

The plan below satisfies all three.

---

## Plan overview (3 layers)

### Layer 1 — Main-thread “spin then park” in `Executor::run()` (easy, big ROI)

Avoid parking the main thread if the pool is about to go idle anyway (very common when you have many short steps).

### Layer 2 — Worker “linger standby” between runs (hot worker(s))

Keep 1–N workers *awake but not running tasks*, waiting for the next `run()` to begin, so they don’t need OS wakeups.

### Layer 3 — Make activation prefer hot standby workers (optional)

Bias `activate_worker()` to pick a hot-standby worker first so you get maximum benefit.

Each layer is independently useful; implement in order and measure after each.

---

## Instrumentation first (so you can tune linger durations)

Add a tiny set of counters/timers (feature-gated) so you can answer:

* how long does `Executor::run()` spend parked vs spinning?
* how often do we go idle within, say, <5µs after entering the wait?
* how long is the typical gap between `run()` returning and the next `run()` call?

### What to instrument

1. In **main thread** (`Executor::run`):

* `main_wait_spins`
* `main_wait_parks`
* `main_wait_park_time_ns` (sum)
* `main_wait_spin_time_ns` (sum)

2. In **workers** (in the “barrier/idle” section):

* `worker_parks`
* `worker_linger_hits` (entered linger)
* `worker_linger_success` (got reactivated during linger)
* `worker_linger_timeout` (linger elapsed then parked)

This lets you tune `MAIN_SPIN` and `WORKER_LINGER` to match your workload.

---

## Layer 1: Main-thread spin-then-park in `Executor::run()`

### Goal

When tasks complete very quickly, avoid a full park/unpark (context-switch heavy). Instead:

* spin briefly checking `pool_is_idle()`
* if not idle soon, fall back to the existing parking behavior

### Implementation sketch

In `crates/nexosim/src/executor/mt_executor.rs`, inside `Executor::run()`:

* Add config `main_spin: Duration` (default `0` → disabled)
* Replace:

```rust
if timeout.is_zero() {
    self.parker.park();
}
```

with:

```rust
if timeout.is_zero() {
    // Hot-wait: avoid parking if we are about to become idle anyway.
    if !self.context.pool_manager.pool_is_idle() && !self.context.main_spin.is_zero() {
        let start = Instant::now();
        while (Instant::now() - start) < self.context.main_spin {
            if self.context.pool_manager.pool_is_idle() {
                break;
            }
            std::hint::spin_loop();
        }
    }

    if !self.context.pool_manager.pool_is_idle() {
        self.parker.park();
    }
}
```

Notes:

* This doesn’t change correctness: you’re still waiting for idle, just with a cheaper waiting mode.
* Even if a worker calls `executor_unparker.unpark()` while you’re spinning, it’s fine: the token will be set; if you later park, you’ll immediately return.

**Tuning suggestion:** start with `main_spin = 2µs` then try `5µs`, `10µs`. You’ll likely see big wins when steps are short.

---

## Layer 2: Worker “linger standby” that does NOT execute tasks

### Goal

Prevent workers from going into a full OS park when they’re likely to be needed again in a few microseconds.

But we must preserve the executor contract, so the worker must:

* become inactive (same as today)
* **not** process tasks while “between runs”
* merely avoid parking for a short window, waiting to be reactivated

### Key trick

Use a **run epoch** gate.

Add to `ExecutorContext`:

```rust
run_epoch: AtomicU64,   // increments at start of each Executor::run()
```

* Main thread increments `run_epoch` at the start of each `run()`.
* Workers in linger standby just watch for `run_epoch` to change.
* They still require **activation** to actually execute tasks, but they won’t need an OS wakeup if they haven’t parked.

### Worker behavior changes

In `run_local_worker` at the barrier:

Currently, once inactive, workers park immediately:

```rust
if pool_manager.try_set_worker_inactive(id) {
    parker.park();
}
```

Change it to:

* If this worker is elected “hot standby” (say 1 worker only),
* and `worker_linger > 0`:

  * don’t park immediately
  * spin/yield for up to `worker_linger` waiting for `run_epoch` to change
  * if `run_epoch` changes, proceed to the normal activation path without ever parking
  * otherwise park as before

### Why run_epoch works

You avoid burning CPU forever; and you avoid incorrectly reacting to random injector state changes. The worker only stays hot if the main thread actually starts another `run()` soon.

### Sketch code for the worker-side linger

Add to `ExecutorContext`:

```rust
worker_linger: Duration,
run_epoch: AtomicU64,
hot_worker_id: usize, // simplest first: pick worker 0, or make it configurable
```

Then in `run_local_worker`, near:

```rust
if pool_manager.try_set_worker_inactive(id) {
    parker.park();
}
```

replace with:

```rust
if pool_manager.try_set_worker_inactive(id) {
    if worker.executor_context.worker_linger.is_zero() || id != worker.executor_context.hot_worker_id {
        parker.park();
    } else {
        // Linger standby: do NOT execute tasks; just avoid OS park briefly.
        let start_epoch = worker.executor_context.run_epoch.load(Ordering::Relaxed);
        let start = Instant::now();

        loop {
            if abort_signal.is_set() {
                return;
            }

            // If a new run began, stop lingering. We’ll fall through and either
            // (a) already be unparked/activated, or (b) get activated shortly.
            let cur_epoch = worker.executor_context.run_epoch.load(Ordering::Relaxed);
            if cur_epoch != start_epoch {
                break;
            }

            if (Instant::now() - start) >= worker.executor_context.worker_linger {
                break;
            }

            std::hint::spin_loop();
            // Optionally yield every N spins to reduce CPU:
            // if spins % 1024 == 0 { std::thread::yield_now(); }
        }

        // If we didn’t see a new run quickly, go to sleep normally.
        // If we did see a new run, we still park *unless* we were already unparked.
        // To keep it simple, do a zero-time park? parking::Parker doesn’t support that.
        // Better: just call park(); if we were unparked while spinning, park returns immediately.
        parker.park();
    }
}
```

Why this remains correct:

* Worker is inactive (same as today)
* Worker does not touch injector/local queue while lingering
* Worker only returns to work after a normal activation/unpark

Also: this preserves your flush_local safety, because during linger there are no tasks executing.

**Tuning:** `worker_linger` often works well in the 5–50µs range.

---

## Layer 3: Bias activation to hot workers (optional but helpful)

Even with Layer 2, if `activate_worker()` wakes a different worker, your hot standby worker’s linger is wasted.

So add a “prefer hot worker” path in `PoolManager`:

### Interface additions

In `pool_manager.rs` implement:

* `activate_specific_worker(id)` — sets that worker active and unparks it if parked
* `try_activate_hot_worker()` — activates a known hot worker if it’s inactive

Then change `Executor::run()`:

```rust
self.context.pool_manager.activate_worker();
```

to something like:

```rust
if !self.context.pool_manager.try_activate_hot_worker(self.context.hot_worker_id) {
    self.context.pool_manager.activate_worker();
}
```

This yields best benefit: the worker you kept hot is the one you use first.

---

## Critical correctness checks to add (debug asserts)

Add these to prevent subtle regressions:

### 1) Assert executor is quiescent before `flush_local`

In `Simulation::step_to_next()` just before:

```rust
self.scheduler_state.flush_local(&mut scheduler_queue);
```

add:

```rust
debug_assert!(self.executor.is_quiescent());
```

Implement `Executor::is_quiescent()` for MT as `pool_manager.pool_is_idle()`.

This will catch any accidental “worker executed tasks out of band”.

### 2) Assert tasks don’t run without `run()` (optional)

Temporarily add a debug-only `run_epoch` check in `schedule_task` to ensure `run_epoch` progressed since last idle. Not mandatory, but useful during development.

---

## Testing strategy

1. Run existing tests including your loom-based ones (nexosim has loom tests in task system).
2. Add one targeted MT executor test:

* Spawn a small number of tasks
* Call `run()` → ensure completion
* Immediately spawn more tasks but **do not call run yet**
* Ensure they do not execute until the next `run()` call

This is specifically to ensure your hot pool didn’t violate the executor contract.

3. Run Days integration tests (`tests/schedule_event_batch.rs`, etc.) with hot pool enabled.

---

## Tuning workflow

Once implemented, tune in this order:

1. Enable only Layer 1 (main spin), tune `main_spin`:

   * start `2µs`, `5µs`, `10µs`
   * watch wall time and CPU usage

2. Enable Layer 2 (worker linger) with 1 hot worker:

   * start `worker_linger = 10µs`, then `20µs`, `50µs`
   * measure:

     * reductions in `worker_parks`
     * reductions in main wait park time

3. Add Layer 3 (activate hot worker first) if linger “success” rate is low.

A good sign you’ve tuned well:

* `worker_linger_success` is high (often >70%),
* `worker_parks` drops noticeably,
* wall time improves with minimal CPU blow-up.

---

## If hot pool still doesn’t beat ST

At that point you’re probably limited by one of:

* remaining `Executor::run()` barrier logic cost (atomic bookkeeping, msg_count semantics)
* mailbox contention/backpressure or cache coherence costs in the model code
* too little parallel work per step even after quantization

But hot pool is still the right next move because it directly targets the “309k+ steps” style overhead.

---

## If you want, I can tailor the plan to your `PoolManager` internals

Right now, I’ve kept PoolManager changes optional/minimal because we didn’t paste `pool_manager.rs`. If you paste that file (or at least the active/searching bitmask representation + `activate_worker`/`try_set_worker_inactive` implementations), I can give you:

* the exact hot-worker activation diff (Layer 3) that matches your bitset logic, and
* a cleaner linger loop that avoids a fallback `park()` when we already know activation happened.
