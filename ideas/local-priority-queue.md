Below is one clean way to implement **B4** in Nexosim:

* **Remove** `flush_local()` entirely.
* Give each worker thread its own **local `PriorityQueue`** (min-heap).
* Keep the existing **global mutex-protected queue** only for *non-worker* scheduling.
* In `Simulation::step_to_next()`, do a **k-way merge** by scanning the `peek()` of each queue to find the next event key (O(#workers) per “pop”), instead of bulk-flushing vectors into a single heap.

### Key detail (important for correctness)

With per-worker queues, actions from the same `(time, origin_id)` can be scheduled by different workers, so you need a deterministic total order across queues.

The simplest (and what I show below) is to **include `seq` in the priority key**:

```rust
type SchedulerKey = (MonotonicTime, usize /*origin_id*/, u64 /*seq*/);
```

Then you don’t need epoch tie-breaking across queues; key order is total, and you can still preserve the “execute sequentially per (time, origin)” behavior by grouping on `(time, origin_id)` (ignoring `seq` for grouping).

---

# 1) `scheduler.rs`: local priority queues + new key type

### A) Change scheduler key/queue types

```rust
// crates/nexosim/src/simulation/scheduler.rs

/// Total order key: time, origin, and sequence number.
pub(crate) type SchedulerKey = (MonotonicTime, usize, u64);

/// Global / local scheduler queues.
pub(crate) type SchedulerQueue = PriorityQueue<SchedulerKey, Action>;
```

This replaces the old:

```rust
pub(crate) type SchedulerQueue = PriorityQueue<(MonotonicTime, usize), Action>;
```

---

### B) Replace `LocalScheduleBuffers` with per-worker `SchedulerQueue`

```rust
// crates/nexosim/src/simulation/scheduler.rs

pub(super) struct LocalScheduleQueues {
    pub(super) queues: Box<[CachePadded<UnsafeCell<SchedulerQueue>>]>,
}

// Safety: each queue is mutated only by its owning worker thread while the executor runs;
// it is read/drained only when the executor is quiescent (in Simulation::step_to_next).
unsafe impl Sync for LocalScheduleQueues {}

impl LocalScheduleQueues {
    #[inline]
    fn insert(&self, worker_id: usize, key: SchedulerKey, action: Action) {
        debug_assert!(worker_id < self.queues.len());
        // Safety: single-writer (worker_id) while executor runs; main thread only touches when quiescent.
        unsafe { &mut *self.queues[worker_id].get() }
            .insert_with_epoch(key, action, 0 /*epoch unused; key is unique*/);
    }
}
```

---

### C) Update `SchedulerState` to hold local queues (and delete `flush_local`)

```rust
// crates/nexosim/src/simulation/scheduler.rs

pub(crate) struct SchedulerState {
    pub(super) scheduler_queue: Arc<Mutex<SchedulerQueue>>, // keep as "global queue" for non-worker scheduling
    pub(super) local_queues: LocalScheduleQueues,
    executor_id: usize,
    origin_seqs: OnceLock<Box<[CachePadded<AtomicU64>]>>,
}

impl SchedulerState {
    pub(crate) fn new(
        scheduler_queue: Arc<Mutex<SchedulerQueue>>,
        executor_id: usize,
        num_workers: usize,
    ) -> Self {
        let num_workers = num_workers.max(1);

        let queues = (0..num_workers)
            .map(|_| CachePadded::new(UnsafeCell::new(PriorityQueue::new())))
            .collect::<Vec<_>>()
            .into_boxed_slice();

        Self {
            scheduler_queue,
            local_queues: LocalScheduleQueues { queues },
            executor_id,
            origin_seqs: OnceLock::new(),
        }
    }

    // DELETE flush_local entirely.
}
```

Also delete the old `LocalScheduleBuffers`, `LocalScheduleItem`, and `flush_local()`.

---

### D) Update worker fast-path scheduling to insert into the worker-local heap

Here’s the pattern for `schedule_event_from` (apply similarly to the other `schedule_*` methods):

```rust
// crates/nexosim/src/simulation/scheduler.rs

pub(crate) fn schedule_event_from<M, F, T, S>(
    &self,
    deadline: impl Deadline,
    func: F,
    arg: T,
    address: impl Into<Address<M>>,
    origin_id: usize,
) -> Result<(), SchedulingError>
where
    M: Model,
    F: for<'a> InputFn<'a, M, T, S>,
    T: Send + Clone + 'static,
    S: Send + 'static,
{
    let sender = address.into().0;
    let action = Action::new(OnceAction::new(process_event(func, arg, sender)));

    // Worker fast-path: write to the calling worker’s local heap.
    if let (Some(worker_id), Some(executor_id)) =
        (crate::executor::worker_id(), crate::executor::executor_id())
    {
        if executor_id == self.state.executor_id {
            let now = self.time();
            let time = deadline.into_time(now);
            if now >= time {
                return Err(SchedulingError::InvalidScheduledTime);
            }

            let seq = self.state.next_seq(origin_id);
            let key: SchedulerKey = (time, origin_id, seq);
            self.state.local_queues.insert(worker_id, key, action);
            return Ok(());
        }
    }

    // Non-worker path: lock global queue.
    let mut scheduler_queue = self.state.scheduler_queue.lock().unwrap();
    let now = self.time();
    let time = deadline.into_time(now);
    if now >= time {
        return Err(SchedulingError::InvalidScheduledTime);
    }

    let seq = self.state.next_seq(origin_id);
    let key: SchedulerKey = (time, origin_id, seq);
    scheduler_queue.insert_with_epoch(key, action, 0);
    Ok(())
}
```

And the same idea for `schedule_event_batch_from` (validate all deadlines first, then insert):

```rust
let seq = self.state.next_seq(origin_id);
let key: SchedulerKey = (time, origin_id, seq);
self.state.local_queues.insert(worker_id, key, action);
```

For the global-queue path:

```rust
scheduler_queue.insert_with_epoch((time, origin_id, seq), action, 0);
```

---

# 2) `simulation.rs`: k-way merge instead of `flush_local()`

This is the core loop rewrite: choose next event by scanning `peek()` across:

* the global queue (mutex-guarded), and
* each worker local queue (`UnsafeCell<SchedulerQueue>`)

### Revised `step_to_next` skeleton

```rust
// crates/nexosim/src/simulation.rs

fn step_to_next(
    &mut self,
    upper_time_bound: Option<MonotonicTime>,
) -> Result<Option<MonotonicTime>, ExecutionError> {
    self.take_halt_flag()?;
    if self.is_terminated {
        return Err(ExecutionError::Terminated);
    }

    use crate::simulation::scheduler::{SchedulerKey, SchedulerQueue};

    #[derive(Clone, Copy, Debug)]
    enum Source {
        Global,
        Worker(usize),
    }

    let upper_time_bound = upper_time_bound.unwrap_or(MonotonicTime::MAX);

    // Lock only the global queue. Local queues are accessed via UnsafeCell under
    // the quiescence guarantee (same as the old flush_local contract).
    let mut global_q = self.scheduler_state.scheduler_queue.lock().unwrap();

    // Helper: find the minimum (time, origin, seq) among all queue heads,
    // while discarding cancelled actions at the head of any queue.
    let mut peek_min = |global_q: &mut std::sync::MutexGuard<SchedulerQueue>| -> Option<(Source, SchedulerKey)> {
        loop {
            let mut best: Option<(Source, SchedulerKey)> = None;
            let mut dropped_cancelled = false;

            // Global head
            if let Some((&key, action)) = global_q.peek() {
                if key.0 <= upper_time_bound {
                    if action.is_cancelled() {
                        global_q.pull(); // discard cancelled
                        dropped_cancelled = true;
                    } else {
                        best = Some((Source::Global, key));
                    }
                }
            }

            // Worker heads
            for (i, cell) in self.scheduler_state.local_queues.queues.iter().enumerate() {
                // Safety: workers are quiescent while we are in step_to_next (executor not running).
                let q = unsafe { &mut *cell.get() };

                if let Some((&key, action)) = q.peek() {
                    if key.0 > upper_time_bound {
                        continue;
                    }
                    if action.is_cancelled() {
                        q.pull(); // discard cancelled
                        dropped_cancelled = true;
                        continue;
                    }
                    if best.as_ref().map_or(true, |(_, best_key)| key < *best_key) {
                        best = Some((Source::Worker(i), key));
                    }
                }
            }

            if dropped_cancelled {
                continue; // head changed; rescan for correct minimum
            }
            return best;
        }
    };

    // Helper: pull the minimum item from a given Source, and if it’s periodic,
    // reinsert its next occurrence into the *same* source queue.
    let mut pull_from =
        |global_q: &mut std::sync::MutexGuard<SchedulerQueue>, src: Source| -> (SchedulerKey, Action) {
            match src {
                Source::Global => {
                    let (key, action) = global_q.pull().unwrap();
                    // Periodic reschedule happens immediately (same as old behavior)
                    if let Some((action_clone, period)) = action.next() {
                        let origin_id = key.1;
                        let seq = self.scheduler_state.next_seq(origin_id);
                        let next_key: SchedulerKey = (key.0 + period, origin_id, seq);
                        global_q.insert_with_epoch(next_key, action_clone, 0);
                    }
                    (key, action)
                }
                Source::Worker(i) => {
                    let q = unsafe { &mut *self.scheduler_state.local_queues.queues[i].get() };
                    let (key, action) = q.pull().unwrap();
                    if let Some((action_clone, period)) = action.next() {
                        let origin_id = key.1;
                        let seq = self.scheduler_state.next_seq(origin_id);
                        let next_key: SchedulerKey = (key.0 + period, origin_id, seq);
                        q.insert_with_epoch(next_key, action_clone, 0);
                    }
                    (key, action)
                }
            }
        };

    // Find the first key at/before the bound.
    let (mut src, mut key) = match peek_min(&mut global_q) {
        Some(x) => x,
        None => return Ok(None),
    };

    // Advance simulation time to the next scheduled time.
    let current_time = key.0;
    self.time.write(current_time);

    loop {
        // Start a group for this (time, origin_id). We ignore seq for grouping,
        // but seq provides the stable total order across worker queues.
        let origin_id = key.1;

        // Pull first action of the group.
        let (pulled_key, first_action) = pull_from(&mut global_q, src);
        debug_assert_eq!(pulled_key.0, current_time);
        debug_assert_eq!(pulled_key.1, origin_id);

        // If there are more actions with same (time, origin), execute them sequentially.
        if let Some((next_src, next_key)) = peek_min(&mut global_q)
            .filter(|(_, k)| k.0 == current_time && k.1 == origin_id)
        {
            let mut seq_fut = SeqFuture::new();
            seq_fut.push(first_action.into_future());

            // Drain all remaining actions for this (time, origin).
            let mut drain_src = next_src;
            let mut drain_key = next_key;
            while drain_key.0 == current_time && drain_key.1 == origin_id {
                let (_k, a) = pull_from(&mut global_q, drain_src);
                seq_fut.push(a.into_future());

                match peek_min(&mut global_q) {
                    Some((s, k)) => {
                        drain_src = s;
                        drain_key = k;
                    }
                    None => break,
                }
            }

            self.executor.spawn_and_forget(seq_fut);
        } else {
            // Single action for this origin at this time: spawn it immediately.
            first_action.spawn_and_forget(&self.executor);
        }

        // Move to the next origin (same time) or finish the timestep.
        match peek_min(&mut global_q) {
            Some((next_src, next_key)) if next_key.0 == current_time => {
                src = next_src;
                key = next_key;
                continue;
            }
            _ => {
                // No more actions at this time (or none before bound).
                drop(global_q); // release mutex before running tasks
                self.synchronize_clock(current_time)?;
                self.run()?;
                return Ok(Some(current_time));
            }
        }
    }
}
```

---

# 3) What else you must update (mechanical)

Because `SchedulerQueue`’s key type changed, you’ll need to mechanically update:

* Any destructuring like `let ((time, origin_id), action) = ...`
  to handle `(time, origin_id, seq)` (or stop destructuring outside the helper).

* All inserts:

  * from `insert_with_epoch((time, origin_id), action, seq)`
  * to `insert_with_epoch((time, origin_id, seq), action, 0)`

This is almost entirely localized to `simulation/scheduler.rs` and `simulation.rs`.

---

# 4) Performance tradeoff (worth calling out)

This removes the **serial flush + global heap push storm**, but it shifts some cost into:

* per-schedule heap insert on worker threads (O(log n) in that worker’s local heap)

In practice, this is often a win when `flush_local()` was large and contended with main-thread work, but you should benchmark.

If you want to reduce per-insert cost further later, a common follow-up is:

* per-worker **time-bucket map** (`BTreeMap<MonotonicTime, Vec<Action>>`) or
* a hybrid “small unsorted buffer + local heap” scheme.

---

If you want, I can also sketch a variant that **does not change the key type** (keeps `(time, origin)` and uses the existing `epoch=seq`), but that requires exposing the epoch from `PriorityQueue::peek()` so the k-way merge can compare `(time, origin, seq)` across queues. The “seq in key” approach above is the simplest to reason about and implement.

