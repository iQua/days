Got it – I’ve updated the analysis based on the captured profile data.


Those stats are the smoking gun.

You have:

* **steps = 309,205** (i.e., you advance global time ~309k times)
* **actions = 4,712,340**
* **groups = 4,702,340**
* **avg_groups/step ≈ 15.21** (max 560)

### What this implies about “why MT is slower”

#### 1) You’re spawning ~4.7 million tiny “action tasks”

In `Simulation::step_to_next()`, each **group** becomes a spawned executor task:

* either `action.spawn_and_forget(&executor)` for a single action, or
* one `SeqFuture` spawn for multiple actions with same `(time, origin)`.

Since **groups ≈ actions** (4,702,340 vs 4,712,340), almost every scheduled action becomes its own group ⇒ **~4.7M spawns**.

That’s *exactly* the kind of workload where MT loses: many tiny tasks + lots of synchronization overhead per spawn.

And MT spawns are expensive here by construction:

* `Executor::spawn_and_forget()` takes a **Mutex** on `active_tasks` every call
  (`self.active_tasks.lock().unwrap()` in `crates/nexosim/src/executor/mt_executor.rs`)
* then it calls `injector.insert_task()`, which takes **another Mutex**
  (`inner: Mutex<Vec<Bucket<...>>>` in `mt_executor/injector.rs`)

So you’re doing on the order of:

* ~4.7M locks on `active_tasks`
* ~4.7M locks on the injector
* plus all the per-task bookkeeping (slab entry, runnable creation, etc.)

Even if uncontended, that’s a *lot* of overhead that ST doesn’t pay (or pays much less for).

This also explains why:

* B1 didn’t matter (flush/heap growth isn’t your bottleneck),
* B4 got slower (you replaced O(1) Vec append with O(log n) heap inserts),
* A2 got slower (batching mailbox push doesn’t help if the runtime overhead is dominated earlier).

#### 2) Parallelism exists, but is “thinly spread” across **309k** timestamps

Average groups per step is only ~15. With 10 threads, the best-case parallel speedup from “within-timestamp” concurrency is bounded and likely small because:

* most model actions are short (send to mailbox / small handler),
* you still pay the per-step barrier and executor coordination overhead,
* and “15” is an average—often it’s much less, with occasional huge spikes (you see max 560).

So MT is paying a lot of fixed overhead while rarely getting a fat “batch of work at one time” to amortize it.

---

## The next optimization that matches these stats: reduce spawn overhead, not scheduler/mailer overhead

### The best target: **stop spawning 1 task per group**

You want to reduce either:

* **number of spawns**, or
* **cost per spawn**.

Below are two concrete options; the first is more “semantics-safe”, the second is more aggressive but can be very effective.

---

## Option E1: Batch-spawn per timestep (huge reduction in mutex lock/unlock frequency)

Right now, for a step with ~15 groups, you do ~15 calls to `spawn_and_forget()` ⇒ ~15 `active_tasks` mutex locks + ~15 injector locks.

Instead:

* collect the futures you’re about to spawn for time `t`,
* call one `executor.spawn_and_forget_batch(futs)` that:

  * takes the `active_tasks` lock **once**,
  * pushes runnables to the injector **in buckets** (using `Injector::push_bucket`),
  * does worker activation once.

This reduces mutex lock/unlock frequency from **O(groups)** to **O(steps)** (≈309k) or **O(steps × buckets)**, which is dramatically smaller than 4.7M.

### Sketch of MT executor API (Nexosim-side)

In `crates/nexosim/src/executor/mt_executor.rs`:

```rust
impl Executor {
    pub(crate) fn spawn_and_forget_batch<I, Fut>(&self, futures: I)
    where
        I: IntoIterator<Item = Fut>,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        // Lock once for all tasks in this batch.
        let mut active_tasks = self.active_tasks.lock().unwrap();

        // Build buckets of runnables and push buckets (1 injector lock / bucket).
        let mut bucket = Bucket::new();

        for fut in futures {
            let entry = active_tasks.vacant_entry();
            let key = entry.key();

            let fut = CancellableFuture::new(fut, key);

            let (runnable, cancel_token) =
                task::spawn_and_forget(fut, schedule_task, self.context.executor_id);

            entry.insert(cancel_token);

            if bucket.push(runnable).is_err() {
                self.context.injector.push_bucket(bucket);
                bucket = Bucket::new();
                let _ = bucket.push(runnable);
            }
        }

        if bucket.0.len() != 0 {
            self.context.injector.push_bucket(bucket);
        }

        // Wake workers once (or relaxed).
        self.context.pool_manager.activate_worker_relaxed();
    }
}
```

You’ll also add a trivial ST version (just loops and calls existing spawn) so the callsite is shared.

### Sketch of `Simulation::step_to_next()` change

Instead of spawning per group immediately, collect spawnables for this timestamp:

* for a single action: `spawnables.push(action.into_future())`
* for a per-origin seq: `spawnables.push(action_sequence)` (the seq future)

Then:

```rust
self.executor.spawn_and_forget_batch(spawnables);
```

This preserves your current semantics (still one runnable per origin-group), but removes *most* of the lock/unlock overhead.

---

## Option E2: One dispatcher task per timestamp (reduces spawns from ~4.7M → ~309k)

This is even bigger: you spawn **one** task per simulation time `t` that drives *all* groups scheduled at that time.

Why it can work well here:

* most scheduled actions are just “send closure to mailbox”
* the heavy work happens in the model tasks after messages are enqueued
* you don’t need 15 separate executor tasks just to enqueue 15 messages

The key is: don’t serialize on one potentially-blocking send. Use a small “poll many futures until all complete” driver (or `FuturesUnordered` if you already depend on `futures`).

This can cut spawned tasks by ~15× on average.

I’d treat this as “try behind a feature flag”, because it can interact with mailbox backpressure patterns (head-of-line blocking if implemented as purely sequential). If you implement it as a concurrent poller, it’s usually fine.

---

## Quick reality check: why you shouldn’t expect 10 threads to beat 1 thread without tackling this

Given your stats:

* ~4.7M groups ⇒ ~4.7M spawns today
* MT spawn path includes multiple mutexes and cross-thread scheduling mechanisms
* avg concurrency per timestamp ~15 (often less)

Even if MT perfectly parallelized model work, the overhead of creating/scheduling 4.7M tiny tasks can easily eat the gains and make MT slower than ST by a couple seconds—which matches your observation.

---

## Two quick experiments to validate this hypothesis

1. **Instrument time spent in spawning inside `step_to_next()`**
   Wrap the spawn section with an `Instant` timer and accumulate a counter.
   If “spawn cost” is a large fraction of wall time, E1/E2 are the right direction.

2. **Run with fewer threads**
   Try `num_threads = 2, 4, 6, 8, 10`.
   With avg_groups/step ~15, it’s common for the best point to be <10 because executor overhead scales with threads.

---

If you tell me whether your `Action::into_future()` yields a `'static` future (it looks like it, since you already spawn it), I can sketch the exact `Simulation::step_to_next()` modification for E1 (batch spawn) in the same “diff” style as before.


---

Answer: Here's Action::into_future(): /// Returns a boxed future that performs the action. pub(crate) fn into_future(self) -> Pin<Box<dyn Future<Output = ()> + Send>> { self.inner.into_future() }

---


Perfect — that signature makes **E1 (batch spawning)** straightforward, because you can store each group as a **`Pin<Box<dyn Future<Output=()> + Send>>`** and feed them to a batch-spawn API.

Given the captured profile:

* you’re doing **~4.7M groups ⇒ ~4.7M spawns**
* and on MT each spawn hits **two mutexes** (`active_tasks` + `injector.inner`)

So the highest-ROI Nexosim-side change now is:

> **keep semantics identical, but amortize spawn overhead by locking `active_tasks` once and the injector once per step**.

Below is a concrete “drop-in” sketch that does exactly that by adding:

1. `Injector::insert_tasks()` (one lock for many runnables)
2. `mt_executor::Executor::spawn_and_forget_batch()` (one `active_tasks` lock + one injector lock)
3. a wrapper method `Executor::spawn_and_forget_batch()` (so `Simulation` can call it)
4. changing `Simulation::step_to_next()` to **collect futures for this timestamp** and call the batch method once

This is deliberately targeted at your stats: it reduces mutex acquisitions from **O(groups)** to **O(steps)**.

---

## 1) Add `Injector::insert_tasks` (single injector lock)

**`crates/nexosim/src/executor/mt_executor/injector.rs`**

```diff
diff --git a/crates/nexosim/src/executor/mt_executor/injector.rs b/crates/nexosim/src/executor/mt_executor/injector.rs
index 1111111..2222222 100644
--- a/crates/nexosim/src/executor/mt_executor/injector.rs
+++ b/crates/nexosim/src/executor/mt_executor/injector.rs
@@ -1,6 +1,7 @@
 use std::sync::atomic::{AtomicBool, Ordering};
 use std::sync::Mutex;
 use std::{mem, vec};
+use std::iter;

 /// An unfair injector queue which stores batches of tasks in bounded-size
 /// buckets.
@@ -58,6 +59,64 @@ impl<T, const BUCKET_CAPACITY: usize> Injector<T, BUCKET_CAPACITY> {
     pub(crate) fn insert_task(&self, task: T) {
         let mut inner = self.inner.lock().unwrap();
@@ -111,6 +170,50 @@ impl<T, const BUCKET_CAPACITY: usize> Injector<T, BUCKET_CAPACITY> {
         self.is_empty.store(false, Ordering::Relaxed);
     }
+
+    /// Inserts multiple tasks while taking the injector lock only once.
+    ///
+    /// This amortizes the mutex overhead of repeated `insert_task()` calls,
+    /// which can dominate when the simulator spawns many tiny tasks.
+    pub(crate) fn insert_tasks<I>(&self, tasks: I)
+    where
+        I: IntoIterator<Item = T>,
+    {
+        let mut inner = self.inner.lock().unwrap();
+        let mut inserted_any = false;
+
+        for task in tasks {
+            inserted_any = true;
+
+            // Mirror `insert_task()`’s semantics:
+            // - prefer pushing into the front bucket
+            // - when it fills, move it to the back and replace with a fresh bucket
+            if let Some(bucket) = inner.first_mut() {
+                if let Err(task) = bucket.push(task) {
+                    let mut new_bucket = Bucket::new();
+                    let _ = new_bucket.push(task); // capacity >= 1
+
+                    let full_bucket = mem::replace(bucket, new_bucket);
+                    inner.push(full_bucket);
+                }
+            } else {
+                // Queue is empty: create the first bucket.
+                let mut new_bucket = Bucket::new();
+                let _ = new_bucket.push(task); // capacity >= 1
+                inner.push(new_bucket);
+            }
+        }
+
+        // Update empty hint if we inserted anything.
+        if inserted_any {
+            self.is_empty.store(false, Ordering::Relaxed);
+        }
+    }
```

Notes:

* No behavior change vs repeated `insert_task()`, just one mutex acquisition.

---

## 2) Add MT `spawn_and_forget_batch` that uses `insert_tasks`

**`crates/nexosim/src/executor/mt_executor.rs`**

```diff
diff --git a/crates/nexosim/src/executor/mt_executor.rs b/crates/nexosim/src/executor/mt_executor.rs
index 3333333..4444444 100644
--- a/crates/nexosim/src/executor/mt_executor.rs
+++ b/crates/nexosim/src/executor/mt_executor.rs
@@ -120,6 +120,7 @@ impl Executor {
     pub(crate) fn spawn_and_forget<T>(&self, future: T)
     where
         T: Future + Send + 'static,
         T::Output: Send + 'static,
     {
@@ -151,6 +152,43 @@ impl Executor {
         self.context.injector.insert_task(runnable);
     }
+
+    /// Spawns many tasks, amortizing internal mutex overhead.
+    ///
+    /// - Takes the `active_tasks` lock once for the whole batch.
+    /// - Takes the injector lock once via `Injector::insert_tasks`.
+    ///
+    /// This is designed for the simulator hot path where millions of tiny
+    /// action futures are spawned.
+    pub(crate) fn spawn_and_forget_batch<I, T>(&self, futures: I)
+    where
+        I: IntoIterator<Item = T>,
+        T: Future + Send + 'static,
+        T::Output: Send + 'static,
+    {
+        let mut active_tasks = self.active_tasks.lock().unwrap();
+        let executor_id = self.context.executor_id;
+
+        self.context.injector.insert_tasks(futures.into_iter().map(|future| {
+            // Book a slot to store the task cancellation token.
+            let task_entry = active_tasks.vacant_entry();
+
+            // Wrap the future so it removes its cancel token from the slab when dropped.
+            let future = CancellableFuture::new(future, task_entry.key());
+
+            let (runnable, cancel_token) = task::spawn_and_forget(future, schedule_task, executor_id);
+            task_entry.insert(cancel_token);
+
+            runnable
+        }));
+    }
```

Key property:

* lock order stays the same as today (`active_tasks` then injector), so you’re not introducing a new deadlock pattern.

---

## 3) Add a wrapper method on Nexosim’s public `Executor` type

You’ll need this in whatever file defines `crate::executor::Executor` (likely `crates/nexosim/src/executor.rs`).

I can’t see your exact enum/struct shape from the snippets, so here are the two common patterns.

### If `Executor` is an enum (common)

```rust
pub(crate) enum Executor {
    SingleThreaded(st_executor::Executor),
    MultiThreaded(mt_executor::Executor),
}

impl Executor {
    pub(crate) fn spawn_and_forget_batch<I, T>(&self, futures: I)
    where
        I: IntoIterator<Item = T>,
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        match self {
            Executor::SingleThreaded(ex) => {
                for fut in futures {
                    ex.spawn_and_forget(fut);
                }
            }
            Executor::MultiThreaded(ex) => {
                ex.spawn_and_forget_batch(futures);
            }
        }
    }
}
```

### If `Executor` is a struct holding `inner: …`

Same idea; delegate to the MT implementation and loop for ST.

---

## 4) Modify `Simulation::step_to_next()` to batch spawns per timestamp

**`crates/nexosim/src/simulation.rs`** (only the “spawn” part changes)

```diff
diff --git a/crates/nexosim/src/simulation.rs b/crates/nexosim/src/simulation.rs
index 5555555..6666666 100644
--- a/crates/nexosim/src/simulation.rs
+++ b/crates/nexosim/src/simulation.rs
@@ -1,6 +1,8 @@
 use std::fmt;
+use std::future::Future;
+use std::pin::Pin;
 use std::time::Duration;
@@ -350,6 +352,10 @@ fn step_to_next(
         let mut current_key = match peek_next_key(&mut scheduler_queue) {
             Some(key) => key,
             None => return Ok(None),
         };
         self.time.write(current_key.0);
+
+        // Collect all action/group futures for this simulation time.
+        // (One group == one future to spawn.)
+        let mut spawn_futs: Vec<Pin<Box<dyn Future<Output = ()> + Send>>> = Vec::with_capacity(32);

         loop {
             let action = pull_next_action(self.scheduler_state.as_ref(), &mut scheduler_queue);
             let mut next_key = peek_next_key(&mut scheduler_queue);
             if next_key != Some(current_key) {
-                action.spawn_and_forget(&self.executor);
+                spawn_futs.push(action.into_future());
             } else {
                 let mut action_sequence = SeqFuture::new();
                 action_sequence.push(action.into_future());
                 loop {
                     let action =
                         pull_next_action(self.scheduler_state.as_ref(), &mut scheduler_queue);
                     action_sequence.push(action.into_future());
                     next_key = peek_next_key(&mut scheduler_queue);
                     if next_key != Some(current_key) {
                         break;
                     }
                 }
-                self.executor.spawn_and_forget(action_sequence);
+                spawn_futs.push(Box::pin(action_sequence));
             }

             current_key = match next_key {
                 Some(k) if k.0 == current_key.0 => k,
                 _ => {
                     drop(scheduler_queue); // make sure the queue's mutex is released.

                     let current_time = current_key.0;
                     self.synchronize_clock(current_time)?;
+
+                    // Batch-spawn all groups at this time (amortizes MT overhead).
+                    self.executor.spawn_and_forget_batch(spawn_futs);
+
                     self.run()?;

                     return Ok(Some(current_time));
                 }
             };
         }
```

Why this helps *given your numbers*:

* Old MT path: ~4.7M groups ⇒ ~4.7M `spawn_and_forget()` calls ⇒ ~9.4M mutex acquisitions.
* New MT path: ~309k steps ⇒ ~309k `spawn_and_forget_batch()` calls ⇒ ~618k mutex acquisitions.

That’s the right order-of-magnitude reduction.

---

## One more tiny tweak I’d do immediately

Given `max_groups/step=560`, set `with_capacity(32)` to something like `64` (or even `128`) to reduce reallocations on fat steps:

```rust
let mut spawn_futs = Vec::with_capacity(64);
```

This is cheap and avoids repeated growth on common steps.

---

## What to expect

This change should not alter simulation semantics (same grouping, same time barrier, same per-origin sequentialization). It only reduces overhead in the MT executor injection path.

If MT is *still* slower after this, the next culprit is usually **the “309k calls to Executor::run()” barrier cost** (park/unpark/search loops). At that point, you’re looking at either:

* reducing `steps` (time quantization / pacing floor), or
* changing the stepping policy (more invasive).

But **with your current stats**, batch-spawning is the next most “code-backed” move.

If you paste your `crates/nexosim/src/executor.rs` (the wrapper), I can rewrite section (3) as an exact diff matching your real type definitions.
