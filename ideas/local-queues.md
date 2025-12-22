They’re two different ways of attacking the same bottleneck (the **single global `scheduler_queue: Mutex<PriorityQueue<...>>`**), but they differ in *where contention goes*, *when scheduled events become visible*, and *how hard it is to preserve ordering/determinism*.

Below I’ll describe each in the context of your Nexosim design:

* the scheduler queue key is `(MonotonicTime, origin_id)`
* ordering within equal keys relies on the `PriorityQueue`’s epoch (FIFO for equal keys)
* `Simulation::step_to_next()` locks the scheduler queue to pick the next time + pull actions
* worker threads call `schedule_event(_batch)` concurrently while model tasks run

---

## Sharding by `origin_id`

### What it is

Replace the **one global** `Arc<Mutex<SchedulerQueue>>` with **many** queues (“shards”), each with its own lock:

```text
shard[0]: Mutex<PriorityQueue<(time, origin_id), Action>>
shard[1]: Mutex<PriorityQueue<(time, origin_id), Action>>
...
```

When scheduling, pick the shard deterministically:

```text
shard_index = origin_id % NUM_SHARDS
lock(shard[shard_index])
insert into that priority queue
unlock
```

### What it buys you

* **Contended lock becomes N smaller locks.**
  If you have lots of independent origins (many models scheduling), they spread across shards, so fewer threads block on the same mutex.
* **Events are still immediately visible** once inserted (no “deferred publish”).
* **Ordering by origin_id is easy to preserve** because the shard selection is based on `origin_id`, and you still store the full `(time, origin_id)` key (or even just `time` if each shard is “pure origin-id partitioned,” but you’ll usually keep the full key).

### What it costs

The hard part moves to the stepper:

`Simulation::step_to_next()` currently does:

1. lock scheduler_queue
2. peek next key
3. pull all actions at the next time
4. unlock
5. run executor

With sharding, the stepper must find the globally earliest scheduled time across shards. That means either:

* **Scan all shards**: lock each shard (or try_lock), peek, compute min, then pull from the winning shard(s), etc.
  This adds per-step overhead proportional to NUM_SHARDS.

or

* Maintain a **global “min-of-shards” structure** (e.g., a min-heap of each shard’s current top key), which is faster but more complex.

So sharding reduces *scheduling* contention, but can increase *stepping* overhead unless you’re careful.

### When it’s a good fit

* Scheduling happens **from many threads concurrently** (worker threads + potentially external threads using `Scheduler`).
* You want **immediate visibility** of scheduled actions.
* You want a relatively straightforward correctness story (still “insert directly into shared queues”).

---

## “Thread-local schedule buffers + merge at step boundary”

### What it is

Instead of inserting into the global priority queue immediately, each worker thread appends scheduled actions to a **thread-local vector**:

```text
TLS buffer for worker i: Vec<(time, origin_id, Action)>
```

Scheduling becomes:

```text
push into Vec  (no mutex, no heap insert)
```

Then, at a synchronization point—**the step boundary**—you merge all thread-local buffers into the global scheduling structure.

In your architecture, the natural barrier is:

* `Simulation::step_to_next()` pulls actions for time T
* spawns them
* calls `self.run()` which blocks until executor is idle for that step
* then returns to step logic to pick next time

So you can merge *after* `run()` completes (or right before choosing the next key), when the executor is quiescent.

### What it buys you

* Scheduling hot path becomes **lock-free (or close to it)**: no `scheduler_queue` mutex, no `BinaryHeap::push` per event.
* You can do **much cheaper bulk merge**:

  * collect all TLS vectors
  * sort by `(time, origin_id, local_seq)` (or stable-merge)
  * heapify once, or batch-insert efficiently
* It tends to scale extremely well when the workload is “tons of tiny schedule operations” (your case).

### What it costs (and this is the important part)

This design shifts complexity to **publish/merge semantics** and **ordering**.

#### 1) Events are not “globally visible” immediately

They only become visible at the flush/merge point.

In a discrete-event simulator, this is often okay because:

* the stepper doesn’t look at “future events” until it finishes the current step anyway,
* and your current race avoidance in `GlobalScheduler::schedule_from` is mostly about time advancing concurrently with scheduling.

But it’s a semantic change: “schedule now” doesn’t immediately mutate the shared priority queue.

#### 2) “Thread-local” is trickier than it sounds because tasks can migrate threads

Your MT executor uses work stealing (see `mt_executor.rs` and stealers). A given model’s future can be polled on different workers over time.

If you literally buffer by **worker thread**, then schedules originating from the same `origin_id` might end up split across multiple TLS buffers over time.

That’s not automatically wrong, but preserving **per-origin scheduling order** becomes more subtle.

In the current design, per-origin ordering for equal `(time, origin_id)` is preserved by:

* the fact that insertion happens into a single `PriorityQueue` that assigns FIFO epochs for equal keys.

With TLS buffers, you need *some way* to preserve an order for equal `(time, origin_id)` across buffers. Typical options:

* attach a **per-origin sequence number** at schedule time (monotonic counter stored in `Context`, since events for a model are logically sequential), and merge-sort by `(time, origin_id, seq)`; or
* avoid per-thread buffering and instead buffer **per origin_id** (which is basically “sharding by origin” but with deferred insertion); or
* accept weaker determinism where tie-order among equal keys could vary run-to-run (often undesirable).

#### 3) External scheduling threads

Nexosim’s `Scheduler` can be cloned and used from threads that are not worker threads.

Thread-local buffering doesn’t naturally apply there unless you:

* also give those threads a buffer and a way to register it for flushing, or
* fall back to the global locked queue for external scheduling.

So you often end up with a hybrid:

* worker-scheduled events go to TLS buffers
* external scheduling goes straight to a locked structure

### When it’s a good fit

* The dominant cost is **scheduler mutex contention / heap insertion cost** under multi-threading.
* Most scheduling happens **from worker threads during a step** (not from arbitrary external threads).
* You can tolerate (or carefully define) “visibility at step boundary,” which usually matches DES semantics anyway.
* You’re willing to do a bit more correctness engineering around ordering/determinism.

---

## Key differences summarized

### Where contention goes

* **Sharding**: reduces contention by splitting the shared queue into many locked queues. Still contended, just less.
* **TLS + merge**: removes contention from the hot path almost entirely; merge happens when workers are idle.

### When scheduled actions become visible

* **Sharding**: immediately (after shard lock acquisition).
* **TLS + merge**: only after flush/merge (typically after the step completes).

### Correctness/ordering complexity

* **Sharding**: relatively straightforward; aligns with current key `(time, origin_id)` design.
* **TLS + merge**: must carefully preserve per-origin FIFO for equal keys, especially because tasks can run on different workers. Needs sequence numbers or a merge discipline.

### Stepper complexity

* **Sharding**: stepper must find global minimum across shards efficiently (scan or maintain a global min-of-shards structure).
* **TLS + merge**: stepper remains simple (one place to merge), but merge work must be efficient.

---

## A useful way to think about it

* **Sharding** is a *data-structure concurrency* solution: “make the shared structure less contended.”
* **TLS + merge** is a *phase separation / batching* solution: “don’t touch the shared structure while parallel work is happening; publish only at barriers.”

In simulators, barrier-style designs can be extremely effective because the simulation already has a natural “step boundary” barrier (your `run()` completion).

---

## Practical note for your codebase

Given your executor can migrate tasks between workers, a pure “thread-local buffers keyed only by worker thread” approach can be correct **only if** you add a stable ordering key (per-origin sequence) for tie cases.

If you don’t want that complexity, the most robust “deferred publish” variant is actually:

* **buffers keyed by origin_id** (or shard-by-origin)
* flushed at step boundary
* inserted via true bulk heap operations

That starts looking like a hybrid of both ideas:

* sharding (by origin_id) for correctness + locality
* merge at boundary to remove lock contention from the hot path

If you tell me whether you care about strict determinism for same-time ties across different origins, I can suggest the simplest merge ordering rule that matches your current semantics.

