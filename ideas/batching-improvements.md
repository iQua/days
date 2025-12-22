Here’s what stands out (from the code you shared) as the most plausible explanation for “**10 threads is ~50% slower than 1 thread**” on `exp_tcp_fattree.toml`, *even after* adding Nexosim bulk scheduling + `Port` batching — plus concrete, code-backed levers to pull next.

---

## 1) The big one: the global `scheduler_queue: Mutex<PriorityQueue<...>>` is still the serialization point

### Why this gets *worse* with more threads

Even with `Context::schedule_event_batch(...)`, **every scheduled departure is still a separate insertion into one global heap**, protected by one `Mutex`.

The critical path is:

* `Port::run()` (Days) schedules up to **1024** departures:

  * `days/src/schedulers/port.rs`: `DEFAULT_RUN_BATCH_SIZE = 1024`
  * calls: `cx.schedule_event_batch(schedule, Self::send_scheduled)` (one call per batch)

* which becomes:

  * `nexosim::model::Context::schedule_event_batch` → `GlobalScheduler::schedule_event_batch_from`
  * `days/crates/nexosim/src/simulation/scheduler.rs`:

    ```rs
    let mut scheduler_queue = self.scheduler_queue.lock().unwrap();
    ...
    for (deadline, arg) in deadlines_and_args {
        let time = deadline.into_time(now);
        let action = Action::new(...);
        scheduler_queue.insert((time, origin_id), action);
    }
    ```

* `scheduler_queue.insert(...)` is:

  * `PriorityQueue::insert` → `BinaryHeap::push` (log N)
  * `days/crates/nexosim/src/util/priority_queue.rs`

So: batching reduced **lock acquisitions**, but it did **not** reduce:

* total number of inserts (still one per packet departure), or
* the fact that inserts are done under a single global mutex, or
* the `O(log N)` heap cost per insert.

With `num_threads=10`, you now have **many workers concurrently trying to do heap inserts**, and only one can proceed at a time → **lock convoy + cache-line bouncing + allocator contention**. With `num_threads=1`, the same work happens but with **no waiting** and far less coherence traffic.

### Why batching can actually increase heap work

Before batching, many “next-step” designs keep the global scheduler queue relatively small (e.g., “next departure only” per port). After batching, you likely have **many more future actions resident in the global priority queue at once**.

That increases:

* heap size `N` → `log N` cost per insert/pop rises,
* memory footprint/cache misses,
* time spent holding the scheduler mutex per batch.

This is a key reason multithreading can regress even if wall-clock “useful compute” per event is unchanged.

---

## 2) `mailbox_capacity = 1` is extremely likely to be amplifying cross-thread overhead

Your config explicitly sets:

```toml
mailbox_capacity = 1
```

And Days applies that to switch mailboxes:

* `Topology::new()` parses `mailbox_capacity`
* `Topology::init_mailboxes()`:

  * `Mailbox::with_capacity(self.mailbox_capacity)`
  * `days/src/topos/topo.rs`

Nexosim mailboxes are bounded MPSC queues:

* `days/crates/nexosim/src/channel/queue.rs`

With capacity 1, the steady-state behavior under bursty traffic is:

* producer tries `Queue::push`
* queue is full → `PushError::Full(...)` → sender awaits / retries / wakes
* consumer pops, releases slot, wakes producers
* repeat **constantly**

On 1 thread, this is “just” frequent polling/yielding overhead.
On 10 threads, this becomes:

* much more cross-core synchronization on the queue’s atomics,
* more wakeups/parking,
* more executor scheduling and potentially more stealing,
* more cache invalidation (enqueue/dequeue positions, stamps).

Also note Nexosim’s own docs warn that too-small mailbox capacity can hamper performance and increase deadlock likelihood (see the big comment block in `days/crates/nexosim/src/simulation.rs`).

**In short:** capacity=1 makes the simulation behave closer to a synchronous handoff chain; the minute you add threads, you pay the cost of synchronization without getting the benefit of buffering.

---

## 3) MT executor overhead can dominate when tasks are tiny and frequently blocked

When `num_threads > 1`, Nexosim uses the MT executor:

* `days/crates/nexosim/src/executor/mt_executor.rs`
* local queues are small:

  * `BUCKET_SIZE = 128`
  * `QUEUE_SIZE = 256`

Two concrete hotspots show up in the code:

### 3.1 Injector queue is a mutex-protected `Vec<Bucket<...>>`

* `injector.rs`: `Injector` uses `Mutex<Vec<Bucket<...>>>`
* Under high wake/schedule rates, worker local queues fill → tasks get pushed to injector → contention on that mutex.

### 3.2 There is an explicit busy-spin in the worker loop

In `run_local_worker`:

```rs
while local_queue.spare_capacity() < bucket_iter.len() {}
```

The comment says “very remote possibility”, but your workload (tons of tiny tasks, lots of wakeups, lots of cross-thread interactions, mailbox_capacity=1) is exactly the sort of environment where edge-case executor paths stop being “remote”.

Even if it’s rare, *when it triggers*, it burns CPU doing nothing — and with 10 threads you can easily end up with multiple workers spinning at once.

---

## 4) Per-event allocations and refcount traffic get worse with threads

Every scheduled action does:

* `Action::new(...)` → `Box<dyn ActionInner>`
* heap allocation for the action object
* and likely additional allocations in closure/future capture paths (depending on inlining)

Inside `schedule_event_batch_from`, per event you also do:

* `sender.clone()` (cloning the mailbox sender handle)
* `func.clone()` (often cheap for fn pointers, but still)
* heap insert operations

On 1 thread, allocator contention is low.
On 10 threads, allocator contention and cache traffic are much higher.

This matters because your “unit of work” (send a packet, enqueue/dequeue, schedule next) is very small.

---

# Concrete improvement opportunities (code-backed), in priority order

## A) Fix the config-level concurrency killer: raise `mailbox_capacity`

**Change:** in `configs/exp_tcp_fattree.toml`, set:

* `mailbox_capacity = 16` (or 64 / 128 for this workload)

Why it’s code-backed:

* Switch mailboxes are created with `Mailbox::with_capacity(self.mailbox_capacity)` in `Topology::init_mailboxes()`.
* Nexosim queue is bounded; capacity=1 maximizes backpressure, wakeups, and retries.

Expected effect:

* fewer send stalls,
* fewer cross-thread wakeups/parks,
* better batching effectiveness because receivers can absorb bursts,
* higher real concurrency (more runnable tasks).

If you want a heuristic: mailbox capacity should usually be at least the typical *fan-in burst* per step for hot switches.

---

## B) Reduce `Port` lookahead so you don’t explode global scheduler queue size

Right now you have:

* `Port::DEFAULT_RUN_BATCH_SIZE = 1024` (`days/src/schedulers/port.rs`)

That’s a huge lookahead. It reduces re-scheduling frequency, but it:

* inflates the global scheduler heap,
* increases `log N` cost,
* increases time holding `scheduler_queue` mutex per batch insert.

**Change options (low effort):**

1. Drop to 64 or 128.
2. Make it configurable from TOML.
3. Add a “max batch horizon” (time-based) like your `ideas/batching.md` suggested.

Why it’s code-backed:

* batching trades local queue work for global scheduler queue work.
* global scheduler queue is a single mutex + binary heap.

This is one of the most likely reasons “10 threads” regresses: the queue becomes so large that lock hold times and cache misses dominate.

---

## C) Make `schedule_event_batch_from` *actually* bulk on the heap side

Right now bulk scheduling locks once, but still does **K individual heap inserts**:

```rs
for (...) {
    scheduler_queue.insert(...); // BinaryHeap::push each time
}
```

**Opportunity:** add a true bulk-insert API to `PriorityQueue` and use heapify/append.

Why this is concrete:

* `PriorityQueue` wraps `BinaryHeap<Item<K,V>>` and controls epoch.
* You can create a `BinaryHeap<Item<...>>` from a `Vec<Item<...>>` in O(K),
  then `append` it to the existing heap (heap rebuild is O(N+K)).

Even if the final complexity isn’t perfect, it’s typically better than **K × log(N)** pushes.

### Sketch of the change (where)

* Add to `days/crates/nexosim/src/util/priority_queue.rs`:

  * `fn insert_batch(&mut self, items: impl IntoIterator<Item=(K,V)>)`
* Update `GlobalScheduler::schedule_event_batch_from` to:

  * build a `Vec<(key, action)>` and call `insert_batch(...)` once.

Impact:

* reduces time under scheduler mutex per batch,
* reduces lock contention across threads,
* reduces heap churn.

---

## D) Shorten the scheduler mutex critical section by moving allocations out of it

In `GlobalScheduler::schedule_event_batch_from`, you currently:

* lock mutex
* validate
* allocate `Action`s and futures
* heap-insert

The heaviest parts are allocations and heap ops.

**Opportunity (semantics-preserving but slightly more complex):**

* inside the lock: read `now`, convert deadlines to absolute times (cheap), validate
* outside the lock: build `Action`s (allocations) and maybe pre-structure items
* re-lock and re-check `now` hasn’t advanced past any scheduled time (or retry)
* insert

This is the “optimistic prepare + commit” pattern.

Why this is code-backed:

* the lock is only needed to avoid the race described in `schedule_from`.
* for Days’ typical execution model, time does not advance during `executor.run()` for a step; so in practice a retry is rare unless external scheduling is happening.

Even if you don’t do the optimistic approach, you can still:

* pre-allocate vectors outside lock,
* minimize work while holding lock.

---

## E) Remove the single global scheduler lock: shard by `origin_id` (moderate change, big win potential)

The scheduler key is `(MonotonicTime, origin_id)` and Nexosim already groups by `origin_id` in `Simulation::step_to_next()`.

That structure makes sharding pretty natural:

* Replace `scheduler_queue: Arc<Mutex<SchedulerQueue>>`
  with something like `Arc<Vec<Mutex<SchedulerQueue>>>` (N shards).
* Route inserts by `shard = origin_id % N`.

Why this is code-backed:

* `origin_id` is already fundamental to ordering semantics in `SchedulerQueue`.
* events from the same origin must preserve scheduling order; sharding by origin preserves that.

Then, in `Simulation::step_to_next()`:

* since it runs after `run()` completes (and in Days, typically no concurrent scheduling),
  you can lock shards in a fixed order and pull the global minimum across shards.

This directly attacks the “many threads contend on one mutex” problem.

---

## F) Executor-specific fixes if you confirm they’re hot

### F1) Kill or soften the busy-spin

In `mt_executor.rs`:

```rs
while local_queue.spare_capacity() < bucket_iter.len() {}
```

Concrete options:

* add `std::hint::spin_loop()` at minimum (reduces power + improves SMT behavior),
* exponential backoff,
* or better: **split the bucket** and only extend what fits; push the remainder back to injector.

### F2) Increase `QUEUE_SIZE` / adjust bucket behavior

If you’re overflowing local queues frequently, the injector mutex becomes a hotspot.

* `QUEUE_SIZE = BUCKET_SIZE * 2` is only 256.
* For a simulator with bursts, bigger local queues often reduce contention.

This is easy to try and is code-local in `mt_executor.rs`.

---

## G) Apply batching (or fewer schedules) in flow sources too

You explicitly noted:

> packet sources still schedule events (per-packet/per-timer)

Even if `Port` batching is perfect, **sources can still hammer the same global scheduler lock**.

Concrete next steps (code-backed by the architecture, even though the file content wasn’t included):

* search in `days/src/flows/*` for repeated `cx.schedule_event(...)` patterns inside hot paths
* replace “many individual schedules” with `cx.schedule_event_batch(...)` where the timing is known
* or redesign: compute a window and schedule fewer timer events.

Given your config is 1000 TCP flows, this can easily be a dominant scheduling source.

---

# Why your symptom matches these bottlenecks

“~50% slower with 10 threads” is exactly what you see when:

* the main hot resource is **a contended mutex** (global scheduler queue),
* plus **tiny mailboxes** cause tons of cross-thread wake/sleep,
* plus tasks are small enough that executor overhead dominates,
* and batching increased the number of outstanding scheduled actions → bigger heap → slower inserts/pops → longer lock hold time.

---

# A practical “most likely to help” sequence

If you want the fastest path to an improvement without deep refactors, do this:

1. **Set `mailbox_capacity = 16` or 64** in `exp_tcp_fattree.toml`.
2. **Reduce `Port::DEFAULT_RUN_BATCH_SIZE`** from 1024 → 64/128 (or cap by time horizon).
3. Add **bulk heap insertion** in `PriorityQueue` and use it from `schedule_event_batch_from`.

Those three are all strongly supported by the code you provided, and they directly target the places where multithreading can become net-negative.

If you do deeper refactors next, shard the scheduler queue (by origin_id) or move to a “thread-local schedule buffers + merge at step boundary” design to remove scheduler mutex contention from the hot path.

---

If you want, I can also outline a minimal instrumentation patch (a few atomics + `Instant` timers) to quantify:

* scheduler mutex wait time vs hold time,
* count of mailbox `PushError::Full`,
* count/duration of the executor spin-loop,
  so you can confirm which of the above is actually dominating in *your* run.

