What’s happening today (why send_and_run dominates)

  - In FIFO Port, each transmitted packet schedules the next transmission by calling cx.schedule_event(...) inside Port::run (src/schedulers/port.rs:252), and that call takes Nexosim’s global scheduler-queue mutex.
  - Because Port::send_and_run (src/schedulers/port.rs:201) executes once per forwarded packet and immediately schedules the next one, you get ~1 contended global-lock acquisition per packet per active port, which caps cross-model concurrency.

  Core idea: turn “per-packet self-scheduling” into “batched lookahead scheduling”
  Batch work so that one Port::run invocation schedules the next K departures (a lookahead window), and the per-departure handler does no further scheduling. Only when the lookahead window is exhausted do we call run again to schedule the next window.

  This reduces the number of “schedule-next” operations from O(packets) to O(packets / K), which is the piece currently driving lock contention.

  ———

  ## Plan (FIFO Port)

  1) Split the hot handler into “send-only” + “refill”

  - Introduce a lightweight event handler like send_scheduled(packet) that:
      - updates stats and calls self.output.send(packet).await, and
      - decrements a counter tracking how many pre-scheduled sends remain,
      - does not call run.
  - Only the final send in a batch (or “counter reached 0”) triggers a refill(now, cx) that calls run_batch(now, cx) to schedule the next batch.

  Result: the expensive “send + schedule-next” path (send_and_run) runs once per batch, not once per packet.

  2) Add a bounded lookahead scheduler: run_batch(now, cx)

  - When the port becomes active (idle → busy) or a batch ends, run_batch:
      - pops up to K packets from self.queue,
      - computes each packet’s service start and departure time (sequentially),
      - updates packet.queueing_delay_update(start) and packet.departure_update(depart),
      - schedules send_scheduled(packet) for each computed departure time,
      - marks the last one as “refill-trigger” (either via a different handler, or by checking a counter after decrement).
  - Update busy_until to the last departure time in the batch.

  3) Preserve existing drop semantics (important because batching pops >1 packet)
  Today DropStrategy::action uses:

  - bytes: self.queue_length (already includes the in-flight packet until it actually departs),
  - packets: self.queue.len() (excludes the currently-transmitting packet because it’s popped at service start).

  If you pop K packets at once, self.queue.len() would shrink too much unless you compensate.
  Add a field like in_flight_scheduled: usize (includes the currently transmitting packet + pre-scheduled future ones). Then in packet_received compute:

  - queue_len_for_drop = self.queue.len() + in_flight_scheduled.saturating_sub(1)
    …and pass that to the drop strategy instead of self.queue.len(). This keeps “exclude exactly one in-service packet, count all other waiting packets” consistent with today.

  4) Choose a conservative batching policy to avoid exploding the global scheduler queue
  Scheduling too far ahead increases the number of pending Nexosim actions at once (you’re moving packets from VecDeque into the global priority queue). Mitigate by bounding lookahead with both:

  - K_max (e.g., 4–32 to start), and/or
  - max_batch_time (stop once cumulative transmit time exceeds some horizon, e.g., 200–500 µs).

  This keeps queue-size growth controlled while still cutting lock traffic meaningfully.

  ———

  ## Required runtime support: “bulk schedule” (to avoid K separate global-lock acquisitions)

  If run_batch just calls cx.schedule_event K times, you still take K mutex locks. The real win requires adding a batching API so those K inserts happen under one lock acquisition:

  - Add something like Context::schedule_event_batch(func, Vec<(deadline,arg)>) in Nexosim (or a fork/patch of it).
      - It locks the scheduler queue once, reads now once, validates all times, then inserts all actions.

  This is the minimal Nexosim change that directly targets the bottleneck you described.

  ———

  ## Validation / rollout (so you don’t change behavior silently)

  - Add a small deterministic integration test for FIFO/RED where arrivals are known and assert:
      - forwarded/dropped counts match baseline,
      - packet time matches expected departures,
      - RED behavior matches (sensitive to queue-length accounting).
  - Run cargo test --features test -- --show-output.
  - Benchmark with configs/exp_tcp_fattree.toml:1 after setting num_threads > 1 and compare total runtime.

  If you want, I can propose concrete batch-size defaults + a minimal Nexosim API surface that won’t leak internals but still enables schedule_event_batch.
