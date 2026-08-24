Your numbers now give you a very clear roadmap.

* With batching spawn, you got **10.28s → 9.93s** (nice).
* But you still have **steps = 309,205**.
* That means you’re paying a **per-step fixed cost** (lock queue → pick next time → spawn groups → `Executor::run()` barrier → repeat) **309k times**.

Even if you made spawning “free”, 309k barriers is enough to dominate wall time.

So the next big wins come from:

1. **reducing the number of distinct timestamps (“steps”)** ⇒ time quantization / pacing floor
2. **reducing the per-step barrier cost** ⇒ changing stepping/executor policy

Below is how I’d proceed, in the lowest-risk order, with concrete implementation sketches.

---

# 1) Reduce steps with time quantization (and keep `packet.time` consistent)

A key constraint in your codebase: **Days uses `packet.time` as the “current simulation time”** in many places (Port/Sink/Source), *not* `cx.time()`, to reduce global-clock contention.

That means a “scheduler-only” quantization (rounding times inside Nexosim scheduler) can easily create:

* action executes at quantized time `Tq`
* but payload packet carries unquantized `T`
* downstream models use `packet.time == now` assumption and drift

So the safe route is:

> Quantize at the **time producers** in Days (where you set `packet.time` / compute departure/arrival times), so the payload time and the scheduled time remain aligned.

You *can* still add a scheduler-side quantization as a backstop later, but start with Days-side to avoid breaking local-time invariants.

## 1.1 Add a global “time quantum” to Days config

Add to `exp_tcp_fattree.toml`:

```toml
time_quantum_ns = 1000  # 1 us to start
```

Parse it in `Topology::new()` (or wherever you parse config) into a `u64` and store it in the topology / pass to components.

## 1.2 Use integer nanoseconds for quantization

Add a helper (Days-side, e.g. `src/utils/time_quant.rs`):

```rust
#[derive(Clone, Copy)]
pub struct TimeQuant {
    pub quantum_ns: u64, // 0 disables
}

impl TimeQuant {
    #[inline]
    pub fn quantize_up_ns(self, t_ns: u64) -> u64 {
        let q = self.quantum_ns;
        if q == 0 { return t_ns; }
        let r = t_ns % q;
        if r == 0 { t_ns } else { t_ns + (q - r) }
    }

    #[inline]
    pub fn s_to_ns_round(t_s: f64) -> u64 {
        // round-to-nearest ns; avoids systematic bias from truncation
        (t_s * 1e9).round().max(0.0) as u64
    }

    #[inline]
    pub fn ns_to_s(t_ns: u64) -> f64 {
        (t_ns as f64) * 1e-9
    }
}
```

Now you can quantize “real” times deterministically.

---

# 2) Where to quantize to reduce steps (highest ROI call sites)

You want to quantize wherever you *create new distinct timestamps*. In your workload, the big ones are:

* **Port departures** (computed via `start_time += timeout`)
* **Wire arrivals** (prop delay)
* **TCP pacing / RTO timeouts** (small intervals create lots of unique times)

## 2.1 Quantize Port departure times (and stop `f64` drift)

Right now in `Port::run()`:

* you compute departure times as `f64` by repeated addition
* then schedule via `Duration::from_secs_f64(start_time - self.time)`
* you also stamp packets with `packet.departure_update(start_time)` (f64)

That combination is perfect for creating lots of slightly-different nanosecond timestamps across ports.

### Fix: keep local time in integer ns, and quantize each departure

Sketch (not full diff, but “exact shape”):

```rust
// in Port struct
pub time_ns: u64,
busy_until_ns: u64,
rate_bps: u64,          // store as integer if possible
time_quant: TimeQuant,  // injected from config
```

In `run(now, cx)`:

```rust
let now_s = ...; // from cx.time(), secs_f64
let now_ns = TimeQuant::s_to_ns_round(now_s);

// sync local time if needed
if self.time_ns == 0 { self.time_ns = now_ns; }

let mut depart_ns = self.time_ns;

let mut schedule: Vec<(Duration, Packet)> = Vec::with_capacity(K);

for _ in 0..K {
    let Some(mut pkt) = self.queue.pop_front() else { break; };

    // service time in ns: (bytes*8)/bps seconds
    let bits = (pkt.size as u64) * 8;
    let tx_ns = (bits * 1_000_000_000u64) / self.rate_bps;

    // sequential service (store-and-forward)
    depart_ns = depart_ns.saturating_add(tx_ns);

    // quantize
    depart_ns = self.time_quant.quantize_up_ns(depart_ns);

    let depart_s = TimeQuant::ns_to_s(depart_ns);

    pkt.queueing_delay_update(TimeQuant::ns_to_s(self.time_ns));
    pkt.departure_update(depart_s);

    schedule.push((Duration::from_nanos(depart_ns - self.time_ns), pkt));
    self.in_flight += 1;
}

self.busy_until_ns = depart_ns;

cx.schedule_event_batch(schedule, Self::send_scheduled).unwrap();
```

**What this buys you:**

* eliminates float accumulation drift
* snaps departures onto a grid (quantum)
* increases timestamp collisions across ports ⇒ **fewer steps**

**Choosing quantum:** for your 10Mbps + 1024B packet, serialization is **819.2µs**.
A **1µs** quantum introduces at most ~0.12% timing jitter relative to that link service time.

Try 1µs first; then 2µs, 5µs, 10µs and watch `steps` and wall time.

---

## 2.2 Quantize Wire propagation times (same reason)

Wherever your `Wire` schedules arrival (you mentioned `days/src/flows/wire.rs`), do the same:

* compute `arrival_ns = send_time_ns + prop_delay_ns`
* quantize `arrival_ns`
* set `packet.time = arrival_s` (or via a setter)
* schedule event at `arrival_ns - now_ns`

This keeps packet timestamps consistent with actual execution times and helps coalesce.

---

## 2.3 TCP pacing floor (targeted step reduction)

Even with port/wire quantized, TCP can still generate very fine-grained steps if pacing intervals get tiny.

You already have:

```rust
const MIN_PACING_INTERVAL: f64 = 1e-9;
```

That essentially allows a pacing loop to create “almost continuous” timestamps.

### Add a configurable pacing floor tied to your quantum

In `TCPPacketSource`, add:

```rust
pacing_floor_ns: u64, // set from config, default = time_quantum_ns
```

Then in `send_packet()`:

```rust
let pacing_interval = if pacing_rate > 0.0 { self.mss as f64 / pacing_rate } else { 0.0 };
let mut pacing_ns = TimeQuant::s_to_ns_round(pacing_interval);

// enforce floor
pacing_ns = pacing_ns.max(self.pacing_floor_ns);

// quantize the *next send time* instead of quantizing interval
let now_ns = TimeQuant::s_to_ns_round(now);
let next_ns = self.time_quant.quantize_up_ns(now_ns + pacing_ns);
let effective_interval_ns = next_ns - now_ns;

self.busy_until = TimeQuant::ns_to_s(next_ns);
return Some(TimeQuant::ns_to_s(effective_interval_ns));
```

That ensures:

* pacing never schedules events closer than your quantum
* send times land on grid ⇒ fewer unique step times

---

# 3) How to tune quantum without flying blind

After each change, rerun and check:

* `steps` (should drop)
* `avg_groups/step` and `max_groups/step` (often rise)
* wall time

Add one more stat: histogram of step deltas. It tells you what quantum will actually collapse times.

### Minimal delta histogram (Nexosim-side)

In `Simulation::step_to_next`, track `dt_ns = current_time - prev_time` and bucket it into powers-of-two or fixed bins (<1us, <2us, <5us, <10us, ...). If most deltas are already >10us, a 1us quantum won’t reduce steps much.

---

# 4) If quantization isn’t enough: change stepping/executor policy

Even after you reduce step count, Nexosim still does:

* spawn groups
* `Executor::run()` until pool idle (workers park)
* next step wakes pool again

That “park/unpark per step” is costly at 100k+ steps.

There are two realistic “policy” directions:

## 4.1 Hot worker pool (avoid park/unpark between steps)

Goal: **don’t let the pool fully park** when it becomes idle; keep it “warm” for the next injection.

This is conceptually like an executor “keepalive” / “linger”:

* When the last worker detects idleness, instead of immediately parking, it spins (or parks with short timeout) for a brief linger window.
* If the main thread injects new tasks quickly (which it does, next step), the worker grabs them without a full unpark/activation cycle.

This requires changes in `mt_executor::run_local_worker` / `pool_manager` logic because today “idle” is defined in terms of workers transitioning to inactive+parked.

A practical design:

* add a separate “quiescent but awake” state
* keep `pool_is_idle()` true for the main thread, but allow one worker to remain awake and polling injector briefly

This can pay off hugely when `steps` is large.

Tradeoff: CPU usage increases (but you can bound it with a short linger, e.g. 10–50µs).

## 4.2 Reduce barriers by making `run()` cheaper to re-enter

You already hit the big two mutexes in spawn; you fixed that.

Next is: the run/idle handshake.
If you want to go further, the bigger refactor is to base quiescence on an atomic “pending work” counter rather than parking workers and unparking the main thread each step.

That’s more invasive but can be very fast:

* increment counter when an action future is injected or a message is enqueued
* decrement when completed/processed
* main thread waits on counter reaching 0 using an `atomic_wait` primitive (or parker)

This avoids 309k full worker deactivate/activate cycles.

---

# 5) Recommended order of work

1. **Days-side quantize Port + Wire using integer ns**

   * this often reduces step count significantly with minimal semantic damage
2. **TCP pacing floor tied to quantum**
3. Re-run the workload. If MT runtime improves meaningfully, stop here.
4. If `steps` is still huge:

   * implement a **hot pool / linger** mode in executor to reduce park/unpark overhead
5. Only then consider larger stepping refactors.

---

If you share (or paste) your `flows/wire.rs` scheduling code and the Port rate representation (is it always integer from config?), I can give you a very tight patch sketch for (2.2) and the exact “integer-ns Port::run” edit that keeps your existing interfaces but removes float drift + adds quantization cleanly.
