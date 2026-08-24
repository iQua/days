use crate::{FlowId, NodeId};

use std::collections::BTreeMap;

/// One level in a deterministic channel-stream capacity histogram.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChannelStreamCapacityLevel {
    pub capacity: usize,
    pub stream_count: usize,
}

pub(crate) fn channel_capacity_distribution(
    capacities: &[usize],
) -> Vec<ChannelStreamCapacityLevel> {
    let mut counts = BTreeMap::<usize, usize>::new();
    for capacity in capacities {
        counts
            .entry(*capacity)
            .and_modify(|count| *count = count.saturating_add(1))
            .or_insert(1);
    }
    counts
        .into_iter()
        .map(|(capacity, stream_count)| ChannelStreamCapacityLevel {
            capacity,
            stream_count,
        })
        .collect()
}

/// Optional upper bounds for device arenas whose production defaults are derived from the full
/// simulation image.
///
/// Each cap is applied independently as `min(derived, cap)`, then raised to the number of records
/// already resident in the input image. A run that later needs more space fails through the
/// existing explicit device-capacity error; records are never truncated. `None` preserves the
/// uncapped production sizing exactly.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DeviceCapacityCaps {
    /// Maximum fallback-FEL records assigned to one LP.
    pub fallback_fel_events_per_lp: Option<usize>,
    /// Maximum queued packet records assigned to one LP.
    pub queue_packets_per_lp: Option<usize>,
    /// Plane-wide starting cap applied independently to each incoming-channel stream. Targeted
    /// retry floors may raise individual streams after a fault; `None` remains uncapped.
    pub channel_events_per_stream: Option<usize>,
    /// Maximum remote staging records assigned independently to one destination LP.
    pub remote_staging_events_per_lp: Option<usize>,
    /// Maximum total producer-outbox records for one round.
    pub outbox_events_total: Option<usize>,
    /// Maximum out-of-order TCP receive ranges assigned to one flow.
    pub tcp_receiver_ranges_per_flow: Option<usize>,
    /// Maximum TCP segment-ledger records assigned to one flow.
    pub tcp_ledger_segments_per_flow: Option<usize>,
    /// Maximum observed-packet, departure, and arrival records assigned to one LP and plane.
    pub observation_events_per_lp: Option<usize>,
}

/// Retry-time lower bounds for capacities that proved too small in an earlier all-or-nothing
/// device attempt. Zero leaves the derived planner capacity unchanged.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DeviceCapacityFloors {
    pub fallback_fel_events_per_lp: usize,
    pub queue_packets_per_lp: usize,
    pub channel_events_per_stream: usize,
    pub service_events_per_stream: usize,
    pub generator_events_per_stream: usize,
    pub remote_staging_events_per_lp: usize,
    pub outbox_events_total: usize,
    pub tcp_receiver_ranges_per_flow: usize,
    pub tcp_ledger_segments_per_flow: usize,
    pub observation_events: usize,
    pub worklist_entries_total: usize,
}

impl DeviceCapacityFloors {
    /// Field-wise maximum of two floor sets.
    ///
    /// Floors are lower bounds, so combining two of them is a maximum in every lane. This is how a
    /// [`CapacityWarmStart`] is applied on top of caller-supplied floors without discarding either.
    #[must_use]
    pub fn merged_with(self, other: Self) -> Self {
        Self {
            fallback_fel_events_per_lp: self
                .fallback_fel_events_per_lp
                .max(other.fallback_fel_events_per_lp),
            queue_packets_per_lp: self.queue_packets_per_lp.max(other.queue_packets_per_lp),
            channel_events_per_stream: self
                .channel_events_per_stream
                .max(other.channel_events_per_stream),
            service_events_per_stream: self
                .service_events_per_stream
                .max(other.service_events_per_stream),
            generator_events_per_stream: self
                .generator_events_per_stream
                .max(other.generator_events_per_stream),
            remote_staging_events_per_lp: self
                .remote_staging_events_per_lp
                .max(other.remote_staging_events_per_lp),
            outbox_events_total: self.outbox_events_total.max(other.outbox_events_total),
            tcp_receiver_ranges_per_flow: self
                .tcp_receiver_ranges_per_flow
                .max(other.tcp_receiver_ranges_per_flow),
            tcp_ledger_segments_per_flow: self
                .tcp_ledger_segments_per_flow
                .max(other.tcp_ledger_segments_per_flow),
            observation_events: self.observation_events.max(other.observation_events),
            worklist_entries_total: self
                .worklist_entries_total
                .max(other.worklist_entries_total),
        }
    }
}

/// The capacity a converged device attempt planned with, replayable as the *starting* capacity of
/// a later run of the same image (T20l fix 3).
///
/// # Why this exists
///
/// The capacity-retry loop converges by discarding attempts. T20l phase 1 §4.1 measured the RQ9
/// frontier running four attempts, three of them discarded, at 90-103% of the successful attempt's
/// device time — the frontier device arm runs the simulation twice over and reports half of it —
/// on top of four plan builds and four uploads. Yet the capacity the fourth attempt planned with
/// is **deterministic derived output** of the first three: same image, same config, same faults,
/// same growth law, same vector. Handing that answer back as the *first* attempt's starting point
/// removes the discarded attempts without changing what is computed.
///
/// # Why it cannot change a result
///
/// Capacity on this path is **refuse-or-run, never semantics** (the T20g invariant): a device arena
/// is either large enough, in which case the run proceeds and produces the complete state the
/// scalar oracle produces, or it is too small, in which case the attempt aborts with a typed
/// [`CapacityRetryRecord`] fault and nothing is emitted. Records are never truncated and no
/// capacity value is readable by the simulation. A warm start therefore moves a run between
/// "refuses once, then runs" and "runs immediately"; it cannot move it between two different
/// answers. The executor tests assert that byte-identity in both directions.
///
/// # Provenance
///
/// The only supported source is a successful run's own [`Self`] snapshot. It is not a compiled
/// image cache and has nothing to do with kernel or pipeline caching: it is a **sizing hint** for
/// host planning, orthogonal to the compiled device image, and it is re-derived by the run that
/// emits it.
///
/// # Shape
///
/// Every vector is a sparse, ascending, key-deduplicated association list, so a snapshot has one
/// canonical form and two runs of the same image emit the same bytes. Entries whose key the
/// replaying image does not plan are simply never consulted; entries below the derived capacity do
/// not bind, because every consumer takes a maximum.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CapacityWarmStart {
    /// Converged lower bounds for the arenas that are sized plane-wide rather than per entity.
    pub floors: DeviceCapacityFloors,
    /// `(immutable channel index, capacity)` for every channel-inbox stream a retry grew.
    pub channel_events_by_stream: Vec<(usize, usize)>,
    /// `(planned base capacity, capacity)` for every TCP receiver-range class a retry grew.
    pub tcp_receiver_ranges_by_base: Vec<(usize, usize)>,
    /// `(flow, capacity)` for every TCP segment-ledger row a retry grew. This is the per-flow
    /// capacity vector the T20i occupancy readback produces.
    pub tcp_ledger_segments_by_flow: Vec<(usize, usize)>,
}

impl CapacityWarmStart {
    /// True when the snapshot constrains nothing, which is exactly the stock starting point.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }
}

/// Collapses an arbitrary association list into the canonical ascending, max-merged form.
#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
fn warm_start_map(pairs: &[(usize, usize)]) -> BTreeMap<usize, usize> {
    let mut merged = BTreeMap::new();
    for (key, capacity) in pairs {
        merged
            .entry(*key)
            .and_modify(|current: &mut usize| *current = (*current).max(*capacity))
            .or_insert(*capacity);
    }
    merged
}

/// Retry-time channel floors keyed by the immutable channel-stream index in the image.
///
/// Channel streams deliberately do not grow by starting-cap class: a capped frontier image puts
/// almost every stream in the same class, which would recreate the plane-wide allocation this
/// retry state exists to avoid. The device reports one deterministic stream per failed attempt,
/// and only that stream receives a higher floor for its replacement plan.
#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ChannelCapacityFloors {
    base_by_stream: Vec<Option<usize>>,
    capacity_by_stream: BTreeMap<usize, usize>,
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
impl ChannelCapacityFloors {
    /// Starts from a warm start's per-stream capacities instead of from nothing.
    ///
    /// Seeding `capacity_by_stream` is exactly the state a retry chain would have reached: both
    /// [`Self::channel`] and [`Self::raise`] read the same `base.max(floor)` expression, so a
    /// seeded floor is indistinguishable from a learned one and the base-stability check the retry
    /// loop depends on is untouched.
    pub(crate) fn warm_started(capacities: &[(usize, usize)]) -> Self {
        Self {
            base_by_stream: Vec::new(),
            capacity_by_stream: warm_start_map(capacities),
        }
    }

    /// The per-stream capacities this run converged on, in canonical ascending order.
    pub(crate) fn converged_capacities(&self) -> Vec<(usize, usize)> {
        self.capacity_by_stream
            .iter()
            .map(|(stream, capacity)| (*stream, *capacity))
            .collect()
    }

    pub(crate) fn channel(&mut self, stream: usize, base: usize) -> Option<usize> {
        if self.base_by_stream.len() <= stream {
            self.base_by_stream.resize(stream.saturating_add(1), None);
        }
        match &mut self.base_by_stream[stream] {
            Some(recorded) if *recorded != base => None,
            Some(_) => Some(base.max(self.capacity_by_stream.get(&stream).copied().unwrap_or(0))),
            slot @ None => {
                *slot = Some(base);
                Some(base.max(self.capacity_by_stream.get(&stream).copied().unwrap_or(0)))
            }
        }
    }

    pub(crate) fn raise(&mut self, stream: usize, capacity: usize, grown: usize) -> bool {
        let Some(base) = self.base_by_stream.get(stream).copied().flatten() else {
            return false;
        };
        if base.max(self.capacity_by_stream.get(&stream).copied().unwrap_or(0)) != capacity {
            return false;
        }
        self.capacity_by_stream
            .entry(stream)
            .and_modify(|floor| *floor = (*floor).max(grown))
            .or_insert(grown);
        true
    }
}

/// Retry-time TCP floors: receiver ranges by planned-capacity class, segment ledgers per flow.
///
/// **Receiver ranges** keep class keying. Equivalent flows grow together after one representative
/// row overflows, adjacent capacity classes stay independent, and retaining each flow's original
/// class prevents a grown row from migrating into another class on a later retry.
///
/// **Segment ledgers do not** — T20i retired class-together growth for this arena. A class-uniform
/// ledger is arithmetically impossible at *both* measured demands, which are two separate figures
/// and not one computation:
///
/// - at T20g's device-measured demand **4,633**, the `tcp_state` plane alone is 48.9 GB —
///   **1.90x boston's whole device** — and the whole plan is 61.7 GB;
/// - at layer 1's whole-run demand **11,058**, the whole plan is 129.1 GB (120.198 GiB), which is
///   **5.01x boston** and 10.06 GB beyond madrid's entire unified pool.
///
/// T20i layer 1 supplied the missing premise: the demand is carried by **377 of 262,144 flows
/// (0.144%)**, and 261,767 flows never leave the derived floor. Per-flow keying at the exact
/// measured peaks therefore costs 24.6 MiB over the plan that already fits, and 0.669 GiB once the
/// `k = 8` factor below is applied — against that 5.01x for class keying. The class mechanism is
/// retained where it is still correct — receiver ranges here, and per-stream keying in
/// [`ChannelCapacityFloors`] — but the ledger is keyed by [`FlowId`].
#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct TcpCapacityFloors {
    receiver_base_by_flow: Vec<Option<usize>>,
    ledger_base_by_flow: Vec<Option<usize>>,
    receiver_ranges: BTreeMap<usize, usize>,
    ledger_segments_by_flow: Vec<usize>,
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
impl TcpCapacityFloors {
    /// Starts from a warm start's receiver classes and per-flow ledger capacities.
    ///
    /// As in [`ChannelCapacityFloors::warm_started`], the seeded lanes are the same lanes the
    /// retry chain writes and are read through the same `base.max(floor)` expression, so the
    /// base-stability checks in [`Self::raise_receiver`] and [`Self::raise_ledger`] keep holding.
    ///
    /// `flows` bounds the per-flow lane at the replaying image's own flow count, so this vector is
    /// never longer than the image justifies. Entries beyond it are inert anyway — [`Self::ledger`]
    /// is only ever called for flows the plan sizes.
    ///
    /// **That clamp bounds the number of keys, not the magnitude of any capacity, and it is not a
    /// safety bound.** A single seeded value is still whatever the caller supplies, and the planner
    /// will size an arena from it; one 13-digit entry priced a 400 TB plan and aborted the
    /// allocator. Magnitudes are bounded where the untrusted input is — at the point a snapshot
    /// **file** is parsed, in `t20f_frontier`'s `warm_start::decode`, which refuses any capacity
    /// larger than a device in the fleet could hold before anything is allocated. A
    /// [`CapacityWarmStart`] built in process has exactly the standing [`DeviceCapacityFloors`] and
    /// the backends' `max_*` overrides already have: its magnitudes are the caller's
    /// responsibility, as they were before this type existed.
    pub(crate) fn warm_started(
        receiver_ranges: &[(usize, usize)],
        ledger_segments: &[(usize, usize)],
        flows: usize,
    ) -> Self {
        let ledger_segments = warm_start_map(ledger_segments);
        let mut ledger_segments_by_flow = vec![
            0;
            ledger_segments
                .keys()
                .last()
                .map_or(0, |flow| flow.saturating_add(1))
                .min(flows)
        ];
        for (flow, capacity) in ledger_segments {
            if flow < ledger_segments_by_flow.len() {
                ledger_segments_by_flow[flow] = capacity;
            }
        }
        Self {
            receiver_base_by_flow: Vec::new(),
            ledger_base_by_flow: Vec::new(),
            receiver_ranges: warm_start_map(receiver_ranges),
            ledger_segments_by_flow,
        }
    }

    /// The receiver-range classes this run converged on, in canonical ascending order.
    pub(crate) fn converged_receiver_ranges(&self) -> Vec<(usize, usize)> {
        self.receiver_ranges
            .iter()
            .map(|(base, capacity)| (*base, *capacity))
            .collect()
    }

    /// The per-flow ledger capacities this run converged on, in canonical ascending order.
    ///
    /// Flows still at zero are omitted: a zero floor binds nothing, and the frontier's snapshot is
    /// smaller for it.
    pub(crate) fn converged_ledger_segments(&self) -> Vec<(usize, usize)> {
        self.ledger_segments_by_flow
            .iter()
            .enumerate()
            .filter(|(_, capacity)| **capacity != 0)
            .map(|(flow, capacity)| (flow, *capacity))
            .collect()
    }

    fn record_base(bases: &mut Vec<Option<usize>>, flow: FlowId, base: usize) -> bool {
        let flow = flow.0 as usize;
        if bases.len() <= flow {
            bases.resize(flow.saturating_add(1), None);
        }
        match &mut bases[flow] {
            Some(recorded) => *recorded == base,
            slot @ None => {
                *slot = Some(base);
                true
            }
        }
    }

    fn recorded_base(bases: &[Option<usize>], flow: FlowId) -> Option<usize> {
        bases.get(flow.0 as usize).copied().flatten()
    }

    pub(crate) fn receiver(&mut self, flow: FlowId, base: usize) -> Option<usize> {
        Self::record_base(&mut self.receiver_base_by_flow, flow, base)
            .then(|| base.max(self.receiver_ranges.get(&base).copied().unwrap_or(0)))
    }

    fn ledger_floor(&self, flow: FlowId) -> usize {
        self.ledger_segments_by_flow
            .get(flow.0 as usize)
            .copied()
            .unwrap_or(0)
    }

    pub(crate) fn ledger(&mut self, flow: FlowId, base: usize) -> Option<usize> {
        Self::record_base(&mut self.ledger_base_by_flow, flow, base)
            .then(|| base.max(self.ledger_floor(flow)))
    }

    pub(crate) fn raise_receiver(&mut self, flow: FlowId, capacity: usize, grown: usize) -> bool {
        let Some(base) = Self::recorded_base(&self.receiver_base_by_flow, flow) else {
            return false;
        };
        if base.max(self.receiver_ranges.get(&base).copied().unwrap_or(0)) != capacity {
            return false;
        }
        self.receiver_ranges
            .entry(base)
            .and_modify(|floor| *floor = (*floor).max(grown))
            .or_insert(grown);
        true
    }

    fn raise_ledger_floor(&mut self, flow: FlowId, grown: usize) {
        let flow = flow.0 as usize;
        if self.ledger_segments_by_flow.len() <= flow {
            self.ledger_segments_by_flow
                .resize(flow.saturating_add(1), 0);
        }
        self.ledger_segments_by_flow[flow] = self.ledger_segments_by_flow[flow].max(grown);
    }

    /// Raises one flow's ledger floor after its own row overflowed.
    ///
    /// This is the vector-less fallback: it is exactly the pre-T20i behaviour with class keying
    /// replaced by flow keying, and it is what runs when a fault carries no occupancy vector.
    pub(crate) fn raise_ledger(&mut self, flow: FlowId, capacity: usize, grown: usize) -> bool {
        let Some(base) = Self::recorded_base(&self.ledger_base_by_flow, flow) else {
            return false;
        };
        if base.max(self.ledger_floor(flow)) != capacity {
            return false;
        }
        self.raise_ledger_floor(flow, grown);
        true
    }

    /// Sizes **every** planned flow from one fault's per-flow occupancy high-water vector.
    ///
    /// Returns the number of flows whose floor moved. `false` from [`Self::raise_ledger`] still
    /// aborts the retry, so the faulting flow's own progress guarantee is unchanged; this call
    /// only adds the other flows the vector exposed.
    ///
    /// Layer 1's first-crossing census is the reason this exists: the deep set accretes
    /// monotonically (6 flows above the derived floor by 200 us, 183 by 300 us, 377 by 1,140 us)
    /// and never sheds a member, so a first-offender chain repairs one flow per attempt on an image
    /// where one attempt costs tens of minutes.
    pub(crate) fn raise_ledger_from_occupancy(&mut self, high_water: &[u32]) -> usize {
        let mut raised = 0;
        for flow in 0..self.ledger_base_by_flow.len() {
            if self.ledger_base_by_flow[flow].is_none() {
                continue;
            }
            let observed = high_water.get(flow).copied().unwrap_or(0) as usize;
            let target = ledger_capacity_from_high_water(observed);
            let flow = FlowId(flow as u64);
            if target > self.ledger_floor(flow) {
                self.raise_ledger_floor(flow, target);
                raised += 1;
            }
        }
        raised
    }
}

/// One failed attempt and the capacity selected for its deterministic replacement attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapacityRetryRecord<A> {
    /// One-based replacement-attempt number.
    pub retry: usize,
    pub arena: A,
    /// LP identity for per-LP and aggregate arenas.
    pub node: Option<NodeId>,
    /// Flow identity for per-flow TCP arenas.
    pub flow: Option<FlowId>,
    /// Immutable `SimulationImage::channels` index for a channel-inbox stream.
    pub stream: Option<usize>,
    pub capacity: usize,
    pub demand: usize,
    pub grown_capacity: usize,
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
// TCP retries deliberately use tight additive growth (+64 receiver ranges, +256 ledger
// segments), rather than the design's arena-wide geometric 2x policy. The 2x policy was measured
// and rejected after it produced an 87.8 GB shared-buffer allocation; additive class growth
// preserves deterministic progress without repeating that allocation.
pub(crate) const TCP_RECEIVER_RETRY_SLACK: usize = 64;
#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) const TCP_LEDGER_RETRY_SLACK: usize = 256;

/// Multiplier applied to a flow's **observed** ledger high-water when a fault reports the whole
/// per-flow occupancy vector.
///
/// T20i layer 1 measured the growth law exactly: a flow whose cumulative ACK stalls behind a hole
/// whose retransmission was also lost holds `occupancy = ssthresh_seg + duplicate_acks`, gaining
/// **one record per duplicate ACK, which is one record per admitted segment**, and no
/// retransmission timeout can end the episode (minimum RTO 1 s against a 1.152 ms horizon,
/// measured `rto_fires=0`). Growth is therefore linear in simulated time from the stall instant
/// `s`: `occupancy(t) = m * (t - s)`. A vector read at fault time `t_f` sizes the next attempt at
/// `k * occupancy(t_f)`, which the same flow does not exhaust until `s + k * (t_f - s)` — so the
/// horizon each attempt reaches grows **geometrically in `k`**, and the attempts a deep flow needs
/// are `ceil(log_k((T - s) / (t_f - s)))`.
///
/// For the RQ9 frontier that ratio is `(1,152 - 110) / (160 - 110) ~= 21`, giving 5 retries at
/// `k = 2`, 3 at `k = 4`, and **2 at `k = 8`**. Past `k = 8` the deep flows stop being the binding
/// constraint — a flow that stalls *after* the vector was read still starts at the derived floor —
/// so `k = 8` is the knee, and larger factors buy nothing.
///
/// The cost is negligible because the demand is carried by 0.144% of flows. Projected whole-plan
/// bytes at the measured per-flow peaks, against boston's 25,757,220,864 B: `k = 1` 17.312 GiB,
/// `k = 2` 17.359 GiB, **`k = 8` 17.957 GiB**, `k = 16` 18.813 GiB. Every one of them fits; the
/// class-uniform alternative at the same demand is 120.198 GiB.
#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) const TCP_LEDGER_OCCUPANCY_SLACK_FACTOR: usize = 8;

/// Constant record allowance added on top of the scaled high-water.
///
/// It matches `TCP_LEDGER_RECOVERY_ALLOWANCE`: one partial cumulative-ACK boundary record plus
/// recovery retransmissions. At the frontier it costs 8 records on each of the 9,298 flows the
/// `k = 8` factor lifts off the derived floor — the flows whose peak is at least 65, since below
/// that `8 * peak + 8` stays under 520 — for **74,384 records = 2,975,360 B (2.98 MB)** in total.
/// That is 0.015% of the 19.28 GB plan, but it is 24.6x the figure this constant originally
/// carried.
#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) const TCP_LEDGER_OCCUPANCY_SLACK_RECORDS: usize = 8;

/// Per-flow ledger capacity implied by one observed occupancy high-water mark.
#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) fn ledger_capacity_from_high_water(high_water: usize) -> usize {
    high_water
        .saturating_mul(TCP_LEDGER_OCCUPANCY_SLACK_FACTOR)
        .saturating_add(TCP_LEDGER_OCCUPANCY_SLACK_RECORDS)
}

/// The faulting flow's own entry in a ledger fault's occupancy vector, when both are present.
#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) fn observed_ledger_high_water(
    high_water: Option<&[u32]>,
    flow: Option<FlowId>,
) -> Option<usize> {
    let (high_water, flow) = (high_water?, flow?);
    Some(high_water.get(flow.0 as usize).copied().unwrap_or(0) as usize)
}

/// The replacement capacity for the flow a ledger fault named.
///
/// With a vector the fault's own flow is sized by the same law as every other flow, and the
/// additive `TCP_LEDGER_RETRY_SLACK` retreat is retired: that constant existed to stop *class*
/// growth from reproducing the 87.8 GB shared-buffer allocation, and per-flow keying removes the
/// blow-up it was defending against. `max(demand, capacity + 1)` keeps the retry loop's strict
/// progress guarantee intact even if a vector were ever short or stale.
#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) fn grown_ledger_capacity(
    capacity: usize,
    demand: usize,
    high_water: Option<usize>,
) -> usize {
    match high_water {
        Some(high_water) => ledger_capacity_from_high_water(high_water)
            .max(demand)
            .max(capacity.saturating_add(1)),
        None => grown_capacity_with_slack(capacity, demand, TCP_LEDGER_RETRY_SLACK),
    }
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) fn grown_capacity(capacity: usize, demand: usize) -> usize {
    let basis = demand.max(capacity.saturating_add(1));
    basis.saturating_mul(2).max(basis)
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) fn grown_capacity_with_slack(capacity: usize, demand: usize, slack: usize) -> usize {
    demand.max(capacity.saturating_add(1)).saturating_add(slack)
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) fn raise_cap_or_floor(
    cap: &mut Option<usize>,
    floor: &mut usize,
    capacity: usize,
    grown: usize,
) {
    if let Some(cap) = cap {
        if *cap == capacity {
            *cap = (*cap).max(grown);
        }
    }

    // `capacity = max(min(derived, cap), resident, floor)`. Equality with `cap` does not prove
    // that the cap alone is binding: `derived` or `resident` may tie it, in which case raising
    // only the cap leaves the effective capacity unchanged. Raise the floor as well; because the
    // retry loop admits only `grown > capacity`, the next effective capacity is at least `grown`
    // while every cap and resident lower bound remains intact (records are never truncated).
    *floor = (*floor).max(grown);
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) fn raise_override_cap_or_floor(
    override_capacity: &mut Option<usize>,
    cap: &mut Option<usize>,
    floor: &mut usize,
    capacity: usize,
    grown: usize,
) {
    match override_capacity {
        Some(override_capacity) if *override_capacity == capacity => {
            *override_capacity = (*override_capacity).max(grown);
        }
        Some(_) | None => raise_cap_or_floor(cap, floor, capacity, grown),
    }
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) fn cap_derived_capacity(derived: usize, cap: Option<usize>, resident: usize) -> usize {
    cap.map_or(derived, |cap| derived.min(cap)).max(resident)
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) fn bound_derived_capacity(
    derived: usize,
    cap: Option<usize>,
    floor: usize,
    resident: usize,
) -> usize {
    cap_derived_capacity(derived, cap, resident).max(floor)
}

#[cfg(test)]
mod tests {
    use super::{
        CapacityWarmStart, ChannelCapacityFloors, DeviceCapacityFloors,
        TCP_LEDGER_OCCUPANCY_SLACK_FACTOR, TCP_LEDGER_OCCUPANCY_SLACK_RECORDS,
        TCP_LEDGER_RETRY_SLACK, TCP_RECEIVER_RETRY_SLACK, TcpCapacityFloors,
        bound_derived_capacity, cap_derived_capacity, grown_capacity, grown_capacity_with_slack,
        grown_ledger_capacity, ledger_capacity_from_high_water, observed_ledger_high_water,
        raise_cap_or_floor, raise_override_cap_or_floor,
    };
    use crate::FlowId;

    fn retry_capacity_sequence(derived: usize, initial_cap: usize, resident: usize) -> Vec<usize> {
        let mut cap = Some(initial_cap);
        let mut floor = 0;
        (0..5)
            .map(|_| {
                let capacity = bound_derived_capacity(derived, cap, floor, resident);
                let demand = capacity.saturating_add(1);
                let grown = grown_capacity(capacity, demand);
                raise_cap_or_floor(&mut cap, &mut floor, capacity, grown);
                capacity
            })
            .collect()
    }

    #[test]
    fn derived_capacity_cap_never_excludes_resident_records() {
        assert_eq!(cap_derived_capacity(100, None, 7), 100);
        assert_eq!(cap_derived_capacity(100, Some(12), 7), 12);
        assert_eq!(cap_derived_capacity(100, Some(3), 7), 7);
        assert_eq!(cap_derived_capacity(1, Some(0), 0), 0);
    }

    #[test]
    fn retry_floor_can_raise_a_capped_derived_capacity() {
        assert_eq!(bound_derived_capacity(100, Some(3), 12, 7), 12);
    }

    #[test]
    fn retry_growth_doubles_reported_demand_and_saturates_safely() {
        assert_eq!(grown_capacity(0, 1), 2);
        assert_eq!(grown_capacity(8, 9), 18);
        assert_eq!(grown_capacity(8, 12), 24);
        assert_eq!(grown_capacity(usize::MAX, usize::MAX), usize::MAX);
    }

    #[test]
    fn reported_demand_supports_tight_additive_retry_slack() {
        assert_eq!(TCP_RECEIVER_RETRY_SLACK, 64);
        assert_eq!(
            grown_capacity_with_slack(4_096, 4_097, TCP_LEDGER_RETRY_SLACK),
            4_353
        );
        assert_eq!(
            grown_capacity_with_slack(usize::MAX, usize::MAX, 256),
            usize::MAX
        );
    }

    /// T20i retirement gate: the ledger no longer grows by capacity class.
    ///
    /// Under the retired policy `raise_ledger(FlowId(7), 520, 777)` also raised flow 8 — and, at
    /// the frontier, all 262,144 flows sharing the derived base of 520, which at layer 1's measured
    /// demand is the 5.01x-boston plan T20g proved impossible. Receiver ranges keep class keying and
    /// are pinned here too, so the retirement is visibly scoped to the one arena it applies to.
    #[test]
    fn tcp_ledger_retry_grows_only_the_faulting_flow() {
        let mut floors = TcpCapacityFloors::default();

        assert_eq!(floors.ledger(FlowId(7), 520), Some(520));
        assert_eq!(floors.ledger(FlowId(8), 520), Some(520));
        assert_eq!(floors.ledger(FlowId(9), 521), Some(521));
        assert!(floors.raise_ledger(FlowId(7), 520, 777));
        assert_eq!(floors.ledger(FlowId(7), 520), Some(777));
        assert_eq!(
            floors.ledger(FlowId(8), 520),
            Some(520),
            "an equivalent flow must NOT inherit the faulting flow's growth"
        );
        assert_eq!(floors.ledger(FlowId(9), 521), Some(521));

        assert!(floors.raise_ledger(FlowId(8), 520, 1_034));
        assert_eq!(floors.ledger(FlowId(7), 520), Some(777));
        assert_eq!(floors.ledger(FlowId(8), 520), Some(1_034));
        assert_eq!(floors.ledger(FlowId(9), 521), Some(521));
        assert_eq!(floors.ledger(FlowId(7), 521), None);
        assert!(!floors.raise_ledger(FlowId(10), 520, 777));
        // A stale capacity — one that is neither the base nor the current floor — is rejected.
        assert!(!floors.raise_ledger(FlowId(7), 520, 900));

        assert_eq!(floors.receiver(FlowId(9), 64), Some(64));
        assert!(floors.raise_receiver(FlowId(9), 64, 129));
        assert_eq!(floors.receiver(FlowId(9), 64), Some(129));
    }

    #[test]
    fn the_occupancy_vector_sizes_every_flow_in_one_replan() {
        let mut floors = TcpCapacityFloors::default();
        for flow in 0..6 {
            assert_eq!(floors.ledger(FlowId(flow), 520), Some(520));
        }
        // The vector's seventh entry belongs to a flow this plan never sized, so the count of
        // raised floors must stay at six: a fault must not invent capacity for an absent flow.
        let high_water = [2_u32, 11_058, 520, 0, 63, 4_474, 9_999];
        assert_eq!(floors.raise_ledger_from_occupancy(&high_water), 6);

        // Every flow is sized from its OWN observation, in one pass.
        assert_eq!(floors.ledger(FlowId(0), 520), Some(520)); // 8*2+8 = 24 < the derived floor
        assert_eq!(floors.ledger(FlowId(1), 520), Some(88_472));
        assert_eq!(floors.ledger(FlowId(2), 520), Some(4_168));
        assert_eq!(floors.ledger(FlowId(3), 520), Some(520));
        assert_eq!(floors.ledger(FlowId(4), 520), Some(520)); // 8*63+8 = 512 < 520
        assert_eq!(floors.ledger(FlowId(5), 520), Some(35_800));

        // Floors only ever rise: a second, smaller vector cannot shrink a plan.
        assert_eq!(floors.raise_ledger_from_occupancy(&[0; 6]), 0);
        assert_eq!(floors.ledger(FlowId(1), 520), Some(88_472));
        assert_eq!(floors.ledger(FlowId(5), 520), Some(35_800));

        // A short vector reads as zero rather than panicking.
        assert_eq!(floors.raise_ledger_from_occupancy(&[]), 0);
    }

    #[test]
    fn ledger_growth_uses_the_vector_and_keeps_strict_progress() {
        assert_eq!(TCP_LEDGER_OCCUPANCY_SLACK_FACTOR, 8);
        assert_eq!(TCP_LEDGER_OCCUPANCY_SLACK_RECORDS, 8);
        assert_eq!(ledger_capacity_from_high_water(0), 8);
        assert_eq!(ledger_capacity_from_high_water(11_058), 88_472);
        assert_eq!(ledger_capacity_from_high_water(usize::MAX), usize::MAX);

        // The faulting flow's own high-water is at least its capacity, so the vector alone already
        // clears the capacity. The additive +256 retreat is retired for this arena.
        assert_eq!(grown_ledger_capacity(520, 521, Some(520)), 4_168);
        assert_eq!(grown_ledger_capacity(4_632, 4_633, Some(4_632)), 37_064);
        // Progress is guaranteed even against a stale or short vector.
        assert_eq!(grown_ledger_capacity(520, 521, Some(0)), 521);
        assert_eq!(grown_ledger_capacity(520, 521, Some(60)), 521);
        // Without a vector the pre-T20i additive growth remains the fallback.
        assert_eq!(grown_ledger_capacity(520, 521, None), 777);
        assert_eq!(
            grown_ledger_capacity(usize::MAX, usize::MAX, Some(usize::MAX)),
            usize::MAX
        );
    }

    #[test]
    fn the_faulting_flows_own_observation_is_read_out_of_the_vector() {
        let vector = [3_u32, 11_058, 7];
        assert_eq!(
            observed_ledger_high_water(Some(&vector), Some(FlowId(1))),
            Some(11_058)
        );
        // A fault that named no flow, or carried no vector, falls back to additive growth.
        assert_eq!(observed_ledger_high_water(Some(&vector), None), None);
        assert_eq!(observed_ledger_high_water(None, Some(FlowId(1))), None);
        // A flow past the end of the vector reads as zero rather than panicking; the retry loop's
        // `max(demand, capacity + 1)` still guarantees strict progress.
        assert_eq!(
            observed_ledger_high_water(Some(&vector), Some(FlowId(9))),
            Some(0)
        );
    }

    #[test]
    fn channel_retry_floor_is_deterministic_for_the_fault_sequence() {
        fn replay(faults: &[(usize, usize)]) -> ChannelCapacityFloors {
            let bases = [8, 8, 12, 8];
            let mut floors = ChannelCapacityFloors::default();
            for &(stream, demand) in faults {
                let capacity = floors
                    .channel(stream, bases[stream])
                    .expect("the stream base must remain stable");
                let grown = grown_capacity(capacity, demand);
                assert!(floors.raise(stream, capacity, grown));
            }
            floors
        }

        let faults = [(2, 13), (0, 9), (2, 27), (3, 10)];
        assert_eq!(replay(&faults), replay(&faults));
    }

    #[test]
    fn channel_retry_grows_only_the_faulting_stream() {
        let bases = [8, 8, 12, 8];
        let mut floors = ChannelCapacityFloors::default();
        let before = bases
            .iter()
            .copied()
            .enumerate()
            .map(|(stream, base)| floors.channel(stream, base).unwrap())
            .collect::<Vec<_>>();

        assert!(floors.raise(1, before[1], 18));
        let after = bases
            .iter()
            .copied()
            .enumerate()
            .map(|(stream, base)| floors.channel(stream, base).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(before, vec![8, 8, 12, 8]);
        assert_eq!(after, vec![8, 18, 12, 8]);
        assert_eq!(&after[..1], &before[..1]);
        assert_eq!(&after[2..], &before[2..]);
    }

    #[test]
    fn channel_retry_rejects_unknown_or_changed_stream_state() {
        let mut floors = ChannelCapacityFloors::default();
        assert_eq!(floors.channel(4, 8), Some(8));
        assert!(!floors.raise(3, 8, 18));
        assert!(!floors.raise(4, 9, 18));
        assert_eq!(floors.channel(4, 9), None);
    }

    /// T20l fix 3: a converged snapshot, replayed, reproduces the retry chain's own capacities.
    ///
    /// The chain here is the shape the RQ9 frontier runs: a channel stream grows once, a receiver
    /// class grows once, and the ledger's occupancy vector sizes every flow at once. Replaying the
    /// snapshot must put every entity at the capacity the last attempt planned, on the first call,
    /// without any fault having occurred — and it must be a fixed point, because a hint that drifts
    /// on replay would not be derived output.
    #[test]
    fn a_converged_snapshot_replays_the_capacity_the_chain_ended_on() {
        let mut channels = ChannelCapacityFloors::default();
        let mut tcp = TcpCapacityFloors::default();

        for (stream, base) in [(0, 8), (1, 8), (2, 12)] {
            assert_eq!(channels.channel(stream, base), Some(base));
        }
        for flow in 0..3 {
            assert_eq!(tcp.receiver(FlowId(flow), 64), Some(64));
            assert_eq!(tcp.ledger(FlowId(flow), 520), Some(520));
        }

        assert!(channels.raise(1, 8, 18));
        assert!(tcp.raise_receiver(FlowId(0), 64, 129));
        assert!(tcp.raise_ledger(FlowId(2), 520, 4_168));
        assert_eq!(tcp.raise_ledger_from_occupancy(&[2, 11_058, 520]), 2);

        let snapshot = CapacityWarmStart {
            floors: DeviceCapacityFloors {
                queue_packets_per_lp: 96,
                ..DeviceCapacityFloors::default()
            },
            channel_events_by_stream: channels.converged_capacities(),
            tcp_receiver_ranges_by_base: tcp.converged_receiver_ranges(),
            tcp_ledger_segments_by_flow: tcp.converged_ledger_segments(),
        };
        assert!(!snapshot.is_empty());
        assert_eq!(snapshot.channel_events_by_stream, vec![(1, 18)]);
        assert_eq!(snapshot.tcp_receiver_ranges_by_base, vec![(64, 129)]);
        // Flow 1's 11,058-record high-water is sized by the vector, flow 2 keeps its own fault's
        // growth, and flow 0's `8 * 2 + 8 = 24` is recorded but does not bind: every consumer takes
        // a maximum against the derived capacity, so a snapshot entry below it is inert.
        assert_eq!(
            snapshot.tcp_ledger_segments_by_flow,
            vec![(0, 24), (1, 88_472), (2, 4_168)]
        );

        // Replay: every entity starts where the chain ended, with no fault in between.
        let mut replayed_channels =
            ChannelCapacityFloors::warm_started(&snapshot.channel_events_by_stream);
        let mut replayed_tcp = TcpCapacityFloors::warm_started(
            &snapshot.tcp_receiver_ranges_by_base,
            &snapshot.tcp_ledger_segments_by_flow,
            3,
        );
        assert_eq!(replayed_channels.channel(0, 8), Some(8));
        assert_eq!(replayed_channels.channel(1, 8), Some(18));
        assert_eq!(replayed_channels.channel(2, 12), Some(12));
        assert_eq!(replayed_tcp.receiver(FlowId(0), 64), Some(129));
        assert_eq!(replayed_tcp.receiver(FlowId(2), 64), Some(129));
        assert_eq!(replayed_tcp.ledger(FlowId(0), 520), Some(520));
        assert_eq!(replayed_tcp.ledger(FlowId(1), 520), Some(88_472));
        assert_eq!(replayed_tcp.ledger(FlowId(2), 520), Some(4_168));

        // Fixed point: the replay emits the snapshot it was given.
        assert_eq!(
            replayed_channels.converged_capacities(),
            snapshot.channel_events_by_stream
        );
        assert_eq!(
            replayed_tcp.converged_receiver_ranges(),
            snapshot.tcp_receiver_ranges_by_base
        );
        assert_eq!(
            replayed_tcp.converged_ledger_segments(),
            snapshot.tcp_ledger_segments_by_flow
        );

        // The base-stability checks the retry loop depends on still hold against seeded floors.
        assert!(replayed_channels.raise(1, 18, 40));
        assert!(!replayed_channels.raise(1, 18, 40));
        assert!(replayed_tcp.raise_ledger(FlowId(1), 88_472, 90_000));
        assert!(!replayed_tcp.raise_ledger(FlowId(1), 88_472, 90_000));
    }

    /// Floors are lower bounds, so combining a caller's with a warm start's is a maximum.
    #[test]
    fn warm_start_floors_merge_lane_by_lane_and_never_shrink() {
        let caller = DeviceCapacityFloors {
            queue_packets_per_lp: 40,
            observation_events: 7,
            ..DeviceCapacityFloors::default()
        };
        let hint = DeviceCapacityFloors {
            queue_packets_per_lp: 12,
            worklist_entries_total: 900,
            ..DeviceCapacityFloors::default()
        };
        let merged = caller.merged_with(hint);
        assert_eq!(merged.queue_packets_per_lp, 40);
        assert_eq!(merged.observation_events, 7);
        assert_eq!(merged.worklist_entries_total, 900);
        assert_eq!(merged, hint.merged_with(caller));
        assert_eq!(
            DeviceCapacityFloors::default().merged_with(DeviceCapacityFloors::default()),
            DeviceCapacityFloors::default()
        );
    }

    /// A hand-written or duplicated association list collapses to one canonical form.
    #[test]
    fn warm_start_lists_are_canonicalized_by_key_and_maximum() {
        let mut floors = ChannelCapacityFloors::warm_started(&[(5, 12), (1, 3), (5, 40), (1, 2)]);
        assert_eq!(floors.converged_capacities(), vec![(1, 3), (5, 40)]);
        assert_eq!(floors.channel(1, 2), Some(3));
        assert_eq!(floors.channel(5, 64), Some(64));

        let mut tcp = TcpCapacityFloors::warm_started(&[], &[(3, 8), (0, 5), (3, 9)], 4);
        assert_eq!(tcp.converged_ledger_segments(), vec![(0, 5), (3, 9)]);
        assert_eq!(tcp.ledger(FlowId(3), 1), Some(9));
        assert_eq!(tcp.ledger(FlowId(2), 1), Some(1));
        assert!(
            TcpCapacityFloors::warm_started(&[], &[], 8)
                .converged_ledger_segments()
                .is_empty()
        );
        assert!(CapacityWarmStart::default().is_empty());

        // A snapshot naming more flows than the replaying image plans keeps only the flows the
        // image has, and cannot ask for an allocation the image does not justify.
        let mut narrow = TcpCapacityFloors::warm_started(&[], &[(0, 5), (usize::MAX, 9)], 2);
        assert_eq!(narrow.converged_ledger_segments(), vec![(0, 5)]);
        assert_eq!(narrow.ledger(FlowId(1), 3), Some(3));
    }

    #[test]
    fn retry_raises_the_effective_capacity_without_discarding_caps() {
        let mut override_capacity = None;
        let mut cap = Some(8);
        let mut floor = 0;
        raise_override_cap_or_floor(&mut override_capacity, &mut cap, &mut floor, 8, 18);
        assert_eq!(override_capacity, None);
        assert_eq!(cap, Some(18));
        assert_eq!(floor, 18);

        cap = None;
        raise_cap_or_floor(&mut cap, &mut floor, 8, 24);
        assert_eq!(cap, None);
        assert_eq!(floor, 24);

        override_capacity = Some(4);
        cap = Some(8);
        raise_override_cap_or_floor(&mut override_capacity, &mut cap, &mut floor, 4, 32);
        assert_eq!(override_capacity, Some(32));
        assert_eq!(cap, Some(8));
        assert_eq!(floor, 24);

        override_capacity = None;
        cap = Some(2_048);
        floor = 0;
        raise_override_cap_or_floor(&mut override_capacity, &mut cap, &mut floor, 49, 100);
        assert_eq!(cap, Some(2_048));
        assert_eq!(floor, 100);
    }

    #[test]
    fn retry_strictly_grows_when_the_derived_capacity_ties_the_cap() {
        assert_eq!(
            retry_capacity_sequence(2_048, 2_048, 0),
            vec![2_048, 4_098, 8_198, 16_398, 32_798]
        );
    }

    #[test]
    fn retry_strictly_grows_when_the_resident_capacity_ties_the_cap() {
        assert_eq!(
            retry_capacity_sequence(100, 512, 512),
            vec![512, 1_026, 2_054, 4_110, 8_222]
        );
    }
}
