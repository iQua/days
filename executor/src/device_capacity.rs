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
/// **Segment ledgers do not** — T20i retired class-together growth for this arena. T20g proved a
/// class-uniform ledger at the measured demand is arithmetically impossible (48.9 GB for one plane
/// = 1.90x boston's whole device; 129.1 GB for the whole plan = beyond madrid's entire unified
/// pool), and T20i layer 1 supplied the missing premise: the demand is carried by **377 of 262,144
/// flows (0.144%)**, and 261,767 flows never leave the derived floor. Per-flow keying therefore
/// costs 24.6 MiB over the plan that already fits, and class keying costs 4.66x boston. The class
/// mechanism is retained where it is still correct — receiver ranges here, and per-stream keying in
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
/// class-uniform alternative at the same demand is 120.195 GiB.
#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) const TCP_LEDGER_OCCUPANCY_SLACK_FACTOR: usize = 8;

/// Constant record allowance added on top of the scaled high-water.
///
/// It matches `TCP_LEDGER_RECOVERY_ALLOWANCE`: one partial cumulative-ACK boundary record plus
/// recovery retransmissions. At the frontier it costs 3,025 records in total — 121 kB.
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
        ChannelCapacityFloors, TCP_LEDGER_OCCUPANCY_SLACK_FACTOR,
        TCP_LEDGER_OCCUPANCY_SLACK_RECORDS, TCP_LEDGER_RETRY_SLACK, TCP_RECEIVER_RETRY_SLACK,
        TcpCapacityFloors, bound_derived_capacity, cap_derived_capacity, grown_capacity,
        grown_capacity_with_slack, grown_ledger_capacity, ledger_capacity_from_high_water,
        observed_ledger_high_water, raise_cap_or_floor, raise_override_cap_or_floor,
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
    /// the frontier, all 262,144 flows sharing the derived base of 520, which is the 4.66x-boston
    /// plane T20g proved impossible. Receiver ranges keep class keying and are pinned here too, so
    /// the retirement is visibly scoped to the one arena it applies to.
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
