use crate::{FlowId, NodeId};

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
use std::collections::BTreeMap;

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
    /// Maximum records assigned to one declared incoming-channel stream.
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

/// Retry-time TCP floors keyed by each row's immutable planned-capacity class.
///
/// Equivalent flows grow together after one representative row overflows. Adjacent capacity
/// classes remain independent, and retaining each flow's original class prevents a grown row from
/// migrating into another class on a later retry.
#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct TcpCapacityClassFloors {
    receiver_base_by_flow: Vec<Option<usize>>,
    ledger_base_by_flow: Vec<Option<usize>>,
    receiver_ranges: BTreeMap<usize, usize>,
    ledger_segments: BTreeMap<usize, usize>,
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
impl TcpCapacityClassFloors {
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

    pub(crate) fn ledger(&mut self, flow: FlowId, base: usize) -> Option<usize> {
        Self::record_base(&mut self.ledger_base_by_flow, flow, base)
            .then(|| base.max(self.ledger_segments.get(&base).copied().unwrap_or(0)))
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

    pub(crate) fn raise_ledger(&mut self, flow: FlowId, capacity: usize, grown: usize) -> bool {
        let Some(base) = Self::recorded_base(&self.ledger_base_by_flow, flow) else {
            return false;
        };
        if base.max(self.ledger_segments.get(&base).copied().unwrap_or(0)) != capacity {
            return false;
        }
        self.ledger_segments
            .entry(base)
            .and_modify(|floor| *floor = (*floor).max(grown))
            .or_insert(grown);
        true
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
        TCP_LEDGER_RETRY_SLACK, TCP_RECEIVER_RETRY_SLACK, TcpCapacityClassFloors,
        bound_derived_capacity, cap_derived_capacity, grown_capacity, grown_capacity_with_slack,
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

    #[test]
    fn tcp_retry_grows_only_the_immutable_capacity_class() {
        let mut floors = TcpCapacityClassFloors::default();

        assert_eq!(floors.ledger(FlowId(7), 520), Some(520));
        assert_eq!(floors.ledger(FlowId(8), 520), Some(520));
        assert_eq!(floors.ledger(FlowId(9), 521), Some(521));
        assert!(floors.raise_ledger(FlowId(7), 520, 777));
        assert_eq!(floors.ledger(FlowId(7), 520), Some(777));
        assert_eq!(floors.ledger(FlowId(8), 520), Some(777));
        assert_eq!(floors.ledger(FlowId(9), 521), Some(521));

        assert!(floors.raise_ledger(FlowId(8), 777, 1_034));
        assert_eq!(floors.ledger(FlowId(7), 520), Some(1_034));
        assert_eq!(floors.ledger(FlowId(8), 520), Some(1_034));
        assert_eq!(floors.ledger(FlowId(9), 521), Some(521));
        assert_eq!(floors.ledger(FlowId(7), 521), None);
        assert!(!floors.raise_ledger(FlowId(10), 520, 777));

        assert_eq!(floors.receiver(FlowId(9), 64), Some(64));
        assert!(floors.raise_receiver(FlowId(9), 64, 129));
        assert_eq!(floors.receiver(FlowId(9), 64), Some(129));
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
