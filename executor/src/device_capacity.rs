use crate::NodeId;

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

/// One failed attempt and the capacity selected for its deterministic replacement attempt.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapacityRetryRecord<A> {
    /// One-based replacement-attempt number.
    pub retry: usize,
    pub arena: A,
    pub node: Option<NodeId>,
    pub capacity: usize,
    pub demand: usize,
    pub grown_capacity: usize,
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
    use super::{bound_derived_capacity, cap_derived_capacity, grown_capacity};

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
}
