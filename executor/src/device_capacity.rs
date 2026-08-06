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
    /// Retry-only lower-bound class. `Some(capacity)` applies the receiver floor only to flows
    /// whose capped derived capacity is at least this value.
    pub tcp_receiver_ranges_floor_trigger: Option<usize>,
    pub tcp_ledger_segments_per_flow: usize,
    /// Retry-only lower-bound class. `Some(capacity)` applies the ledger floor only to flows whose
    /// capped derived capacity is at least this value.
    pub tcp_ledger_segments_floor_trigger: Option<usize>,
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
pub(crate) fn raise_cap_or_floor(
    cap: &mut Option<usize>,
    floor: &mut usize,
    capacity: usize,
    grown: usize,
) {
    match cap {
        Some(cap) if *cap == capacity => *cap = (*cap).max(grown),
        Some(_) | None => *floor = (*floor).max(grown),
    }
}

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) fn raise_cap_or_class_floor(
    cap: &mut Option<usize>,
    floor_trigger: &mut Option<usize>,
    floor: &mut usize,
    capacity: usize,
    grown: usize,
) {
    match cap {
        Some(cap) if *cap == capacity => *cap = (*cap).max(grown),
        Some(_) | None => {
            *floor_trigger = Some(floor_trigger.map_or(capacity, |trigger| trigger.min(capacity)));
            *floor = (*floor).max(grown);
        }
    }
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

#[cfg(any(
    test,
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) fn bound_derived_capacity_for_class(
    derived: usize,
    cap: Option<usize>,
    floor_trigger: Option<usize>,
    floor: usize,
    resident: usize,
) -> usize {
    let capacity = cap_derived_capacity(derived, cap, resident);
    match floor_trigger {
        Some(trigger) if capacity >= trigger => capacity.max(floor),
        Some(_) => capacity,
        None => capacity.max(floor),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        bound_derived_capacity, bound_derived_capacity_for_class, cap_derived_capacity,
        grown_capacity, raise_cap_or_class_floor, raise_cap_or_floor, raise_override_cap_or_floor,
    };

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
    fn retry_class_floor_does_not_inflate_smaller_flows() {
        assert_eq!(
            bound_derived_capacity_for_class(100, Some(200), Some(100), 240, 0),
            240
        );
        assert_eq!(
            bound_derived_capacity_for_class(99, Some(200), Some(100), 240, 0),
            99
        );
        assert_eq!(
            bound_derived_capacity_for_class(50, Some(40), Some(40), 80, 0),
            80
        );
    }

    #[test]
    fn retry_growth_doubles_reported_demand_and_saturates_safely() {
        assert_eq!(grown_capacity(0, 1), 2);
        assert_eq!(grown_capacity(8, 9), 18);
        assert_eq!(grown_capacity(8, 12), 24);
        assert_eq!(grown_capacity(usize::MAX, usize::MAX), usize::MAX);
    }

    #[test]
    fn retry_raises_the_active_capacity_layer_without_globalizing_a_cap() {
        let mut override_capacity = None;
        let mut cap = Some(8);
        let mut floor = 0;
        raise_override_cap_or_floor(&mut override_capacity, &mut cap, &mut floor, 8, 18);
        assert_eq!(override_capacity, None);
        assert_eq!(cap, Some(18));
        assert_eq!(floor, 0);

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
    fn tcp_retry_raises_a_cap_then_scopes_a_formula_floor() {
        let mut cap = Some(4_096);
        let mut trigger = None;
        let mut floor = 0;

        raise_cap_or_class_floor(&mut cap, &mut trigger, &mut floor, 4_096, 8_194);
        assert_eq!(cap, Some(8_194));
        assert_eq!(trigger, None);
        assert_eq!(floor, 0);

        raise_cap_or_class_floor(&mut cap, &mut trigger, &mut floor, 4_174, 8_350);
        assert_eq!(cap, Some(8_194));
        assert_eq!(trigger, Some(4_174));
        assert_eq!(floor, 8_350);
    }
}
