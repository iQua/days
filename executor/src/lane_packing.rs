/// Device active-lane packing policy.
///
/// Packing changes only the active-worklist permutation. It does not change semantic ownership,
/// launch geometry, transition limits, or canonical result ordering.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(u64)]
pub enum DeviceLanePacking {
    /// Retains stable ascending-LP compaction exactly as produced by the device prefix scan.
    #[default]
    Unpacked = 0,
    /// Packs the hottest predicted LPs first, so grid order is descending LPT issue order.
    Descending = 1,
    /// Retains the descending arm's 32-lane groups and reverses the full groups' issue order.
    /// The final short group remains trailing because dense unchanged launch geometry cannot move
    /// it ahead of a full group without mixing their lanes.
    Ascending = 2,
}

impl DeviceLanePacking {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Unpacked => "unpacked",
            Self::Descending => "descending",
            Self::Ascending => "ascending",
        }
    }

    #[cfg(any(
        test,
        feature = "cuda",
        all(feature = "metal-spike", target_vendor = "apple")
    ))]
    pub(crate) const fn device_code(self) -> u64 {
        self as u64
    }

    #[cfg(any(
        test,
        feature = "cuda",
        all(feature = "metal-spike", target_vendor = "apple")
    ))]
    #[allow(dead_code)] // Consumed only when a device backend is enabled.
    pub(crate) const fn is_packed(self) -> bool {
        !matches!(self, Self::Unpacked)
    }
}

/// Exact counter-only work for one physical SIMD32 lane group.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LanePackingGroupCounters {
    pub lanes: u64,
    pub work: u64,
    pub maximum_lane_work: u64,
}

/// Counter-only device mechanism evidence, grouped by semantic round and grid order.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LanePackingCounters {
    pub rounds: Vec<Vec<LanePackingGroupCounters>>,
}

impl LanePackingCounters {
    pub fn lane_time_utilization(&self) -> f64 {
        let useful = self
            .rounds
            .iter()
            .flatten()
            .map(|group| u128::from(group.work))
            .sum::<u128>();
        let capacity = self
            .rounds
            .iter()
            .flatten()
            .map(|group| u128::from(group.lanes) * u128::from(group.maximum_lane_work))
            .sum::<u128>();
        if capacity == 0 {
            1.0
        } else {
            useful as f64 / capacity as f64
        }
    }

    pub fn group_maxima(&self) -> Vec<u64> {
        self.rounds
            .iter()
            .flatten()
            .map(|group| group.maximum_lane_work)
            .collect()
    }
}

#[cfg(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
pub(crate) fn decode_lane_packing_counters(
    words: &[u64],
    node_count: usize,
    rounds: u64,
) -> Result<Option<LanePackingCounters>, String> {
    #[cfg(not(feature = "lane-packing-counters"))]
    {
        let _ = (words, node_count, rounds);
        Ok(None)
    }
    #[cfg(feature = "lane-packing-counters")]
    {
        let rounds = usize::try_from(rounds)
            .map_err(|_| "lane-packing round count does not fit usize".to_owned())?;
        let group_capacity = node_count.div_ceil(32);
        let stride = group_capacity
            .checked_mul(2)
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| "lane-packing counter stride overflows usize".to_owned())?;
        let counter_base = node_count
            .checked_mul(4)
            .ok_or_else(|| "lane-packing counter base overflows usize".to_owned())?;
        let required = rounds
            .checked_mul(stride)
            .and_then(|value| counter_base.checked_add(value))
            .ok_or_else(|| "lane-packing counter extent overflows usize".to_owned())?;
        if words.len() < required {
            return Err(format!(
                "lane-packing counter plane has {} words but {required} are required",
                words.len()
            ));
        }

        let mut decoded = LanePackingCounters {
            rounds: Vec::with_capacity(rounds),
        };
        for round in 0..rounds {
            let base = counter_base + round * stride;
            let active = usize::try_from(words[base])
                .map_err(|_| "lane-packing active count does not fit usize".to_owned())?;
            if active > node_count {
                return Err(format!(
                    "lane-packing round {round} records {active} active LPs for {node_count} nodes"
                ));
            }
            let group_count = active.div_ceil(32);
            let mut groups = Vec::with_capacity(group_count);
            for group in 0..group_count {
                let record = base + 1 + group * 2;
                groups.push(LanePackingGroupCounters {
                    lanes: (active - group * 32).min(32) as u64,
                    work: words[record],
                    maximum_lane_work: words[record + 1],
                });
            }
            decoded.rounds.push(groups);
        }
        Ok(Some(decoded))
    }
}

#[cfg(test)]
mod tests {
    use super::DeviceLanePacking;

    #[test]
    fn device_codes_are_stable_and_labels_are_explicit() {
        assert_eq!(DeviceLanePacking::Unpacked.device_code(), 0);
        assert_eq!(DeviceLanePacking::Descending.device_code(), 1);
        assert_eq!(DeviceLanePacking::Ascending.device_code(), 2);
        assert_eq!(DeviceLanePacking::Unpacked.label(), "unpacked");
        assert_eq!(DeviceLanePacking::Descending.label(), "descending");
        assert_eq!(DeviceLanePacking::Ascending.label(), "ascending");
    }
}
