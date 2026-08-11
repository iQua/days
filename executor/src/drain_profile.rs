use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::ops::Range;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HeadVisitProfile {
    pub heads_visited: usize,
    pub selected_events: u128,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LookupIterationProfile {
    pub outbound_degree: usize,
    pub binary_search_iterations: usize,
    pub lookups: u128,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutboundDegreeProfile {
    pub outbound_degree: usize,
    pub producer_count: usize,
    pub remote_emissions: u128,
    pub binary_search_lookups: u128,
    pub binary_search_iterations: u128,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DrainProfile {
    pub selected_events: u128,
    pub head_visits: u128,
    pub head_visit_histogram: Vec<HeadVisitProfile>,
    pub remote_emissions: u128,
    pub binary_search_lookups: u128,
    pub binary_search_iterations: u128,
    pub lookup_iteration_histogram: Vec<LookupIterationProfile>,
    pub outbound_degrees: Vec<OutboundDegreeProfile>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DrainProfileError(String);

impl DrainProfileError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for DrainProfileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl Error for DrainProfileError {}

/// One immutable channel lookup represented in the exact device-counter layout.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DrainProfileChannel {
    pub source: usize,
    pub target: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ChannelLookup {
    outbound_degree: usize,
    binary_search_iterations: usize,
}

/// Exact raw layout shared by the CUDA and Metal drain-counter kernels.
///
/// Each LP owns a disjoint head-count histogram row. Each lowered channel has one immutable source,
/// so its emission counter also has one writer. The diagnostic path therefore uses no atomics and
/// cannot make block completion order part of the observation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DrainProfileLayout {
    head_ranges: Vec<Range<usize>>,
    channel_lookups: Vec<ChannelLookup>,
    producer_counts_by_degree: BTreeMap<usize, usize>,
    head_histogram_offset: usize,
    channel_emissions_offset: usize,
    words: usize,
}

impl DrainProfileLayout {
    pub fn new(
        maximum_heads_by_node: &[usize],
        channels: &[DrainProfileChannel],
    ) -> Result<Self, DrainProfileError> {
        if maximum_heads_by_node.contains(&0) {
            return Err(DrainProfileError::new(
                "every LP needs at least the fallback-heap head bin",
            ));
        }

        let mut outbound = vec![Vec::<(u64, usize)>::new(); maximum_heads_by_node.len()];
        for (channel, descriptor) in channels.iter().enumerate() {
            let entries = outbound.get_mut(descriptor.source).ok_or_else(|| {
                DrainProfileError::new(format!(
                    "channel {channel} source {} is outside {} LPs",
                    descriptor.source,
                    maximum_heads_by_node.len()
                ))
            })?;
            entries.push((descriptor.target, channel));
        }

        let mut producer_counts_by_degree = BTreeMap::new();
        for (source, entries) in outbound.iter_mut().enumerate() {
            entries.sort_unstable();
            if entries.windows(2).any(|pair| pair[0].0 == pair[1].0) {
                return Err(DrainProfileError::new(format!(
                    "source {source} has duplicate target channels"
                )));
            }
            *producer_counts_by_degree.entry(entries.len()).or_insert(0) += 1;
        }

        let mut channel_lookups = vec![
            ChannelLookup {
                outbound_degree: 0,
                binary_search_iterations: 0,
            };
            channels.len()
        ];
        for entries in &outbound {
            let degree = entries.len();
            for &(target, channel) in entries.iter() {
                channel_lookups[channel] = ChannelLookup {
                    outbound_degree: degree,
                    binary_search_iterations: if degree == 1 {
                        0
                    } else {
                        lower_bound_iterations(entries, target)
                    },
                };
            }
        }

        let head_histogram_offset = maximum_heads_by_node.len();
        let mut next = head_histogram_offset;
        let mut head_ranges = Vec::with_capacity(maximum_heads_by_node.len());
        for &maximum_heads in maximum_heads_by_node {
            let end = next.checked_add(maximum_heads).ok_or_else(|| {
                DrainProfileError::new("drain head-histogram layout overflows usize")
            })?;
            head_ranges.push(next..end);
            next = end;
        }
        let channel_emissions_offset = next;
        let words = next
            .checked_add(channels.len())
            .ok_or_else(|| DrainProfileError::new("drain channel layout overflows usize"))?
            .max(1);

        Ok(Self {
            head_ranges,
            channel_lookups,
            producer_counts_by_degree,
            head_histogram_offset,
            channel_emissions_offset,
            words,
        })
    }

    pub const fn words(&self) -> usize {
        self.words
    }

    pub const fn head_histogram_offset(&self) -> usize {
        self.head_histogram_offset
    }

    pub const fn channel_emissions_offset(&self) -> usize {
        self.channel_emissions_offset
    }

    pub fn zeroed_words(&self) -> Vec<u64> {
        vec![0; self.words]
    }

    pub fn decode(&self, words: &[u64]) -> Result<DrainProfile, DrainProfileError> {
        if words.len() != self.words {
            return Err(DrainProfileError::new(format!(
                "drain profile readback has {} words, expected {}",
                words.len(),
                self.words
            )));
        }
        if let Some((node, flag)) = words[..self.head_histogram_offset]
            .iter()
            .copied()
            .enumerate()
            .find(|(_, flag)| *flag != 0)
        {
            return Err(DrainProfileError::new(format!(
                "drain profile node {node} reported diagnostic invariant flag {flag}"
            )));
        }

        let maximum_heads = self
            .head_ranges
            .iter()
            .map(|range| range.len())
            .max()
            .unwrap_or(0);
        let mut histogram = vec![0_u128; maximum_heads];
        for range in &self.head_ranges {
            for (index, &count) in words[range.clone()].iter().enumerate() {
                histogram[index] = histogram[index]
                    .checked_add(u128::from(count))
                    .ok_or_else(|| DrainProfileError::new("head histogram total overflows u128"))?;
            }
        }
        let mut selected_events = 0_u128;
        let mut head_visits = 0_u128;
        let mut head_visit_histogram = Vec::new();
        for (index, selected) in histogram.into_iter().enumerate() {
            if selected == 0 {
                continue;
            }
            let heads = index + 1;
            selected_events = selected_events
                .checked_add(selected)
                .ok_or_else(|| DrainProfileError::new("selected-event total overflows u128"))?;
            head_visits =
                head_visits
                    .checked_add(selected.checked_mul(heads as u128).ok_or_else(|| {
                        DrainProfileError::new("head-visit product overflows u128")
                    })?)
                    .ok_or_else(|| DrainProfileError::new("head-visit total overflows u128"))?;
            head_visit_histogram.push(HeadVisitProfile {
                heads_visited: heads,
                selected_events: selected,
            });
        }

        let mut degrees = self
            .producer_counts_by_degree
            .iter()
            .map(|(&degree, &producer_count)| {
                (
                    degree,
                    OutboundDegreeProfile {
                        outbound_degree: degree,
                        producer_count,
                        remote_emissions: 0,
                        binary_search_lookups: 0,
                        binary_search_iterations: 0,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let mut lookup_histogram = BTreeMap::<(usize, usize), u128>::new();
        for (channel, lookup) in self.channel_lookups.iter().enumerate() {
            let emissions = u128::from(words[self.channel_emissions_offset + channel]);
            let row = degrees
                .get_mut(&lookup.outbound_degree)
                .expect("every channel degree has a producer row");
            row.remote_emissions = row
                .remote_emissions
                .checked_add(emissions)
                .ok_or_else(|| DrainProfileError::new("remote-emission total overflows u128"))?;
            if lookup.outbound_degree >= 2 {
                row.binary_search_lookups = row
                    .binary_search_lookups
                    .checked_add(emissions)
                    .ok_or_else(|| DrainProfileError::new("lookup total overflows u128"))?;
                let iterations = emissions
                    .checked_mul(lookup.binary_search_iterations as u128)
                    .ok_or_else(|| DrainProfileError::new("iteration product overflows u128"))?;
                row.binary_search_iterations = row
                    .binary_search_iterations
                    .checked_add(iterations)
                    .ok_or_else(|| DrainProfileError::new("iteration total overflows u128"))?;
                let count = lookup_histogram
                    .entry((lookup.outbound_degree, lookup.binary_search_iterations))
                    .or_default();
                *count = count.checked_add(emissions).ok_or_else(|| {
                    DrainProfileError::new("lookup histogram total overflows u128")
                })?;
            }
        }
        let outbound_degrees = degrees.into_values().collect::<Vec<_>>();
        let remote_emissions = checked_row_sum(&outbound_degrees, |row| row.remote_emissions)?;
        let binary_search_lookups =
            checked_row_sum(&outbound_degrees, |row| row.binary_search_lookups)?;
        let binary_search_iterations =
            checked_row_sum(&outbound_degrees, |row| row.binary_search_iterations)?;
        let lookup_iteration_histogram = lookup_histogram
            .into_iter()
            .filter(|(_, lookups)| *lookups != 0)
            .map(
                |((outbound_degree, binary_search_iterations), lookups)| LookupIterationProfile {
                    outbound_degree,
                    binary_search_iterations,
                    lookups,
                },
            )
            .collect();

        Ok(DrainProfile {
            selected_events,
            head_visits,
            head_visit_histogram,
            remote_emissions,
            binary_search_lookups,
            binary_search_iterations,
            lookup_iteration_histogram,
            outbound_degrees,
        })
    }

    /// Decodes counters and reconciles every LP-owned histogram row with that LP's independent
    /// final transition counter.
    ///
    /// The per-LP check rejects compensating misses/double-counts that leave the run-wide sum
    /// unchanged. Device profiling APIs use this checked entry point; the unchecked decoder is
    /// retained for layout-focused tests and tooling that has no execution result to reconcile.
    pub fn decode_checked(
        &self,
        words: &[u64],
        expected_transitions_by_lp: &[u64],
    ) -> Result<DrainProfile, DrainProfileError> {
        let profile = self.decode(words)?;
        if expected_transitions_by_lp.len() != self.head_ranges.len() {
            return Err(DrainProfileError::new(format!(
                "drain profile has {} LP transition totals, expected {}",
                expected_transitions_by_lp.len(),
                self.head_ranges.len()
            )));
        }
        for (node, (range, &expected)) in self
            .head_ranges
            .iter()
            .zip(expected_transitions_by_lp)
            .enumerate()
        {
            let selected = words[range.clone()]
                .iter()
                .try_fold(0_u128, |total, &count| {
                    total.checked_add(u128::from(count)).ok_or_else(|| {
                        DrainProfileError::new(format!(
                            "drain profile LP {node} selected-event total overflows u128"
                        ))
                    })
                })?;
            if selected != u128::from(expected) {
                return Err(DrainProfileError::new(format!(
                    "drain profile LP {node} selected {selected} events but executed {expected} transitions"
                )));
            }
        }
        Ok(profile)
    }

    #[doc(hidden)]
    pub fn set_node_flag_for_testing(&self, words: &mut [u64], node: usize, flag: u64) {
        assert!(node < self.head_histogram_offset);
        words[node] = flag;
    }

    #[doc(hidden)]
    pub fn set_head_bin_for_testing(
        &self,
        words: &mut [u64],
        node: usize,
        heads_visited: usize,
        count: u64,
    ) {
        let range = &self.head_ranges[node];
        assert!((1..=range.len()).contains(&heads_visited));
        words[range.start + heads_visited - 1] = count;
    }

    #[doc(hidden)]
    pub fn set_channel_emissions_for_testing(&self, words: &mut [u64], channel: usize, count: u64) {
        assert!(channel < self.channel_lookups.len());
        words[self.channel_emissions_offset + channel] = count;
    }
}

fn lower_bound_iterations(entries: &[(u64, usize)], target: u64) -> usize {
    let mut low = 0;
    let mut high = entries.len();
    let mut iterations = 0;
    while low < high {
        iterations += 1;
        let middle = low + (high - low) / 2;
        if entries[middle].0 < target {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    iterations
}

fn checked_row_sum(
    rows: &[OutboundDegreeProfile],
    value: impl Fn(&OutboundDegreeProfile) -> u128,
) -> Result<u128, DrainProfileError> {
    rows.iter().try_fold(0_u128, |total, row| {
        total
            .checked_add(value(row))
            .ok_or_else(|| DrainProfileError::new("drain profile total overflows u128"))
    })
}
