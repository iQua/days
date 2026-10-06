//! Stage-aware and window-aware arena sizing, shared by the host projection and both device
//! planners (P16 G2; design note `evidence/P16/colldev-design.md` §4.2, rulings G7 and G8).
//!
//! # Stages (ruling G7)
//!
//! The planners size the remote-staging, legacy event-heap and host-queue arenas by summing a
//! per-flow bound over every flow. A host that sources `k` stages of one collective is then charged
//! as if all `k` ran at once, although a stage is released only once its local predecessor (a
//! stage on the same host) is complete. The unfinished stages of a host form a forest under the
//! local-predecessor relation (a finished or stopped stage sends nothing more, so it is left out),
//! and stages on one root-to-leaf path run one at a time. At most `leaves` stages of a host are
//! active at once, and each can leave at most one completed generation of stale resends behind it
//! (the next stage's completion needs its own last packet acknowledged, and that packet left the
//! same FIFO class queue after the stale ones). So the stages of one host occupy an arena slot for
//! at most `2 × leaves` stages at once, each within its own per-flow bound:
//!
//! ```text
//! charge(host, slot, class) = min(sum of the bounds, 2 × leaves × max of the bounds)
//! ```
//!
//! The `min` keeps every charge at or below today's sum. Non-stage flows are charged as before, and
//! an image without stages builds nothing here, so its plan is unchanged byte for byte.
//!
//! A compute stage's timer is one fallback-heap event at its source while the stage runs; at most
//! `min(leaves, unfinished compute stages)` of a host's timers are pending at once.
//!
//! The host queue is the exception (fix rounds 1 and 2, review F1 and R1-F1). The per-flow
//! host-queue bound of a windowless queue pair or a TCP stage is a lookahead horizon bound, which
//! fails whenever its host queue backs up (behind a PFC pause or a busier pacer, with Go-back-N
//! copies or a congestion window that the backlog does not limit). The summed bounds of a host's
//! stages absorbed that; one chain's charge did not, and the retries grew with chain length. So
//! only a windowed queue pair, whose window bound survives a backlog (ruling G8), has its
//! host-queue charge grouped; every other stage keeps its summed host-queue charge. Remote
//! staging, the event heap and the outbox stay grouped for every stage.
//!
//! # Windows (ruling G8)
//!
//! A queue pair with a window keeps `next_psn − snd_una < w ≤ window_bytes`, so at most
//! `ceil(window_bytes / mtu) + 1` of its data packets are unacknowledged, and so waiting in its
//! source host's queue, at once. Its receiver sends at most one ACK or NACK per data packet, and
//! the sender has heard of none that still wait in the receiver's host queue, so the same bound
//! covers its feedback there. Both bounds hold however long a pause lasts. They replace the
//! horizon bound in the host queue of a windowed pair, and its feedback joins the receiver's host
//! queue; a windowed stage's bounds are charged with its host's chains. Windowless pairs and TCP
//! keep today's bound. Typed capacity retries remain the recovery path for what the bounds leave
//! out (Go-back-N duplicates behind a rewind).
//!
//! Everything here is decided once per plan, with sorted vectors and no map iteration order.

use crate::{FlowGeneratorKind, GeneratorStatus, PacketKind, SimulationImage, StageRole};

const NO_GROUP: u32 = u32::MAX;

/// Arena-charge classes: the same slot is charged separately per class.
pub(crate) const CLASS_DATA: u8 = 0;
pub(crate) const CLASS_FEEDBACK: u8 = 1;

/// The charge class of a packet kind: data, or the feedback that travels the reverse route.
pub(crate) fn charge_class(kind: PacketKind) -> u8 {
    if kind.is_data() {
        CLASS_DATA
    } else {
        CLASS_FEEDBACK
    }
}

/// Which flows of an image share a concurrency bound, decided once per plan.
///
/// `None` from [`SizingConcurrency::for_image`] means neither rule applies (no stage and no
/// windowed queue pair), and every planner then takes its unchanged path.
pub(crate) struct SizingConcurrency {
    /// Per flow: the concurrency group of an unfinished stage (one group per source host), or
    /// [`NO_GROUP`]. Empty when the image has no stage.
    groups: Vec<u32>,
    /// Per group: the stage generations that may hold packets at once, `2 × leaves`.
    generations: Vec<usize>,
    /// Per group: the source node and its pending compute-timer bound,
    /// `min(leaves, unfinished compute stages)`.
    compute_timers: Vec<(usize, usize)>,
    /// Per flow: `ceil(window_bytes / mtu) + 1` for a windowed queue pair, else zero. Empty when
    /// the image has no windowed queue pair.
    window_packets: Vec<usize>,
    /// Whether any host carries a stage (the stage region is planned exactly then).
    has_stages: bool,
}

impl SizingConcurrency {
    /// Builds the groups and windows, or returns `None` (allocating nothing) when no host carries
    /// a stage and no queue pair has a window.
    pub(crate) fn for_image(image: &SimulationImage) -> Option<Self> {
        let mut has_stages = false;
        let mut has_queue_pairs = false;
        for state in &image.host_states {
            has_stages |= !state.stages.is_empty();
            // Every queue pair has a receiver, so a host walk finds them without the generators.
            has_queue_pairs |= state.roce_receivers.is_some();
        }
        let window_packets = if has_queue_pairs {
            window_packets(image)
        } else {
            Vec::new()
        };
        if !has_stages && window_packets.is_empty() {
            return None;
        }
        let mut concurrency = Self {
            groups: Vec::new(),
            generations: Vec::new(),
            compute_timers: Vec::new(),
            window_packets,
            has_stages,
        };
        if has_stages {
            concurrency.group_stages(image);
        }
        Some(concurrency)
    }

    fn group_stages(&mut self, image: &SimulationImage) {
        let flow_count = image.flows.len();
        // Whether a stage names the flow as its local predecessor (always a stage of the same
        // host). A successor of an unfinished stage is itself unfinished: it is released only
        // once its predecessor completes.
        let mut has_successor = vec![false; flow_count];
        for state in &image.host_states {
            for stage in state.stages.iter().flatten() {
                if let Some(predecessor) = stage.dependencies.local_predecessor {
                    if let Some(slot) = has_successor.get_mut(predecessor.0 as usize) {
                        *slot = true;
                    }
                }
            }
        }
        let mut groups = vec![NO_GROUP; flow_count];
        for state in &image.host_states {
            if state.stages.is_empty() {
                continue;
            }
            let group = u32::try_from(self.generations.len()).unwrap_or(NO_GROUP - 1);
            let mut leaves = 0_usize;
            let mut compute = 0_usize;
            let mut source = None;
            for (position, generator) in state.generators.iter().enumerate() {
                let Some(stage) = state.stage(position) else {
                    continue;
                };
                if !matches!(
                    generator.next_emission.status,
                    GeneratorStatus::Scheduled | GeneratorStatus::Blocked
                ) {
                    continue;
                }
                let flow = generator.flow.0 as usize;
                groups[flow] = group;
                source.get_or_insert(image.flows[flow].source.0 as usize);
                leaves += usize::from(!has_successor[flow]);
                compute += usize::from(matches!(stage.role, StageRole::Compute(_)));
            }
            if let Some(source) = source {
                self.generations.push(leaves.saturating_mul(2));
                self.compute_timers.push((source, leaves.min(compute)));
            }
        }
        self.groups = groups;
    }

    /// Whether any host carries a stage.
    pub(crate) fn has_stages(&self) -> bool {
        self.has_stages
    }

    /// The concurrency group of `flow` if it is an unfinished stage.
    pub(crate) fn group(&self, flow: usize) -> Option<u32> {
        self.groups
            .get(flow)
            .copied()
            .filter(|group| *group != NO_GROUP)
    }

    /// The concurrency group in which `flow`'s host-queue source bound is charged: its stage group
    /// for a windowed queue pair, whose window bound survives a backlog; otherwise none, so the
    /// bound is charged alone (fix rounds 1 and 2, review F1 and R1-F1).
    pub(crate) fn host_queue_group(&self, flow: usize) -> Option<u32> {
        self.window_packets(flow).and_then(|_| self.group(flow))
    }

    /// `ceil(window_bytes / mtu) + 1` if `flow` is a windowed queue pair.
    pub(crate) fn window_packets(&self, flow: usize) -> Option<usize> {
        self.window_packets
            .get(flow)
            .copied()
            .filter(|packets| *packets != 0)
    }

    /// Adds each host's pending compute-timer bound to its fallback-heap capacity.
    pub(crate) fn add_compute_timers(&self, capacities: &mut [usize]) {
        for &(source, timers) in &self.compute_timers {
            capacities[source] = capacities[source].saturating_add(timers);
        }
    }
}

/// Per flow, `ceil(window_bytes / mtu) + 1` for each windowed queue pair and zero otherwise; empty
/// when no queue pair has a window.
fn window_packets(image: &SimulationImage) -> Vec<usize> {
    let mut packets = Vec::new();
    for generator in image.host_states.iter().flat_map(|state| &state.generators) {
        let FlowGeneratorKind::Roce(roce) = generator.kind else {
            continue;
        };
        if roce.window_bytes == 0 {
            continue;
        }
        if packets.is_empty() {
            packets = vec![0; image.flows.len()];
        }
        packets[generator.flow.0 as usize] =
            usize::try_from(roce.window_bytes.div_ceil(roce.pacer.mtu_bytes.max(1)))
                .unwrap_or(usize::MAX)
                .saturating_add(1);
    }
    packets
}

/// Per-flow arena bounds of stage flows, gathered so each host's stages are charged together.
#[derive(Default)]
pub(crate) struct ConcurrentCharges {
    entries: Vec<(u32, usize, u8, usize)>,
}

impl ConcurrentCharges {
    /// Records the bound `bound` of a stage of `group` on arena slot `slot`.
    pub(crate) fn push(&mut self, group: u32, slot: usize, class: u8, bound: usize) {
        if bound != 0 {
            self.entries.push((group, slot, class, bound));
        }
    }

    /// Adds each `(group, slot, class)`'s charge, `min(sum, 2 × leaves × max)`, to its slot.
    pub(crate) fn apply(mut self, concurrency: &SizingConcurrency, capacities: &mut [usize]) {
        // The fold is a sum and a maximum, so the order inside one key does not matter.
        self.entries
            .sort_unstable_by_key(|&(group, slot, class, _)| (group, slot, class));
        let mut start = 0;
        while start < self.entries.len() {
            let (group, slot, class, _) = self.entries[start];
            let mut sum = 0_usize;
            let mut max = 0_usize;
            let mut end = start;
            while end < self.entries.len() && self.entries[end].0 == group {
                let entry = self.entries[end];
                if (entry.1, entry.2) != (slot, class) {
                    break;
                }
                sum = sum.saturating_add(entry.3);
                max = max.max(entry.3);
                end += 1;
            }
            let concurrent = max.saturating_mul(concurrency.generations[group as usize]);
            capacities[slot] = capacities[slot].saturating_add(sum.min(concurrent));
            start = end;
        }
    }
}

/// Charges one per-flow bound on `slot`: directly for a flow outside every group, or to `charges`
/// for an unfinished stage.
#[inline]
pub(crate) fn charge(
    capacities: &mut [usize],
    charges: &mut ConcurrentCharges,
    group: Option<u32>,
    slot: usize,
    class: u8,
    bound: usize,
) {
    match group {
        None => capacities[slot] = capacities[slot].saturating_add(bound),
        Some(group) => charges.push(group, slot, class, bound),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        CollectiveStage, ComputeStage, ConstantGenerator, FlowDescriptor, FlowGeneratorKind,
        FlowGeneratorState, FlowId, GeneratorFeedbackState, GeneratorTermination, HostState,
        LinkId, NodeId, PayloadId, ScheduledEmission, StageDependencies,
    };

    fn generator(flow: u64, status: GeneratorStatus) -> FlowGeneratorState {
        FlowGeneratorState {
            flow: FlowId(flow),
            packets_emitted: 0,
            bytes_emitted: 0,
            next_emission: ScheduledEmission {
                status,
                departure_time_ns: 0,
                payload: PayloadId(0),
            },
            rng_state: 0,
            feedback: GeneratorFeedbackState {
                arrivals: 0,
                outstanding_bytes: 0,
                unacknowledged_bytes: 0,
            },
            kind: FlowGeneratorKind::Constant(ConstantGenerator {
                first_departure_ns: 0,
                interval_ns: 1,
                packet_size_bytes: 0,
                termination: GeneratorTermination::Bytes(0),
            }),
        }
    }

    fn stage(local: Option<u64>) -> CollectiveStage {
        CollectiveStage {
            role: StageRole::Compute(ComputeStage {
                compute_id: 0,
                group_size: 2,
                rank: 0,
                duration_ns: 1,
            }),
            dependencies: StageDependencies {
                local_predecessor: local.map(FlowId),
                inbound_predecessor: None,
                inbound_predecessor_bytes: 0,
                local_predecessor_complete: local.is_none(),
                inbound_predecessor_complete: true,
                inbound_bytes_received: 0,
            },
            activated: local.is_none(),
        }
    }

    fn host(
        generators: Vec<FlowGeneratorState>,
        stages: Vec<Option<CollectiveStage>>,
    ) -> HostState {
        HostState {
            egress_link: LinkId(0),
            queue: std::collections::VecDeque::new(),
            in_service: None,
            tx_ready_pending: false,
            generators,
            stages,
            tcp_receivers: Vec::new(),
            dcqcn_receivers: Vec::new(),
            roce_receivers: None,
            pfc: None,
            next_origin_seq: 0,
            next_payload_seq: 0,
            sourced_packets: 0,
            departed_packets: 0,
            received_packets: 0,
        }
    }

    /// Host A (node 0) owns flow 0 (a finished root), flows 1 and 4 (both after flow 0), flow 7
    /// (after flow 1) and flow 5 (no stage): its unfinished stages 1, 4 and 7 form two chains,
    /// 1 -> 7 and 4. Host B (node 1) owns flow 2 (a running root) and flow 3 (after flow 2): one
    /// chain. Host C owns flow 6 and no stage.
    fn image() -> SimulationImage {
        use GeneratorStatus::{Blocked, Finished, Scheduled};
        let a = host(
            vec![
                generator(0, Finished),
                generator(1, Blocked),
                generator(4, Blocked),
                generator(5, Scheduled),
                generator(7, Blocked),
            ],
            vec![
                Some(stage(None)),
                Some(stage(Some(0))),
                Some(stage(Some(0))),
                None,
                Some(stage(Some(1))),
            ],
        );
        let b = host(
            vec![generator(2, Scheduled), generator(3, Blocked)],
            vec![Some(stage(None)), Some(stage(Some(2)))],
        );
        let sources = [0, 0, 1, 1, 0, 0, 2, 0];
        SimulationImage {
            stop_time_ns: 0,
            nodes: Vec::new(),
            host_states: vec![a, b, host(vec![generator(6, Scheduled)], Vec::new())],
            switch_states: Vec::new(),
            flows: (0..8)
                .map(|id| FlowDescriptor {
                    id: FlowId(id),
                    source: NodeId(sources[id as usize]),
                    target: NodeId(2),
                    priority: 0,
                    feedback_priority: 0,
                    route: Vec::new(),
                    reverse_route: Vec::new(),
                })
                .collect(),
            initial_packets: Vec::new(),
            links: Vec::new(),
            channels: Vec::new(),
            initial_events: Vec::new(),
            seed: 0,
        }
    }

    #[test]
    fn a_stageless_image_builds_nothing() {
        let mut stageless = image();
        for state in &mut stageless.host_states {
            state.stages.clear();
        }
        assert!(SizingConcurrency::for_image(&stageless).is_none());
    }

    #[test]
    fn unfinished_stages_group_by_host_and_count_their_chains() {
        let concurrency = SizingConcurrency::for_image(&image()).expect("stages");
        assert!(concurrency.has_stages());
        // The finished root and the plain flow are charged alone.
        let groups = (0..8)
            .map(|flow| concurrency.group(flow))
            .collect::<Vec<_>>();
        assert_eq!(
            groups,
            [
                None,
                Some(0),
                Some(1),
                Some(1),
                Some(0),
                None,
                None,
                Some(0)
            ]
        );
        // Host A: two chains (1 -> 7 and 4); host B: one chain (2 -> 3).
        assert_eq!(concurrency.generations, [4, 2]);
        // Pending compute timers: min(chains, unfinished compute stages).
        assert_eq!(concurrency.compute_timers, [(0, 2), (1, 1)]);
        let mut timers = vec![0; 3];
        concurrency.add_compute_timers(&mut timers);
        assert_eq!(timers, [2, 1, 0]);
    }

    #[test]
    fn only_windowed_queue_pairs_get_a_window_bound() {
        let mut image = image();
        for state in &mut image.host_states {
            state.stages.clear();
        }
        let roce = |window_bytes| {
            FlowGeneratorKind::Roce(crate::RoceGenerator {
                pacer: crate::RocePacer {
                    first_pacing_time_ns: 0,
                    pacing_interval_ns: 1,
                    mtu_bytes: 1_000,
                    total_bytes: 1_000_000,
                    credit_quanta: 0,
                },
                controller: crate::DcqcnController::pristine(crate::DcqcnControllerConfig {
                    initial_rate_bps: 1,
                    minimum_rate_bps: 1,
                    maximum_rate_bps: 1,
                    additive_rate_bps: 1,
                    hyper_rate_bps: 1,
                    g_q63: 1,
                    alpha_interval_ns: 1,
                    decrease_interval_ns: 1,
                    increase_interval_ns: 1,
                    fast_recovery_steps: 1,
                    clamp_target_rate: false,
                }),
                pacing_timer_payload: PayloadId(9),
                next_psn: 0,
                snd_una: 0,
                rto_deadline_ns: 0,
                rto_ns: 0,
                window_bytes,
                pacer_armed: true,
                variable_window: false,
                window_parked: false,
            })
        };
        image.host_states[0].generators[1].kind = roce(0);
        image.host_states[0].roce_receivers = Some(Box::new([]));
        // A queue pair without a window: neither rule applies.
        assert!(SizingConcurrency::for_image(&image).is_none());
        image.host_states[0].generators[2].kind = roce(50_001);
        let concurrency = SizingConcurrency::for_image(&image).expect("a windowed pair");
        assert!(!concurrency.has_stages());
        assert_eq!(concurrency.window_packets(1), None);
        // ceil(50,001 / 1,000) + 1.
        assert_eq!(concurrency.window_packets(4), Some(52));
        assert_eq!(concurrency.group(4), None);
    }

    #[test]
    fn a_group_is_charged_the_lesser_of_its_sum_and_its_concurrent_generations() {
        let concurrency = SizingConcurrency::for_image(&image()).expect("stages");
        let mut capacities = vec![1; 3];
        let mut charges = ConcurrentCharges::default();
        // Host A's three stages fit under four generations of the largest: their sum.
        for bound in [5, 7, 3] {
            charge(&mut capacities, &mut charges, Some(0), 2, CLASS_DATA, bound);
        }
        // Host B's three equal bounds exceed two generations: two of them. Its feedback and a
        // second slot are charged apart, and a flow outside every group is added directly.
        for bound in [10, 10, 10] {
            charge(&mut capacities, &mut charges, Some(1), 2, CLASS_DATA, bound);
        }
        charge(&mut capacities, &mut charges, Some(1), 2, CLASS_FEEDBACK, 4);
        charge(&mut capacities, &mut charges, Some(1), 0, CLASS_DATA, 6);
        charge(&mut capacities, &mut charges, None, 1, CLASS_DATA, 9);
        assert_eq!(capacities, [1, 10, 1]);
        charges.apply(&concurrency, &mut capacities);
        assert_eq!(capacities, [1 + 6, 10, 1 + 15 + 20 + 4]);
    }
}
