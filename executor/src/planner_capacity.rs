//! Linear-pass lookup tables shared by the production device planners.

use std::collections::HashMap;

use crate::{
    EventKind, FlowGeneratorKind, GeneratorStatus, NodeId, NodeKind, PacketKind, SimulationImage,
    TcpGenerator, device_sizing::paced_single_source_queue_bound,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TcpMinimumPacketSize {
    #[cfg(any(
        feature = "cuda",
        all(feature = "metal-spike", target_vendor = "apple")
    ))]
    One,
}

impl TcpMinimumPacketSize {
    #[cfg(any(test, feature = "planner-test-hooks"))]
    fn legacy_uses_precomputed_table(self) -> bool {
        match self {
            #[cfg(any(
                feature = "cuda",
                all(feature = "metal-spike", target_vendor = "apple")
            ))]
            Self::One => true,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PlannerCapacityMode {
    Precomputed,
    #[cfg(any(test, feature = "planner-test-hooks"))]
    Legacy,
}

#[derive(Clone, Copy, Debug)]
struct GeneratorLocation {
    host: usize,
    generator: usize,
}

pub(crate) struct PlannerCapacityContext {
    mode: PlannerCapacityMode,
    tcp_minimum_packet_size: TcpMinimumPacketSize,
    source_queue_bounds: Vec<usize>,
    generator_round_bursts: Vec<usize>,
    minimum_packet_sizes: Vec<[u64; 2]>,
    tcp_ledger_segment_bounds: Vec<usize>,
    tcp_receiver_range_bounds: Vec<usize>,
    tcp_fallback_timer_bounds: Vec<usize>,
    flow_to_generator: Vec<Option<GeneratorLocation>>,
    host_to_lp: Vec<Option<NodeId>>,
    #[cfg(any(test, feature = "planner-test-hooks"))]
    lookahead: Option<u64>,
}

impl PlannerCapacityContext {
    pub(crate) fn new(
        image: &SimulationImage,
        data_counts: &[usize],
        lookahead: Option<u64>,
        tcp_minimum_packet_size: TcpMinimumPacketSize,
        mode: PlannerCapacityMode,
    ) -> Self {
        #[cfg(any(test, feature = "planner-test-hooks"))]
        if mode == PlannerCapacityMode::Legacy {
            return Self {
                mode,
                tcp_minimum_packet_size,
                source_queue_bounds: Vec::new(),
                generator_round_bursts: Vec::new(),
                minimum_packet_sizes: if tcp_minimum_packet_size.legacy_uses_precomputed_table() {
                    precompute_minimum_packet_sizes(image, tcp_minimum_packet_size)
                } else {
                    Vec::new()
                },
                tcp_ledger_segment_bounds: Vec::new(),
                tcp_receiver_range_bounds: Vec::new(),
                tcp_fallback_timer_bounds: Vec::new(),
                flow_to_generator: Vec::new(),
                host_to_lp: Vec::new(),
                lookahead,
            };
        }

        let flow_count = image.flows.len();
        let mut flows_per_source = vec![0_usize; image.nodes.len()];
        for flow in &image.flows {
            flows_per_source[flow.source.0 as usize] =
                flows_per_source[flow.source.0 as usize].saturating_add(1);
        }

        let mut initial_data_count = vec![0_usize; flow_count];
        let mut initial_data_payload = vec![None; flow_count];
        let mut initial_data_size = vec![0_u64; flow_count];
        let mut payload_to_flow = HashMap::with_capacity(image.initial_packets.len());
        let mut minimum_packet_sizes = vec![[0_u64; 2]; flow_count];
        for packet in &image.initial_packets {
            let flow = packet.flow.0 as usize;
            let class = usize::from(!packet.kind.is_data());
            update_minimum_packet_size(&mut minimum_packet_sizes[flow][class], packet.size_bytes);
            if packet.kind == PacketKind::Data {
                initial_data_count[flow] = initial_data_count[flow].saturating_add(1);
                initial_data_payload[flow].get_or_insert(packet.id);
                initial_data_size[flow] = packet.size_bytes;
                payload_to_flow.insert(packet.id, flow);
            }
        }

        let mut flow_to_generator = vec![None; flow_count];
        let mut generator_round_bursts = vec![0_usize; flow_count];
        for (host, state) in image.host_states.iter().enumerate() {
            for (generator_slot, generator) in state.generators.iter().enumerate() {
                let flow = generator.flow.0 as usize;
                flow_to_generator[flow].get_or_insert(GeneratorLocation {
                    host,
                    generator: generator_slot,
                });
                let size = match generator.kind {
                    FlowGeneratorKind::Constant(constant) => constant.packet_size_bytes,
                    FlowGeneratorKind::Tcp(tcp) => match tcp_minimum_packet_size {
                        #[cfg(any(
                            feature = "cuda",
                            all(feature = "metal-spike", target_vendor = "apple")
                        ))]
                        TcpMinimumPacketSize::One => {
                            let _ = tcp;
                            1
                        }
                    },
                    FlowGeneratorKind::Rate(rate) => {
                        crate::device_sizing::finite_generator_minimum_packet_size(
                            rate.total_bytes,
                            generator.bytes_emitted,
                            rate.packet_size_bytes,
                        )
                    }
                    FlowGeneratorKind::Collective(collective) => {
                        crate::device_sizing::finite_generator_minimum_packet_size(
                            collective.chunk_bytes,
                            generator.bytes_emitted,
                            collective.packet_size_bytes,
                        )
                    }
                    FlowGeneratorKind::Dcqcn(dcqcn) => {
                        crate::device_sizing::finite_generator_minimum_packet_size(
                            dcqcn.rate.total_bytes,
                            generator.bytes_emitted,
                            dcqcn.rate.packet_size_bytes,
                        )
                    }
                };
                update_minimum_packet_size(&mut minimum_packet_sizes[flow][0], size);

                if !matches!(
                    generator.next_emission.status,
                    GeneratorStatus::Scheduled | GeneratorStatus::Blocked
                ) {
                    continue;
                }
                let packet_count = data_counts[flow];
                let burst = match generator.kind {
                    FlowGeneratorKind::Constant(constant) => {
                        interval_burst(packet_count, constant.interval_ns, lookahead)
                    }
                    FlowGeneratorKind::Rate(rate) => {
                        interval_burst(packet_count, rate.pacing_interval_ns, lookahead)
                    }
                    FlowGeneratorKind::Tcp(_)
                    | FlowGeneratorKind::Collective(_)
                    | FlowGeneratorKind::Dcqcn(_) => packet_count,
                };
                generator_round_bursts[flow] = generator_round_bursts[flow].saturating_add(burst);
            }
        }
        materialize_minimum_packet_sizes(&mut minimum_packet_sizes);

        let mut tcp_ledger_segment_bounds = vec![1_usize; flow_count];
        let mut tcp_receiver_range_bounds = vec![1_usize; flow_count];
        let mut tcp_fallback_timer_bounds = vec![0_usize; flow_count];
        for (flow, location) in flow_to_generator.iter().copied().enumerate() {
            let Some(location) = location else {
                continue;
            };
            let FlowGeneratorKind::Tcp(tcp) =
                image.host_states[location.host].generators[location.generator].kind
            else {
                continue;
            };
            tcp_ledger_segment_bounds[flow] =
                crate::device_sizing::tcp_ledger_segment_bound(tcp, data_counts[flow]);
            tcp_receiver_range_bounds[flow] =
                crate::device_sizing::tcp_receiver_range_bound(tcp, data_counts[flow]);
            tcp_fallback_timer_bounds[flow] =
                crate::device_sizing::tcp_fallback_timer_packet_bound(data_counts[flow]);
        }

        let mut matching_arrivals = vec![0_usize; flow_count];
        for event in &image.initial_events {
            let Some(&flow_index) = payload_to_flow.get(&event.payload) else {
                continue;
            };
            let flow = &image.flows[flow_index];
            let source = &image.nodes[flow.source.0 as usize];
            if source.kind != NodeKind::Host {
                continue;
            }
            let state = &image.host_states[source.state_slot as usize];
            let [generator] = state.generators.as_slice() else {
                continue;
            };
            if event.target == flow.source
                && event.kind == EventKind::PacketArrival
                && event.payload == generator.next_emission.payload
                && event.key.time_ns == generator.next_emission.departure_time_ns
            {
                matching_arrivals[flow_index] = matching_arrivals[flow_index].saturating_add(1);
            }
        }

        let source_queue_bounds = image
            .flows
            .iter()
            .enumerate()
            .map(|(flow_index, flow)| {
                let packet_count = data_counts[flow_index];
                if packet_count == 0 {
                    return 0;
                }
                let Some(first_link) = flow.route.first().copied() else {
                    return packet_count;
                };
                let link = image.links[first_link.0 as usize];
                let serialization = crate::time::serialization_time_ns(
                    minimum_packet_sizes[flow_index][0],
                    link.rate_bps,
                )
                .expect("device validation established a positive finite serialization interval");
                let horizon_bound = crate::device_sizing::horizon_queue_packet_bound(
                    packet_count,
                    lookahead,
                    serialization,
                    link.propagation_ns,
                );
                if flows_per_source[flow.source.0 as usize] != 1 {
                    return horizon_bound;
                }
                let source = &image.nodes[flow.source.0 as usize];
                if source.kind != NodeKind::Host {
                    return packet_count;
                }
                let state = &image.host_states[source.state_slot as usize];
                let [generator] = state.generators.as_slice() else {
                    return packet_count;
                };
                if generator.flow.0 as usize != flow_index
                    || generator.next_emission.status != GeneratorStatus::Scheduled
                    || generator.packets_emitted != 0
                    || generator.bytes_emitted != 0
                    || !state.queue.is_empty()
                    || state.in_service.is_some()
                    || state.tx_ready_pending
                    || initial_data_count[flow_index] != 1
                    || initial_data_payload[flow_index] != Some(generator.next_emission.payload)
                    || matching_arrivals[flow_index] != 1
                {
                    return packet_count;
                }
                if first_link != state.egress_link {
                    return packet_count;
                }
                let FlowGeneratorKind::Constant(constant) = generator.kind else {
                    return packet_count;
                };
                if initial_data_size[flow_index] != constant.packet_size_bytes {
                    return packet_count;
                }
                let serialization = crate::time::serialization_time_ns(
                    constant.packet_size_bytes,
                    link.rate_bps,
                )
                .expect("device validation established a positive finite serialization interval");
                let paced = paced_single_source_queue_bound(
                    packet_count,
                    constant.interval_ns,
                    serialization,
                );
                if paced == packet_count {
                    packet_count
                } else {
                    state.queue.len().saturating_add(paced).min(packet_count)
                }
            })
            .collect();

        let mut host_to_lp = vec![None; image.host_states.len()];
        for node in &image.nodes {
            if node.kind == NodeKind::Host {
                if let Some(owner) = host_to_lp.get_mut(node.state_slot as usize) {
                    owner.get_or_insert(node.id);
                }
            }
        }

        let context = Self {
            mode,
            tcp_minimum_packet_size,
            source_queue_bounds,
            generator_round_bursts,
            minimum_packet_sizes,
            tcp_ledger_segment_bounds,
            tcp_receiver_range_bounds,
            tcp_fallback_timer_bounds,
            flow_to_generator,
            host_to_lp,
            #[cfg(any(test, feature = "planner-test-hooks"))]
            lookahead,
        };
        context.debug_assert_sampled_minimum_packet_sizes(image);
        context
    }

    pub(crate) fn source_queue_packet_bound(
        &self,
        image: &SimulationImage,
        flow: usize,
        packet_count: usize,
    ) -> usize {
        #[cfg(any(test, feature = "planner-test-hooks"))]
        if self.mode == PlannerCapacityMode::Legacy {
            let legacy = legacy_source_queue_packet_bound(image, flow, packet_count);
            if image
                .flows
                .iter()
                .filter(|candidate| candidate.source == image.flows[flow].source)
                .count()
                == 1
            {
                return legacy;
            }
            let Some(first_link) = image.flows[flow].route.first().copied() else {
                return legacy;
            };
            let link = image.links[first_link.0 as usize];
            return self.horizon_queue_packet_bound(
                image,
                flow,
                packet_count,
                PacketKind::Data,
                link,
                self.lookahead,
            );
        }
        let _ = (image, packet_count, self.mode);
        self.source_queue_bounds[flow]
    }

    pub(crate) fn generator_round_burst(
        &self,
        image: &SimulationImage,
        flow: usize,
        packet_count: usize,
        lookahead: Option<u64>,
    ) -> usize {
        #[cfg(any(test, feature = "planner-test-hooks"))]
        if self.mode == PlannerCapacityMode::Legacy {
            return legacy_generator_round_burst(image, flow, packet_count, lookahead);
        }
        let _ = (image, packet_count, lookahead);
        self.generator_round_bursts[flow]
    }

    pub(crate) fn minimum_packet_size(
        &self,
        image: &SimulationImage,
        flow: usize,
        packet_kind: PacketKind,
    ) -> u64 {
        #[cfg(any(test, feature = "planner-test-hooks"))]
        if self.mode == PlannerCapacityMode::Legacy {
            if self.tcp_minimum_packet_size.legacy_uses_precomputed_table() {
                return self.minimum_packet_sizes[flow][usize::from(!packet_kind.is_data())];
            }
            return legacy_minimum_packet_size(
                image,
                flow,
                packet_kind,
                self.tcp_minimum_packet_size,
            );
        }
        let _ = (image, self.tcp_minimum_packet_size);
        self.minimum_packet_sizes[flow][usize::from(!packet_kind.is_data())]
    }

    pub(crate) fn tcp_generator(
        &self,
        image: &SimulationImage,
        flow: usize,
    ) -> Option<TcpGenerator> {
        #[cfg(any(test, feature = "planner-test-hooks"))]
        if self.mode == PlannerCapacityMode::Legacy {
            return legacy_tcp_generator(image, flow);
        }
        let location = self.flow_to_generator[flow]?;
        match image.host_states[location.host].generators[location.generator].kind {
            FlowGeneratorKind::Tcp(tcp) => Some(tcp),
            FlowGeneratorKind::Constant(_)
            | FlowGeneratorKind::Rate(_)
            | FlowGeneratorKind::Collective(_)
            | FlowGeneratorKind::Dcqcn(_) => None,
        }
    }

    pub(crate) fn tcp_ledger_segment_bound(
        &self,
        image: &SimulationImage,
        flow: usize,
        whole_flow_segments: usize,
    ) -> usize {
        #[cfg(any(test, feature = "planner-test-hooks"))]
        if self.mode == PlannerCapacityMode::Legacy {
            return self.tcp_generator(image, flow).map_or(1, |tcp| {
                crate::device_sizing::tcp_ledger_segment_bound(tcp, whole_flow_segments)
            });
        }
        let _ = (image, whole_flow_segments);
        self.tcp_ledger_segment_bounds[flow]
    }

    pub(crate) fn tcp_receiver_range_bound(
        &self,
        image: &SimulationImage,
        flow: usize,
        whole_flow_segments: usize,
    ) -> usize {
        #[cfg(any(test, feature = "planner-test-hooks"))]
        if self.mode == PlannerCapacityMode::Legacy {
            return self.tcp_generator(image, flow).map_or(1, |tcp| {
                crate::device_sizing::tcp_receiver_range_bound(tcp, whole_flow_segments)
            });
        }
        let _ = (image, whole_flow_segments);
        self.tcp_receiver_range_bounds[flow]
    }

    pub(crate) fn tcp_fallback_timer_bound(
        &self,
        image: &SimulationImage,
        flow: usize,
        whole_flow_attempts: usize,
    ) -> usize {
        #[cfg(any(test, feature = "planner-test-hooks"))]
        if self.mode == PlannerCapacityMode::Legacy {
            return usize::from(self.tcp_generator(image, flow).is_some()).saturating_mul(
                crate::device_sizing::tcp_fallback_timer_packet_bound(whole_flow_attempts),
            );
        }
        let _ = (image, whole_flow_attempts);
        self.tcp_fallback_timer_bounds[flow]
    }

    pub(crate) fn horizon_queue_packet_bound(
        &self,
        image: &SimulationImage,
        flow: usize,
        whole_flow_packets: usize,
        packet_kind: PacketKind,
        link: crate::LinkDescriptor,
        lookahead: Option<u64>,
    ) -> usize {
        let serialization = crate::time::serialization_time_ns(
            self.minimum_packet_size(image, flow, packet_kind),
            link.rate_bps,
        )
        .expect("device validation established a positive finite serialization interval");
        crate::device_sizing::horizon_queue_packet_bound(
            whole_flow_packets,
            lookahead,
            serialization,
            link.propagation_ns,
        )
    }

    pub(crate) fn host_lp(&self, image: &SimulationImage, host_slot: usize) -> Option<NodeId> {
        #[cfg(any(test, feature = "planner-test-hooks"))]
        if self.mode == PlannerCapacityMode::Legacy {
            return image
                .nodes
                .iter()
                .find(|node| node.kind == NodeKind::Host && node.state_slot as usize == host_slot)
                .map(|node| node.id);
        }
        let _ = image;
        self.host_to_lp[host_slot]
    }

    /// Same gating story as `device_sizing::exact_plan_report`: the only callers are
    /// `cuda::assert_cuda_planner_bit_equal_for_testing` and
    /// `metal::assert_metal_planner_bit_equal_for_testing`, both `planner-test-hooks` functions in
    /// modules that only exist under `cuda` / `metal-spike`.
    #[cfg(all(
        feature = "planner-test-hooks",
        any(
            feature = "cuda",
            all(feature = "metal-spike", target_vendor = "apple")
        )
    ))]
    pub(crate) fn matches_legacy(
        image: &SimulationImage,
        data_counts: &[usize],
        lookahead: Option<u64>,
        tcp_minimum_packet_size: TcpMinimumPacketSize,
    ) -> bool {
        let precomputed = Self::new(
            image,
            data_counts,
            lookahead,
            tcp_minimum_packet_size,
            PlannerCapacityMode::Precomputed,
        );
        let legacy = Self::new(
            image,
            data_counts,
            lookahead,
            tcp_minimum_packet_size,
            PlannerCapacityMode::Legacy,
        );
        let flow_values_equal = image.flows.iter().enumerate().all(|(flow, _)| {
            let packet_count = data_counts[flow];
            precomputed.source_queue_packet_bound(image, flow, packet_count)
                == legacy.source_queue_packet_bound(image, flow, packet_count)
                && precomputed.generator_round_burst(image, flow, packet_count, lookahead)
                    == legacy.generator_round_burst(image, flow, packet_count, lookahead)
                && [PacketKind::Data, PacketKind::Feedback]
                    .into_iter()
                    .all(|kind| {
                        precomputed.minimum_packet_size(image, flow, kind)
                            == legacy.minimum_packet_size(image, flow, kind)
                    })
                && precomputed.tcp_generator(image, flow) == legacy.tcp_generator(image, flow)
                && precomputed.tcp_ledger_segment_bound(image, flow, packet_count)
                    == legacy.tcp_ledger_segment_bound(image, flow, packet_count)
                && precomputed.tcp_receiver_range_bound(image, flow, packet_count)
                    == legacy.tcp_receiver_range_bound(image, flow, packet_count)
                && precomputed.tcp_fallback_timer_bound(image, flow, packet_count)
                    == legacy.tcp_fallback_timer_bound(image, flow, packet_count)
        });
        flow_values_equal
            && image
                .host_states
                .iter()
                .enumerate()
                .all(|(host, _)| precomputed.host_lp(image, host) == legacy.host_lp(image, host))
    }

    #[cfg(debug_assertions)]
    fn debug_assert_sampled_minimum_packet_sizes(&self, image: &SimulationImage) {
        if self.mode != PlannerCapacityMode::Precomputed {
            return;
        }
        for flow in sampled_flow_indices(image.flows.len()) {
            for packet_kind in [PacketKind::Data, PacketKind::Feedback] {
                debug_assert_eq!(
                    self.minimum_packet_sizes[flow][usize::from(!packet_kind.is_data())],
                    legacy_minimum_packet_size(
                        image,
                        flow,
                        packet_kind,
                        self.tcp_minimum_packet_size,
                    )
                );
            }
        }
    }

    #[cfg(not(debug_assertions))]
    fn debug_assert_sampled_minimum_packet_sizes(&self, _image: &SimulationImage) {}
}

fn interval_burst(packet_count: usize, interval_ns: u64, lookahead: Option<u64>) -> usize {
    lookahead.map_or(packet_count, |lookahead| {
        packet_count.min(
            usize::try_from(lookahead / interval_ns)
                .unwrap_or(usize::MAX)
                .saturating_add(1),
        )
    })
}

#[cfg(any(test, feature = "planner-test-hooks"))]
fn precompute_minimum_packet_sizes(
    image: &SimulationImage,
    tcp_minimum_packet_size: TcpMinimumPacketSize,
) -> Vec<[u64; 2]> {
    let mut minimums = vec![[0_u64; 2]; image.flows.len()];
    for packet in &image.initial_packets {
        let flow = packet.flow.0 as usize;
        let class = usize::from(!packet.kind.is_data());
        update_minimum_packet_size(&mut minimums[flow][class], packet.size_bytes);
    }
    for generator in image.host_states.iter().flat_map(|state| &state.generators) {
        let flow = generator.flow.0 as usize;
        let size = match generator.kind {
            FlowGeneratorKind::Constant(constant) => constant.packet_size_bytes,
            FlowGeneratorKind::Tcp(tcp) => match tcp_minimum_packet_size {
                #[cfg(any(
                    feature = "cuda",
                    all(feature = "metal-spike", target_vendor = "apple")
                ))]
                TcpMinimumPacketSize::One => {
                    let _ = tcp;
                    1
                }
            },
            FlowGeneratorKind::Rate(rate) => {
                crate::device_sizing::finite_generator_minimum_packet_size(
                    rate.total_bytes,
                    generator.bytes_emitted,
                    rate.packet_size_bytes,
                )
            }
            FlowGeneratorKind::Collective(collective) => {
                crate::device_sizing::finite_generator_minimum_packet_size(
                    collective.chunk_bytes,
                    generator.bytes_emitted,
                    collective.packet_size_bytes,
                )
            }
            FlowGeneratorKind::Dcqcn(dcqcn) => {
                crate::device_sizing::finite_generator_minimum_packet_size(
                    dcqcn.rate.total_bytes,
                    generator.bytes_emitted,
                    dcqcn.rate.packet_size_bytes,
                )
            }
        };
        update_minimum_packet_size(&mut minimums[flow][0], size);
    }
    materialize_minimum_packet_sizes(&mut minimums);
    minimums
}

fn update_minimum_packet_size(minimum: &mut u64, size: u64) {
    debug_assert_ne!(size, 0, "device validation rejects zero packet sizes");
    if *minimum == 0 || size < *minimum {
        *minimum = size;
    }
}

fn materialize_minimum_packet_sizes(minimums: &mut [[u64; 2]]) {
    for minimum in minimums {
        for size in minimum {
            if *size == 0 {
                *size = 1;
            }
        }
    }
}

#[cfg(debug_assertions)]
fn sampled_flow_indices(flow_count: usize) -> Vec<usize> {
    const SAMPLE_COUNT: usize = 16;
    if flow_count <= SAMPLE_COUNT {
        return (0..flow_count).collect();
    }
    let mut sampled = Vec::with_capacity(SAMPLE_COUNT);
    sampled.extend(0..4);
    sampled.extend(flow_count - 4..flow_count);
    let mut state = 0x4d59_5df4_d0f3_3173_u64;
    while sampled.len() < SAMPLE_COUNT {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let flow = (state as usize) % flow_count;
        if !sampled.contains(&flow) {
            sampled.push(flow);
        }
    }
    sampled
}

#[cfg(any(debug_assertions, test, feature = "planner-test-hooks"))]
fn legacy_minimum_packet_size(
    image: &SimulationImage,
    flow: usize,
    packet_kind: PacketKind,
    tcp_minimum_packet_size: TcpMinimumPacketSize,
) -> u64 {
    image
        .initial_packets
        .iter()
        .filter(|packet| {
            packet.flow.0 as usize == flow && packet.kind.is_data() == packet_kind.is_data()
        })
        .map(|packet| packet.size_bytes)
        .chain(
            packet_kind
                .is_data()
                .then(|| {
                    image
                        .host_states
                        .iter()
                        .flat_map(|state| &state.generators)
                        .filter(move |generator| generator.flow.0 as usize == flow)
                        .map(move |generator| match generator.kind {
                            FlowGeneratorKind::Constant(constant) => constant.packet_size_bytes,
                            FlowGeneratorKind::Tcp(tcp) => match tcp_minimum_packet_size {
                                #[cfg(any(
                                    feature = "cuda",
                                    all(feature = "metal-spike", target_vendor = "apple")
                                ))]
                                TcpMinimumPacketSize::One => {
                                    let _ = tcp;
                                    1
                                }
                            },
                            FlowGeneratorKind::Rate(rate) => {
                                crate::device_sizing::finite_generator_minimum_packet_size(
                                    rate.total_bytes,
                                    generator.bytes_emitted,
                                    rate.packet_size_bytes,
                                )
                            }
                            FlowGeneratorKind::Collective(collective) => {
                                crate::device_sizing::finite_generator_minimum_packet_size(
                                    collective.chunk_bytes,
                                    generator.bytes_emitted,
                                    collective.packet_size_bytes,
                                )
                            }
                            FlowGeneratorKind::Dcqcn(dcqcn) => {
                                crate::device_sizing::finite_generator_minimum_packet_size(
                                    dcqcn.rate.total_bytes,
                                    generator.bytes_emitted,
                                    dcqcn.rate.packet_size_bytes,
                                )
                            }
                        })
                })
                .into_iter()
                .flatten(),
        )
        .min()
        .unwrap_or(1)
}

#[cfg(any(test, feature = "planner-test-hooks"))]
fn legacy_tcp_generator(image: &SimulationImage, flow: usize) -> Option<TcpGenerator> {
    image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .find(|generator| generator.flow.0 as usize == flow)
        .and_then(|generator| match generator.kind {
            FlowGeneratorKind::Tcp(tcp) => Some(tcp),
            FlowGeneratorKind::Constant(_)
            | FlowGeneratorKind::Rate(_)
            | FlowGeneratorKind::Collective(_)
            | FlowGeneratorKind::Dcqcn(_) => None,
        })
}

#[cfg(any(test, feature = "planner-test-hooks"))]
fn legacy_generator_round_burst(
    image: &SimulationImage,
    flow: usize,
    packet_count: usize,
    lookahead: Option<u64>,
) -> usize {
    image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter(|generator| {
            generator.flow.0 as usize == flow
                && matches!(
                    generator.next_emission.status,
                    GeneratorStatus::Scheduled | GeneratorStatus::Blocked
                )
        })
        .map(|generator| match generator.kind {
            FlowGeneratorKind::Constant(constant) => {
                interval_burst(packet_count, constant.interval_ns, lookahead)
            }
            FlowGeneratorKind::Rate(rate) => {
                interval_burst(packet_count, rate.pacing_interval_ns, lookahead)
            }
            FlowGeneratorKind::Tcp(_)
            | FlowGeneratorKind::Collective(_)
            | FlowGeneratorKind::Dcqcn(_) => packet_count,
        })
        .fold(0, usize::saturating_add)
}

#[cfg(any(test, feature = "planner-test-hooks"))]
fn legacy_source_queue_packet_bound(
    image: &SimulationImage,
    flow_index: usize,
    packet_count: usize,
) -> usize {
    if packet_count == 0 {
        return 0;
    }
    let flow = &image.flows[flow_index];
    if image
        .flows
        .iter()
        .filter(|candidate| candidate.source == flow.source)
        .count()
        != 1
    {
        return packet_count;
    }
    let source = &image.nodes[flow.source.0 as usize];
    if source.kind != NodeKind::Host {
        return packet_count;
    }
    let state = &image.host_states[source.state_slot as usize];
    let [generator] = state.generators.as_slice() else {
        return packet_count;
    };
    if generator.flow.0 as usize != flow_index
        || generator.next_emission.status != GeneratorStatus::Scheduled
        || generator.packets_emitted != 0
        || generator.bytes_emitted != 0
        || !state.queue.is_empty()
        || state.in_service.is_some()
        || state.tx_ready_pending
    {
        return packet_count;
    }
    let mut initial_data_packets = image
        .initial_packets
        .iter()
        .filter(|packet| packet.flow.0 as usize == flow_index && packet.kind == PacketKind::Data);
    let Some(initial_packet) = initial_data_packets.next() else {
        return packet_count;
    };
    if initial_data_packets.next().is_some() || initial_packet.id != generator.next_emission.payload
    {
        return packet_count;
    }
    if image
        .initial_events
        .iter()
        .filter(|event| {
            event.target == flow.source
                && event.kind == EventKind::PacketArrival
                && event.payload == generator.next_emission.payload
                && event.key.time_ns == generator.next_emission.departure_time_ns
        })
        .count()
        != 1
    {
        return packet_count;
    }
    let Some(first_link) = flow.route.first().copied() else {
        return packet_count;
    };
    if first_link != state.egress_link {
        return packet_count;
    }
    let FlowGeneratorKind::Constant(constant) = generator.kind else {
        return packet_count;
    };
    if initial_packet.size_bytes != constant.packet_size_bytes {
        return packet_count;
    }
    let link = image.links[first_link.0 as usize];
    let serialization =
        crate::time::serialization_time_ns(constant.packet_size_bytes, link.rate_bps)
            .expect("device validation established a positive finite serialization interval");
    let paced = paced_single_source_queue_bound(packet_count, constant.interval_ns, serialization);
    if paced == packet_count {
        packet_count
    } else {
        state.queue.len().saturating_add(paced).min(packet_count)
    }
}

#[cfg(test)]
mod tests {
    use super::{materialize_minimum_packet_sizes, update_minimum_packet_size};

    #[cfg(feature = "cuda")]
    use std::collections::VecDeque;

    #[cfg(feature = "cuda")]
    use super::{PlannerCapacityContext, PlannerCapacityMode, TcpMinimumPacketSize};
    #[cfg(feature = "cuda")]
    use crate::{
        FlowDescriptor, FlowGeneratorKind, FlowGeneratorState, FlowId, GeneratorFeedbackState,
        GeneratorStatus, HostState, LinkDescriptor, LinkId, NodeDescriptor, NodeId, NodeKind,
        PacketKind, PayloadId, ScheduledEmission, SimulationImage, TcpCongestionControl,
        TcpGenerator,
    };

    #[test]
    fn packet_size_cache_distinguishes_exact_u64_boundary_from_absence() {
        let mut minimums = [[0, 0]];
        update_minimum_packet_size(&mut minimums[0][0], u64::MAX);
        materialize_minimum_packet_sizes(&mut minimums);
        assert_eq!(minimums, [[u64::MAX, 1]]);
    }

    #[cfg(feature = "cuda")]
    #[test]
    fn tcp_generator_uses_one_byte_minimum_for_horizon_queue_sizing() {
        const MSS_BYTES: u64 = 1_000;
        const PACKET_COUNT: usize = 100;
        const LOOKAHEAD_NS: u64 = 64;

        let link = LinkDescriptor {
            id: LinkId(0),
            source: NodeId(0),
            target: NodeId(1),
            rate_bps: 8_000_000_000,
            propagation_ns: 0,
        };
        let empty_host = || HostState {
            egress_link: link.id,
            queue: VecDeque::new(),
            in_service: None,
            tx_ready_pending: false,
            generators: vec![],
            tcp_receivers: vec![],
            dcqcn_receivers: vec![],
            next_origin_seq: 0,
            next_payload_seq: 0,
            sourced_packets: 0,
            departed_packets: 0,
            received_packets: 0,
        };
        let mut source = empty_host();
        source.generators.push(FlowGeneratorState {
            flow: FlowId(0),
            packets_emitted: 0,
            bytes_emitted: 0,
            next_emission: ScheduledEmission {
                status: GeneratorStatus::Scheduled,
                departure_time_ns: 0,
                payload: PayloadId(0),
            },
            rng_state: 1,
            feedback: GeneratorFeedbackState {
                arrivals: 0,
                outstanding_bytes: 0,
                unacknowledged_bytes: 0,
            },
            kind: FlowGeneratorKind::Tcp(TcpGenerator::new(
                99 * MSS_BYTES + 1,
                MSS_BYTES,
                40,
                TcpCongestionControl::reno(MSS_BYTES),
            )),
        });
        let image = SimulationImage {
            stop_time_ns: 1_000,
            nodes: vec![
                NodeDescriptor {
                    id: NodeId(0),
                    kind: NodeKind::Host,
                    state_slot: 0,
                },
                NodeDescriptor {
                    id: NodeId(1),
                    kind: NodeKind::Host,
                    state_slot: 1,
                },
            ],
            host_states: vec![source, empty_host()],
            switch_states: vec![],
            flows: vec![FlowDescriptor {
                id: FlowId(0),
                source: NodeId(0),
                target: NodeId(1),
                priority: 0,
                route: vec![link.id],
                reverse_route: vec![],
            }],
            initial_packets: vec![],
            links: vec![link],
            channels: vec![],
            initial_events: vec![],
            seed: 1,
        };
        let context = PlannerCapacityContext::new(
            &image,
            &[PACKET_COUNT],
            Some(LOOKAHEAD_NS),
            TcpMinimumPacketSize::One,
            PlannerCapacityMode::Precomputed,
        );

        assert_eq!(context.minimum_packet_size(&image, 0, PacketKind::Data), 1);
        let one_byte_serialization = crate::time::serialization_time_ns(1, link.rate_bps).unwrap();
        let mss_serialization =
            crate::time::serialization_time_ns(MSS_BYTES, link.rate_bps).unwrap();
        let expected = crate::device_sizing::horizon_queue_packet_bound(
            PACKET_COUNT,
            Some(LOOKAHEAD_NS),
            one_byte_serialization,
            link.propagation_ns,
        );
        let unsafe_mss_bound = crate::device_sizing::horizon_queue_packet_bound(
            PACKET_COUNT,
            Some(LOOKAHEAD_NS),
            mss_serialization,
            link.propagation_ns,
        );
        assert_ne!(expected, unsafe_mss_bound);
        assert_eq!(
            context.horizon_queue_packet_bound(
                &image,
                0,
                PACKET_COUNT,
                PacketKind::Data,
                link,
                Some(LOOKAHEAD_NS),
            ),
            expected
        );
    }
}
