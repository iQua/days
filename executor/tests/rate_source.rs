use std::collections::VecDeque;

use days_executor::{
    Backend, CpuConfig, Event, EventFelClass, EventKey, EventKind, FlowDescriptor,
    FlowGeneratorKind, FlowGeneratorState, FlowId, GeneratorFeedbackState, GeneratorStatus,
    HostState, LinkDescriptor, LinkId, NodeDescriptor, NodeId, NodeKind, ObservationMode,
    PacketDescriptor, PacketKind, PayloadId, RateGenerator, RemoteChannel, RunResult,
    ScheduledEmission, SimulationImage, event_fel_class, event_phase, rate_source_lookahead,
    rate_transitions_csv, run_cpu_with_observations, run_scalar_rounds,
    run_scalar_with_observations, validate,
};
#[cfg(feature = "cuda")]
use days_executor::{CudaConfig, run_cuda_with_observations};
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
use days_executor::{MetalConfig, run_metal_with_observations};

const SOURCE: NodeId = NodeId(0);
const SINK: NodeId = NodeId(1);
const FORWARD: LinkId = LinkId(0);
const REVERSE: LinkId = LinkId(1);
const FLOW: FlowId = FlowId(0);
const TOKEN: PayloadId = PayloadId(0);

fn rate_image(rate: RateGenerator, status: GeneratorStatus, stop_time_ns: u64) -> SimulationImage {
    let token_size = rate.packet_size_bytes.min(rate.total_bytes);
    let token = PacketDescriptor {
        id: TOKEN,
        flow: FLOW,
        size_bytes: token_size,
        ecn_marked: false,
        kind: PacketKind::Data,
    };
    let forward = LinkDescriptor {
        id: FORWARD,
        source: SOURCE,
        target: SINK,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };

    SimulationImage {
        stop_time_ns,
        nodes: vec![
            NodeDescriptor {
                id: SOURCE,
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: SINK,
                kind: NodeKind::Host,
                state_slot: 1,
            },
        ],
        host_states: vec![
            HostState {
                egress_link: FORWARD,
                queue: VecDeque::new(),
                in_service: None,
                tx_ready_pending: false,
                generators: vec![FlowGeneratorState {
                    flow: FLOW,
                    packets_emitted: 0,
                    bytes_emitted: 0,
                    next_emission: ScheduledEmission {
                        status,
                        departure_time_ns: rate.first_pacing_time_ns,
                        payload: TOKEN,
                    },
                    rng_state: 25,
                    feedback: GeneratorFeedbackState {
                        arrivals: 0,
                        outstanding_bytes: 0,
                        unacknowledged_bytes: 0,
                    },
                    kind: FlowGeneratorKind::Rate(rate),
                }],
                tcp_receivers: vec![],
                dcqcn_receivers: vec![],
                next_origin_seq: 1,
                next_payload_seq: 1,
                sourced_packets: 0,
                departed_packets: 0,
                received_packets: 0,
            },
            HostState {
                egress_link: REVERSE,
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
            },
        ],
        switch_states: vec![],
        flows: vec![FlowDescriptor {
            id: FLOW,
            source: SOURCE,
            target: SINK,
            priority: 0,
            route: vec![FORWARD],
            reverse_route: vec![REVERSE],
        }],
        initial_packets: vec![token],
        links: vec![
            forward,
            LinkDescriptor {
                id: REVERSE,
                source: SINK,
                target: SOURCE,
                rate_bps: 8_000_000_000,
                propagation_ns: 0,
            },
        ],
        channels: vec![
            RemoteChannel::for_packet_link(forward, rate.packet_size_bytes)
                .expect("forward delay must fit"),
        ],
        initial_events: vec![Event {
            key: EventKey {
                time_ns: rate.first_pacing_time_ns,
                phase: event_phase(EventKind::PacingTimer),
                origin_node: SOURCE,
                origin_seq: 0,
            },
            target: SOURCE,
            kind: EventKind::PacingTimer,
            payload: TOKEN,
        }],
        seed: 25,
    }
}

fn checkpoint_image(original: &SimulationImage, checkpoint: &RunResult) -> SimulationImage {
    let mut image = original.clone();
    image.host_states.clone_from(&checkpoint.host_states);
    image.switch_states.clone_from(&checkpoint.switch_states);
    image
        .initial_packets
        .clone_from(&checkpoint.resident_packets);
    image.initial_events.clone_from(&checkpoint.pending_events);
    image
}

fn rate_state(result: &RunResult) -> (FlowGeneratorState, RateGenerator) {
    let generator = result.host_states[0].generators[0];
    let FlowGeneratorKind::Rate(rate) = generator.kind else {
        panic!("fixture generator must remain a rate source")
    };
    (generator, rate)
}

#[test]
fn scheduled_rate_deadline_after_stop_reserves_zero_future_packets() {
    let mut image = rate_image(
        RateGenerator {
            first_pacing_time_ns: 11,
            pacing_interval_ns: 1,
            packet_size_bytes: 1,
            total_bytes: 1,
            rate_numerator_bits_per_second: 8_000_000_000,
            rate_denominator: 1,
            credit_quanta: 0,
        },
        GeneratorStatus::Scheduled,
        10,
    );
    validate(&image, Backend::Scalar)
        .expect("a scheduled deadline beyond stop is accepted at ordinary counters");
    image.host_states[0].sourced_packets = u64::MAX;

    validate(&image, Backend::Scalar)
        .expect("a pacing event beyond stop cannot increment the source counter");
    let result = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("the terminal run must perform no source transition");
    assert_eq!(result.summary.sourced_packets, 0);
    validate(&checkpoint_image(&image, &result), Backend::Scalar)
        .expect("the no-op terminal checkpoint must preserve zero executable work");
}

#[test]
fn scheduled_rate_deadline_after_stop_still_checks_emission_bookkeeping() {
    let mut image = rate_image(
        RateGenerator {
            first_pacing_time_ns: 11,
            pacing_interval_ns: 1,
            packet_size_bytes: 1,
            total_bytes: 1,
            rate_numerator_bits_per_second: 8_000_000_000,
            rate_denominator: 1,
            credit_quanta: 0,
        },
        GeneratorStatus::Scheduled,
        10,
    );
    let next_token = PayloadId::from_node_sequence(SOURCE, image.nodes.len() as u64, 1)
        .expect("small fixture identity must fit");
    image.initial_packets[0].id = next_token;
    image.initial_events[0].payload = next_token;
    let generator = &mut image.host_states[0].generators[0];
    generator.packets_emitted = 1;
    generator.next_emission.payload = next_token;
    image.host_states[0].next_payload_seq = 2;

    let error = validate(&image, Backend::Scalar)
        .expect_err("stop truncation must not hide inconsistent rate-source bookkeeping")
        .to_string();
    assert!(
        error.contains("records 0 emitted bytes, expected 1"),
        "expected an emitted-byte bookkeeping diagnostic, got: {error}"
    );
}

#[test]
fn scheduled_rate_deadline_at_stop_remains_executable() {
    let mut image = rate_image(
        RateGenerator {
            first_pacing_time_ns: 10,
            pacing_interval_ns: 1,
            packet_size_bytes: 1,
            total_bytes: 1,
            rate_numerator_bits_per_second: 8_000_000_000,
            rate_denominator: 1,
            credit_quanta: 0,
        },
        GeneratorStatus::Scheduled,
        10,
    );
    image.host_states[0].sourced_packets = u64::MAX;
    let error = validate(&image, Backend::Scalar)
        .expect_err("the inclusive stop-time pacing event still sources one packet")
        .to_string();
    assert!(
        error.contains("sourced_packets") && error.contains("remaining upper bound 1"),
        "expected an inclusive stop-time counter diagnostic, got: {error}"
    );

    image.host_states[0].sourced_packets = u64::MAX - 1;
    validate(&image, Backend::Scalar).expect("one executable boundary packet still fits");
    let result = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("the stop-time event must execute");
    assert_eq!(result.summary.sourced_packets, 1);
}

#[test]
fn blocked_rate_ticks_are_reserved_in_origin_sequence_capacity() {
    let packet_cost = 8_u128 * 1_000_000_000;
    let mut image = rate_image(
        RateGenerator {
            first_pacing_time_ns: 1,
            pacing_interval_ns: 1,
            packet_size_bytes: 1,
            total_bytes: 1,
            rate_numerator_bits_per_second: 1,
            rate_denominator: 1,
            credit_quanta: packet_cost - 3,
        },
        GeneratorStatus::Blocked,
        3,
    );
    image.host_states[0].next_origin_seq = u64::MAX - 3;

    let error = validate(&image, Backend::Scalar)
        .expect_err("successor pacing timers must be included in origin-sequence capacity")
        .to_string();
    assert!(
        error.contains("origin sequence space overflows") && error.contains("NodeId(0)"),
        "expected the pacing-timer origin-capacity diagnostic, got: {error}"
    );
    let error = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect_err("execution demonstrates the origin-capacity fault validation must prevent")
        .to_string();
    assert!(
        error.contains("origin sequence overflow") && error.contains("NodeId(0)"),
        "expected the protected runtime fault, got: {error}"
    );
}

#[test]
fn blocked_rate_deadline_beyond_stop_reserves_no_origin_capacity() {
    let packet_cost = 8_u128 * 1_000_000_000;
    let mut image = rate_image(
        RateGenerator {
            first_pacing_time_ns: 11,
            pacing_interval_ns: 1,
            packet_size_bytes: 1,
            total_bytes: 1,
            rate_numerator_bits_per_second: 1,
            rate_denominator: 1,
            credit_quanta: packet_cost - 2,
        },
        GeneratorStatus::Blocked,
        10,
    );
    image.host_states[0].next_origin_seq = u64::MAX;

    validate(&image, Backend::Scalar)
        .expect("a blocked pacing deadline beyond stop cannot allocate a successor timer");
    let result = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("the beyond-stop timer must not execute");
    assert_eq!(result.host_states[0].next_origin_seq, u64::MAX);
}

#[test]
fn blocked_tick_at_stop_reserves_only_packets_it_can_emit() {
    let packet_cost = 8_u128 * 1_000_000_000;
    let mut image = rate_image(
        RateGenerator {
            first_pacing_time_ns: 10,
            pacing_interval_ns: 1,
            packet_size_bytes: 1,
            total_bytes: 1,
            rate_numerator_bits_per_second: 1,
            rate_denominator: 1,
            credit_quanta: packet_cost - 2,
        },
        GeneratorStatus::Blocked,
        10,
    );
    image.host_states[0].sourced_packets = u64::MAX;

    validate(&image, Backend::Scalar)
        .expect("the stop-time blocked tick cannot emit a packet before the horizon");
    let result = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("the stop-time blocked tick must stop without sourcing a packet");
    assert_eq!(result.summary.sourced_packets, 0);
    assert_eq!(
        result.host_states[0].generators[0].next_emission.status,
        GeneratorStatus::Stopped
    );
}

#[test]
fn scheduled_tick_at_stop_does_not_reserve_later_packets() {
    let mut image = rate_image(
        RateGenerator {
            first_pacing_time_ns: 10,
            pacing_interval_ns: 1,
            packet_size_bytes: 1,
            total_bytes: 2,
            rate_numerator_bits_per_second: 8_000_000_000,
            rate_denominator: 1,
            credit_quanta: 0,
        },
        GeneratorStatus::Scheduled,
        10,
    );
    image.host_states[0].sourced_packets = u64::MAX - 1;

    validate(&image, Backend::Scalar)
        .expect("only the executable stop-time packet must be reserved");
    let result = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("the one executable stop-time packet fits the counter boundary");
    assert_eq!(result.summary.sourced_packets, 1);
    assert_eq!(
        result.host_states[0].generators[0].next_emission.status,
        GeneratorStatus::Stopped
    );
}

#[test]
fn payload_capacity_counts_emissions_before_a_terminal_blocked_tick() {
    let packet_cost = 8_u128 * 1_000_000_000;
    let mut image = rate_image(
        RateGenerator {
            first_pacing_time_ns: 1,
            pacing_interval_ns: 1,
            packet_size_bytes: 1,
            total_bytes: 2,
            rate_numerator_bits_per_second: 1,
            rate_denominator: 1,
            credit_quanta: packet_cost - 1,
        },
        GeneratorStatus::Scheduled,
        2,
    );
    image.host_states[0].next_payload_seq = u64::MAX;

    let error = validate(&image, Backend::Scalar)
        .expect_err("the first emission allocates the token reused by the final blocked tick")
        .to_string();
    assert!(
        error.contains("payload identity sequence") && error.contains("reserving 1"),
        "expected the exact payload-capacity diagnostic, got: {error}"
    );
}

#[test]
fn exact_rational_credit_produces_remainder_cadence_and_status_transitions() {
    assert_eq!(
        event_fel_class(EventKind::PacingTimer),
        EventFelClass::FallbackHeap
    );
    let image = rate_image(
        RateGenerator {
            first_pacing_time_ns: 1,
            pacing_interval_ns: 1,
            packet_size_bytes: 1,
            total_bytes: 3,
            rate_numerator_bits_per_second: 10_000_000_000,
            rate_denominator: 3,
            credit_quanta: 0,
        },
        GeneratorStatus::Blocked,
        20,
    );
    validate(&image, Backend::Scalar).expect("canonical rational rate must validate");

    let blocked_prefix = run_scalar_with_observations(&image, Some(3), ObservationMode::Full)
        .expect("two blocked pacing ticks must execute");
    let (generator, rate) = rate_state(&blocked_prefix);
    assert_eq!(generator.next_emission.status, GeneratorStatus::Scheduled);
    assert_eq!(generator.next_emission.departure_time_ns, 3);
    assert_eq!(rate.credit_quanta, 20_000_000_000);
    assert_eq!(blocked_prefix.summary.sourced_packets, 0);

    let full = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("rate source must finish");
    let source_departures = full
        .departures
        .iter()
        .map(|departure| departure.time_ns)
        .collect::<Vec<_>>();
    assert_eq!(source_departures, vec![4, 6, 9]);
    assert_eq!(full.summary.sourced_packets, 3);
    assert_eq!(full.summary.sourced_bytes, 3);
    let (generator, rate) = rate_state(&full);
    assert_eq!(generator.next_emission.status, GeneratorStatus::Finished);
    assert_eq!(rate.credit_quanta, 8_000_000_000);
}

#[test]
fn rate_certificate_is_generated_byte_for_byte_by_the_scalar_oracle() {
    let image = rate_image(
        RateGenerator {
            first_pacing_time_ns: 1,
            pacing_interval_ns: 1,
            packet_size_bytes: 1,
            total_bytes: 2,
            rate_numerator_bits_per_second: 10_000_000_000,
            rate_denominator: 3,
            credit_quanta: 0,
        },
        GeneratorStatus::Blocked,
        20,
    );
    let result = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    let csv = rate_transitions_csv(
        &result
            .diagnostics
            .as_ref()
            .expect("full scalar observation retains diagnostics")
            .mechanism_transitions,
    )
    .unwrap();
    assert_eq!(
        csv,
        include_str!("../../lean/fixtures/p10c/rate_executor_accept.csv")
    );
}

#[test]
fn final_partial_packet_uses_its_exact_smaller_credit_cost() {
    let mut image = rate_image(
        RateGenerator {
            first_pacing_time_ns: 1,
            pacing_interval_ns: 1,
            packet_size_bytes: 3,
            total_bytes: 7,
            rate_numerator_bits_per_second: 12_000_000_000,
            rate_denominator: 1,
            credit_quanta: 0,
        },
        GeneratorStatus::Blocked,
        20,
    );
    image.channels[0] = RemoteChannel::for_packet_link(image.links[0], 1).unwrap();
    validate(&image, Backend::Scalar).expect("partial-final rate image must validate");

    let result = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("partial-final rate source must finish");
    let sizes = result
        .observed_packets
        .iter()
        .filter(|packet| packet.kind == PacketKind::Data)
        .map(|packet| packet.size_bytes)
        .collect::<Vec<_>>();

    assert_eq!(sizes, vec![3, 3, 1]);
    assert_eq!(result.summary.sourced_bytes, 7);
    assert_eq!(
        rate_state(&result).0.next_emission.status,
        GeneratorStatus::Finished
    );
}

#[test]
fn pacing_timer_linkage_rejects_missing_and_duplicate_events() {
    let image = rate_image(
        RateGenerator {
            first_pacing_time_ns: 1,
            pacing_interval_ns: 1,
            packet_size_bytes: 1,
            total_bytes: 1,
            rate_numerator_bits_per_second: 8_000_000_000,
            rate_denominator: 1,
            credit_quanta: 0,
        },
        GeneratorStatus::Scheduled,
        10,
    );

    let mut missing = image.clone();
    missing.initial_events.clear();
    missing.host_states[0].next_origin_seq = 0;
    assert_eq!(
        validate(&missing, Backend::Scalar)
            .expect_err("an active rate source must own a timer")
            .to_string(),
        "flow FlowId(0) rate source has 0 matching PacingTimer events; expected 1"
    );

    let mut duplicate = image;
    let mut second = duplicate.initial_events[0];
    second.key.origin_seq = 1;
    duplicate.initial_events.push(second);
    duplicate.host_states[0].next_origin_seq = 2;
    assert_eq!(
        validate(&duplicate, Backend::Scalar)
            .expect_err("a rate source must not own duplicate timers")
            .to_string(),
        "flow FlowId(0) rate source has 2 matching PacingTimer events; expected 1"
    );
}

#[test]
fn credit_boundary_and_representability_are_validated_exactly() {
    let packet_cost = 8_u128 * 1_000_000_000;
    let boundary = rate_image(
        RateGenerator {
            first_pacing_time_ns: 1,
            pacing_interval_ns: 1,
            packet_size_bytes: 1,
            total_bytes: 1,
            rate_numerator_bits_per_second: 1,
            rate_denominator: 1,
            credit_quanta: packet_cost - 1,
        },
        GeneratorStatus::Scheduled,
        10,
    );
    validate(&boundary, Backend::Scalar).expect("cost minus one is a legal credit boundary");

    let mut saturated = boundary;
    let FlowGeneratorKind::Rate(ref mut rate) = saturated.host_states[0].generators[0].kind else {
        unreachable!()
    };
    rate.credit_quanta = packet_cost;
    validate(&saturated, Backend::Scalar)
        .expect("credit at packet cost is a reachable carried-credit boundary");

    let mut next_tick_overflow = saturated;
    let FlowGeneratorKind::Rate(ref mut rate) =
        next_tick_overflow.host_states[0].generators[0].kind
    else {
        unreachable!()
    };
    rate.credit_quanta = u128::MAX;
    assert_eq!(
        validate(&next_tick_overflow, Backend::Scalar)
            .expect_err("the immediately produced next credit must remain representable")
            .to_string(),
        "flow FlowId(0) rate source next pacing credit exceeds u128"
    );

    let overflow = rate_image(
        RateGenerator {
            first_pacing_time_ns: 1,
            pacing_interval_ns: 1,
            packet_size_bytes: u64::MAX,
            total_bytes: u64::MAX,
            rate_numerator_bits_per_second: 1,
            rate_denominator: u64::MAX,
            credit_quanta: 0,
        },
        GeneratorStatus::Blocked,
        10,
    );
    assert_eq!(
        validate(&overflow, Backend::Scalar)
            .expect_err("unrepresentable packet credit must be rejected")
            .to_string(),
        "flow FlowId(0) rate source packet credit cost exceeds u128"
    );

    let denominator = u64::MAX;
    let scale = u128::from(denominator) * 1_000_000_000;
    let packet_size = u64::try_from(u128::MAX / (8 * scale)).unwrap();
    let accumulation_overflow = rate_image(
        RateGenerator {
            first_pacing_time_ns: 1,
            pacing_interval_ns: u64::MAX,
            packet_size_bytes: packet_size,
            total_bytes: packet_size,
            rate_numerator_bits_per_second: u64::MAX / 2,
            rate_denominator: denominator,
            credit_quanta: 0,
        },
        GeneratorStatus::Blocked,
        10,
    );
    assert_eq!(
        validate(&accumulation_overflow, Backend::Scalar)
            .expect_err("future credit accumulation must remain representable")
            .to_string(),
        "flow FlowId(0) rate source credit plus one tick can exceed u128"
    );
}

#[test]
fn checkpoint_accepts_credit_carried_into_a_smaller_final_packet() {
    let mut image = rate_image(
        RateGenerator {
            first_pacing_time_ns: 1,
            pacing_interval_ns: 1,
            packet_size_bytes: 3,
            total_bytes: 4,
            rate_numerator_bits_per_second: 24_000_000_000,
            rate_denominator: 1,
            credit_quanta: 8_000_000_000,
        },
        GeneratorStatus::Scheduled,
        10,
    );
    image.channels[0] = RemoteChannel::for_packet_link(image.links[0], 1).unwrap();
    validate(&image, Backend::Scalar).expect("reachable pre-emission state must validate");

    let prefix = run_scalar_with_observations(&image, Some(2), ObservationMode::Full)
        .expect("the first full-packet pacing tick must execute");
    let (generator, rate) = rate_state(&prefix);
    assert_eq!(generator.bytes_emitted, 3);
    assert_eq!(generator.next_emission.status, GeneratorStatus::Scheduled);
    assert_eq!(rate.credit_quanta, 8_000_000_000);

    validate(&checkpoint_image(&image, &prefix), Backend::Scalar)
        .expect("a produced checkpoint may carry exact credit for its smaller final packet");
}

#[test]
fn blocked_rate_source_reserves_every_pacing_tick_in_the_time_domain() {
    let image = rate_image(
        RateGenerator {
            first_pacing_time_ns: u64::MAX - 2,
            pacing_interval_ns: 3,
            packet_size_bytes: 1,
            total_bytes: 1,
            rate_numerator_bits_per_second: 1,
            rate_denominator: 1,
            credit_quanta: 0,
        },
        GeneratorStatus::Blocked,
        u64::MAX,
    );

    let error = validate(&image, Backend::Scalar)
        .expect_err("the blocked source's next pacing deadline exceeds u64")
        .to_string();
    assert!(
        error.contains("latest pacing time exceeds u64"),
        "expected a pacing-time closure diagnostic, got: {error}"
    );
}

#[test]
fn blocked_rate_token_is_reserved_exactly_once() {
    let mut image = rate_image(
        RateGenerator {
            first_pacing_time_ns: 1,
            pacing_interval_ns: 2,
            packet_size_bytes: 1,
            total_bytes: 1,
            rate_numerator_bits_per_second: 1,
            rate_denominator: 1,
            credit_quanta: 0,
        },
        GeneratorStatus::Blocked,
        10,
    );
    image.host_states[0].sourced_packets = u64::MAX - 1;

    validate(&image, Backend::Scalar).expect(
        "the resident Blocked pacing token is part of the generator's one remaining packet, not a second future packet",
    );
}

#[test]
fn time_zero_is_a_legal_rate_source_deadline() {
    let image = rate_image(
        RateGenerator {
            first_pacing_time_ns: 0,
            pacing_interval_ns: 1,
            packet_size_bytes: 1,
            total_bytes: 1,
            rate_numerator_bits_per_second: 8_000_000_000,
            rate_denominator: 1,
            credit_quanta: 0,
        },
        GeneratorStatus::Scheduled,
        10,
    );

    validate(&image, Backend::Scalar).expect("absolute pacing time zero is representable");
    let result = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("the time-zero source must execute");
    assert_eq!(result.summary.sourced_packets, 1);
    assert_eq!(
        rate_state(&result).0.next_emission.status,
        GeneratorStatus::Finished
    );
}

#[test]
fn final_partial_packet_rejects_an_unsafe_channel_delay_certificate() {
    let image = rate_image(
        RateGenerator {
            first_pacing_time_ns: 1,
            pacing_interval_ns: 1,
            packet_size_bytes: 3,
            total_bytes: 4,
            rate_numerator_bits_per_second: 24_000_000_000,
            rate_denominator: 1,
            credit_quanta: 8_000_000_000,
        },
        GeneratorStatus::Scheduled,
        10,
    );

    let error = validate(&image, Backend::Scalar)
        .expect_err("the one-byte final packet invalidates the nominal three-byte delay bound")
        .to_string();
    assert!(
        error.contains("min_delay_ns 3, exceeding derived bound 1"),
        "expected exact final-packet channel bound, got: {error}"
    );
}

#[test]
fn finished_rate_source_owns_no_timer_capacity() {
    let mut image = rate_image(
        RateGenerator {
            first_pacing_time_ns: 1,
            pacing_interval_ns: 1,
            packet_size_bytes: 1,
            total_bytes: 1,
            rate_numerator_bits_per_second: 8_000_000_000,
            rate_denominator: 1,
            credit_quanta: 0,
        },
        GeneratorStatus::Scheduled,
        10,
    );
    let generator = &mut image.host_states[0].generators[0];
    generator.packets_emitted = 1;
    generator.bytes_emitted = 1;
    generator.next_emission.status = GeneratorStatus::Finished;
    image.initial_packets.clear();
    image.initial_events.clear();
    image.host_states[0].next_origin_seq = 0;

    validate(&image, Backend::Scalar)
        .expect("a terminal rate source must need no packet or timer reservation");
    let result = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("a terminal rate source has no executable work");
    assert!(result.pending_events.is_empty());
    assert_eq!(result.summary.sourced_packets, 0);
}

#[test]
fn rate_checkpoint_and_cpu_worker_matrix_match_scalar() {
    let image = rate_image(
        RateGenerator {
            first_pacing_time_ns: 1,
            pacing_interval_ns: 1,
            packet_size_bytes: 1,
            total_bytes: 5,
            rate_numerator_bits_per_second: 10_000_000_000,
            rate_denominator: 3,
            credit_quanta: 0,
        },
        GeneratorStatus::Blocked,
        30,
    );
    let uninterrupted = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("uninterrupted rate run must execute");
    let prefix = run_scalar_with_observations(&image, Some(7), ObservationMode::Full)
        .expect("rate prefix must execute");
    let checkpoint = checkpoint_image(&image, &prefix);
    validate(&checkpoint, Backend::Scalar).expect("rate checkpoint must validate");
    let resumed = run_scalar_with_observations(&checkpoint, None, ObservationMode::Full)
        .expect("rate checkpoint must resume");

    assert_eq!(resumed.host_states, uninterrupted.host_states);
    assert_eq!(resumed.resident_packets, uninterrupted.resident_packets);
    assert_eq!(resumed.pending_events, uninterrupted.pending_events);

    for workers in [1, 2, 4] {
        validate(&image, Backend::Cpu { workers }).expect("CPU rate image must validate");
        let cpu = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .expect("CPU rate run must execute");
        assert_eq!(cpu.result, uninterrupted, "worker count {workers}");
    }
}

#[test]
fn device_backends_accept_rate_sources() {
    let image = rate_image(
        RateGenerator {
            first_pacing_time_ns: 1,
            pacing_interval_ns: 1,
            packet_size_bytes: 1,
            total_bytes: 1,
            rate_numerator_bits_per_second: 8_000_000_000,
            rate_denominator: 1,
            credit_quanta: 0,
        },
        GeneratorStatus::Scheduled,
        10,
    );

    for backend in [Backend::Metal, Backend::Cuda] {
        validate(&image, backend)
            .unwrap_or_else(|error| panic!("{backend} must accept rate sources: {error}"));
    }
}

#[cfg(any(
    feature = "cuda",
    all(feature = "metal-spike", target_vendor = "apple")
))]
fn adversarial_device_rate_images() -> Vec<SimulationImage> {
    let blocked = rate_image(
        RateGenerator {
            first_pacing_time_ns: 1,
            pacing_interval_ns: 1,
            packet_size_bytes: 1,
            total_bytes: 5,
            rate_numerator_bits_per_second: 10_000_000_000,
            rate_denominator: 3,
            credit_quanta: 0,
        },
        GeneratorStatus::Blocked,
        30,
    );
    let mut partial = rate_image(
        RateGenerator {
            first_pacing_time_ns: 1,
            pacing_interval_ns: 1,
            packet_size_bytes: 3,
            total_bytes: 4,
            rate_numerator_bits_per_second: 24_000_000_000,
            rate_denominator: 1,
            credit_quanta: 8_000_000_000,
        },
        GeneratorStatus::Scheduled,
        10,
    );
    partial.channels[0] = RemoteChannel::for_packet_link(partial.links[0], 1).unwrap();
    let time_zero = rate_image(
        RateGenerator {
            first_pacing_time_ns: 0,
            pacing_interval_ns: 2,
            packet_size_bytes: 2,
            total_bytes: 4,
            rate_numerator_bits_per_second: 8_000_000_000,
            rate_denominator: 1,
            credit_quanta: 0,
        },
        GeneratorStatus::Scheduled,
        10,
    );
    let prefix = run_scalar_with_observations(&blocked, Some(7), ObservationMode::Summary)
        .expect("rate checkpoint prefix must execute");
    let checkpoint = checkpoint_image(&blocked, &prefix);
    vec![blocked, partial, time_zero, checkpoint]
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[test]
fn metal_rate_pacing_and_checkpoints_match_scalar() {
    for (image_index, image) in adversarial_device_rate_images().into_iter().enumerate() {
        for horizon in [Some(2), None] {
            let expected =
                run_scalar_with_observations(&image, horizon, ObservationMode::Summary).unwrap();
            for streams_enabled in [true, false] {
                for round_threads_per_threadgroup in [32, 256] {
                    let actual = run_metal_with_observations(
                        &image,
                        horizon,
                        MetalConfig {
                            streams_enabled,
                            round_threads_per_threadgroup,
                            ..MetalConfig::default()
                        },
                        ObservationMode::Summary,
                    )
                    .unwrap_or_else(|error| {
                        panic!(
                            "image={image_index} horizon={horizon:?} streams={streams_enabled} geometry={round_threads_per_threadgroup}: {error}"
                        )
                    });
                    assert_eq!(actual.result, expected);
                }
            }
        }
    }
}

#[cfg(feature = "cuda")]
#[test]
fn cuda_rate_pacing_and_checkpoints_match_scalar() {
    for image in adversarial_device_rate_images() {
        for horizon in [Some(2), None] {
            let expected =
                run_scalar_with_observations(&image, horizon, ObservationMode::Summary).unwrap();
            for streams_enabled in [true, false] {
                for round_threads_per_block in [32, 256] {
                    let actual = run_cuda_with_observations(
                        &image,
                        horizon,
                        CudaConfig {
                            streams_enabled,
                            round_threads_per_block,
                            ..CudaConfig::default()
                        },
                        ObservationMode::Summary,
                    )
                    .unwrap_or_else(|error| {
                        panic!(
                            "horizon={horizon:?} streams={streams_enabled} geometry={round_threads_per_block}: {error}"
                        )
                    });
                    assert_eq!(actual.result, expected);
                }
            }
        }
    }
}

#[test]
fn current_global_horizon_has_zero_pacing_interval_effect() {
    let image = rate_image(
        RateGenerator {
            first_pacing_time_ns: 1,
            pacing_interval_ns: 3,
            packet_size_bytes: 3,
            total_bytes: 6,
            rate_numerator_bits_per_second: 8_000_000_000,
            rate_denominator: 1,
            credit_quanta: 0,
        },
        GeneratorStatus::Scheduled,
        20,
    );
    let mut image = image;
    image.links[0].rate_bps = 24_000_000_000;
    image.channels[0] = RemoteChannel::for_packet_link(image.links[0], 3)
        .expect("one-nanosecond channel delay must fit");
    validate(&image, Backend::Scalar).expect("horizon fixture must validate");
    assert_eq!(rate_source_lookahead(&image)[0].lower_bound_ns, 3);

    let run = run_scalar_rounds(&image, None).expect("round-mode rate run must execute");
    assert_eq!(image.channels[0].min_delay_ns, 1);
    assert_eq!(run.rounds[0].frontier_ns, 1);
    assert_eq!(run.rounds[0].exclusive_horizon_ns, 2);
    assert_eq!(run.rounds[0].horizon_advance_ns, 1);
    assert_eq!(
        run.rounds[0].horizon_advance_ns,
        u128::from(image.channels[0].min_delay_ns),
        "the current global horizon uses channel delay and gains zero from the 3 ns pacing bound"
    );

    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("paced lower-bound fixture must execute");
    let departure_times = scalar
        .departures
        .iter()
        .map(|departure| departure.time_ns)
        .collect::<Vec<_>>();
    assert_eq!(departure_times, vec![2, 5]);
    assert!(
        departure_times
            .windows(2)
            .all(|pair| pair[1] - pair[0] >= 3),
        "validated emissions must be separated by at least the pacing interval"
    );
}
