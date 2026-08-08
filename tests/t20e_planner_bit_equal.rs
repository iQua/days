//! The equality gate proves that the retained legacy planner and the precomputed planner produce
//! the same values. It is not a specification oracle: primitives shared by both paths can drift
//! without making this gate fail, so those primitives require focused independent tests.
#![cfg(all(
    feature = "test",
    any(
        feature = "cuda",
        feature = "cuda-planner-test",
        all(feature = "metal-spike", target_vendor = "apple")
    )
))]

use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};

use days::scenario::compile_config;
use days_executor::{
    ChannelStreamCapacityLevel, DeviceCapacityCaps, DeviceSizingReport, Event, EventKey, EventKind,
    FlowDescriptor, FlowGeneratorKind, FlowGeneratorState, FlowId, GeneratorFeedbackState,
    GeneratorStatus, HostState, LinkDescriptor, LinkId, NodeDescriptor, NodeId, NodeKind,
    ObservationMode, PacketDescriptor, PacketKind, PayloadId, RateGenerator, RemoteChannel,
    ScheduledEmission, SimulationImage, event_phase,
};
#[cfg(any(feature = "cuda", feature = "cuda-planner-test"))]
use days_executor::{
    CudaConfig, assert_cuda_planner_bit_equal_for_testing, size_cuda_plan_for_testing,
};
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
use days_executor::{
    MetalConfig, assert_metal_planner_bit_equal_for_testing, size_metal_plan_for_testing,
};
use tempfile::NamedTempFile;

const FIXTURES: &[&str] = &[
    // Constant/open-loop FIFO and exact-rate source pacing.
    "configs/benchmarks/baseline/fattree_k4_f8_st.toml",
    // Sustained closed-loop TCP and the short TCP corpus.
    "configs/benchmarks/tcp/fattree_k4_tcp_cubic_f16_smoke.toml",
    "configs/benchmarks/p11/rq9_closed_k16.toml",
    // Multi-cohort width-via-load planning.
    "configs/benchmarks/width_via_load/fattree_k32_target_w01000.toml",
];

const TEST_CAPS: DeviceCapacityCaps = DeviceCapacityCaps {
    fallback_fel_events_per_lp: Some(8),
    queue_packets_per_lp: Some(4),
    channel_events_per_stream: Some(2),
    remote_staging_events_per_lp: Some(2),
    outbox_events_total: Some(64),
    tcp_receiver_ranges_per_flow: Some(4),
    tcp_ledger_segments_per_flow: Some(4),
    observation_events_per_lp: Some(2),
};

fn fixture_path(relative: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn compile_fixture(relative: &str) -> SimulationImage {
    let path = fixture_path(relative);
    compile_config(&path)
        .unwrap_or_else(|error| panic!("failed to lower {}: {error}", path.display()))
}

fn compile_variant(name: &str, replacements: &[(&str, &str)]) -> SimulationImage {
    let source = fixture_path("configs/benchmarks/baseline/fattree_k4_f8_st.toml");
    let mut config = fs::read_to_string(&source)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", source.display()));
    for (from, to) in replacements {
        assert!(config.contains(from), "{name}: missing replacement {from}");
        config = config.replacen(from, to, 1);
    }
    let file = NamedTempFile::new().expect("temporary planner fixture must open");
    fs::write(file.path(), config).expect("temporary planner fixture must be written");
    compile_config(file.path()).unwrap_or_else(|error| panic!("failed to lower {name}: {error}"))
}

fn rate_image() -> SimulationImage {
    let source = NodeId(0);
    let sink = NodeId(1);
    let forward_id = LinkId(0);
    let reverse_id = LinkId(1);
    let flow = FlowId(0);
    let token = PayloadId(0);
    let rate = RateGenerator {
        first_pacing_time_ns: 0,
        pacing_interval_ns: 2,
        packet_size_bytes: 2,
        total_bytes: 4,
        rate_numerator_bits_per_second: 8_000_000_000,
        rate_denominator: 1,
        credit_quanta: 0,
    };
    let packet = PacketDescriptor {
        id: token,
        flow,
        size_bytes: rate.packet_size_bytes,
        ecn_marked: false,
        kind: PacketKind::Data,
    };
    let forward = LinkDescriptor {
        id: forward_id,
        source,
        target: sink,
        rate_bps: 8_000_000_000,
        propagation_ns: 0,
    };
    let empty_host = |egress_link| HostState {
        egress_link,
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
    let mut source_state = empty_host(forward_id);
    source_state.generators.push(FlowGeneratorState {
        flow,
        packets_emitted: 0,
        bytes_emitted: 0,
        next_emission: ScheduledEmission {
            status: GeneratorStatus::Scheduled,
            departure_time_ns: 0,
            payload: token,
        },
        rng_state: 25,
        feedback: GeneratorFeedbackState {
            arrivals: 0,
            outstanding_bytes: 0,
            unacknowledged_bytes: 0,
        },
        kind: FlowGeneratorKind::Rate(rate),
    });
    source_state.next_origin_seq = 1;
    source_state.next_payload_seq = 1;

    SimulationImage {
        stop_time_ns: 1_000,
        nodes: vec![
            NodeDescriptor {
                id: source,
                kind: NodeKind::Host,
                state_slot: 0,
            },
            NodeDescriptor {
                id: sink,
                kind: NodeKind::Host,
                state_slot: 1,
            },
        ],
        host_states: vec![source_state, empty_host(reverse_id)],
        switch_states: vec![],
        flows: vec![FlowDescriptor {
            id: flow,
            source,
            target: sink,
            priority: 0,
            route: vec![forward_id],
            reverse_route: vec![reverse_id],
        }],
        initial_packets: vec![packet],
        links: vec![
            forward,
            LinkDescriptor {
                id: reverse_id,
                source: sink,
                target: source,
                rate_bps: 8_000_000_000,
                propagation_ns: 0,
            },
        ],
        channels: vec![
            RemoteChannel::for_packet_link(forward, rate.packet_size_bytes)
                .expect("rate channel delay must fit"),
        ],
        initial_events: vec![Event {
            key: EventKey {
                time_ns: 0,
                phase: event_phase(EventKind::PacingTimer),
                origin_node: source,
                origin_seq: 0,
            },
            target: source,
            kind: EventKind::PacingTimer,
            payload: token,
        }],
        seed: 25,
    }
}

fn compile_dcqcn_rejection_fixture() -> SimulationImage {
    let source = fixture_path("configs/dcqcn_1s.toml");
    let mut config = fs::read_to_string(&source)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", source.display()));
    let buffer_capacity = "buffer_capacity = [0, 0, 0, 0, 0, 0, 0, 0]";
    assert!(config.contains(buffer_capacity));
    config = config.replacen(
        buffer_capacity,
        &format!(
            "{buffer_capacity}\nxoff = [0, 0, 0, 0, 0, 0, 0, 0]\nxon = [0, 0, 0, 0, 0, 0, 0, 0]"
        ),
        1,
    );
    let file = NamedTempFile::new().expect("temporary DCQCN fixture must open");
    fs::write(file.path(), config).expect("temporary DCQCN fixture must be written");
    compile_config(file.path())
        .unwrap_or_else(|error| panic!("failed to lower DCQCN rejection fixture: {error}"))
}

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
fn metal_config(streams_enabled: bool, capped: bool) -> MetalConfig {
    if !capped {
        return MetalConfig {
            streams_enabled,
            ..MetalConfig::default()
        };
    }
    MetalConfig {
        streams_enabled,
        capacity_caps: TEST_CAPS,
        max_fel_events_per_lp: Some(8),
        max_channel_events_per_stream: Some(2),
        max_queue_packets_per_lp: Some(4),
        max_outbox_events: None,
        max_observations: Some(2),
        ..MetalConfig::default()
    }
}

#[cfg(any(feature = "cuda", feature = "cuda-planner-test"))]
fn cuda_config(streams_enabled: bool, capped: bool) -> CudaConfig {
    if !capped {
        return CudaConfig {
            streams_enabled,
            ..CudaConfig::default()
        };
    }
    CudaConfig {
        streams_enabled,
        capacity_caps: TEST_CAPS,
        max_fel_events_per_lp: Some(8),
        max_channel_events_per_stream: Some(2),
        max_queue_packets_per_lp: Some(4),
        max_outbox_events: None,
        max_observations: Some(2),
        ..CudaConfig::default()
    }
}

fn assert_planners_equal(
    label: &str,
    image: &SimulationImage,
    streams_enabled: bool,
    observation_mode: ObservationMode,
    capped: bool,
) {
    #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
    assert_metal_planner_bit_equal_for_testing(
        image,
        None,
        metal_config(streams_enabled, capped),
        observation_mode,
    )
    .unwrap_or_else(|error| panic!("Metal planner differs for {label}: {error}"));

    #[cfg(any(feature = "cuda", feature = "cuda-planner-test"))]
    assert_cuda_planner_bit_equal_for_testing(
        image,
        None,
        cuda_config(streams_enabled, capped),
        observation_mode,
    )
    .unwrap_or_else(|error| panic!("CUDA planner differs for {label}: {error}"));
}

#[test]
fn device_planners_are_bit_equal_to_legacy_planning_across_fixture_families() {
    for relative in FIXTURES {
        let image = compile_fixture(relative);
        let capped = *relative != "configs/benchmarks/baseline/fattree_k4_f8_st.toml";
        assert_planners_equal(relative, &image, true, ObservationMode::Full, capped);
    }

    // SP, WRR, and accepted ECN-threshold policy variants use the same small config-backed image.
    for (name, image) in [
        (
            "deficit-round-robin",
            compile_variant(
                "deficit-round-robin",
                &[("discipline = \"FIFO\"", "discipline = \"DRR\"")],
            ),
        ),
        (
            "weighted-fair-queue",
            compile_variant(
                "weighted-fair-queue",
                &[("discipline = \"FIFO\"", "discipline = \"WFQ\"")],
            ),
        ),
        (
            "static-priority",
            compile_variant(
                "static-priority",
                &[("discipline = \"FIFO\"", "discipline = \"SP\"")],
            ),
        ),
        (
            "weighted-round-robin",
            compile_variant(
                "weighted-round-robin",
                &[("discipline = \"FIFO\"", "discipline = \"WRR\"")],
            ),
        ),
        (
            "ecn-threshold",
            compile_variant(
                "ecn-threshold",
                &[(
                    "drop = \"TailDrop\"",
                    "drop = \"ECN_THRESHOLD\"\necn_threshold = 0.5",
                )],
            ),
        ),
    ] {
        assert_planners_equal(name, &image, true, ObservationMode::Full, false);
    }

    assert_planners_equal(
        "rate-generator",
        &rate_image(),
        true,
        ObservationMode::Full,
        false,
    );
}

fn assert_0695_initial_plan(
    backend: &str,
    image: &SimulationImage,
    strict: DeviceSizingReport,
    retry_enabled: DeviceSizingReport,
    expected_total_device_bytes: usize,
) {
    const EVENT_WORDS_AT_0695F02: usize = 14;

    assert_eq!(
        retry_enabled, strict,
        "{backend} retry state must be inert before a fault"
    );
    assert_eq!(
        strict.channel_stream_capacity_distribution,
        vec![ChannelStreamCapacityLevel {
            capacity: 2,
            stream_count: image.channels.len(),
        }],
        "the plane-wide cap remains the starting capacity for every stream",
    );
    let stream_slots = strict.event_arenas.channel_stream_event_slots
        + strict.event_arenas.service_stream_event_slots
        + strict.event_arenas.generator_stream_event_slots;
    let stream_records = strict
        .planes
        .iter()
        .find(|plane| plane.name == "stream_records")
        .expect("stream-record plane must exist");
    assert_eq!(stream_records.words, stream_slots * EVENT_WORDS_AT_0695F02);
    assert_eq!(strict.total_device_bytes, expected_total_device_bytes);
}

#[test]
fn channel_starting_cap_preserves_the_0695f02_initial_plan_bytes() {
    let image = compile_fixture("configs/benchmarks/baseline/fattree_k4_f8_st.toml");
    let capacity_caps = DeviceCapacityCaps {
        channel_events_per_stream: Some(2),
        ..DeviceCapacityCaps::default()
    };

    #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
    assert_0695_initial_plan(
        "Metal",
        &image,
        size_metal_plan_for_testing(
            &image,
            None,
            MetalConfig {
                capacity_caps,
                max_capacity_retries: 0,
                ..MetalConfig::default()
            },
            ObservationMode::Summary,
        )
        .expect("strict Metal initial capped plan must size"),
        size_metal_plan_for_testing(
            &image,
            None,
            MetalConfig {
                capacity_caps,
                max_capacity_retries: 16,
                ..MetalConfig::default()
            },
            ObservationMode::Summary,
        )
        .expect("retry-enabled Metal initial capped plan must size"),
        // T20i widened the per-flow TCP ledger metadata row from 4 words to 6 (ring head +
        // occupancy high-water). This fixture has 8 TCP flows, so the anchor moves by exactly
        // 8 * 2 * 8 = 128 B. Nothing else in the plan changed.
        //
        // T21 stopped allocating the legacy exchange outbox in a streams-enabled plan, which is
        // what `MetalConfig::default()` selects here. The plane held 54,432 words (435,456 B) and
        // now holds one record (112 B), so the anchor moves by exactly -435,344 B. No capacity
        // number moved with it: `P_OUTBOX_CAPACITY` is still the derived 54,432, so this plan
        // faults and grows exactly where it did before. Nothing else in the plan changed.
        1_269_928 + 128 - 435_344,
    );

    #[cfg(any(feature = "cuda", feature = "cuda-planner-test"))]
    assert_0695_initial_plan(
        "CUDA",
        &image,
        size_cuda_plan_for_testing(
            &image,
            None,
            CudaConfig {
                capacity_caps,
                max_capacity_retries: 0,
                ..CudaConfig::default()
            },
            ObservationMode::Summary,
        )
        .expect("strict CUDA initial capped plan must size"),
        size_cuda_plan_for_testing(
            &image,
            None,
            CudaConfig {
                capacity_caps,
                max_capacity_retries: 16,
                ..CudaConfig::default()
            },
            ObservationMode::Summary,
        )
        .expect("retry-enabled CUDA initial capped plan must size"),
        // T20i ledger-metadata widening: 8 flows * 2 words * 8 B = 128 B. T21 streams-path outbox
        // elision: -435,344 B. See the Metal anchor for both.
        1_269_904 + 128 - 435_344,
    );
}

/// T21: the legacy exchange outbox is storage only the legacy exchange path can address.
///
/// `days_exchange_prefix` (compaction offsets), `days_exchange_scatter` (the copy in) and
/// `days_exchange_merge` (the drain out) are the only kernels that index the plane, and on both
/// backends each of those accesses is inside a branch that has already tested
/// `params[P_STREAMS_ENABLED] == 0`. A streams-enabled plan therefore allocates one record; a
/// streams-disabled plan still allocates the whole derived arena. `P_OUTBOX_CAPACITY` — which the
/// streams path *does* read, to bound a round's remote production — is derived and uploaded
/// identically in both cases, so this is a storage change and not a capacity change.
#[test]
fn streams_enabled_plans_carry_one_outbox_record_and_legacy_plans_carry_the_arena() {
    const ONE_RECORD_WORDS: usize = 14;

    for relative in FIXTURES {
        let image = compile_fixture(relative);

        #[cfg(any(feature = "cuda", feature = "cuda-planner-test"))]
        {
            let streams = size_cuda_plan_for_testing(
                &image,
                None,
                cuda_config(true, false),
                ObservationMode::Summary,
            )
            .expect("streams-enabled CUDA plan must size");
            let legacy = size_cuda_plan_for_testing(
                &image,
                None,
                cuda_config(false, false),
                ObservationMode::Summary,
            )
            .expect("legacy-exchange CUDA plan must size");
            assert_outbox_partition("CUDA", relative, &streams, &legacy, ONE_RECORD_WORDS);
        }

        #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
        {
            let streams = size_metal_plan_for_testing(
                &image,
                None,
                metal_config(true, false),
                ObservationMode::Summary,
            )
            .expect("streams-enabled Metal plan must size");
            let legacy = size_metal_plan_for_testing(
                &image,
                None,
                metal_config(false, false),
                ObservationMode::Summary,
            )
            .expect("legacy-exchange Metal plan must size");
            assert_outbox_partition("Metal", relative, &streams, &legacy, ONE_RECORD_WORDS);
        }
    }
}

#[cfg(any(
    feature = "cuda",
    feature = "cuda-planner-test",
    all(feature = "metal-spike", target_vendor = "apple")
))]
fn assert_outbox_partition(
    backend: &str,
    fixture: &str,
    streams: &DeviceSizingReport,
    legacy: &DeviceSizingReport,
    one_record_words: usize,
) {
    let plane = |report: &DeviceSizingReport, name: &str| {
        report
            .planes
            .iter()
            .find(|plane| plane.name == name)
            .unwrap_or_else(|| panic!("{name} plane must exist"))
            .words
    };
    let streams_outbox = plane(streams, "outbox");
    let legacy_outbox = plane(legacy, "outbox");

    assert_eq!(
        streams_outbox, one_record_words,
        "{backend} {fixture}: a streams-enabled plan must carry one outbox record"
    );
    assert!(
        legacy_outbox > streams_outbox,
        "{backend} {fixture}: the legacy exchange path must keep its whole outbox arena \
         (streams {streams_outbox} words, legacy {legacy_outbox} words)"
    );
    // The derived outbox capacity — what both paths upload as `P_OUTBOX_CAPACITY` — is the sum of
    // the per-producer remote-staging capacities, so uncapped the legacy arena is exactly the
    // `remote_staging` plane. Both plans size `remote_staging` identically, which is the visible
    // consequence of the capacity number being untouched: only the storage moved.
    assert_eq!(
        legacy_outbox,
        plane(legacy, "remote_staging"),
        "{backend} {fixture}: the uncapped legacy outbox arena is the remote-staging arena"
    );
    assert_eq!(
        plane(streams, "remote_staging"),
        plane(legacy, "remote_staging"),
        "{backend} {fixture}: the derived remote capacity must not depend on the exchange path"
    );
}

#[test]
fn device_planner_equality_covers_stream_and_observation_modes() {
    let image = compile_fixture("configs/benchmarks/baseline/fattree_k4_f8_st.toml");
    for streams_enabled in [false, true] {
        for observation_mode in [ObservationMode::Summary, ObservationMode::Full] {
            assert_planners_equal(
                "mode-matrix",
                &image,
                streams_enabled,
                observation_mode,
                true,
            );
        }
    }
}

#[test]
fn unsupported_device_families_are_rejected_before_planning() {
    let mut planner_rejections = 0;
    for relative in ["configs/collective_tcp.toml", "configs/pfc.toml"] {
        let path = fixture_path(relative);
        let image = match compile_config(Path::new(&path)) {
            Ok(image) => image,
            Err(error) => {
                assert!(
                    matches!(relative, "configs/collective_tcp.toml" | "configs/pfc.toml"),
                    "unexpected lowering rejection for {}: {error}",
                    path.display()
                );
                continue;
            }
        };
        planner_rejections += 1;

        #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
        assert!(
            assert_metal_planner_bit_equal_for_testing(
                &image,
                None,
                metal_config(true, true),
                ObservationMode::Summary,
            )
            .is_err(),
            "Metal must reject {} before planning",
            path.display()
        );

        #[cfg(any(feature = "cuda", feature = "cuda-planner-test"))]
        assert!(
            assert_cuda_planner_bit_equal_for_testing(
                &image,
                None,
                cuda_config(true, true),
                ObservationMode::Summary,
            )
            .is_err(),
            "CUDA must reject {} before planning",
            path.display()
        );
    }

    let dcqcn = compile_dcqcn_rejection_fixture();
    planner_rejections += 1;

    #[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
    assert!(
        assert_metal_planner_bit_equal_for_testing(
            &dcqcn,
            None,
            metal_config(true, true),
            ObservationMode::Summary,
        )
        .is_err(),
        "Metal must reject DCQCN before planning"
    );

    #[cfg(any(feature = "cuda", feature = "cuda-planner-test"))]
    assert!(
        assert_cuda_planner_bit_equal_for_testing(
            &dcqcn,
            None,
            cuda_config(true, true),
            ObservationMode::Summary,
        )
        .is_err(),
        "CUDA must reject DCQCN before planning"
    );

    assert_ne!(
        planner_rejections, 0,
        "device rejection branch must execute"
    );
}
