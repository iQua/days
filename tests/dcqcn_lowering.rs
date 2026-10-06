use std::fs;

use days::scenario::compile_config;
use days_executor::{
    Backend, DcqcnController, EventKind, FlowGeneratorKind, GeneratorStatus,
    MechanismTransitionRecord, ObservationMode, PacketKind, dcqcn_transitions_csv,
    run_cpu_with_observations, run_scalar_with_observations, validate,
};
use tempfile::TempDir;

fn write_dcqcn_config(directory: &TempDir) -> std::path::PathBuf {
    let path = directory.path().join("dcqcn.toml");
    fs::write(
        &path,
        r#"
seed = 26
duration = 0.0005
edges = [[0, 1]]
hosts = [0, 1]

[switch]
port_rate = 100_000_000_000
capacity = 1
discipline = "FIFO"
drop = "ECN_THRESHOLD"
ecn_threshold = 1.0

[link]
propagation_ns = 100

[[flow]]
flow_type = "DCQCN"
priority = 0
graph = [[0, 1]]

[flow.traffic]
initial_delay = 0.0
size = 20_000
arr_dist = { type = "Uniform", low = 1, high = 1 }
pkt_size_dist = { type = "DiscreteUniform", low = 1000, high = 1000 }

[flow.traffic.dcqcn]
rate_gbps = 10.0
min_rate_gbps = 1.0
max_rate_gbps = 20.0
g = 0.5
ai_rate_gbps = 0.5
hai_rate_gbps = 1.0
rp_timer_ns = 100000
cnp_interval_ns = 10000
pacing_interval_ns = 1000
cnp_priority = 0
"#,
    )
    .unwrap();
    path
}

fn write_dcqcn_pfc_config(directory: &TempDir) -> std::path::PathBuf {
    let path = write_dcqcn_config(directory);
    let config = fs::read_to_string(&path).unwrap().replace(
        "[link]\npropagation_ns = 100",
        r#"[link]
mode = "Pfc"
propagation_ns = 100

[link.pfc]
xoff = [1000, 0, 0, 0, 0, 0, 0, 0]
xon = [500, 0, 0, 0, 0, 0, 0, 0]
pause_quanta = [1, 0, 0, 0, 0, 0, 0, 0]
buffer_capacity = [20000, 0, 0, 0, 0, 0, 0, 0]"#,
    );
    fs::write(&path, config).unwrap();
    path
}

fn dcqcn_image() -> days_executor::SimulationImage {
    let directory = TempDir::new().unwrap();
    compile_config(write_dcqcn_config(&directory)).unwrap()
}

fn compile_dcqcn_with_rates(
    initial: &str,
    minimum: &str,
    maximum: &str,
) -> Result<days_executor::SimulationImage, days::scenario::CompileError> {
    let directory = TempDir::new().unwrap();
    let path = write_dcqcn_config(&directory);
    let config = fs::read_to_string(&path)
        .unwrap()
        .replace("rate_gbps = 10.0", &format!("rate_gbps = {initial}"))
        .replace("min_rate_gbps = 1.0", &format!("min_rate_gbps = {minimum}"))
        .replace(
            "max_rate_gbps = 20.0",
            &format!("max_rate_gbps = {maximum}"),
        );
    fs::write(&path, config).unwrap();
    compile_config(path)
}

fn fix_dcqcn_rate(image: &mut days_executor::SimulationImage) {
    let FlowGeneratorKind::Dcqcn(mut dcqcn) = image.host_states[0].generators[0].kind else {
        unreachable!()
    };
    dcqcn.controller.config.minimum_rate_bps = dcqcn.controller.current_rate_bps;
    dcqcn.controller.config.maximum_rate_bps = dcqcn.controller.current_rate_bps;
    dcqcn.rate.rate_numerator_bits_per_second = dcqcn.controller.current_rate_bps;
    image.host_states[0].generators[0].kind = FlowGeneratorKind::Dcqcn(dcqcn);
}

fn finish_dcqcn_data(image: &mut days_executor::SimulationImage) {
    let state = &mut image.host_states[0];
    let generator = &mut state.generators[0];
    let FlowGeneratorKind::Dcqcn(dcqcn) = generator.kind else {
        unreachable!()
    };
    generator.packets_emitted = dcqcn.rate.total_bytes / dcqcn.rate.packet_size_bytes;
    generator.bytes_emitted = dcqcn.rate.total_bytes;
    generator.next_emission.status = GeneratorStatus::Finished;
    state.next_payload_seq = generator.packets_emitted + 1;
    let pacing_payload = generator.next_emission.payload;
    image
        .initial_events
        .retain(|event| event.payload != pacing_payload);
    image
        .initial_packets
        .retain(|packet| packet.id != pacing_payload);
}

fn finish_dcqcn_with_ce_in_flight(image: &mut days_executor::SimulationImage) {
    let flow = image.flows[0].clone();
    let pacing_payload = image.host_states[0].generators[0].next_emission.payload;
    let mut data = image
        .initial_packets
        .iter()
        .find(|packet| packet.id == pacing_payload)
        .copied()
        .unwrap();
    let mut arrival = image
        .initial_events
        .iter()
        .find(|event| event.payload == pacing_payload)
        .copied()
        .unwrap();
    finish_dcqcn_data(image);
    data.ecn_marked = true;
    arrival.kind = EventKind::RemoteArrival;
    arrival.target = flow.target;
    let arrival_origin = image.links[flow.route.last().unwrap().0 as usize].source;
    arrival.key.origin_node = arrival_origin;
    arrival.key.origin_seq = 0;
    arrival.key.time_ns = 1;
    arrival.key.phase = 0;
    let origin = image.nodes[arrival_origin.0 as usize];
    match origin.kind {
        days_executor::NodeKind::Host => {
            image.host_states[origin.state_slot as usize].next_origin_seq = 1;
        }
        days_executor::NodeKind::Switch => {
            image.switch_states[origin.state_slot as usize].next_origin_seq = 1;
        }
    }
    image.initial_packets.push(data);
    image.initial_packets.sort_by_key(|packet| packet.id);
    image.initial_events.push(arrival);
    image.initial_events.sort_by_key(|event| event.key);
}

fn blocked_dcqcn_service_boundary() -> days_executor::SimulationImage {
    const DUPLICATED_TOKEN_DELAY_NS: u64 = 922_337_203_685_452_262;
    const NON_PRIMARY_PER_PACKET_DELAY_NS: u64 = 318;

    let mut image = dcqcn_image();
    let generator = &mut image.host_states[0].generators[0];
    let FlowGeneratorKind::Dcqcn(mut dcqcn) = generator.kind else {
        unreachable!()
    };
    dcqcn.rate.pacing_interval_ns = 1;
    generator.next_emission.status = GeneratorStatus::Blocked;
    generator.kind = FlowGeneratorKind::Dcqcn(dcqcn);

    let flow = image.flows[0].clone();
    for link in &mut image.links {
        link.rate_bps = u64::MAX;
        link.propagation_ns = 0;
    }
    let primary = flow.route[0];
    let primary_serialization = image.links[primary.0 as usize]
        .delay_ns(dcqcn.rate.packet_size_bytes)
        .unwrap();
    image.links[primary.0 as usize].propagation_ns =
        DUPLICATED_TOKEN_DELAY_NS - primary_serialization;

    let non_primary_links = flow.route.len() - 1 + flow.reverse_route.len();
    let adjusted_reverse = flow.reverse_route[0];
    let adjusted_delay =
        NON_PRIMARY_PER_PACKET_DELAY_NS - u64::try_from(non_primary_links - 1).unwrap();
    let adjusted_serialization = image.links[adjusted_reverse.0 as usize]
        .delay_ns(dcqcn.cnp_size_bytes)
        .unwrap();
    image.links[adjusted_reverse.0 as usize].propagation_ns =
        adjusted_delay - adjusted_serialization;

    for channel in &mut image.channels {
        let minimum_size = if flow.route.contains(&channel.link) {
            dcqcn.rate.packet_size_bytes
        } else {
            dcqcn.cnp_size_bytes
        };
        channel.min_delay_ns = image.links[channel.link.0 as usize]
            .delay_ns(minimum_size)
            .unwrap();
    }
    image
}

#[test]
fn dcqcn_lowers_to_rate_controller_receiver_and_one_pacing_event() {
    let directory = TempDir::new().unwrap();
    let image = compile_config(write_dcqcn_config(&directory)).unwrap();
    let generator = &image.host_states[0].generators[0];
    let FlowGeneratorKind::Dcqcn(dcqcn) = generator.kind else {
        panic!("expected exact DCQCN generator")
    };
    assert_eq!(dcqcn.rate.rate_numerator_bits_per_second, 10_000_000_000);
    let config = dcqcn.controller.config;
    // g = 0.5 in Q63; the Mellanox-form timers at their SimAI defaults but the RP timer.
    assert_eq!(config.g_q63, 1 << 62);
    assert_eq!(
        (
            config.alpha_interval_ns,
            config.decrease_interval_ns,
            config.increase_interval_ns,
            config.fast_recovery_steps,
            config.clamp_target_rate
        ),
        (1_000, 4_000, 100_000, 1, false)
    );
    assert_eq!(dcqcn.controller, DcqcnController::pristine(config));
    assert_eq!(image.host_states[1].dcqcn_receivers.len(), 1);
    assert_eq!(
        image.host_states[1].dcqcn_receivers[0].cnp_interval_ns,
        10_000
    );
    // The controller owns no token and no event (P16 ruling D2): one pacing tick per flow.
    assert!(
        image
            .initial_packets
            .iter()
            .all(|packet| !packet.kind.is_timer_token())
    );
    assert_eq!(
        image
            .initial_events
            .iter()
            .filter(|event| event.kind == days_executor::EventKind::PacingTimer)
            .count(),
        1
    );
}

#[test]
fn dcqcn_decimal_rates_lower_losslessly_through_u64_max() {
    let above_binary64_integer = compile_dcqcn_with_rates(
        "9007199.254740993",
        "9007199.254740993",
        "9007199.254740993",
    )
    .expect("2^53 + 1 bps is exactly representable in the image");
    let FlowGeneratorKind::Dcqcn(dcqcn) = above_binary64_integer.host_states[0].generators[0].kind
    else {
        unreachable!()
    };
    assert_eq!(
        dcqcn.rate.rate_numerator_bits_per_second,
        9_007_199_254_740_993
    );

    let exact_max = compile_dcqcn_with_rates(
        "18446744073.709551615",
        "18446744073.709551615",
        "18446744073.709551615",
    )
    .expect("the exact u64::MAX bps decimal must lower");
    let FlowGeneratorKind::Dcqcn(dcqcn) = exact_max.host_states[0].generators[0].kind else {
        unreachable!()
    };
    assert_eq!(dcqcn.rate.rate_numerator_bits_per_second, u64::MAX);

    let one_past = compile_dcqcn_with_rates(
        "18446744073.709551616",
        "18446744073.709551616",
        "18446744073.709551616",
    )
    .expect_err("one bps past u64::MAX must reject");
    assert!(one_past.to_string().contains("u64"), "{one_past}");

    let nonrepresentable = compile_dcqcn_with_rates("0.0000000001", "0", "1")
        .expect_err("a tenth of one bps is not exactly representable");
    assert!(
        nonrepresentable
            .to_string()
            .contains("exact representation"),
        "{nonrepresentable}"
    );
}

#[test]
fn blocked_dcqcn_pacing_token_is_counted_once_at_service_boundaries() {
    let below_max = blocked_dcqcn_service_boundary();
    validate(&below_max, Backend::Scalar)
        .expect("the review fixture's logical service bound fits below u64::MAX");

    // With no controller timer (P16 ruling D2), the latest departure is the last pacing tick
    // (159,999 ns) rather than the last control tick, so the exact boundary moved: the reverse
    // link may take exactly 17,000 ns more per CNP before the service bound passes u64::MAX
    // (bisected when this test was re-pinned).
    let mut exact = below_max;
    let reverse = exact.flows[0].reverse_route[0];
    exact.links[reverse.0 as usize].propagation_ns += 17_000;
    exact
        .channels
        .iter_mut()
        .find(|channel| channel.link == reverse)
        .expect("reverse channel exists")
        .min_delay_ns += 17_000;
    validate(&exact, Backend::Scalar).expect("the exact u64 service-time boundary must validate");

    let mut one_past = exact;
    let reverse = one_past.flows[0].reverse_route[0];
    one_past.links[reverse.0 as usize].propagation_ns += 1;
    one_past
        .channels
        .iter_mut()
        .find(|channel| channel.link == reverse)
        .expect("reverse channel exists")
        .min_delay_ns += 1;
    let error = validate(&one_past, Backend::Scalar)
        .expect_err("one additional nanosecond per CNP is genuinely beyond u64")
        .to_string();
    assert!(error.contains("conservative service bound"), "{error}");
}

#[test]
fn canonical_dcqcn_cnp_checkpoint_keeps_validating() {
    let mut image = dcqcn_image();
    let flow = image.flows[0].clone();
    let target = image.nodes[flow.target.0 as usize];
    let node_count = image.nodes.len() as u64;
    let state = &mut image.host_states[target.state_slot as usize];
    let cnp_payload = days_executor::PayloadId(target.id.0 + node_count * state.next_payload_seq);
    state.next_payload_seq += 1;
    let origin_seq = state.next_origin_seq;
    state.next_origin_seq += 1;
    state.sourced_packets += 1;
    state.received_packets += 1;
    state.queue.push_back(cnp_payload);
    state.tx_ready_pending = true;
    state.dcqcn_receivers[0].last_cnp_time_ns = Some(1);
    let trigger_payload = image.host_states[0].generators[0].next_emission.payload;
    image.initial_packets.push(days_executor::PacketDescriptor {
        id: cnp_payload,
        flow: flow.id,
        size_bytes: 64,
        ecn_marked: false,
        kind: PacketKind::DcqcnCnp(days_executor::DcqcnCnpHeader { trigger_payload }),
    });
    image.initial_packets.sort_by_key(|packet| packet.id);
    image.initial_events.push(days_executor::Event {
        key: days_executor::EventKey {
            time_ns: 1,
            phase: days_executor::event_phase(EventKind::TxReady),
            origin_node: target.id,
            origin_seq,
        },
        target: target.id,
        kind: EventKind::TxReady,
        payload: cnp_payload,
    });
    image.initial_events.sort_by_key(|event| event.key);

    validate(&image, Backend::Scalar).expect("a canonical 64-byte NotECT CNP checkpoint is legal");
    validate(&image, Backend::Cpu { workers: 2 })
        .expect("the canonical CNP checkpoint is legal on CPU");

    let mut undersized = image;
    undersized
        .initial_packets
        .iter_mut()
        .find(|packet| packet.id == cnp_payload)
        .expect("CNP remains resident")
        .size_bytes = 63;
    let error = validate(&undersized, Backend::Scalar)
        .expect_err("a 63-byte CNP violates the exact wire contract")
        .to_string();
    assert!(error.contains("exact 64-byte NotECT"), "{error}");
}

#[test]
fn ecn_marking_generates_cnp_and_exact_scalar_cpu_controller_trajectory() {
    let directory = TempDir::new().unwrap();
    let image = compile_config(write_dcqcn_config(&directory)).unwrap();
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full).unwrap();
    assert!(
        scalar
            .observed_packets
            .iter()
            .any(|packet| matches!(packet.kind, PacketKind::DcqcnCnp(_)))
    );
    assert!(
        scalar
            .observed_packets
            .iter()
            .any(|packet| packet.ecn_marked)
    );
    let diagnostics = scalar
        .diagnostics
        .as_ref()
        .expect("full scalar observation retains diagnostics");
    let dcqcn_records = diagnostics
        .mechanism_transitions
        .iter()
        .filter_map(|record| match record {
            MechanismTransitionRecord::Dcqcn(record) => Some(record),
            _ => None,
        })
        .collect::<Vec<_>>();
    let csv = dcqcn_transitions_csv(&diagnostics.mechanism_transitions).unwrap();
    let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("lean/fixtures/p10c/dcqcn_executor_trace_accept.csv");
    if std::env::var_os("DAYS_UPDATE_DCQCN_TRACE_FIXTURE").is_some() {
        fs::write(&fixture_path, &csv).unwrap();
    }
    assert_eq!(csv, fs::read_to_string(fixture_path).unwrap());
    // Feedback rows from the CNPs, a tick row per pacing tick (ruling D17), and at least one cut.
    assert!(
        dcqcn_records
            .iter()
            .any(|record| record.kind == days_executor::DcqcnTransitionKind::Feedback)
    );
    assert!(
        dcqcn_records
            .iter()
            .any(|record| record.kind == days_executor::DcqcnTransitionKind::Tick)
    );
    assert!(
        dcqcn_records
            .iter()
            .any(|record| record.advance.decrease_cuts != 0)
    );
    assert!(dcqcn_records.iter().any(|record| record.frozen));

    for workers in [1, 2, 4] {
        let cpu = run_cpu_with_observations(
            &image,
            None,
            days_executor::CpuConfig {
                workers,
                ..days_executor::CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap();
        assert_eq!(cpu.result, scalar, "worker count {workers}");
    }
}

#[test]
fn terminal_dcqcn_checkpoint_revalidates() {
    let directory = TempDir::new().unwrap();
    let image = compile_config(write_dcqcn_config(&directory)).unwrap();
    let result =
        run_scalar_with_observations(&image, Some(250_000), ObservationMode::Full).unwrap();
    let checkpoint = days_executor::SimulationImage {
        stop_time_ns: image.stop_time_ns,
        nodes: image.nodes.clone(),
        host_states: result.host_states,
        switch_states: result.switch_states,
        flows: image.flows.clone(),
        initial_packets: result.resident_packets,
        links: image.links.clone(),
        channels: image.channels.clone(),
        initial_events: result.pending_events,
        seed: image.seed,
        stage_joins: image.stage_joins.clone(),
    };
    validate(&checkpoint, Backend::Scalar).unwrap();
    validate(&checkpoint, Backend::Cpu { workers: 4 }).unwrap();
}

#[test]
fn nonrepresentable_dcqcn_parameters_and_cnp_priority_are_rejected() {
    let directory = TempDir::new().unwrap();
    let path = write_dcqcn_config(&directory);
    let base = fs::read_to_string(&path).unwrap();

    // g lowers exactly to Q63 where it is dyadic (1/1024 failed to lower in ppb in P15), is
    // rounded half to even otherwise, and is refused outside 0..=1 or beyond 38 decimal places.
    fs::write(&path, base.replace("g = 0.5", "g = 0.0009765625")).unwrap();
    let image = compile_config(&path).unwrap();
    let FlowGeneratorKind::Dcqcn(dcqcn) = image.host_states[0].generators[0].kind else {
        unreachable!()
    };
    assert_eq!(dcqcn.controller.config.g_q63, 1 << 53);
    fs::write(&path, base.replace("g = 0.5", "g = 0.001")).unwrap();
    let image = compile_config(&path).unwrap();
    let FlowGeneratorKind::Dcqcn(dcqcn) = image.host_states[0].generators[0].kind else {
        unreachable!()
    };
    // round_half_even(2^63 / 1000) = 9,223,372,036,854,775.808 -> ...776.
    assert_eq!(dcqcn.controller.config.g_q63, 9_223_372_036_854_776);
    for (g, expected) in [
        ("1.5", "0..=1"),
        ("-0.25", "0..=1"),
        (
            "0.000000000000000000000000000000000000001",
            "38 decimal places",
        ),
    ] {
        fs::write(&path, base.replace("g = 0.5", &format!("g = {g}"))).unwrap();
        let error = compile_config(&path).unwrap_err().to_string();
        assert!(error.contains(expected), "g = {g}: {error}");
    }
    for (key, value) in [
        ("mi_factor", "0.5"),
        ("rtt_ns", "100000"),
        ("increase_byte_threshold", "1000"),
    ] {
        fs::write(
            &path,
            base.replace(
                "cnp_priority = 0",
                &format!("cnp_priority = 0\n{key} = {value}"),
            ),
        )
        .unwrap();
        let error = compile_config(&path).unwrap_err().to_string();
        assert!(
            error.contains(key) && error.contains("removed in P16"),
            "{key}: {error}"
        );
    }

    fs::write(&path, base.replace("cnp_priority = 0", "cnp_priority = 8")).unwrap();
    assert!(
        compile_config(&path)
            .unwrap_err()
            .to_string()
            .contains("CNP priority")
    );
}

/// P15 (orchestrator ruling D6): `cnp_priority` is the flow's feedback priority. It defaults to
/// the flow's priority (before P15 it defaulted to 0 and had to equal the flow priority), any
/// 0..=7 value is accepted, and every config accepted before lowers to the same image.
#[test]
fn dcqcn_cnp_priority_is_the_flows_feedback_priority() {
    let directory = TempDir::new().unwrap();
    let path = write_dcqcn_config(&directory);
    let base = fs::read_to_string(&path).unwrap();

    let unchanged = compile_config(&path).expect("the accepted config still lowers");
    assert_eq!(
        unchanged.flows[0].feedback_priority,
        unchanged.flows[0].priority
    );
    assert!(!format!("{unchanged:?}").contains("feedback_priority"));

    let defaulted = base
        .replace("priority = 0\ngraph", "priority = 3\ngraph")
        .replace("cnp_priority = 0\n", "");
    fs::write(&path, defaulted).unwrap();
    let image = compile_config(&path).expect("an omitted CNP priority defaults to the flow's");
    assert_eq!(
        (image.flows[0].priority, image.flows[0].feedback_priority),
        (3, 3)
    );

    fs::write(&path, base.replace("cnp_priority = 0", "cnp_priority = 7")).unwrap();
    let image = compile_config(&path).expect("a separate CNP class lowers");
    assert_eq!(
        (image.flows[0].priority, image.flows[0].feedback_priority),
        (0, 7)
    );
    assert!(format!("{image:?}").contains("feedback_priority: 7"));
    assert_eq!(
        image.flows[0].packet_priority(PacketKind::DcqcnCnp(days_executor::DcqcnCnpHeader {
            trigger_payload: days_executor::PayloadId(0),
        })),
        7
    );
    assert_eq!(image.flows[0].packet_priority(PacketKind::Data), 0);
    // P15 lane R4: both device backends run a feedback class apart from the data class (ruling
    // D2, the PFC region's per-flow class word).
    for backend in [Backend::Metal, Backend::Cuda] {
        validate(&image, backend)
            .unwrap_or_else(|error| panic!("{backend:?} accepts a feedback class: {error}"));
    }
}

#[test]
fn dcqcn_validator_covers_state_ranges_and_exact_credit_representability() {
    let directory = TempDir::new().unwrap();
    let image = compile_config(write_dcqcn_config(&directory)).unwrap();

    let mut inconsistent = image.clone();
    let FlowGeneratorKind::Dcqcn(mut dcqcn) = inconsistent.host_states[0].generators[0].kind else {
        unreachable!()
    };
    dcqcn.controller.current_rate_bps += 1;
    inconsistent.host_states[0].generators[0].kind = FlowGeneratorKind::Dcqcn(dcqcn);
    assert!(
        validate(&inconsistent, Backend::Scalar)
            .unwrap_err()
            .to_string()
            .contains("inconsistent")
    );

    let mut receiver_mismatch = image.clone();
    receiver_mismatch.host_states[1].dcqcn_receivers[0].cnp_size_bytes += 1;
    assert!(
        validate(&receiver_mismatch, Backend::Scalar)
            .unwrap_err()
            .to_string()
            .contains("receiver")
    );

    // Mellanox-form state ranges: the stage saturates at fast_recovery_times + 1, alpha is at
    // most one, a controller is pristine until its first feedback, and both rates stay within
    // [2 * floor((minimum - 1) / 2), maximum].
    for (edit, expected) in [
        (
            (|controller: &mut DcqcnController| {
                controller.armed = true;
                controller.stage = controller.config.fast_recovery_steps + 2;
            }) as fn(&mut DcqcnController),
            "stage above",
        ),
        (
            |controller| {
                controller.armed = true;
                controller.alpha_q63 = days_executor::DCQCN_ALPHA_ONE + 1;
            },
            "alpha above one",
        ),
        (
            |controller| controller.next_alpha_ns = 1,
            "moved before its first feedback",
        ),
        (
            |controller| {
                controller.armed = true;
                controller.target_rate_bps = controller.config.maximum_rate_bps + 1;
            },
            "reachable range",
        ),
    ] {
        let mut corrupt = image.clone();
        let FlowGeneratorKind::Dcqcn(mut dcqcn) = corrupt.host_states[0].generators[0].kind else {
            unreachable!()
        };
        edit(&mut dcqcn.controller);
        corrupt.host_states[0].generators[0].kind = FlowGeneratorKind::Dcqcn(dcqcn);
        let error = validate(&corrupt, Backend::Scalar).unwrap_err().to_string();
        assert!(error.contains(expected), "{expected}: {error}");
    }

    let mut exact = image.clone();
    let FlowGeneratorKind::Dcqcn(mut dcqcn) = exact.host_states[0].generators[0].kind else {
        unreachable!()
    };
    let ticks = 1
        + (exact.stop_time_ns
            - exact.host_states[0].generators[0]
                .next_emission
                .departure_time_ns)
            / dcqcn.rate.pacing_interval_ns;
    let future_credit = u128::from(dcqcn.controller.config.maximum_rate_bps)
        * u128::from(dcqcn.rate.pacing_interval_ns)
        * u128::from(ticks);
    dcqcn.rate.credit_quanta = u128::MAX - future_credit;
    exact.host_states[0].generators[0].kind = FlowGeneratorKind::Dcqcn(dcqcn);
    validate(&exact, Backend::Scalar).expect("the exact u128 boundary must remain accepted");

    let mut one_past = exact;
    let FlowGeneratorKind::Dcqcn(mut dcqcn) = one_past.host_states[0].generators[0].kind else {
        unreachable!()
    };
    dcqcn.rate.credit_quanta += 1;
    one_past.host_states[0].generators[0].kind = FlowGeneratorKind::Dcqcn(dcqcn);
    assert!(
        validate(&one_past, Backend::Scalar)
            .unwrap_err()
            .to_string()
            .contains("credit can exceed u128")
    );
}

#[test]
fn dcqcn_closes_forward_and_reverse_channels_and_rejects_device_state_planes() {
    let directory = TempDir::new().unwrap();
    let image = compile_config(write_dcqcn_config(&directory)).unwrap();
    let flow = &image.flows[0];

    let mut missing_forward = image.clone();
    let forward = flow.route[0];
    missing_forward
        .channels
        .retain(|channel| channel.link != forward);
    assert!(validate(&missing_forward, Backend::Scalar).is_err());

    let mut missing_reverse = image.clone();
    let reverse = flow.reverse_route[0];
    missing_reverse
        .channels
        .retain(|channel| channel.link != reverse);
    assert!(validate(&missing_reverse, Backend::Scalar).is_err());

    let mut finished_with_ce_in_flight = image.clone();
    finish_dcqcn_with_ce_in_flight(&mut finished_with_ce_in_flight);
    validate(&finished_with_ce_in_flight, Backend::Scalar)
        .expect("a finished source retains the CNP path for its in-flight CE data");

    let mut missing_resident_reverse = finished_with_ce_in_flight;
    missing_resident_reverse
        .channels
        .retain(|channel| channel.link != reverse);
    assert!(validate(&missing_resident_reverse, Backend::Scalar).is_err());

    let mut unmarked_before_marker = image.clone();
    let pacing_payload = unmarked_before_marker.host_states[0].generators[0]
        .next_emission
        .payload;
    let data = unmarked_before_marker
        .initial_packets
        .iter()
        .find(|packet| packet.id == pacing_payload)
        .copied()
        .unwrap();
    let mut arrival = unmarked_before_marker
        .initial_events
        .iter()
        .find(|event| event.payload == pacing_payload)
        .copied()
        .unwrap();
    finish_dcqcn_data(&mut unmarked_before_marker);
    let first_channel = unmarked_before_marker
        .channels
        .iter()
        .find(|channel| channel.link == flow.route[0])
        .copied()
        .unwrap();
    arrival.kind = EventKind::RemoteArrival;
    arrival.target = first_channel.target;
    arrival.key.origin_node = first_channel.source;
    arrival.key.phase = 0;
    arrival.key.time_ns = 1;
    unmarked_before_marker.initial_packets.push(data);
    unmarked_before_marker
        .initial_packets
        .sort_by_key(|packet| packet.id);
    unmarked_before_marker.initial_events.push(arrival);
    unmarked_before_marker
        .initial_events
        .sort_by_key(|event| event.key);
    validate(&unmarked_before_marker, Backend::Scalar)
        .expect("resident ECT0 data before an ECN queue retains its future CNP path");
    unmarked_before_marker
        .channels
        .retain(|channel| channel.link != reverse);
    assert!(validate(&unmarked_before_marker, Backend::Scalar).is_err());

    // P14 Lane B: both device backends run the DCQCN controller and CNP planes.
    for backend in [Backend::Metal, Backend::Cuda] {
        validate(&image, backend)
            .unwrap_or_else(|error| panic!("{backend} accepts DCQCN planes: {error}"));
    }
}

#[test]
fn pfc_frame_bound_includes_only_executable_reverse_dcqcn_cnp_work() {
    let directory = TempDir::new().unwrap();
    let image = compile_config(write_dcqcn_pfc_config(&directory)).unwrap();
    let reverse_links = image.flows[0]
        .reverse_route
        .iter()
        .copied()
        .collect::<std::collections::BTreeSet<_>>();
    let reverse_bounds = image
        .switch_states
        .iter()
        .flat_map(|state| &state.queues)
        .flat_map(|queue| queue.pfc.as_ref().into_iter())
        .flat_map(|pfc| &pfc.ingresses)
        .filter(|ingress| reverse_links.contains(&ingress.controlled_link))
        .map(|ingress| ingress.max_frame_bytes[0])
        .collect::<Vec<_>>();
    assert!(!reverse_bounds.is_empty());
    assert!(reverse_bounds.iter().all(|bound| *bound == 64));
    validate(&image, Backend::Scalar).expect("the exact 64-byte reverse CNP bound must validate");

    let mut undersized = image.clone();
    for ingress in undersized
        .switch_states
        .iter_mut()
        .flat_map(|state| &mut state.queues)
        .flat_map(|queue| queue.pfc.as_mut().into_iter())
        .flat_map(|pfc| &mut pfc.ingresses)
        .filter(|ingress| reverse_links.contains(&ingress.controlled_link))
    {
        ingress.max_frame_bytes[0] = 63;
    }
    let error = validate(&undersized, Backend::Scalar)
        .expect_err("a PFC checkpoint must reserve the reachable 64-byte CNP")
        .to_string();
    assert!(
        error.contains("maximum frame bound 63") && error.contains("reachable frame size 64"),
        "{error}"
    );

    let mut resident_ce = image.clone();
    finish_dcqcn_with_ce_in_flight(&mut resident_ce);
    validate(&resident_ce, Backend::Scalar)
        .expect("a finished source keeps the 64-byte CNP bound for resident CE data");
    for ingress in resident_ce
        .switch_states
        .iter_mut()
        .flat_map(|state| &mut state.queues)
        .flat_map(|queue| queue.pfc.as_mut().into_iter())
        .flat_map(|pfc| &mut pfc.ingresses)
        .filter(|ingress| reverse_links.contains(&ingress.controlled_link))
    {
        ingress.max_frame_bytes[0] = 63;
    }
    assert!(
        validate(&resident_ce, Backend::Scalar)
            .unwrap_err()
            .to_string()
            .contains("reachable frame size 64")
    );

    let mut terminal = image;
    finish_dcqcn_data(&mut terminal);
    for ingress in terminal
        .switch_states
        .iter_mut()
        .flat_map(|state| &mut state.queues)
        .flat_map(|queue| queue.pfc.as_mut().into_iter())
        .flat_map(|pfc| &mut pfc.ingresses)
        .filter(|ingress| reverse_links.contains(&ingress.controlled_link))
    {
        ingress.max_frame_bytes[0] = 0;
    }
    validate(&terminal, Backend::Scalar)
        .expect("a terminal DCQCN source without resident CE data owns no future CNP frame");
}

#[test]
fn dcqcn_origin_capacity_counts_data_and_pacing_successors_exactly() {
    let mut pacing = dcqcn_image();
    fix_dcqcn_rate(&mut pacing);
    // Twenty data transmissions create 60 source-link events, and the pacing process creates
    // exactly 19 successor timers after its resident first timer.
    let exact_pacing_origin_bound = 79;
    pacing.host_states[0].next_origin_seq = u64::MAX - exact_pacing_origin_bound;
    validate(&pacing, Backend::Scalar).expect("the exact pacing origin boundary must fit");
    let mut one_past = pacing.clone();
    one_past.host_states[0].next_origin_seq += 1;
    assert!(
        validate(&one_past, Backend::Scalar)
            .unwrap_err()
            .to_string()
            .contains("origin sequence space overflows")
    );

    // A finished source owns no successor at all: its controller has no timer event (P16), so
    // its origin sequence may sit at u64::MAX.
    let mut finished = dcqcn_image();
    finish_dcqcn_data(&mut finished);
    finished.host_states[0].next_origin_seq = u64::MAX;
    validate(&finished, Backend::Scalar).expect("a finished DCQCN source owns no successor");
}

#[test]
fn dcqcn_cnp_counter_origin_and_payload_capacity_are_exact() {
    let mut exact = dcqcn_image();
    fix_dcqcn_rate(&mut exact);
    let flow = exact.flows[0].clone();
    let target = flow.target;
    let target_state_slot = exact.nodes[target.0 as usize].state_slot as usize;
    let node_count = exact.nodes.len() as u64;
    let cnp_count = 20_u64;
    let cnp_origin_events = cnp_count * 3;
    let maximum_payload_sequence = (u64::MAX - target.0) / node_count;
    let target_state = &mut exact.host_states[target_state_slot];
    target_state.sourced_packets = u64::MAX - cnp_count;
    target_state.next_origin_seq = u64::MAX - cnp_origin_events;
    target_state.next_payload_seq = maximum_payload_sequence - (cnp_count - 1);
    validate(&exact, Backend::Scalar).expect("all exact CNP capacity boundaries must fit");

    let mut counter_past = exact.clone();
    counter_past.host_states[target_state_slot].sourced_packets += 1;
    let error = validate(&counter_past, Backend::Scalar)
        .unwrap_err()
        .to_string();
    assert!(error.contains("sourced_packets") && error.contains("remaining upper bound 20"));

    let mut origin_past = exact.clone();
    origin_past.host_states[target_state_slot].next_origin_seq += 1;
    assert!(
        validate(&origin_past, Backend::Scalar)
            .unwrap_err()
            .to_string()
            .contains("origin sequence space overflows")
    );

    let mut payload_past = exact;
    payload_past.host_states[target_state_slot].next_payload_seq += 1;
    let error = validate(&payload_past, Backend::Scalar)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("payload identity") && error.contains("reserving 20"),
        "{error}"
    );
}

#[test]
fn terminal_and_beyond_stop_dcqcn_own_zero_future_capacity() {
    let mut terminal = dcqcn_image();
    finish_dcqcn_data(&mut terminal);
    let FlowGeneratorKind::Dcqcn(mut dcqcn) = terminal.host_states[0].generators[0].kind else {
        unreachable!()
    };
    // An armed controller at its extremes: instants at u64::MAX never fire and own no capacity.
    dcqcn.controller.armed = true;
    dcqcn.controller.decrease_pending = true;
    dcqcn.controller.increase_armed = true;
    dcqcn.controller.next_alpha_ns = u64::MAX;
    dcqcn.controller.next_decrease_ns = u64::MAX;
    dcqcn.controller.next_increase_ns = u64::MAX;
    dcqcn.controller.alpha_q63 = 0;
    terminal.host_states[0].generators[0].kind = FlowGeneratorKind::Dcqcn(dcqcn);
    terminal.host_states[0].sourced_packets = u64::MAX;
    terminal.host_states[1].sourced_packets = u64::MAX;
    terminal.host_states[0].next_origin_seq = u64::MAX;
    terminal.host_states[1].next_origin_seq = u64::MAX;
    terminal.host_states[0].next_payload_seq = u64::MAX;
    terminal.host_states[1].next_payload_seq = u64::MAX;
    validate(&terminal, Backend::Scalar)
        .expect("terminal data and an armed controller own zero future capacity");

    let mut beyond = dcqcn_image();
    let generator = &mut beyond.host_states[0].generators[0];
    generator.next_emission.departure_time_ns = beyond.stop_time_ns + 1_000;
    let pacing_payload = generator.next_emission.payload;
    beyond
        .initial_events
        .iter_mut()
        .find(|event| event.payload == pacing_payload && event.kind == EventKind::PacingTimer)
        .unwrap()
        .key
        .time_ns = beyond.stop_time_ns + 1_000;
    let FlowGeneratorKind::Dcqcn(mut dcqcn) = beyond.host_states[0].generators[0].kind else {
        unreachable!()
    };
    // An armed controller at its extremes: instants at u64::MAX never fire and own no capacity.
    dcqcn.controller.armed = true;
    dcqcn.controller.decrease_pending = true;
    dcqcn.controller.increase_armed = true;
    dcqcn.controller.next_alpha_ns = u64::MAX;
    dcqcn.controller.next_decrease_ns = u64::MAX;
    dcqcn.controller.next_increase_ns = u64::MAX;
    dcqcn.controller.alpha_q63 = 0;
    beyond.host_states[0].generators[0].kind = FlowGeneratorKind::Dcqcn(dcqcn);
    beyond.host_states[0].sourced_packets = u64::MAX;
    beyond.host_states[1].sourced_packets = u64::MAX;
    beyond.host_states[0].next_origin_seq = u64::MAX;
    beyond.host_states[1].next_origin_seq = u64::MAX;
    beyond.host_states[0].next_payload_seq = u64::MAX;
    beyond.host_states[1].next_payload_seq = u64::MAX;
    validate(&beyond, Backend::Scalar)
        .expect("an active pacing deadline beyond stop owns zero future capacity");
}

#[test]
fn dcqcn_reverse_cnp_service_time_closes_at_u64_max() {
    let mut exact = dcqcn_image();
    fix_dcqcn_rate(&mut exact);
    let generator = exact.host_states[0].generators[0];
    let FlowGeneratorKind::Dcqcn(dcqcn) = generator.kind else {
        unreachable!()
    };
    let flow = exact.flows[0].clone();
    let packet_count =
        (dcqcn.rate.total_bytes - generator.bytes_emitted).div_ceil(dcqcn.rate.packet_size_bytes);
    let latest_pacing_time = generator.next_emission.departure_time_ns
        + (packet_count - 1) * dcqcn.rate.pacing_interval_ns;
    let forward_service = flow
        .route
        .iter()
        .map(|link| {
            exact.links[link.0 as usize]
                .delay_ns(dcqcn.rate.packet_size_bytes)
                .unwrap()
                * packet_count
        })
        .sum::<u64>();
    let reverse_index = flow.reverse_route[0].0 as usize;
    let other_reverse_service = flow
        .reverse_route
        .iter()
        .skip(1)
        .map(|link| {
            exact.links[link.0 as usize]
                .delay_ns(dcqcn.cnp_size_bytes)
                .unwrap()
                * packet_count
        })
        .sum::<u64>();
    let reverse_serialization = {
        let mut link = exact.links[reverse_index];
        link.propagation_ns = 0;
        link.delay_ns(dcqcn.cnp_size_bytes).unwrap()
    };
    let reverse_delay =
        (u64::MAX - latest_pacing_time - forward_service - other_reverse_service) / packet_count;
    assert!(reverse_delay > reverse_serialization);
    exact.links[reverse_index].propagation_ns = reverse_delay - reverse_serialization;
    let exact_channel_delay = exact.links[reverse_index]
        .delay_ns(dcqcn.cnp_size_bytes)
        .unwrap();
    exact
        .channels
        .iter_mut()
        .find(|channel| channel.link == flow.reverse_route[0])
        .unwrap()
        .min_delay_ns = exact_channel_delay;
    validate(&exact, Backend::Scalar).expect("the exact reverse CNP time boundary must fit");

    let mut one_past = exact;
    one_past.links[reverse_index].propagation_ns += 1;
    one_past
        .channels
        .iter_mut()
        .find(|channel| channel.link == flow.reverse_route[0])
        .unwrap()
        .min_delay_ns += 1;
    let error = validate(&one_past, Backend::Scalar)
        .unwrap_err()
        .to_string();
    assert!(
        error.contains("maximum generator departure time")
            && error.contains("conservative service bound"),
        "{error}"
    );
}

/// A DCQCN flow whose first pacing tick cannot cover a packet lowers `Blocked`, as the validator's
/// next-tick rule requires (it failed to lower at `main` 9ff20ea: lowering predicted `Scheduled`
/// for every unreliable DCQCN flow). `configs/p16/dcqcn_mlx_blocked.toml` paces 5 Gb/s in 1 us
/// ticks against 1,000-byte packets: 5,000 bits of credit against 8,000.
#[test]
fn a_dcqcn_flow_below_one_packet_per_tick_lowers_blocked() {
    let image = compile_config(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("configs/p16/dcqcn_mlx_blocked.toml"),
    )
    .expect("the fixture lowers and validates");
    let statuses = image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter(|generator| matches!(generator.kind, FlowGeneratorKind::Dcqcn(_)))
        .map(|generator| generator.next_emission.status)
        .collect::<Vec<_>>();
    assert_eq!(
        statuses,
        [GeneratorStatus::Blocked, GeneratorStatus::Blocked]
    );
}
