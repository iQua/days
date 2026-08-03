use std::fs;

use days::scenario::compile_config;
use days_executor::{
    Backend, DcqcnIncreaseStage, EventKind, FlowGeneratorKind, GeneratorStatus,
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
mi_factor = 0.5
rtt_ns = 100000
cnp_interval_ns = 10000
pacing_interval_ns = 1000
cnp_priority = 0
increase_byte_threshold = 1000
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

fn move_control_beyond_stop(image: &mut days_executor::SimulationImage) {
    let FlowGeneratorKind::Dcqcn(mut dcqcn) = image.host_states[0].generators[0].kind else {
        unreachable!()
    };
    dcqcn.controller.next_control_time_ns = image.stop_time_ns + 1;
    let control_payload = dcqcn.control_timer_payload;
    image.host_states[0].generators[0].kind = FlowGeneratorKind::Dcqcn(dcqcn);
    image
        .initial_events
        .retain(|event| event.payload != control_payload);
}

#[test]
fn dcqcn_lowers_to_rate_controller_receiver_and_two_timer_tokens() {
    let directory = TempDir::new().unwrap();
    let image = compile_config(write_dcqcn_config(&directory)).unwrap();
    let generator = &image.host_states[0].generators[0];
    let FlowGeneratorKind::Dcqcn(dcqcn) = generator.kind else {
        panic!("expected exact DCQCN generator")
    };
    assert_eq!(dcqcn.rate.rate_numerator_bits_per_second, 10_000_000_000);
    assert_eq!(dcqcn.controller.config.g_ppb, 500_000_000);
    assert_eq!(dcqcn.controller.config.decrease_ppb, 500_000_000);
    assert_eq!(dcqcn.controller.stage, DcqcnIncreaseStage::Hyper);
    assert_eq!(image.host_states[1].dcqcn_receivers.len(), 1);
    assert_eq!(
        image
            .initial_packets
            .iter()
            .filter(|packet| matches!(packet.kind, PacketKind::DcqcnControlTimer))
            .count(),
        1
    );
    assert_eq!(
        image
            .initial_events
            .iter()
            .filter(|event| event.kind == days_executor::EventKind::PacingTimer)
            .count(),
        2
    );
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
    let dcqcn_records = scalar
        .mechanism_transitions
        .iter()
        .filter_map(|record| match record {
            MechanismTransitionRecord::Dcqcn(record) => Some(record),
            _ => None,
        })
        .collect::<Vec<_>>();
    let csv = dcqcn_transitions_csv(&scalar.mechanism_transitions).unwrap();
    let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("lean/fixtures/p10c/dcqcn_executor_trace_accept.csv");
    if std::env::var_os("DAYS_UPDATE_DCQCN_TRACE_FIXTURE").is_some() {
        fs::write(&fixture_path, &csv).unwrap();
    }
    assert_eq!(csv, fs::read_to_string(fixture_path).unwrap());
    assert!(dcqcn_records.iter().any(|record| {
        record.kind == days_executor::DcqcnTransitionKind::Cnp && record.applied
    }));
    assert!(
        dcqcn_records
            .iter()
            .any(|record| { record.kind == days_executor::DcqcnTransitionKind::Control })
    );
    assert!(
        dcqcn_records
            .iter()
            .any(|record| { record.kind == days_executor::DcqcnTransitionKind::Bytes })
    );

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
fn terminal_dcqcn_checkpoint_with_dormant_control_timer_revalidates() {
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
    };
    validate(&checkpoint, Backend::Scalar).unwrap();
    validate(&checkpoint, Backend::Cpu { workers: 4 }).unwrap();
}

#[test]
fn nonrepresentable_dcqcn_parameters_and_cnp_priority_are_rejected() {
    let directory = TempDir::new().unwrap();
    let path = write_dcqcn_config(&directory);
    let base = fs::read_to_string(&path).unwrap();

    fs::write(&path, base.replace("g = 0.5", "g = 0.0000000005")).unwrap();
    assert!(
        compile_config(&path)
            .unwrap_err()
            .to_string()
            .contains("ppb")
    );

    fs::write(&path, base.replace("cnp_priority = 0", "cnp_priority = 7")).unwrap();
    assert!(
        compile_config(&path)
            .unwrap_err()
            .to_string()
            .contains("CNP priority")
    );
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
    receiver_mismatch.host_states[1].dcqcn_receivers[0].cnp_interval_ns += 1;
    assert!(
        validate(&receiver_mismatch, Backend::Scalar)
            .unwrap_err()
            .to_string()
            .contains("receiver")
    );

    let mut invalid_hyper_stage = image.clone();
    let FlowGeneratorKind::Dcqcn(mut dcqcn) = invalid_hyper_stage.host_states[0].generators[0].kind
    else {
        unreachable!()
    };
    dcqcn.controller.stage = DcqcnIncreaseStage::Hyper;
    dcqcn.controller.stage_steps = 1;
    invalid_hyper_stage.host_states[0].generators[0].kind = FlowGeneratorKind::Dcqcn(dcqcn);
    assert!(
        validate(&invalid_hyper_stage, Backend::Scalar)
            .unwrap_err()
            .to_string()
            .contains("fixed-point controller state is out of range")
    );

    let mut seen_without_timestamp = image.clone();
    let FlowGeneratorKind::Dcqcn(mut dcqcn) =
        seen_without_timestamp.host_states[0].generators[0].kind
    else {
        unreachable!()
    };
    dcqcn.controller.cnp_seen = true;
    dcqcn.controller.last_cnp_time_ns = None;
    seen_without_timestamp.host_states[0].generators[0].kind = FlowGeneratorKind::Dcqcn(dcqcn);
    assert!(
        validate(&seen_without_timestamp, Backend::Scalar)
            .unwrap_err()
            .to_string()
            .contains("fixed-point controller state is out of range")
    );

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
fn dcqcn_controller_counter_and_cnp_deadline_capacity_are_exact() {
    let image = dcqcn_image();

    let mut exact_cnp = image.clone();
    let FlowGeneratorKind::Dcqcn(mut dcqcn) = exact_cnp.host_states[0].generators[0].kind else {
        unreachable!()
    };
    dcqcn.controller.last_cnp_time_ns = Some(
        u64::MAX
            .checked_sub(dcqcn.controller.config.cnp_interval_ns)
            .unwrap(),
    );
    exact_cnp.host_states[0].generators[0].kind = FlowGeneratorKind::Dcqcn(dcqcn);
    validate(&exact_cnp, Backend::Scalar).expect("the exact CNP deadline boundary must fit");

    let mut cnp_one_past = exact_cnp;
    let FlowGeneratorKind::Dcqcn(mut dcqcn) = cnp_one_past.host_states[0].generators[0].kind else {
        unreachable!()
    };
    dcqcn.controller.last_cnp_time_ns = dcqcn.controller.last_cnp_time_ns.map(|last| last + 1);
    cnp_one_past.host_states[0].generators[0].kind = FlowGeneratorKind::Dcqcn(dcqcn);
    assert!(
        validate(&cnp_one_past, Backend::Scalar)
            .unwrap_err()
            .to_string()
            .contains("CNP interval deadline can exceed u64")
    );

    let mut exact_bytes = image;
    let generator = exact_bytes.host_states[0].generators[0];
    let FlowGeneratorKind::Dcqcn(mut dcqcn) = generator.kind else {
        unreachable!()
    };
    let executable_bytes = dcqcn.rate.total_bytes - generator.bytes_emitted;
    dcqcn.controller.cnp_seen = true;
    dcqcn.controller.last_cnp_time_ns = Some(0);
    dcqcn.controller.bytes_since_increase = u64::MAX - executable_bytes;
    exact_bytes.host_states[0].generators[0].kind = FlowGeneratorKind::Dcqcn(dcqcn);
    validate(&exact_bytes, Backend::Scalar).expect("the exact byte-counter boundary must fit");

    let mut bytes_one_past = exact_bytes;
    let FlowGeneratorKind::Dcqcn(mut dcqcn) = bytes_one_past.host_states[0].generators[0].kind
    else {
        unreachable!()
    };
    dcqcn.controller.bytes_since_increase += 1;
    bytes_one_past.host_states[0].generators[0].kind = FlowGeneratorKind::Dcqcn(dcqcn);
    assert!(
        validate(&bytes_one_past, Backend::Scalar)
            .unwrap_err()
            .to_string()
            .contains("byte counter can exceed u64")
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

    for backend in [Backend::Metal, Backend::Cuda] {
        let error = validate(&image, backend).unwrap_err().to_string();
        assert!(error.contains("DCQCN controller or CNP state planes"));
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
fn dcqcn_origin_capacity_counts_data_pacing_and_control_successors_exactly() {
    let mut pacing = dcqcn_image();
    fix_dcqcn_rate(&mut pacing);
    move_control_beyond_stop(&mut pacing);
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

    let mut control = dcqcn_image();
    finish_dcqcn_data(&mut control);
    // Control deadlines are 100k, 200k, ..., 500k: five executions and four successors.
    control.host_states[0].next_origin_seq = u64::MAX - 4;
    validate(&control, Backend::Scalar).expect("the exact control origin boundary must fit");
    let mut one_past = control;
    one_past.host_states[0].next_origin_seq += 1;
    assert!(
        validate(&one_past, Backend::Scalar)
            .unwrap_err()
            .to_string()
            .contains("origin sequence space overflows")
    );
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
    move_control_beyond_stop(&mut terminal);
    let FlowGeneratorKind::Dcqcn(mut dcqcn) = terminal.host_states[0].generators[0].kind else {
        unreachable!()
    };
    dcqcn.controller.bytes_since_increase = u64::MAX;
    dcqcn.controller.last_cnp_time_ns = Some(u64::MAX);
    terminal.host_states[0].generators[0].kind = FlowGeneratorKind::Dcqcn(dcqcn);
    terminal.host_states[0].sourced_packets = u64::MAX;
    terminal.host_states[1].sourced_packets = u64::MAX;
    terminal.host_states[0].next_origin_seq = u64::MAX;
    terminal.host_states[1].next_origin_seq = u64::MAX;
    terminal.host_states[0].next_payload_seq = u64::MAX;
    terminal.host_states[1].next_payload_seq = u64::MAX;
    validate(&terminal, Backend::Scalar)
        .expect("terminal data and beyond-stop control state own zero future capacity");

    let mut beyond = dcqcn_image();
    move_control_beyond_stop(&mut beyond);
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
    dcqcn.controller.bytes_since_increase = u64::MAX;
    dcqcn.controller.last_cnp_time_ns = Some(u64::MAX);
    beyond.host_states[0].generators[0].kind = FlowGeneratorKind::Dcqcn(dcqcn);
    beyond.host_states[0].sourced_packets = u64::MAX;
    beyond.host_states[1].sourced_packets = u64::MAX;
    beyond.host_states[0].next_origin_seq = u64::MAX;
    beyond.host_states[1].next_origin_seq = u64::MAX;
    beyond.host_states[0].next_payload_seq = u64::MAX;
    beyond.host_states[1].next_payload_seq = u64::MAX;
    validate(&beyond, Backend::Scalar)
        .expect("active pacing and control deadlines beyond stop own zero future capacity");
}

#[test]
fn dcqcn_control_timer_deadline_closes_at_u64_max() {
    let mut exact = dcqcn_image();
    finish_dcqcn_data(&mut exact);
    let FlowGeneratorKind::Dcqcn(mut dcqcn) = exact.host_states[0].generators[0].kind else {
        unreachable!()
    };
    dcqcn.controller.config.control_interval_ns = 1;
    dcqcn.controller.next_control_time_ns = u64::MAX - 1;
    let control_payload = dcqcn.control_timer_payload;
    exact.host_states[0].generators[0].kind = FlowGeneratorKind::Dcqcn(dcqcn);
    exact.stop_time_ns = u64::MAX - 1;
    let control_event = exact
        .initial_events
        .iter_mut()
        .find(|event| event.payload == control_payload)
        .unwrap();
    control_event.key.time_ns = u64::MAX - 1;
    validate(&exact, Backend::Scalar).expect("the exact maximum successor deadline must fit");

    let mut one_past = exact;
    one_past.stop_time_ns = u64::MAX;
    let FlowGeneratorKind::Dcqcn(mut dcqcn) = one_past.host_states[0].generators[0].kind else {
        unreachable!()
    };
    dcqcn.controller.next_control_time_ns = u64::MAX;
    let control_payload = dcqcn.control_timer_payload;
    one_past.host_states[0].generators[0].kind = FlowGeneratorKind::Dcqcn(dcqcn);
    one_past
        .initial_events
        .iter_mut()
        .find(|event| event.payload == control_payload)
        .unwrap()
        .key
        .time_ns = u64::MAX;
    assert!(
        validate(&one_past, Backend::Scalar)
            .unwrap_err()
            .to_string()
            .contains("control timer successor exceeds u64")
    );
}

#[test]
fn dcqcn_reverse_cnp_service_time_closes_at_u64_max() {
    let mut exact = dcqcn_image();
    fix_dcqcn_rate(&mut exact);
    move_control_beyond_stop(&mut exact);
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
