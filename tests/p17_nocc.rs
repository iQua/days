//! P17 lane nocc (user ruling, Oct 9): RoCE queue pairs without congestion control, on Scalar and
//! CPU (`days-gpu/evidence/P17/nocc/design.md` §3).
//!
//! A `"none"` pair paces at line rate; Go-back-N, ACK/NACK, the timeout and PFC are unchanged.
//! Switches still mark its data and its receiver still echoes CE; its sender ignores the echo, so
//! its inert controller never moves and no DCQCN record is written for it. Each fixture is pinned
//! by its mechanism contract and by Scalar = CPU at 1 to 4 workers under full observation, and
//! the pinned-DCQCN twins (the P16 htsim workaround) send and deliver every packet at the same
//! instants as the `"none"` fixtures.

use std::path::Path;

use days::scenario::compile_config;
use days_executor::{
    Backend, CpuConfig, DcqcnController, FlowGeneratorKind, GeneratorStatus,
    MechanismTransitionRecord, ObservationMode, PacketKind, PfcControlAction,
    RoceCongestionControl, RoceGenerator, RoceSenderKind, RoceTransitionRecord, RunResult,
    SimulationImage, roce_sender_transitions_csv, run_cpu_with_observations,
    run_scalar_with_observations, validate,
};

const LINE_RATE_BPS: u64 = 1_000_000_000;

fn lower(name: &str) -> SimulationImage {
    compile_config(Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("configs/p17/{name}")))
        .unwrap_or_else(|error| panic!("configs/p17/{name} must lower: {error}"))
}

/// The Scalar full-observation result, after checking that every CPU worker count matches it.
fn run_identical(name: &str) -> RunResult {
    let image = lower(name);
    let scalar = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .unwrap_or_else(|error| panic!("{name}: Scalar run failed: {error}"));
    for workers in 1..=4 {
        let cpu = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Full,
        )
        .unwrap_or_else(|error| panic!("{name}: CPU run with {workers} workers failed: {error}"));
        assert!(
            cpu.result == scalar,
            "{name}: CPU with {workers} workers differs from Scalar"
        );
    }
    scalar
}

fn queue_pairs(result: &RunResult) -> Vec<(u64, GeneratorStatus, RoceGenerator)> {
    result
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter_map(|generator| match generator.kind {
            FlowGeneratorKind::Roce(roce) => {
                Some((generator.flow.0, generator.next_emission.status, roce))
            }
            _ => None,
        })
        .collect()
}

#[derive(Debug, Default)]
struct Contract {
    finished_pairs: usize,
    echoes: usize,
    nacks: usize,
    retransmissions: usize,
    pauses: usize,
    dropped: u128,
    /// DCQCN controller records, by flow.
    dcqcn_records: Vec<u64>,
    /// The rate every crediting tick read, by flow.
    tick_rates: Vec<(u64, u64)>,
}

fn contract(result: &RunResult) -> Contract {
    let mut contract = Contract {
        dropped: result.summary.dropped_packets,
        ..Contract::default()
    };
    for (_, status, roce) in queue_pairs(result) {
        if status == GeneratorStatus::Finished {
            assert_eq!(roce.snd_una, roce.pacer.total_bytes);
            contract.finished_pairs += 1;
        }
    }
    for packet in &result.observed_packets {
        match packet.kind {
            PacketKind::RoceData(header) if header.retransmission => contract.retransmissions += 1,
            PacketKind::RoceAck(header) => contract.echoes += usize::from(header.ce_echo),
            PacketKind::RoceNack(header) => {
                contract.nacks += 1;
                contract.echoes += usize::from(header.ce_echo);
            }
            _ => {}
        }
    }
    for record in &result
        .diagnostics
        .as_ref()
        .expect("full observation carries diagnostics")
        .mechanism_transitions
    {
        match record {
            MechanismTransitionRecord::PfcControl(control)
                if control.action == PfcControlAction::Pause =>
            {
                contract.pauses += 1;
            }
            MechanismTransitionRecord::Dcqcn(dcqcn) => contract.dcqcn_records.push(dcqcn.flow.0),
            MechanismTransitionRecord::Roce(RoceTransitionRecord::Sender(sender)) => {
                if let Some(rate) = sender.rate_bps {
                    assert_eq!(sender.kind, RoceSenderKind::Tick);
                    contract.tick_rates.push((sender.flow.0, rate));
                }
            }
            _ => {}
        }
    }
    contract
}

/// The no-CC pairs of `image`, by flow.
fn nocc_flows(image: &SimulationImage) -> Vec<u64> {
    image
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter_map(|generator| match generator.kind {
            FlowGeneratorKind::Roce(roce)
                if roce.congestion_control == RoceCongestionControl::None =>
            {
                Some(generator.flow.0)
            }
            _ => None,
        })
        .collect()
}

/// Every no-CC pair of `result` holds the controller it was lowered with, and every tick it
/// credited read the line rate.
fn assert_nocc_pairs_kept_line_rate(name: &str, image: &SimulationImage, result: &RunResult) {
    let nocc = nocc_flows(image);
    assert!(
        !nocc.is_empty(),
        "{name}: no pair without congestion control"
    );
    let contract = contract(result);
    for (flow, _, roce) in queue_pairs(result) {
        if !nocc.contains(&flow) {
            continue;
        }
        assert_eq!(roce.congestion_control, RoceCongestionControl::None);
        assert_eq!(
            roce.controller,
            DcqcnController::fixed_rate(LINE_RATE_BPS),
            "{name}: flow {flow}'s controller moved"
        );
        assert!(
            !contract.dcqcn_records.contains(&flow),
            "{name}: flow {flow} has a DCQCN record"
        );
        let rates = contract
            .tick_rates
            .iter()
            .filter(|(tick_flow, _)| *tick_flow == flow)
            .collect::<Vec<_>>();
        assert!(!rates.is_empty(), "{name}: flow {flow} never credited");
        assert!(
            rates.iter().all(|(_, rate)| *rate == LINE_RATE_BPS),
            "{name}: flow {flow} credited off line rate"
        );
    }
}

#[test]
fn marked_nocc_pairs_echo_but_never_move_their_controller() {
    let image = lower("nocc_marked.toml");
    let result = run_identical("nocc_marked.toml");
    let contract = contract(&result);
    assert_eq!(contract.finished_pairs, 4, "{contract:?}");
    assert!(
        contract.echoes > 0,
        "switches mark and receivers echo: {contract:?}"
    );
    assert!(contract.pauses > 0, "{contract:?}");
    assert_eq!(contract.dropped, 0);
    assert!(contract.dcqcn_records.is_empty(), "{contract:?}");
    assert_nocc_pairs_kept_line_rate("nocc_marked", &image, &result);
}

#[test]
fn unmarked_nocc_pairs_complete_without_echo() {
    let image = lower("nocc_unmarked.toml");
    let result = run_identical("nocc_unmarked.toml");
    let contract = contract(&result);
    assert_eq!(contract.finished_pairs, 4, "{contract:?}");
    assert_eq!((contract.echoes, contract.dropped), (0, 0), "{contract:?}");
    assert!(contract.dcqcn_records.is_empty(), "{contract:?}");
    assert_nocc_pairs_kept_line_rate("nocc_unmarked", &image, &result);
}

#[test]
fn mixed_pairs_cut_only_under_dcqcn() {
    let image = lower("nocc_mixed.toml");
    let nocc = nocc_flows(&image);
    assert_eq!(nocc.len(), 2);
    let result = run_identical("nocc_mixed.toml");
    let contract = contract(&result);
    assert_eq!(contract.finished_pairs, 4, "{contract:?}");
    assert!(contract.echoes > 0, "{contract:?}");
    assert_nocc_pairs_kept_line_rate("nocc_mixed", &image, &result);
    // Some DCQCN pair cut below line rate on a tick.
    let dcqcn_cut = contract
        .tick_rates
        .iter()
        .any(|(flow, rate)| !nocc.contains(flow) && *rate < LINE_RATE_BPS);
    assert!(dcqcn_cut, "a DCQCN pair must cut: {contract:?}");
    assert!(
        contract
            .dcqcn_records
            .iter()
            .all(|flow| !nocc.contains(flow))
    );
}

#[test]
fn lossy_nocc_pairs_recover_by_go_back_n() {
    let image = lower("nocc_gbn_lossy.toml");
    let result = run_identical("nocc_gbn_lossy.toml");
    let contract = contract(&result);
    assert_eq!(contract.finished_pairs, 4, "{contract:?}");
    assert!(contract.dropped > 0, "{contract:?}");
    assert!(contract.nacks > 0, "{contract:?}");
    assert!(contract.retransmissions > 0, "{contract:?}");
    assert!(contract.dcqcn_records.is_empty(), "{contract:?}");
    assert_nocc_pairs_kept_line_rate("nocc_gbn_lossy", &image, &result);
}

#[test]
fn nocc_ring_all_reduce_finishes_every_stage() {
    let image = lower("nocc_ring_lossless.toml");
    let result = run_identical("nocc_ring_lossless.toml");
    let contract = contract(&result);
    let pairs = queue_pairs(&result);
    assert_eq!(pairs.len(), 24);
    assert_eq!(contract.finished_pairs, 24, "{contract:?}");
    assert_eq!(contract.dropped, 0, "{contract:?}");
    assert!(contract.pauses > 0, "{contract:?}");
    assert!(contract.dcqcn_records.is_empty(), "{contract:?}");
    assert_nocc_pairs_kept_line_rate("nocc_ring_lossless", &image, &result);
}

/// The P16 workaround, a DCQCN controller pinned at line rate on the same pacing grid, sends and
/// delivers every packet at the same instants as `"none"`, marked or not (design note R9). Only
/// the controller state, the seeds and the DCQCN records differ.
#[test]
fn pinned_dcqcn_twins_send_and_deliver_exactly_as_none() {
    for (nocc, pinned) in [
        ("nocc_marked.toml", "nocc_pinned_marked.toml"),
        ("nocc_unmarked.toml", "nocc_pinned_unmarked.toml"),
    ] {
        let none = run_identical(nocc);
        let twin = run_identical(pinned);
        assert!(!none.departures.is_empty());
        assert_eq!(none.departures, twin.departures, "{nocc}: departures");
        assert_eq!(none.arrivals, twin.arrivals, "{nocc}: arrivals");
        assert_eq!(none.observed_packets, twin.observed_packets, "{nocc}");
        assert_eq!(none.summary, twin.summary, "{nocc}: summary");
        assert_eq!(none.switch_states, twin.switch_states, "{nocc}: switches");
        assert_eq!(none.pending_events, twin.pending_events, "{nocc}");
        assert_eq!(none.resident_packets, twin.resident_packets, "{nocc}");
        let finish = |result: &RunResult| {
            queue_pairs(result)
                .into_iter()
                .map(|(flow, status, roce)| (flow, status, roce.snd_una, roce.next_psn))
                .collect::<Vec<_>>()
        };
        assert_eq!(finish(&none), finish(&twin), "{nocc}: queue pairs");
    }
}

/// Validation pins a no-CC pair's controller to the inert fixed-rate form and refuses a variable
/// window (design note §2).
#[test]
fn validation_refuses_a_moved_controller_or_a_variable_window() {
    let image = lower("nocc_marked.toml");
    validate(&image, Backend::Scalar).expect("the lowered image is valid");
    type Tamper = fn(&mut RoceGenerator);
    let tampers: [(&str, Tamper); 4] = [
        ("armed", |roce| {
            roce.controller.on_feedback(0);
        }),
        ("rate", |roce| roce.controller.current_rate_bps -= 2),
        ("variable window", |roce| {
            roce.window_bytes = 8_000;
            roce.variable_window = true;
        }),
        ("dcqcn config", |roce| {
            roce.controller = DcqcnController::pristine(days_executor::DcqcnControllerConfig {
                g_q63: 1,
                ..roce.controller.config
            });
        }),
    ];
    for (what, tamper) in tampers {
        let mut tampered = image.clone();
        let generator = tampered
            .host_states
            .iter_mut()
            .flat_map(|state| &mut state.generators)
            .find(|generator| matches!(generator.kind, FlowGeneratorKind::Roce(_)))
            .expect("a queue pair");
        let FlowGeneratorKind::Roce(mut roce) = generator.kind else {
            unreachable!()
        };
        tamper(&mut roce);
        generator.kind = FlowGeneratorKind::Roce(roce);
        let error = validate(&tampered, Backend::Scalar)
            .expect_err(what)
            .to_string();
        assert!(
            error.contains("without congestion control"),
            "{what}: {error}"
        );
    }
}

/// The rendering of a no-CC pair names its mode; a DCQCN pair renders as it did on `main`.
#[test]
fn only_nocc_pairs_render_their_mode() {
    for (name, nocc) in [
        ("nocc_marked.toml", true),
        ("nocc_pinned_marked.toml", false),
    ] {
        let image = lower(name);
        let generator = image
            .host_states
            .iter()
            .flat_map(|state| &state.generators)
            .find(|generator| matches!(generator.kind, FlowGeneratorKind::Roce(_)))
            .expect("a queue pair");
        let rendered = format!("{generator:#?}");
        assert_eq!(
            rendered.contains("congestion_control: None"),
            nocc,
            "{name}: {rendered}"
        );
        assert_eq!(rendered.contains("congestion_control"), nocc, "{name}");
    }
}

/// qp-schema Amendment 7 (design note R6): the sender CSV appends a `congestion_control` column,
/// `dcqcn` or `none`, on every row of each pair.
#[test]
fn the_sender_csv_names_each_pairs_congestion_control() {
    let image = lower("nocc_mixed.toml");
    let nocc = nocc_flows(&image);
    let result = run_identical("nocc_mixed.toml");
    let csv = roce_sender_transitions_csv(
        &result
            .diagnostics
            .as_ref()
            .expect("full observation carries diagnostics")
            .mechanism_transitions,
    )
    .expect("sender CSV");
    let mut lines = csv.lines();
    let header = lines
        .next()
        .expect("a header")
        .split(',')
        .collect::<Vec<_>>();
    assert_eq!(header.len(), 46);
    assert_eq!(header[45], "congestion_control");
    let flow = header
        .iter()
        .position(|column| *column == "flow_id")
        .unwrap();
    let mut seen = [0_usize; 2];
    for line in lines {
        let fields = line.split(',').collect::<Vec<_>>();
        assert_eq!(fields.len(), 46, "{line}");
        let none = nocc.contains(&fields[flow].parse::<u64>().unwrap());
        assert_eq!(fields[45], if none { "none" } else { "dcqcn" }, "{line}");
        seen[usize::from(none)] += 1;
    }
    assert!(seen[0] > 0 && seen[1] > 0, "{seen:?}");
}
