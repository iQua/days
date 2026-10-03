//! P15 lane R1: the `configs/p15` RoCE queue-pair fixtures run on Scalar and CPU.
//!
//! Each fixture is pinned two ways:
//!
//! 1. **Mechanism contract**: the counts that make the fixture worth running (Go-back-N
//!    retransmissions, NACKs, CNPs, PFC frames, drops, completions or stalls) are asserted, so an
//!    edit that stops a fixture exercising its mechanism fails here.
//! 2. **Cross-backend identity**: Scalar and CPU at worker counts 1 to 4 return byte-identical
//!    full-observation `RunResult`s. Device backends refuse queue pairs in this phase.

use std::path::Path;

use days::scenario::compile_config;
use days_executor::{
    CpuConfig, FlowGeneratorKind, GeneratorStatus, MechanismTransitionRecord, ObservationMode,
    PacketKind, PfcControlAction, RoceSenderKind, RoceTransitionRecord, RunResult, SimulationImage,
    run_cpu_with_observations, run_scalar_with_observations,
};

fn lower(name: &str) -> SimulationImage {
    compile_config(Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("configs/p15/{name}")))
        .unwrap_or_else(|error| panic!("configs/p15/{name} must lower: {error}"))
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

#[derive(Debug, Default)]
struct Contract {
    queue_pairs: usize,
    finished_pairs: usize,
    complete_receivers: usize,
    fresh_data: usize,
    retransmissions: usize,
    acks: usize,
    nacks: usize,
    /// ACKs and NACKs echoing CE (P16 ruling D4: a queue pair's receiver sends no CNP).
    echoes: usize,
    cnps: usize,
    /// PFC pause and resume transitions (the frames are switch-sourced control, not observed).
    pfc_controls: usize,
    /// PFC pauses applied at a host's egress (host-link PFC).
    host_pauses: usize,
    /// Sender ticks that found their queue pair's data class paused, and RESUME restarts.
    class_paused_ticks: usize,
    resumes: usize,
    timeouts: usize,
    dropped: u128,
}

fn contract(result: &RunResult) -> Contract {
    let mut contract = Contract {
        dropped: result.summary.dropped_packets,
        ..Contract::default()
    };
    for generator in result
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
    {
        if let FlowGeneratorKind::Roce(roce) = generator.kind {
            contract.queue_pairs += 1;
            if generator.next_emission.status == GeneratorStatus::Finished {
                assert_eq!(roce.snd_una, roce.pacer.total_bytes);
                contract.finished_pairs += 1;
            }
        }
    }
    for receiver in result
        .host_states
        .iter()
        .filter_map(|state| state.roce_receivers.as_deref())
        .flatten()
    {
        if receiver.expected_psn == receiver.total_bytes {
            contract.complete_receivers += 1;
        }
    }
    for packet in &result.observed_packets {
        match packet.kind {
            PacketKind::RoceData(header) if header.retransmission => contract.retransmissions += 1,
            PacketKind::RoceData(_) => contract.fresh_data += 1,
            PacketKind::RoceAck(header) => {
                contract.acks += 1;
                contract.echoes += usize::from(header.ce_echo);
            }
            PacketKind::RoceNack(header) => {
                contract.nacks += 1;
                contract.echoes += usize::from(header.ce_echo);
            }
            PacketKind::DcqcnCnp(_) => contract.cnps += 1,
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
            MechanismTransitionRecord::PfcControl(control) => {
                contract.pfc_controls += 1;
                if result
                    .host_states
                    .iter()
                    .any(|state| state.egress_link == control.controlled_link)
                    && control.action == PfcControlAction::Pause
                {
                    contract.host_pauses += 1;
                }
            }
            MechanismTransitionRecord::Roce(RoceTransitionRecord::Sender(sender))
                if sender.kind == RoceSenderKind::Resume =>
            {
                assert!(!sender.class_paused, "{sender:?}");
                contract.resumes += 1;
            }
            MechanismTransitionRecord::Roce(RoceTransitionRecord::Sender(sender))
                if sender.class_paused =>
            {
                assert_eq!(sender.kind, RoceSenderKind::Tick, "{sender:?}");
                contract.class_paused_ticks += 1;
            }
            MechanismTransitionRecord::Roce(RoceTransitionRecord::Sender(sender))
                if sender.kind == RoceSenderKind::Timeout =>
            {
                contract.timeouts += 1;
            }
            _ => {}
        }
    }
    contract
}

#[test]
fn lossless_pfc_completes_without_loss_or_recovery() {
    let result = run_identical("roce_lossless_pfc.toml");
    let contract = contract(&result);
    assert_eq!(contract.queue_pairs, 2, "{contract:?}");
    assert_eq!(contract.finished_pairs, 2, "{contract:?}");
    assert_eq!(contract.complete_receivers, 2, "{contract:?}");
    assert_eq!(contract.fresh_data, 400, "{contract:?}");
    assert_eq!(
        (
            contract.dropped,
            contract.nacks,
            contract.retransmissions,
            contract.timeouts
        ),
        (0, 0, 0, 0),
        "{contract:?}"
    );
    assert_eq!(
        contract.acks, 400,
        "one ACK per in-order packet: {contract:?}"
    );
    assert!(contract.pfc_controls > 0, "{contract:?}");
}

/// Host-link PFC (§10.1 of `evidence/P15/hostpfc-design.md`): the switches pause the sender NICs,
/// the paused queue pairs park and resume, and nothing is lost, with no retransmission timeout to
/// fall back on.
#[test]
fn host_pfc_incast_pauses_sender_nics_and_loses_nothing() {
    let image = lower("hostpfc_incast_lossless.toml");
    let paused_hosts = image
        .host_states
        .iter()
        .filter(|state| state.pfc.is_some())
        .count();
    assert_eq!(
        paused_hosts, 4,
        "every host link carries data or feedback, so every host owns egress pause state"
    );
    let result = run_identical("hostpfc_incast_lossless.toml");
    let contract = contract(&result);
    assert_eq!(contract.queue_pairs, 3, "{contract:?}");
    assert_eq!(contract.finished_pairs, 3, "{contract:?}");
    assert_eq!(contract.complete_receivers, 3, "{contract:?}");
    assert_eq!(contract.fresh_data, 3000, "{contract:?}");
    assert_eq!(
        (
            contract.dropped,
            contract.nacks,
            contract.retransmissions,
            contract.timeouts
        ),
        (0, 0, 0, 0),
        "{contract:?}"
    );
    assert!(contract.host_pauses > 0, "{contract:?}");
    assert!(contract.class_paused_ticks > 0, "{contract:?}");
    assert!(contract.resumes > 0, "{contract:?}");
    no_host_starts_a_paused_class(&image, &result);
}

/// Host egress eligibility (ruling H1 (a)), checked semantically: no host starts transmitting a
/// packet whose class (`packet_priority`) is paused at that host, per the host's `PfcControl`
/// records. A packet's first departure is its originating host's (data from the flow's source,
/// feedback from its target; departures are in event-key order), and the service started one
/// serialization time earlier on the host's egress link, at a `TxReady` (phase 2) that follows
/// every PFC frame (phase 0) of that instant. Returns the number of packets a pause held and its
/// RESUME released: host service starts at the instant a RESUME unpaused the packet's class there.
fn no_host_starts_a_paused_class(image: &SimulationImage, result: &RunResult) -> usize {
    use std::collections::{BTreeMap, BTreeSet};
    let records = &result
        .diagnostics
        .as_ref()
        .expect("full observation carries diagnostics")
        .mechanism_transitions;
    let host_nodes = image
        .nodes
        .iter()
        .filter(|node| node.kind == days_executor::NodeKind::Host)
        .filter(|node| image.host_states[node.state_slot as usize].pfc.is_some())
        .map(|node| node.id)
        .collect::<BTreeSet<_>>();
    // (host, class) -> that class's pause transitions at the host, in key order.
    let mut controls = BTreeMap::<(days_executor::NodeId, u8), Vec<(u64, bool)>>::new();
    for record in records {
        if let MechanismTransitionRecord::PfcControl(control) = record {
            if host_nodes.contains(&control.node) {
                controls
                    .entry((control.node, control.priority))
                    .or_default()
                    .push((control.key.time_ns, !control.after_controllers.is_empty()));
            }
        }
    }
    let paused_at = |node, class: u8, time_ns: u64| {
        controls.get(&(node, class)).is_some_and(|transitions| {
            transitions
                .iter()
                .rev()
                .find(|(at, _)| *at <= time_ns)
                .is_some_and(|(_, paused)| *paused)
        })
    };
    let packets = result
        .observed_packets
        .iter()
        .map(|packet| (packet.id, *packet))
        .collect::<BTreeMap<_, _>>();
    let mut seen = BTreeSet::new();
    let mut released = 0;
    for departure in &result.departures {
        if !seen.insert(departure.payload) {
            continue;
        }
        let packet = packets[&departure.payload];
        let flow = &image.flows[packet.flow.0 as usize];
        let host = if packet.kind.is_feedback() {
            flow.target
        } else {
            flow.source
        };
        if !host_nodes.contains(&host) {
            continue;
        }
        let state = &image.host_states[image.nodes[host.0 as usize].state_slot as usize];
        let link = image.links[state.egress_link.0 as usize];
        let serialization_ns =
            link.delay_ns(packet.size_bytes).expect("link delay") - link.propagation_ns;
        let start_ns = departure.time_ns - serialization_ns;
        let class = flow.packet_priority(packet.kind);
        assert!(
            !paused_at(host, class, start_ns),
            "host {host:?} started {packet:?} (class {class}) at {start_ns} ns while that class \
             was paused there"
        );
        released += usize::from(
            start_ns > 0
                && paused_at(host, class, start_ns - 1)
                && controls[&(host, class)]
                    .iter()
                    .any(|(at, paused)| *at == start_ns && !paused),
        );
    }
    released
}

/// Fix round 1 of the host-PFC review (M1, M2): several queue pairs on one host, and a TCP flow on
/// the paused class beside them (`configs/p15/hostpfc_multi_qp_tcp.toml`).
/// - One RESUME at host 1 restarts several pause-parked pairs: `resume` rows share event keys.
/// - The TCP flow's packets wait at host 2 while class 3 is paused there, so egress eligibility
///   decides real service starts: no host starts a paused-class packet, checked semantically.
/// - Lossless, every queue pair and the TCP flow complete, and Scalar = CPU at 1-4 workers with
///   full observation (records included).
#[test]
fn several_queue_pairs_and_tcp_on_a_paused_class() {
    let image = lower("hostpfc_multi_qp_tcp.toml");
    let result = run_identical("hostpfc_multi_qp_tcp.toml");
    let contract = contract(&result);
    assert_eq!(
        (
            contract.queue_pairs,
            contract.finished_pairs,
            contract.complete_receivers
        ),
        (5, 5, 5),
        "{contract:?}"
    );
    assert_eq!(contract.dropped, 0, "{contract:?}");
    assert!(contract.host_pauses > 0, "{contract:?}");
    let tcp_done = result
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter_map(|generator| match generator.kind {
            FlowGeneratorKind::Tcp(tcp) => Some(tcp.highest_ack == tcp.total_bytes),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        tcp_done,
        [true],
        "the TCP flow on the paused class completes"
    );
    // Amendment 2: some RESUME restarts two or more pairs, one row each at the RESUME's key.
    let mut resumes_by_key = std::collections::BTreeMap::<_, usize>::new();
    for record in &result
        .diagnostics
        .as_ref()
        .expect("full observation")
        .mechanism_transitions
    {
        if let MechanismTransitionRecord::Roce(RoceTransitionRecord::Sender(sender)) = record {
            if sender.kind == RoceSenderKind::Resume {
                *resumes_by_key.entry(sender.key).or_default() += 1;
            }
        }
    }
    assert!(
        resumes_by_key.values().any(|rows| *rows >= 2),
        "no RESUME restarted two pairs: {resumes_by_key:?}"
    );
    // Egress eligibility is exercised: packets of the paused class wait in a host queue and start
    // at the RESUME that releases them, and no host starts a paused-class packet.
    assert!(
        no_host_starts_a_paused_class(&image, &result) > 0,
        "no packet was held by a host pause and released by its RESUME"
    );
}

/// `configs/p15/hostpfc_incast_lossless.toml` with its `host_links` line replaced.
fn host_links_variant(test: &str, line: &str) -> SimulationImage {
    let text = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("configs/p15/hostpfc_incast_lossless.toml"),
    )
    .expect("read the host-PFC fixture");
    assert_eq!(text.matches("\nhost_links = true\n").count(), 1);
    let path = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("p15_hostpfc_{test}_{}.toml", std::process::id()));
    std::fs::write(&path, text.replace("\nhost_links = true\n", line)).expect("write variant");
    let image = compile_config(&path).expect("the variant lowers");
    let _ = std::fs::remove_file(&path);
    image
}

/// The mechanism is what keeps the incast lossless: without host-link PFC the senders overrun
/// their first switch.
#[test]
fn without_host_links_the_host_pfc_incast_drops() {
    let image = host_links_variant("off", "\nhost_links = false\n");
    assert!(image.host_states.iter().all(|state| state.pfc.is_none()));
    let result = run_scalar_with_observations(&image, None, ObservationMode::Summary)
        .expect("the Scalar run succeeds");
    assert!(result.summary.dropped_packets > 0, "{:?}", result.summary);
}

/// `host_links` defaults to off, and off lowers exactly as a PFC image without the key does.
#[test]
fn host_links_default_off() {
    assert_eq!(
        host_links_variant("default", "\n"),
        host_links_variant("explicit", "\nhost_links = false\n")
    );
}

#[test]
fn lossy_go_back_n_recovers_every_loss_by_nack() {
    let result = run_identical("roce_gbn_lossy.toml");
    let contract = contract(&result);
    assert_eq!(contract.finished_pairs, 2, "{contract:?}");
    assert_eq!(contract.complete_receivers, 2, "{contract:?}");
    assert!(contract.dropped > 0, "{contract:?}");
    assert!(contract.nacks > 0, "{contract:?}");
    assert!(contract.retransmissions > 0, "{contract:?}");
}

#[test]
fn timeout_arm_completes_through_retransmission_timeouts() {
    let result = run_identical("roce_timeout.toml");
    let contract = contract(&result);
    assert_eq!(
        (contract.queue_pairs, contract.finished_pairs),
        (1, 1),
        "{contract:?}"
    );
    assert!(contract.timeouts > 0, "{contract:?}");
    assert!(contract.nacks > 0, "{contract:?}");
    assert!(contract.retransmissions > 0, "{contract:?}");
}

#[test]
fn nack_only_profile_has_no_timeout_and_can_stall() {
    let result = run_identical("roce_nack_only.toml");
    let contract = contract(&result);
    assert!(contract.nacks > 0, "{contract:?}");
    assert_eq!(contract.timeouts, 0, "{contract:?}");
    assert!(
        result
            .pending_events
            .iter()
            .all(|event| event.kind != days_executor::EventKind::RetransmissionTimeout),
        "the timeout is off"
    );
    // roce_timeout.toml is this scenario with the timeout on, and completes.
    assert_eq!(
        (
            contract.queue_pairs,
            contract.finished_pairs,
            contract.complete_receivers
        ),
        (1, 0, 0),
        "with the timeout off an unrepaired loss stalls the queue pair: {contract:?}"
    );
    let stalled = result
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .find_map(|generator| match generator.kind {
            FlowGeneratorKind::Roce(roce) => Some((generator.next_emission.status, roce)),
            _ => None,
        })
        .expect("the fixture has a queue pair");
    assert_eq!(stalled.0, GeneratorStatus::Blocked);
    assert!(!stalled.1.pacer_armed, "the stalled pair's pacer is parked");
    assert!(stalled.1.snd_una < stalled.1.pacer.total_bytes);
}

/// The P15 fixture's CNPs are ECN echoes on ACKs since P16 (rulings D4-D6): the congestion signal
/// still flows under PFC, with no CNP at all.
#[test]
fn echoes_under_pfc_complete_every_queue_pair() {
    let result = run_identical("roce_cnp_under_pfc.toml");
    let contract = contract(&result);
    assert_eq!(contract.finished_pairs, 4, "{contract:?}");
    assert!(contract.echoes > 0 && contract.cnps == 0, "{contract:?}");
    assert!(contract.pfc_controls > 0, "{contract:?}");
    assert_eq!(contract.dropped, 0, "{contract:?}");
}

#[test]
fn a_separate_feedback_class_changes_the_run() {
    let shared = run_identical("roce_cnp_under_pfc.toml");
    let separate = run_identical("roce_feedback_priority.toml");
    let contract = contract(&separate);
    assert_eq!(contract.finished_pairs, 4, "{contract:?}");
    assert!(contract.echoes > 0 && contract.cnps == 0, "{contract:?}");
    assert_ne!(
        shared.departures, separate.departures,
        "feedback on an unpaused class must change the schedule"
    );
}

#[test]
fn tcp_and_queue_pairs_share_a_bottleneck() {
    let result = run_identical("roce_mixed_tcp.toml");
    let contract = contract(&result);
    assert_eq!(contract.finished_pairs, 2, "{contract:?}");
    let tcp_done = result
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter_map(|generator| match generator.kind {
            FlowGeneratorKind::Tcp(tcp) => Some(tcp.highest_ack == tcp.total_bytes),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(tcp_done, [true]);
}

/// FNV-1a64 over the pretty `Debug` rendering, the `result_fnv1a64` the `days` CLI prints.
/// P15 lane R4 fix round 1 (device review M1): the bidirectional DRR and WRR host-link PFC
/// fixtures, whose feedback (class 0) waits in switch queues behind a paused data class (3), run
/// identically on Scalar and CPU at 1-4 workers. `tests/p15_device_qp.rs` holds Metal and CUDA to
/// the same Scalar result.
#[test]
fn bidirectional_drr_and_wrr_host_pfc_fixtures_match_cpu_at_one_to_four_workers() {
    for name in ["hostpfc_bidir_drr.toml", "hostpfc_bidir_wrr.toml"] {
        run_identical(name);
    }
}

fn fingerprint(value: &impl std::fmt::Debug) -> (u64, u64) {
    let text = format!("{value:#?}");
    let hash = text.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
    });
    (text.len() as u64, hash)
}

/// The Scalar summary-mode result of a fixture: the complete state every backend must return.
fn scalar_anchor(name: &str) -> (u64, u64) {
    let image = lower(name);
    let result =
        days_executor::run_scalar_with_observations(&image, None, ObservationMode::Summary)
            .unwrap_or_else(|error| panic!("{name}: Scalar run failed: {error}"));
    fingerprint(&result)
}

/// Frozen at authoring (`b448d08`, 2026-10-01, sim), with `roce_mixed_tcp` re-frozen at `e32bf1f`
/// when it moved to an ACK every 4 packets; every anchor re-frozen at P16 D1 (2026-10-03, Mac) for
/// the Mellanox-form controller, the ECN echo (no CNP) and the queue-pair window fields
/// (`days-gpu/evidence/P16/dcqcn-impl/anchors.md`): the anchors the device lane proves Metal and CUDA
/// against. `run_identical` shows CPU at 1-4 workers equal to Scalar; the `days` CLI cross-check
/// (Scalar and CPU at 2 workers) is `days-gpu/evidence/P15/qp-impl/sim/p15_anchors.tsv`.
const ANCHORS: [(&str, u64, u64); 7] = [
    ("roce_lossless_pfc.toml", 45_211, 0x86d4_4c03_e8a2_fe87),
    ("roce_gbn_lossy.toml", 45_468, 0xb0bb_425a_1abc_c97f),
    ("roce_timeout.toml", 34_182, 0x8e8e_74eb_ee72_2ced),
    ("roce_nack_only.toml", 34_161, 0xfa68_ecea_f3fb_dba3),
    ("roce_cnp_under_pfc.toml", 56_702, 0x55d3_b5d2_ce26_fd5b),
    ("roce_feedback_priority.toml", 56_749, 0xf5d8_63d1_7bfd_52b6),
    ("roce_mixed_tcp.toml", 51_617, 0x1259_f21e_71bb_205e),
];

/// Host-link PFC anchors (`p15/hostpfc`, frozen at `c26865f` on the Mac; the sim gate's CLI
/// confirms them on Linux, Scalar and CPU at 2 workers); re-frozen at P16 D1 with the others.
const HOST_PFC_ANCHORS: [(&str, u64, u64); 2] = [
    (
        "hostpfc_incast_lossless.toml",
        66_858,
        0x7533_75b4_1b13_00cf,
    ),
    // Fix round 1: the Summary anchor of the multi-QP and TCP variant (frozen on the Mac at
    // c762428's code; the sim gate's CLI confirms it on Linux).
    ("hostpfc_multi_qp_tcp.toml", 77_170, 0x13f3_943d_8d1c_1069),
];

#[test]
fn p15_fixtures_match_their_frozen_anchors() {
    for (name, bytes, fnv1a64) in ANCHORS.into_iter().chain(HOST_PFC_ANCHORS) {
        let actual = scalar_anchor(name);
        assert_eq!(
            actual,
            (bytes, fnv1a64),
            "{name}: frozen anchor moved (got bytes={} fnv1a64={:016x})",
            actual.0,
            actual.1
        );
    }
}

/// The HPCC 64->1 incast with host-link PFC (§10.2 of `evidence/P15/hostpfc-design.md`): router 0
/// pauses the sender NICs, nothing is lost, and every queue pair completes with HPCC's profile
/// (no retransmission timeout). Release-only, like the anchor below.
#[test]
#[ignore = "release-only: 64 queue pairs on a 390-host Dragonfly embedding"]
fn hpcc_incast_with_host_pfc_loses_nothing() {
    let image = lower("hpcc_incast64_dragonfly.toml");
    let result = run_scalar_with_observations(&image, None, ObservationMode::Full)
        .expect("the Scalar run succeeds");
    let contract = contract(&result);
    assert_eq!(contract.queue_pairs, 64, "{contract:?}");
    assert_eq!(contract.finished_pairs, 64, "{contract:?}");
    assert_eq!(contract.complete_receivers, 64, "{contract:?}");
    assert_eq!(
        (contract.dropped, contract.timeouts),
        (0, 0),
        "{contract:?}"
    );
    assert!(contract.host_pauses > 0, "{contract:?}");
    assert!(contract.resumes > 0, "{contract:?}");
}

/// The HPCC cross-check fixture's anchor; ignored in the default matrix (64 queue pairs at 100
/// Gbps; run it in release: `cargo test --release -p days --test p15_roce_fixtures -- --ignored`).
#[test]
#[ignore = "release-only: 64 queue pairs on a 390-host Dragonfly embedding"]
fn hpcc_fixture_matches_its_frozen_anchor() {
    assert_eq!(
        scalar_anchor("hpcc_incast64_dragonfly.toml"),
        // Re-frozen at c26865f: the fixture re-sized for host-link PFC (hostpfc-design.md §10.2);
        // re-frozen at P16 D1 for the Mellanox-form controller and the ECN echo.
        (1_237_835, 0x6dcb_f0e0_afac_8968)
    );
}
