//! T21 (P12) E5 fixture, completion, width, sizing, identity, and protocol-vector gates.
//!
//! E5 is the frozen wide-TCP fair row. Its fixtures deliberately preserve the design probe's
//! single `[[flow_set]]`: semantic flow identity participates in `FatTreeEcmp`, so expanding the
//! same endpoints into `[[flow]]` blocks changes the trajectory. The first authoring gates pin the
//! ruled form and byte-for-byte regeneration before expensive characterization is trusted.

use std::fmt::{self, Debug, Write as _};
use std::path::PathBuf;
use std::process::Command;

use days::scenario::compile_config;
use days_executor::{
    ArrivalDisposition, Backend, CpuConfig, FlowGeneratorKind, ObservationMode, PacketKind,
    RunResult, SimulationImage, TcpTransitionInput, run_cpu_with_observations,
    run_scalar_rounds_with_observations, size_default_device_plan, validate,
};

const FNV1A64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

const PRIMARY_FIXTURE: &str = "e5_wide_k32_q200.toml";
const BACKUP_FIXTURE: &str = "e5_wide_k32_q256.toml";
const GOLDEN_VECTORS: &str = "e5_wide_k32_q200_protocol_golden.csv";
const FLOW_COUNT: usize = 8_192;
const FLOW_BYTES: u64 = 1_048_576;
const MSS_BYTES: u64 = 1_460;
const ACK_BYTES: u64 = 40;
const TOTAL_BYTES: u128 = FLOW_COUNT as u128 * FLOW_BYTES as u128;
const HORIZON_NS: u64 = 3_000_000_000;
const PRIMARY_COMPLETION_NS: u64 = 1_000_362_708;

const FROZEN_PRIMARY_DROPS: u128 = 549;
/// Design-probe identity: all data attempts minus `flow_count * ceil(bytes / MSS)`.
const FROZEN_PRIMARY_RETRANSMISSIONS: u128 = 9_551;
/// Full-observation packet-header counters. Window-limited sends split 8,935 additional fresh
/// packets, so the probe identity above is 8,935 + 616 rather than an exact retransmit flag count.
const FROZEN_PRIMARY_EXPLICIT_ORIGINALS: u128 = 5_898_983;
const FROZEN_PRIMARY_EXPLICIT_RETRANSMISSIONS: u128 = 616;
const FROZEN_PRIMARY_DATA_DROPS: u128 = 479;
const FROZEN_PRIMARY_ACK_DROPS: u128 = 70;
const FROZEN_PRIMARY_RTO_FIRES: u128 = 2;
const FROZEN_PRIMARY_ROUNDS: usize = 664;
const FROZEN_PRIMARY_TRANSITIONS: u128 = 212_378_014;
const FROZEN_PRIMARY_WEIGHTED_WIDTH_CEILING: u128 = 36_493;
const FROZEN_PRIMARY_WIDE_SHARE_TENTHS_PERCENT: u128 = 798;
const FROZEN_BACKUP_DROPS: u128 = 157;
const FROZEN_BACKUP_RETRANSMISSIONS: u128 = 8_508;

const GIB_BYTES: u128 = 1_u128 << 30;
const DEVICE_CAP_BYTES: u128 = 22 * GIB_BYTES;
#[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
const FROZEN_PRIMARY_PLAN_BYTES: u128 = 15_586_184_248;
#[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
const FROZEN_BACKUP_PLAN_BYTES: u128 = 16_022_723_512;
const PRIMARY_ANCHOR_BYTES: u64 = 50_572_617;
const PRIMARY_ANCHOR_FNV1A64: u64 = 0x56f7_b241_57e2_e852;

#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
const E5_CAPACITY_CAPS: days_executor::DeviceCapacityCaps = days_executor::DeviceCapacityCaps {
    fallback_fel_events_per_lp: Some(16_384),
    queue_packets_per_lp: Some(2_048),
    channel_events_per_stream: Some(2_048),
    remote_staging_events_per_lp: Some(2_048),
    outbox_events_total: Some(2_000_000),
    tcp_receiver_ranges_per_flow: Some(64),
    tcp_ledger_segments_per_flow: Some(4_096),
    observation_events_per_lp: Some(512),
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Fingerprint {
    bytes: u64,
    fnv1a64: u64,
}

struct FingerprintWriter(Fingerprint);

impl fmt::Write for FingerprintWriter {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        self.0.bytes = self
            .0
            .bytes
            .checked_add(value.len() as u64)
            .ok_or(fmt::Error)?;
        self.0.fnv1a64 = value.bytes().fold(self.0.fnv1a64, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(FNV1A64_PRIME)
        });
        Ok(())
    }
}

fn fingerprint(value: &impl Debug) -> Fingerprint {
    let mut writer = FingerprintWriter(Fingerprint {
        bytes: 0,
        fnv1a64: FNV1A64_OFFSET_BASIS,
    });
    write!(&mut writer, "{value:#?}").expect("debug serialization length must fit in u64");
    writer.0
}

fn assert_primary_anchor(actual: Fingerprint) {
    assert_eq!(
        actual,
        Fingerprint {
            bytes: PRIMARY_ANCHOR_BYTES,
            fnv1a64: PRIMARY_ANCHOR_FNV1A64,
        },
        "E5 primary completion fingerprint moved (got bytes={} fnv1a64={:016x})",
        actual.bytes,
        actual.fnv1a64
    );
}

fn p12_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/benchmarks/p12")
        .join(name)
}

fn lower(name: &str) -> SimulationImage {
    let path = p12_path(name);
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
}

#[derive(Clone, Copy, Default)]
struct GoldenFlow {
    demand_bytes: u64,
    acked_bytes: u64,
    completion_ns: Option<u64>,
    drop_packets: u64,
    data_drop_packets: u64,
    ack_drop_packets: u64,
    original_data_packets: u64,
    retransmit_data_packets: u64,
    rto_fires: u64,
    final_cwnd_bytes: u64,
    final_ssthresh_bytes: u64,
}

/// Stable endpoint/work projection used by the GeDES port. This deliberately reads Full-mode
/// packet descriptors and TCP transition records: `RunSummary` has no original/retransmission or
/// per-flow drop counters, while `TcpDataHeader::retransmission` and `Timeout` transitions expose
/// those identities without changing executor state.
fn protocol_golden_csv(image: &SimulationImage, result: &RunResult) -> String {
    let mut rows = vec![GoldenFlow::default(); image.flows.len()];
    for host in &result.host_states {
        for generator in &host.generators {
            let FlowGeneratorKind::Tcp(tcp) = generator.kind else {
                continue;
            };
            let row = &mut rows[generator.flow.0 as usize];
            row.demand_bytes = tcp.total_bytes;
            row.acked_bytes = tcp.highest_ack;
            row.final_cwnd_bytes = tcp.control.cwnd_bytes(tcp.mss_bytes);
            row.final_ssthresh_bytes = tcp.control.ssthresh_bytes();
        }
    }

    for packet in &result.observed_packets {
        if let PacketKind::TcpData(header) = packet.kind {
            let row = &mut rows[packet.flow.0 as usize];
            if header.retransmission {
                row.retransmit_data_packets += 1;
            } else {
                row.original_data_packets += 1;
            }
        }
    }
    for arrival in &result.arrivals {
        if arrival.disposition != ArrivalDisposition::Dropped {
            continue;
        }
        let packet = result
            .observed_packets
            .binary_search_by_key(&arrival.payload, |packet| packet.id)
            .ok()
            .map(|index| result.observed_packets[index])
            .expect("every dropped payload must have a Full-mode descriptor");
        let row = &mut rows[packet.flow.0 as usize];
        row.drop_packets += 1;
        match packet.kind {
            PacketKind::TcpData(_) => row.data_drop_packets += 1,
            PacketKind::TcpAck(_) => row.ack_drop_packets += 1,
            _ => panic!("E5 contains only TCP data and ACK packets"),
        }
    }
    let diagnostics = result
        .diagnostics
        .as_ref()
        .expect("protocol golden generation requires Full observations");
    for transition in &diagnostics.tcp_transitions {
        let row = &mut rows[transition.flow.0 as usize];
        match transition.input {
            TcpTransitionInput::NewAck { acknowledgment, .. }
                if acknowledgment >= row.demand_bytes =>
            {
                assert!(
                    row.completion_ns.replace(transition.key.time_ns).is_none(),
                    "flow {:?} completed more than once",
                    transition.flow
                );
            }
            TcpTransitionInput::Timeout { .. } => row.rto_fires += 1,
            _ => {}
        }
    }

    let mut csv = String::new();
    csv.push_str("# P12 E5-primary Days AGO Reno protocol golden vectors.\n");
    csv.push_str("# Generated by tests/t21_p12_e5.rs::e5_primary_protocol_golden_vectors_match.\n");
    csv.push_str(
        "# completion_ns is the sender's cumulative-ACK completion instant. Drops include\n",
    );
    csv.push_str("# data and ACK packets; original/retransmit use TcpDataHeader.retransmission;\n");
    csv.push_str(
        "# rto_fires counts TcpTransitionInput::Timeout. The aggregate final-window fields\n",
    );
    csv.push_str("# are blank because summing per-flow cwnd/ssthresh has no protocol meaning.\n");
    csv.push_str(
        "record,flow_id,source_node,target_node,flow_count,demand_bytes,acked_bytes,completion_ns,\
drop_packets,data_drop_packets,ack_drop_packets,original_data_packets,retransmit_data_packets,\
rto_fires,final_cwnd_bytes,final_ssthresh_bytes\n",
    );

    let mut aggregate = GoldenFlow::default();
    for (flow, row) in image.flows.iter().zip(&rows) {
        let completion_ns = row
            .completion_ns
            .unwrap_or_else(|| panic!("flow {:?} has no completion transition", flow.id));
        writeln!(
            csv,
            "flow,{},{},{},,{},{},{},{},{},{},{},{},{},{},{}",
            flow.id.0,
            flow.source.0,
            flow.target.0,
            row.demand_bytes,
            row.acked_bytes,
            completion_ns,
            row.drop_packets,
            row.data_drop_packets,
            row.ack_drop_packets,
            row.original_data_packets,
            row.retransmit_data_packets,
            row.rto_fires,
            row.final_cwnd_bytes,
            row.final_ssthresh_bytes,
        )
        .expect("writing to String cannot fail");
        aggregate.demand_bytes += row.demand_bytes;
        aggregate.acked_bytes += row.acked_bytes;
        aggregate.completion_ns = Some(
            aggregate
                .completion_ns
                .unwrap_or_default()
                .max(completion_ns),
        );
        aggregate.drop_packets += row.drop_packets;
        aggregate.data_drop_packets += row.data_drop_packets;
        aggregate.ack_drop_packets += row.ack_drop_packets;
        aggregate.original_data_packets += row.original_data_packets;
        aggregate.retransmit_data_packets += row.retransmit_data_packets;
        aggregate.rto_fires += row.rto_fires;
    }
    writeln!(
        csv,
        "aggregate,,,,{FLOW_COUNT},{},{},{},{},{},{},{},{},{},,",
        aggregate.demand_bytes,
        aggregate.acked_bytes,
        aggregate.completion_ns.expect("at least one completion"),
        aggregate.drop_packets,
        aggregate.data_drop_packets,
        aggregate.ack_drop_packets,
        aggregate.original_data_packets,
        aggregate.retransmit_data_packets,
        aggregate.rto_fires,
    )
    .expect("writing to String cannot fail");

    assert_eq!(aggregate.demand_bytes as u128, TOTAL_BYTES);
    assert_eq!(aggregate.acked_bytes as u128, TOTAL_BYTES);
    assert_eq!(aggregate.completion_ns, Some(PRIMARY_COMPLETION_NS));
    assert_eq!(aggregate.drop_packets as u128, FROZEN_PRIMARY_DROPS);
    assert_eq!(
        aggregate.data_drop_packets as u128,
        FROZEN_PRIMARY_DATA_DROPS
    );
    assert_eq!(aggregate.ack_drop_packets as u128, FROZEN_PRIMARY_ACK_DROPS);
    let nominal_segments = FLOW_COUNT as u128 * u128::from(FLOW_BYTES.div_ceil(MSS_BYTES));
    assert_eq!(
        aggregate.original_data_packets as u128,
        FROZEN_PRIMARY_EXPLICIT_ORIGINALS
    );
    assert_eq!(
        aggregate.retransmit_data_packets as u128,
        FROZEN_PRIMARY_EXPLICIT_RETRANSMISSIONS
    );
    assert_eq!(
        u128::from(aggregate.original_data_packets + aggregate.retransmit_data_packets)
            - nominal_segments,
        FROZEN_PRIMARY_RETRANSMISSIONS,
        "the design study's inferred-retransmission probe identity moved"
    );
    assert_eq!(aggregate.rto_fires as u128, FROZEN_PRIMARY_RTO_FIRES);
    csv
}

#[test]
fn e5_generator_reproduces_committed_flow_set_fixtures() {
    let output_dir = tempfile::tempdir().expect("temporary generator output");
    let output = Command::new("python3")
        .arg(p12_path("gen_e5_wide_tcp.py"))
        .arg("--out-dir")
        .arg(output_dir.path())
        .output()
        .expect("E5 generator must run");
    assert!(
        output.status.success(),
        "E5 generator failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    for fixture in [PRIMARY_FIXTURE, BACKUP_FIXTURE] {
        assert_eq!(
            std::fs::read(output_dir.path().join(fixture)).expect("generated fixture"),
            std::fs::read(p12_path(fixture)).expect("committed fixture"),
            "{fixture} must be generated byte-for-byte; regenerate it instead of hand-editing"
        );
    }
}

#[test]
fn e5_fixtures_use_the_ruled_flow_set_form() {
    for fixture in [PRIMARY_FIXTURE, BACKUP_FIXTURE] {
        let text = std::fs::read_to_string(p12_path(fixture)).expect("E5 fixture must be readable");
        let table = text.parse::<toml::Table>().expect("E5 fixture must parse");
        assert!(
            !table.contains_key("flow"),
            "{fixture} must not use the round-1 explicit-flow form"
        );
        let flow_sets = table["flow_set"]
            .as_array()
            .expect("E5 must use `[[flow_set]]`");
        assert_eq!(flow_sets.len(), 1, "E5 needs one semantic flow set");
        assert_eq!(flow_sets[0]["flow_count"].as_integer(), Some(8_192));
        assert_eq!(flow_sets[0]["pairing"].as_str(), Some("SwitchOffsetHalf"));
    }
}

#[test]
fn e5_protocol_golden_format_and_aggregate_are_pinned() {
    const HEADER: &str = "record,flow_id,source_node,target_node,flow_count,demand_bytes,acked_bytes,\
completion_ns,drop_packets,data_drop_packets,ack_drop_packets,original_data_packets,\
retransmit_data_packets,rto_fires,final_cwnd_bytes,final_ssthresh_bytes";
    const AGGREGATE: &str =
        "aggregate,,,,8192,8589934592,8589934592,1000362708,549,479,70,5898983,616,2,,";

    let text = std::fs::read_to_string(p12_path(GOLDEN_VECTORS))
        .expect("E5 protocol golden vectors must be committed");
    let records = text
        .lines()
        .filter(|line| !line.starts_with('#'))
        .collect::<Vec<_>>();
    assert_eq!(records.len(), FLOW_COUNT + 2, "header + flows + aggregate");
    assert_eq!(records[0], HEADER);
    assert_eq!(records.last().copied(), Some(AGGREGATE));
    for (flow_id, line) in records[1..=FLOW_COUNT].iter().enumerate() {
        let fields = line.split(',').collect::<Vec<_>>();
        assert_eq!(fields.len(), 16, "flow row {flow_id} width changed");
        assert_eq!(fields[0], "flow");
        assert_eq!(fields[1].parse::<usize>(), Ok(flow_id));
        assert_eq!(fields[5].parse::<u64>(), Ok(FLOW_BYTES));
        assert_eq!(fields[6].parse::<u64>(), Ok(FLOW_BYTES));
        assert!(
            !fields[7].is_empty(),
            "flow {flow_id} completion is missing"
        );
        assert!(
            !fields[14].is_empty(),
            "flow {flow_id} final cwnd is missing"
        );
        assert!(
            !fields[15].is_empty(),
            "flow {flow_id} final ssthresh is missing"
        );
    }
}

#[test]
fn e5_fixtures_pin_the_frozen_contract() {
    for (fixture, capacity) in [(PRIMARY_FIXTURE, 200_i64), (BACKUP_FIXTURE, 256)] {
        let text = std::fs::read_to_string(p12_path(fixture)).expect("E5 fixture must be readable");
        let table = text.parse::<toml::Table>().expect("E5 fixture must parse");
        assert_eq!(table["seed"].as_integer(), Some(51_001));
        assert_eq!(table["duration"].as_float(), Some(3.0));
        assert_eq!(table["topology"]["category"].as_str(), Some("FatTree"));
        assert_eq!(table["topology"]["fat_tree"]["k"].as_integer(), Some(32));
        assert_eq!(
            table["topology"]["fat_tree"]["hosts_per_edge"].as_integer(),
            Some(16)
        );
        assert_eq!(
            table["switch"]["port_rate"].as_integer(),
            Some(100_000_000_000)
        );
        assert_eq!(table["switch"]["capacity"].as_integer(), Some(capacity));
        assert_eq!(table["switch"]["discipline"].as_str(), Some("FIFO"));
        assert_eq!(table["switch"]["drop"].as_str(), Some("TailDrop"));
        assert_eq!(table["link"]["propagation_ns"].as_integer(), Some(1_000));
        assert_eq!(table["routing"]["policy"].as_str(), Some("FatTreeEcmp"));

        let flow_set = &table["flow_set"][0];
        assert_eq!(flow_set["flow_type"].as_str(), Some("TCP"));
        assert_eq!(flow_set["flow_count"].as_integer(), Some(8_192));
        assert_eq!(flow_set["pairing"].as_str(), Some("SwitchOffsetHalf"));
        assert_eq!(flow_set["traffic"]["initial_delay"].as_float(), Some(0.0));
        assert_eq!(
            flow_set["traffic"]["size"].as_integer(),
            Some(FLOW_BYTES as i64)
        );
        assert_eq!(
            flow_set["traffic"]["pkt_size_dist"]["low"].as_integer(),
            Some(MSS_BYTES as i64)
        );
        assert_eq!(
            flow_set["traffic"]["pkt_size_dist"]["high"].as_integer(),
            Some(MSS_BYTES as i64)
        );
        assert_eq!(
            flow_set["traffic"]["tcp"]["cc_algorithm"].as_str(),
            Some("TCPReno")
        );

        let image = lower(fixture);
        assert_eq!(image.stop_time_ns, HORIZON_NS);
        assert_eq!(image.flows.len(), FLOW_COUNT);
        let mut tcp_flows = 0_usize;
        for host in &image.host_states {
            for generator in &host.generators {
                if let FlowGeneratorKind::Tcp(tcp) = generator.kind {
                    tcp_flows += 1;
                    assert_eq!(tcp.total_bytes, FLOW_BYTES);
                    assert_eq!(tcp.mss_bytes, MSS_BYTES);
                    assert_eq!(tcp.ack_size_bytes, ACK_BYTES);
                }
            }
        }
        assert_eq!(tcp_flows, FLOW_COUNT);
    }
}

#[test]
fn e5_projected_device_plans_are_derived_and_fit_boston() {
    for fixture in [PRIMARY_FIXTURE, BACKUP_FIXTURE] {
        let report = size_default_device_plan(&lower(fixture))
            .unwrap_or_else(|error| panic!("{fixture} device plan must size: {error}"));
        let derived_total = report
            .planes
            .iter()
            .map(|plane| plane.bytes as u128)
            .sum::<u128>();
        assert_eq!(derived_total, report.total_device_bytes as u128);
        assert!(
            derived_total <= DEVICE_CAP_BYTES,
            "{fixture} plan is {:.6} GiB, above the 22 GiB acceptance cap",
            derived_total as f64 / GIB_BYTES as f64
        );
        println!(
            "E5 conservative host projection {fixture}: bytes={derived_total} gib={:.9} \
             headroom_bytes={}",
            derived_total as f64 / GIB_BYTES as f64,
            DEVICE_CAP_BYTES - derived_total
        );
    }
}

/// Exact production Metal sizing under the campaign's retained capped-plan policy. The generic
/// host projection above is deliberately more conservative; this is the surface that produces the
/// design's 14.516 GiB primary and 14.922 GiB backup classes.
#[test]
#[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
fn e5_exact_capped_metal_plans_match_the_frozen_classes() {
    use days_executor::{MetalConfig, size_metal_plan_for_testing};

    for (fixture, expected_bytes, expected_milli_gib) in [
        (PRIMARY_FIXTURE, FROZEN_PRIMARY_PLAN_BYTES, 14_516_u128),
        (BACKUP_FIXTURE, FROZEN_BACKUP_PLAN_BYTES, 14_922_u128),
    ] {
        let report = size_metal_plan_for_testing(
            &lower(fixture),
            None,
            MetalConfig {
                capacity_caps: E5_CAPACITY_CAPS,
                ..MetalConfig::default()
            },
            ObservationMode::Summary,
        )
        .unwrap_or_else(|error| panic!("{fixture} exact Metal plan must size: {error}"));
        let derived_total = report
            .planes
            .iter()
            .map(|plane| plane.bytes as u128)
            .sum::<u128>();
        assert_eq!(derived_total, report.total_device_bytes as u128);
        assert_eq!(derived_total, expected_bytes);
        assert!(derived_total <= DEVICE_CAP_BYTES);
        assert_eq!(
            (derived_total * 1_000 + GIB_BYTES / 2) / GIB_BYTES,
            expected_milli_gib
        );
        println!(
            "E5 exact capped Metal plan {fixture}: bytes={derived_total} gib={:.9} \
             headroom_bytes={}",
            derived_total as f64 / GIB_BYTES as f64,
            DEVICE_CAP_BYTES - derived_total
        );
    }
}

#[test]
#[ignore = "E5 primary flow-set scalar count/width gate: 8,192 x 1 MiB TCP flows to completion"]
fn e5_primary_flow_set_fixture_reproduces_the_frozen_scalar_probe() {
    let image = lower(PRIMARY_FIXTURE);
    let run = run_scalar_rounds_with_observations(&image, None, ObservationMode::Summary)
        .expect("primary E5 scalar round run must succeed");

    // Diagnose every frozen quantity before asserting any of them. These are the hard fixture-form
    // gate: a mismatch must be reported rather than re-pinned.
    let mut completed = 0_usize;
    let mut demanded = 0_u128;
    let mut acked = 0_u128;
    let mut bytes_in_flight = 0_u128;
    let mut armed_timers = 0_usize;
    for host in &run.result.host_states {
        for generator in &host.generators {
            let FlowGeneratorKind::Tcp(tcp) = generator.kind else {
                continue;
            };
            demanded += u128::from(tcp.total_bytes);
            acked += u128::from(tcp.highest_ack);
            bytes_in_flight += u128::from(tcp.bytes_in_flight);
            completed += usize::from(tcp.highest_ack >= tcp.total_bytes);
            armed_timers += usize::from(tcp.active_timer.is_some());
        }
    }

    let transitions = run
        .rounds
        .iter()
        .map(|round| u128::from(round.events_processed))
        .sum::<u128>();
    let weighted_width_numerator = run
        .rounds
        .iter()
        .map(|round| round.active_lp_count as u128 * u128::from(round.events_processed))
        .sum::<u128>();
    let weighted_width_ceiling = weighted_width_numerator.div_ceil(transitions);
    let wide_transitions = run
        .rounds
        .iter()
        .filter(|round| round.active_lp_count >= 30_000)
        .map(|round| u128::from(round.events_processed))
        .sum::<u128>();
    let wide_share_tenths_percent = (wide_transitions * 1_000 + transitions / 2) / transitions;

    // Every delivered TCP data attempt immediately sources one ACK. Therefore sourced minus
    // received is the number of data attempts; subtract the fixed original segment count to get
    // the design study's inferred retransmission-attempt definition.
    let original_segments = FLOW_COUNT as u128 * u128::from(FLOW_BYTES.div_ceil(MSS_BYTES));
    let data_attempts = run
        .result
        .summary
        .sourced_packets
        .saturating_sub(run.result.summary.received_packets);
    let retransmissions = data_attempts.saturating_sub(original_segments);
    let last_round = run
        .rounds
        .last()
        .expect("E5 must execute at least one round");
    let anchor = fingerprint(&run.result);

    println!(
        "E5 PRIMARY flow-set diagnostic: completed={completed}/{FLOW_COUNT} demanded={demanded} \
         acked={acked} drops={} retransmissions={retransmissions} rounds={} \
         transitions={transitions} weightedWidthNumerator={weighted_width_numerator} \
         weightedWidthCeiling={weighted_width_ceiling} wideTransitions={wide_transitions} \
         wideShareTenthsPercent={wide_share_tenths_percent} finalFrontierNs={} \
         finalExclusiveHorizonNs={} bytesInFlight={bytes_in_flight} armedTimers={armed_timers} \
         residentPackets={} pendingEvents={} anchorBytes={} anchorFnv1a64={:016x}",
        run.result.summary.dropped_packets,
        run.rounds.len(),
        last_round.frontier_ns,
        last_round.exclusive_horizon_ns,
        run.result.resident_packets.len(),
        run.result.pending_events.len(),
        anchor.bytes,
        anchor.fnv1a64,
    );

    assert_eq!(completed, FLOW_COUNT, "every E5 flow must complete");
    assert_eq!(demanded, TOTAL_BYTES, "E5 demand changed");
    assert_eq!(acked, TOTAL_BYTES, "E5 ACKed bytes changed");
    assert_eq!(bytes_in_flight, 0, "completed E5 must have no flight");
    assert_eq!(armed_timers, 0, "completed E5 must have no armed timer");
    assert!(run.result.resident_packets.is_empty(), "E5 must drain");
    assert!(run.result.pending_events.is_empty(), "E5 must drain");
    assert_eq!(
        run.result.summary.dropped_packets, FROZEN_PRIMARY_DROPS,
        "E5 flow-set fixture does not reproduce the frozen probe drops; stop and report"
    );
    assert_eq!(
        retransmissions, FROZEN_PRIMARY_RETRANSMISSIONS,
        "E5 flow-set fixture does not reproduce the frozen probe retransmissions; stop and report"
    );
    assert_eq!(run.rounds.len(), FROZEN_PRIMARY_ROUNDS);
    assert_eq!(last_round.frontier_ns, PRIMARY_COMPLETION_NS);
    assert_eq!(transitions, FROZEN_PRIMARY_TRANSITIONS);
    assert_eq!(
        weighted_width_ceiling,
        FROZEN_PRIMARY_WEIGHTED_WIDTH_CEILING
    );
    assert_eq!(
        wide_share_tenths_percent,
        FROZEN_PRIMARY_WIDE_SHARE_TENTHS_PERCENT
    );
    assert_primary_anchor(anchor);
}

#[test]
#[ignore = "E5 primary completion anchor on CPU with 2 and 4 workers"]
fn e5_primary_completion_anchor_is_identical_on_cpu_x2_and_x4() {
    let image = lower(PRIMARY_FIXTURE);
    for workers in [2_usize, 4] {
        validate(&image, Backend::Cpu { workers })
            .unwrap_or_else(|error| panic!("E5 CPU x{workers} validation failed: {error}"));
        let run = run_cpu_with_observations(
            &image,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Summary,
        )
        .unwrap_or_else(|error| panic!("E5 CPU x{workers} run failed: {error}"));
        let anchor = fingerprint(&run.result);
        let last_round = run.rounds.last().expect("E5 CPU run must execute");
        println!(
            "E5 PRIMARY CPU x{workers} anchor: bytes={} fnv1a64={:016x} rounds={} \
             transitions={} completionNs={}",
            anchor.bytes,
            anchor.fnv1a64,
            run.rounds.len(),
            run.rounds
                .iter()
                .map(|round| u128::from(round.semantic.events_processed))
                .sum::<u128>(),
            last_round.semantic.frontier_ns,
        );
        assert_primary_anchor(anchor);
        assert_eq!(run.rounds.len(), FROZEN_PRIMARY_ROUNDS);
        assert_eq!(last_round.semantic.frontier_ns, PRIMARY_COMPLETION_NS);
    }
}

#[test]
#[cfg(all(feature = "metal-spike", target_vendor = "apple"))]
#[ignore = "E5 primary completion anchor on production Metal with the capped device plan"]
fn e5_primary_completion_anchor_is_identical_on_metal() {
    use days_executor::{MetalConfig, MetalExecutor};

    let image = lower(PRIMARY_FIXTURE);
    validate(&image, Backend::Metal).expect("E5 Metal validation failed");
    let executor = MetalExecutor::new().expect("Metal executor must initialize");
    let run = executor
        .run_with_observations(
            &image,
            None,
            MetalConfig {
                capacity_caps: E5_CAPACITY_CAPS,
                ..MetalConfig::default()
            },
            ObservationMode::Summary,
        )
        .unwrap_or_else(|error| panic!("E5 Metal completion run failed: {error}"));
    let anchor = fingerprint(&run.result);
    println!(
        "E5 PRIMARY Metal anchor: bytes={} fnv1a64={:016x} rounds={} transitions={} \
         capacityRetries={}",
        anchor.bytes,
        anchor.fnv1a64,
        run.rounds,
        run.transitions,
        run.capacity_retry_trace.len(),
    );
    assert_primary_anchor(anchor);
    assert_eq!(run.rounds, FROZEN_PRIMARY_ROUNDS as u64);
    assert_eq!(run.transitions, FROZEN_PRIMARY_TRANSITIONS as u64);
    assert!(
        run.capacity_retry_trace.is_empty(),
        "E5's frozen capped plan must run without capacity repair: {:?}",
        run.capacity_retry_trace
    );
}

#[test]
#[ignore = "E5 backup flow-set scalar count gate: 8,192 x 1 MiB TCP flows to completion"]
fn e5_backup_flow_set_fixture_reproduces_the_frozen_counts() {
    let run =
        run_scalar_rounds_with_observations(&lower(BACKUP_FIXTURE), None, ObservationMode::Summary)
            .expect("backup E5 scalar round run must succeed");

    let mut completed = 0_usize;
    let mut demanded = 0_u128;
    let mut acked = 0_u128;
    for host in &run.result.host_states {
        for generator in &host.generators {
            let FlowGeneratorKind::Tcp(tcp) = generator.kind else {
                continue;
            };
            completed += usize::from(tcp.highest_ack >= tcp.total_bytes);
            demanded += u128::from(tcp.total_bytes);
            acked += u128::from(tcp.highest_ack);
        }
    }
    let original_segments = FLOW_COUNT as u128 * u128::from(FLOW_BYTES.div_ceil(MSS_BYTES));
    let data_attempts = run
        .result
        .summary
        .sourced_packets
        .saturating_sub(run.result.summary.received_packets);
    let retransmissions = data_attempts.saturating_sub(original_segments);
    let last_round = run.rounds.last().expect("E5 backup must execute");
    println!(
        "E5 BACKUP flow-set diagnostic: completed={completed}/{FLOW_COUNT} demanded={demanded} \
         acked={acked} drops={} retransmissions={retransmissions} rounds={} transitions={} \
         finalFrontierNs={} finalExclusiveHorizonNs={} residentPackets={} pendingEvents={}",
        run.result.summary.dropped_packets,
        run.rounds.len(),
        run.rounds
            .iter()
            .map(|round| u128::from(round.events_processed))
            .sum::<u128>(),
        last_round.frontier_ns,
        last_round.exclusive_horizon_ns,
        run.result.resident_packets.len(),
        run.result.pending_events.len(),
    );
    assert_eq!(completed, FLOW_COUNT);
    assert_eq!(demanded, TOTAL_BYTES);
    assert_eq!(acked, TOTAL_BYTES);
    assert_eq!(run.result.summary.dropped_packets, FROZEN_BACKUP_DROPS);
    assert_eq!(retransmissions, FROZEN_BACKUP_RETRANSMISSIONS);
    assert!(run.result.resident_packets.is_empty());
    assert!(run.result.pending_events.is_empty());
    assert!(last_round.frontier_ns < HORIZON_NS);
}

#[test]
#[ignore = "E5 primary Full-observation protocol golden-vector generation/identity gate"]
fn e5_primary_protocol_golden_vectors_match() {
    let image = lower(PRIMARY_FIXTURE);
    let run = run_scalar_rounds_with_observations(&image, None, ObservationMode::Full)
        .expect("E5 primary Full-observation scalar run must succeed");
    let csv = protocol_golden_csv(&image, &run.result);
    let path = p12_path(GOLDEN_VECTORS);
    if std::env::var_os("E5_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, &csv).expect("golden vector update must succeed");
        println!("updated {}", path.display());
    }
    assert_eq!(
        csv.as_bytes(),
        std::fs::read(&path)
            .unwrap_or_else(|error| panic!("{} must be readable: {error}", path.display())),
        "E5 protocol vectors moved; inspect the endpoint/work delta before regenerating"
    );
    let aggregate = csv.lines().last().expect("aggregate line");
    println!("E5 protocol golden {aggregate}");
}
