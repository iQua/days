//! T21 (P12) E5 fixture, completion, width, sizing, identity, and protocol-vector gates.
//!
//! E5 is the frozen wide-TCP fair row. Its fixtures deliberately preserve the design probe's
//! single `[[flow_set]]`: semantic flow identity participates in `FatTreeEcmp`, so expanding the
//! same endpoints into `[[flow]]` blocks changes the trajectory. The first authoring gates pin the
//! ruled form and byte-for-byte regeneration before expensive characterization is trusted.

use std::collections::BTreeMap;
use std::fmt::{self, Debug, Write as _};
use std::path::PathBuf;
use std::process::Command;

use days::scenario::compile_config;
use days_executor::{
    ArrivalDisposition, Backend, CpuConfig, FlowGeneratorKind, ObservationMode, PacketDescriptor,
    PacketKind, RunResult, SimulationImage, TcpCongestionControl, TcpTransitionInput,
    TcpTransitionRecord, run_cpu_with_observations, run_scalar_rounds_with_observations,
    size_default_device_plan, validate,
};

const FNV1A64_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV1A64_PRIME: u64 = 0x0000_0100_0000_01b3;

const PRIMARY_FIXTURE: &str = "e5_wide_k32_q200.toml";
const CUBIC_FIXTURE: &str = "e5_wide_k32_q200_cubic.toml";
const BACKUP_FIXTURE: &str = "e5_wide_k32_q256.toml";
const GOLDEN_VECTORS: &str = "e5_wide_k32_q200_protocol_golden.csv";
const FLOW_COUNT: usize = 8_192;
const FLOW_BYTES: u64 = 1_048_576;
const MSS_BYTES: u64 = 1_460;
const ACK_BYTES: u64 = 40;
const TOTAL_BYTES: u128 = FLOW_COUNT as u128 * FLOW_BYTES as u128;
const HORIZON_NS: u64 = 3_000_000_000;
const PRIMARY_COMPLETION_NS: u64 = 1_000_362_708;
const CUBIC_COMPLETION_NS: u64 = 1_538_391;

const FROZEN_PRIMARY_DROPS: u128 = 549;
/// Design-probe identity: all data attempts minus `flow_count * ceil(bytes / MSS)`.
const FROZEN_PRIMARY_DATA_ATTEMPT_EXCESS: u128 = 9_551;
/// Full-observation packet-header counters. Window-limited sends split 8,935 additional fresh
/// packets, so the probe identity above is 8,935 + 616 rather than an exact retransmit flag count.
const FROZEN_PRIMARY_EXPLICIT_ORIGINALS: u128 = 5_898_983;
const FROZEN_PRIMARY_EXPLICIT_RETRANSMISSIONS: u128 = 616;
const FROZEN_PRIMARY_RETRANSMIT_DATA_BYTES: u128 = 894_219;
const FROZEN_PRIMARY_ACK_PACKETS: u128 = 5_899_120;
const FROZEN_PRIMARY_ACK_BYTES: u128 = 235_964_800;
const FROZEN_PRIMARY_DATA_DROPS: u128 = 479;
const FROZEN_PRIMARY_DATA_DROP_BYTES: u128 = 696_163;
const FROZEN_PRIMARY_ACK_DROPS: u128 = 70;
const FROZEN_PRIMARY_DROP_BYTES: u128 = 698_963;
const FROZEN_PRIMARY_FAST_RETRANSMIT_TRIGGERS: u128 = 317;
const FROZEN_PRIMARY_RTO_FIRES: u128 = 2;
const FROZEN_PRIMARY_ROUNDS: usize = 664;
const FROZEN_PRIMARY_TRANSITIONS: u128 = 212_378_014;
const FROZEN_PRIMARY_WEIGHTED_WIDTH_NUMERATOR: u128 = 7_750_198_793_034;
const FROZEN_PRIMARY_WEIGHTED_WIDTH_CEILING: u128 = 36_493;
const FROZEN_PRIMARY_WIDE_TRANSITIONS: u128 = 169_492_561;
const FROZEN_PRIMARY_WIDE_SHARE_TENTHS_PERCENT: u128 = 798;
const FROZEN_PRIMARY_EXCLUSIVE_HORIZON_NS: u128 = 1_000_363_709;
const FROZEN_CUBIC_DROPS: u128 = 1_140;
const FROZEN_CUBIC_DATA_ATTEMPT_EXCESS: u128 = 27_537;
const FROZEN_CUBIC_EXPLICIT_RETRANSMISSIONS: u128 = 1_175;
const FROZEN_CUBIC_ROUNDS: usize = 1_533;
const FROZEN_CUBIC_TRANSITIONS: u128 = 213_009_053;
const FROZEN_CUBIC_WEIGHTED_WIDTH_NUMERATOR: u128 = 7_825_235_917_504;
const FROZEN_CUBIC_WEIGHTED_WIDTH_CEILING: u128 = 36_737;
const FROZEN_CUBIC_WIDE_TRANSITIONS: u128 = 176_590_358;
const FROZEN_CUBIC_WIDE_SHARE_TENTHS_PERCENT: u128 = 829;
const FROZEN_CUBIC_EXCLUSIVE_HORIZON_NS: u128 = 1_539_392;
const FROZEN_BACKUP_DROPS: u128 = 157;
const FROZEN_BACKUP_DATA_ATTEMPT_EXCESS: u128 = 8_508;
const FROZEN_BACKUP_ROUNDS: usize = 611;
const FROZEN_BACKUP_TRANSITIONS: u128 = 212_351_652;
const FROZEN_PRIMARY_LEDGER_RECORDS_PER_FLOW_CAP: usize = 727;
#[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
const E5_TCP_FIXED_WORDS_PER_FLOW: u128 = 7 + 64 * 2 + 6;
#[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
const TCP_LEDGER_RECORD_WORDS: u128 = 5;

const GIB_BYTES: u128 = 1_u128 << 30;
const DEVICE_CAP_BYTES: u128 = 22 * GIB_BYTES;
#[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
const FROZEN_PRE_O212_PRIMARY_PLAN_BYTES: u128 = 15_586_577_464;
#[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
const FROZEN_PRE_O212_CUBIC_PLAN_BYTES: u128 = 15_607_041_624;
#[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
const FROZEN_PRE_O212_BACKUP_PLAN_BYTES: u128 = 16_023_116_728;
const PRIMARY_ANCHOR_BYTES: u64 = 50_572_617;
const PRIMARY_ANCHOR_FNV1A64: u64 = 0x56f7_b241_57e2_e852;
const CUBIC_ANCHOR_BYTES: u64 = 52_532_113;
const CUBIC_ANCHOR_FNV1A64: u64 = 0xc9b7_5b2b_b56f_6a78;

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

fn assert_cubic_anchor(actual: Fingerprint) {
    assert_eq!(
        actual,
        Fingerprint {
            bytes: CUBIC_ANCHOR_BYTES,
            fnv1a64: CUBIC_ANCHOR_FNV1A64,
        },
        "E5 CUBIC completion fingerprint moved (got bytes={} fnv1a64={:016x})",
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
    start_ns: Option<u64>,
    first_send_ns: Option<u64>,
    acked_bytes: u64,
    completion_ns: Option<u64>,
    original_data_packets: u64,
    original_data_bytes: u64,
    retransmit_data_packets: u64,
    retransmit_data_bytes: u64,
    ack_packets: u64,
    ack_bytes: u64,
    drop_packets: u64,
    drop_bytes: u64,
    data_drop_packets: u64,
    data_drop_bytes: u64,
    ack_drop_packets: u64,
    ack_drop_bytes: u64,
    fast_retransmit_triggers: u64,
    rto_fires: u64,
    final_cwnd_bytes: u64,
    final_ssthresh_bytes: u64,
    final_rto_ns: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LedgerHighWater {
    per_flow: Vec<usize>,
    aggregate: usize,
}

/// Replays the Scalar Full trace through the canonical ledger count operations. Initial attempts
/// are seeded from the image; later attempts are created after their equal-time sender transition,
/// so advancing ACKs are applied before the replacement/refill attempts they cause. The aggregate
/// is the conservative sum of per-flow peaks: stronger than a simultaneous peak for a plan whose
/// capacity is also summed per flow.
fn scalar_ledger_high_water(image: &SimulationImage, result: &RunResult) -> LedgerHighWater {
    let diagnostics = result
        .diagnostics
        .as_ref()
        .expect("ledger high-water requires Full observations");
    let mut attempts = vec![Vec::<&PacketDescriptor>::new(); image.flows.len()];
    for packet in &result.observed_packets {
        let PacketKind::TcpData(_) = packet.kind else {
            continue;
        };
        attempts[packet.flow.0 as usize].push(packet);
    }
    let mut acknowledgments = vec![Vec::<&TcpTransitionRecord>::new(); image.flows.len()];
    for transition in &diagnostics.tcp_transitions {
        let TcpTransitionInput::NewAck { .. } = transition.input else {
            continue;
        };
        acknowledgments[transition.flow.0 as usize].push(transition);
    }
    let mut per_flow = vec![0_usize; image.flows.len()];
    for flow in 0..image.flows.len() {
        attempts[flow].sort_unstable_by_key(|packet| {
            let PacketKind::TcpData(header) = packet.kind else {
                unreachable!()
            };
            (header.sent_time_ns, packet.id)
        });
        acknowledgments[flow].sort_unstable_by_key(|transition| transition.key);
        let mut ledger = BTreeMap::<u64, u64>::new();
        for packet in image
            .initial_packets
            .iter()
            .filter(|packet| packet.flow.0 as usize == flow)
        {
            let PacketKind::TcpData(header) = packet.kind else {
                continue;
            };
            ledger.insert(header.sequence, header.sequence + packet.size_bytes);
        }
        let mut peak = ledger.len();
        let mut attempt = 0_usize;
        let mut acknowledgment = 0_usize;
        while attempt < attempts[flow].len() || acknowledgment < acknowledgments[flow].len() {
            let attempt_time = attempts[flow].get(attempt).map(|packet| {
                let PacketKind::TcpData(header) = packet.kind else {
                    unreachable!()
                };
                header.sent_time_ns
            });
            let acknowledgment_time = acknowledgments[flow]
                .get(acknowledgment)
                .map(|transition| transition.key.time_ns);
            let time_ns = match (attempt_time, acknowledgment_time) {
                (Some(left), Some(right)) => left.min(right),
                (Some(time), None) | (None, Some(time)) => time,
                (None, None) => unreachable!(),
            };
            while acknowledgments[flow]
                .get(acknowledgment)
                .is_some_and(|transition| transition.key.time_ns == time_ns)
            {
                let TcpTransitionInput::NewAck {
                    acknowledgment: ack,
                    ..
                } = acknowledgments[flow][acknowledgment].input
                else {
                    unreachable!()
                };
                let mut unacknowledged = ledger.split_off(&ack);
                if let Some((sequence, end)) = ledger.pop_last() {
                    if ack < end {
                        assert!(sequence < ack);
                        unacknowledged.insert(ack, end);
                    }
                }
                ledger = unacknowledged;
                acknowledgment += 1;
            }
            while attempts[flow].get(attempt).is_some_and(|packet| {
                let PacketKind::TcpData(header) = packet.kind else {
                    unreachable!()
                };
                header.sent_time_ns == time_ns
            }) {
                let packet = attempts[flow][attempt];
                let PacketKind::TcpData(header) = packet.kind else {
                    unreachable!()
                };
                if let Some(original_end) =
                    ledger.insert(header.sequence, header.sequence + packet.size_bytes)
                {
                    assert_eq!(
                        original_end,
                        header.sequence + packet.size_bytes,
                        "flow {flow} replaced ledger sequence {} with a different end",
                        header.sequence
                    );
                }
                peak = peak.max(ledger.len());
                attempt += 1;
            }
        }
        assert!(
            ledger.is_empty(),
            "flow {flow} did not drain its Scalar ledger"
        );
        per_flow[flow] = peak;
    }
    let aggregate = per_flow.iter().sum();
    LedgerHighWater {
        per_flow,
        aggregate,
    }
}

/// Stable endpoint/work projection used by the GeDES port. This deliberately reads Full-mode
/// packet descriptors and TCP transition records: `RunSummary` has no original/retransmission or
/// per-flow drop counters, while `TcpDataHeader::retransmission` and `Timeout` transitions expose
/// those identities without changing executor state.
fn protocol_golden_csv(image: &SimulationImage, result: &RunResult) -> String {
    let mut rows = vec![GoldenFlow::default(); image.flows.len()];
    for packet in &image.initial_packets {
        let PacketKind::TcpData(header) = packet.kind else {
            continue;
        };
        let row = &mut rows[packet.flow.0 as usize];
        row.start_ns = Some(
            row.start_ns
                .map_or(header.sent_time_ns, |start| start.min(header.sent_time_ns)),
        );
    }
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
            row.final_rto_ns = tcp.rto_ns;
        }
    }

    for packet in &result.observed_packets {
        let row = &mut rows[packet.flow.0 as usize];
        match packet.kind {
            PacketKind::TcpData(header) => {
                row.first_send_ns = Some(
                    row.first_send_ns
                        .map_or(header.sent_time_ns, |first| first.min(header.sent_time_ns)),
                );
                if header.retransmission {
                    row.retransmit_data_packets += 1;
                    row.retransmit_data_bytes += packet.size_bytes;
                } else {
                    row.original_data_packets += 1;
                    row.original_data_bytes += packet.size_bytes;
                }
            }
            PacketKind::TcpAck(_) => {
                row.ack_packets += 1;
                row.ack_bytes += packet.size_bytes;
            }
            _ => panic!("E5 contains only TCP data and ACK packets"),
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
        row.drop_bytes += packet.size_bytes;
        match packet.kind {
            PacketKind::TcpData(_) => {
                row.data_drop_packets += 1;
                row.data_drop_bytes += packet.size_bytes;
            }
            PacketKind::TcpAck(_) => {
                row.ack_drop_packets += 1;
                row.ack_drop_bytes += packet.size_bytes;
            }
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
            TcpTransitionInput::DuplicateAck { .. }
                if transition.before.duplicate_acks() == 2
                    && transition.after.duplicate_acks() == 3 =>
            {
                row.fast_retransmit_triggers += 1;
            }
            TcpTransitionInput::Timeout { .. } => row.rto_fires += 1,
            _ => {}
        }
    }

    let mut csv = String::new();
    csv.push_str("# P12 E5-primary Days AGO Reno protocol golden vectors.\n");
    csv.push_str("# Generated by tests/t21_p12_e5.rs::e5_primary_protocol_golden_vectors_match.\n");
    csv.push_str(
        "# start/first_send come from initial/observed data sent_time_ns; completion is the\n",
    );
    csv.push_str(
        "# sender's cumulative-ACK completion instant. Packet and drop fields include bytes;\n",
    );
    csv.push_str(
        "# original/retransmit use TcpDataHeader.retransmission. Fast retransmit is the exact\n",
    );
    csv.push_str(
        "# third-duplicate-ACK trigger; rto_fires counts TcpTransitionInput::Timeout. Aggregate\n",
    );
    csv.push_str(
        "# final cwnd/ssthresh/RTO fields are blank because sums have no protocol meaning.\n",
    );
    csv.push_str(
        "record,flow_id,source_node,target_node,flow_count,demand_bytes,start_ns,first_send_ns,\
completion_ns,acked_bytes,original_data_packets,original_data_bytes,retransmit_data_packets,\
retransmit_data_bytes,ack_packets,ack_bytes,drop_packets,drop_bytes,data_drop_packets,\
data_drop_bytes,ack_drop_packets,ack_drop_bytes,fast_retransmit_triggers,rto_fires,\
final_cwnd_bytes,final_ssthresh_bytes,final_rto_ns\n",
    );

    let mut aggregate = GoldenFlow::default();
    for (flow, row) in image.flows.iter().zip(&rows) {
        let completion_ns = row
            .completion_ns
            .unwrap_or_else(|| panic!("flow {:?} has no completion transition", flow.id));
        let start_ns = row
            .start_ns
            .unwrap_or_else(|| panic!("flow {:?} has no configured start", flow.id));
        let first_send_ns = row
            .first_send_ns
            .unwrap_or_else(|| panic!("flow {:?} has no data attempt", flow.id));
        writeln!(
            csv,
            "flow,{},{},{},,{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            flow.id.0,
            flow.source.0,
            flow.target.0,
            row.demand_bytes,
            start_ns,
            first_send_ns,
            completion_ns,
            row.acked_bytes,
            row.original_data_packets,
            row.original_data_bytes,
            row.retransmit_data_packets,
            row.retransmit_data_bytes,
            row.ack_packets,
            row.ack_bytes,
            row.drop_packets,
            row.drop_bytes,
            row.data_drop_packets,
            row.data_drop_bytes,
            row.ack_drop_packets,
            row.ack_drop_bytes,
            row.fast_retransmit_triggers,
            row.rto_fires,
            row.final_cwnd_bytes,
            row.final_ssthresh_bytes,
            row.final_rto_ns,
        )
        .expect("writing to String cannot fail");
        aggregate.demand_bytes += row.demand_bytes;
        aggregate.start_ns = Some(
            aggregate
                .start_ns
                .map_or(start_ns, |earliest| earliest.min(start_ns)),
        );
        aggregate.first_send_ns = Some(
            aggregate
                .first_send_ns
                .map_or(first_send_ns, |earliest| earliest.min(first_send_ns)),
        );
        aggregate.acked_bytes += row.acked_bytes;
        aggregate.completion_ns = Some(
            aggregate
                .completion_ns
                .unwrap_or_default()
                .max(completion_ns),
        );
        aggregate.drop_packets += row.drop_packets;
        aggregate.drop_bytes += row.drop_bytes;
        aggregate.data_drop_packets += row.data_drop_packets;
        aggregate.data_drop_bytes += row.data_drop_bytes;
        aggregate.ack_drop_packets += row.ack_drop_packets;
        aggregate.ack_drop_bytes += row.ack_drop_bytes;
        aggregate.original_data_packets += row.original_data_packets;
        aggregate.original_data_bytes += row.original_data_bytes;
        aggregate.retransmit_data_packets += row.retransmit_data_packets;
        aggregate.retransmit_data_bytes += row.retransmit_data_bytes;
        aggregate.ack_packets += row.ack_packets;
        aggregate.ack_bytes += row.ack_bytes;
        aggregate.fast_retransmit_triggers += row.fast_retransmit_triggers;
        aggregate.rto_fires += row.rto_fires;
    }
    writeln!(
        csv,
        "aggregate,,,,{FLOW_COUNT},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},,,",
        aggregate.demand_bytes,
        aggregate.start_ns.expect("at least one start"),
        aggregate.first_send_ns.expect("at least one send"),
        aggregate.completion_ns.expect("at least one completion"),
        aggregate.acked_bytes,
        aggregate.original_data_packets,
        aggregate.original_data_bytes,
        aggregate.retransmit_data_packets,
        aggregate.retransmit_data_bytes,
        aggregate.ack_packets,
        aggregate.ack_bytes,
        aggregate.drop_packets,
        aggregate.drop_bytes,
        aggregate.data_drop_packets,
        aggregate.data_drop_bytes,
        aggregate.ack_drop_packets,
        aggregate.ack_drop_bytes,
        aggregate.fast_retransmit_triggers,
        aggregate.rto_fires,
    )
    .expect("writing to String cannot fail");

    assert_eq!(aggregate.demand_bytes as u128, TOTAL_BYTES);
    assert_eq!(aggregate.start_ns, Some(0));
    assert_eq!(aggregate.first_send_ns, Some(0));
    assert_eq!(aggregate.acked_bytes as u128, TOTAL_BYTES);
    assert_eq!(aggregate.original_data_bytes as u128, TOTAL_BYTES);
    assert_eq!(aggregate.completion_ns, Some(PRIMARY_COMPLETION_NS));
    assert_eq!(aggregate.drop_packets as u128, FROZEN_PRIMARY_DROPS);
    assert_eq!(
        aggregate.data_drop_packets as u128,
        FROZEN_PRIMARY_DATA_DROPS
    );
    assert_eq!(aggregate.ack_drop_packets as u128, FROZEN_PRIMARY_ACK_DROPS);
    assert_eq!(aggregate.drop_bytes as u128, FROZEN_PRIMARY_DROP_BYTES);
    assert_eq!(
        aggregate.data_drop_bytes as u128,
        FROZEN_PRIMARY_DATA_DROP_BYTES
    );
    assert_eq!(
        aggregate.drop_packets,
        aggregate.data_drop_packets + aggregate.ack_drop_packets
    );
    assert_eq!(
        aggregate.drop_bytes,
        aggregate.data_drop_bytes + aggregate.ack_drop_bytes
    );
    assert_eq!(
        aggregate.ack_drop_bytes,
        aggregate.ack_drop_packets * ACK_BYTES
    );
    assert_eq!(aggregate.ack_bytes, aggregate.ack_packets * ACK_BYTES);
    assert_eq!(
        aggregate.original_data_packets + aggregate.retransmit_data_packets,
        aggregate.ack_packets + aggregate.data_drop_packets
    );
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
        aggregate.retransmit_data_bytes as u128,
        FROZEN_PRIMARY_RETRANSMIT_DATA_BYTES
    );
    assert_eq!(aggregate.ack_packets as u128, FROZEN_PRIMARY_ACK_PACKETS);
    assert_eq!(aggregate.ack_bytes as u128, FROZEN_PRIMARY_ACK_BYTES);
    assert_eq!(
        aggregate.fast_retransmit_triggers as u128,
        FROZEN_PRIMARY_FAST_RETRANSMIT_TRIGGERS
    );
    assert_eq!(
        u128::from(aggregate.original_data_packets + aggregate.retransmit_data_packets)
            - nominal_segments,
        FROZEN_PRIMARY_DATA_ATTEMPT_EXCESS,
        "the design study's inferred data-attempt excess moved"
    );
    assert_eq!(aggregate.rto_fires as u128, FROZEN_PRIMARY_RTO_FIRES);
    let ledger = scalar_ledger_high_water(image, result);
    let max_per_flow = ledger.per_flow.iter().copied().max().unwrap_or_default();
    assert!(
        ledger
            .per_flow
            .iter()
            .all(|observed| *observed <= FROZEN_PRIMARY_LEDGER_RECORDS_PER_FLOW_CAP),
        "Scalar E5 per-flow ledger high-water exceeded the capped plan's \
         {FROZEN_PRIMARY_LEDGER_RECORDS_PER_FLOW_CAP}-record capacity: max={max_per_flow}"
    );
    let aggregate_capacity = FLOW_COUNT * FROZEN_PRIMARY_LEDGER_RECORDS_PER_FLOW_CAP;
    assert!(
        ledger.aggregate <= aggregate_capacity,
        "Scalar E5 aggregate ledger high-water {} exceeded capped-plan capacity {aggregate_capacity}",
        ledger.aggregate
    );
    println!(
        "E5 PRIMARY Scalar ledger high-water: maxPerFlow={max_per_flow} \
         sumOfPerFlowPeaks={} perFlowCapacity={FROZEN_PRIMARY_LEDGER_RECORDS_PER_FLOW_CAP} \
         aggregateCapacity={aggregate_capacity}",
        ledger.aggregate
    );
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

    let output = Command::new("python3")
        .arg(p12_path("gen_e5_wide_tcp.py"))
        .args(["--controller", "cubic"])
        .arg("--out-dir")
        .arg(output_dir.path())
        .output()
        .expect("E5 CUBIC generator must run");
    assert!(
        output.status.success(),
        "E5 CUBIC generator failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::read(output_dir.path().join(CUBIC_FIXTURE)).expect("generated CUBIC fixture"),
        std::fs::read(p12_path(CUBIC_FIXTURE)).expect("committed CUBIC fixture"),
        "{CUBIC_FIXTURE} must be generated byte-for-byte; regenerate it instead of hand-editing"
    );
}

#[test]
fn e5_fixtures_use_the_ruled_flow_set_form() {
    for fixture in [PRIMARY_FIXTURE, CUBIC_FIXTURE, BACKUP_FIXTURE] {
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
    const HEADER: &str = "record,flow_id,source_node,target_node,flow_count,demand_bytes,start_ns,\
first_send_ns,completion_ns,acked_bytes,original_data_packets,original_data_bytes,\
retransmit_data_packets,retransmit_data_bytes,ack_packets,ack_bytes,drop_packets,drop_bytes,\
data_drop_packets,data_drop_bytes,ack_drop_packets,ack_drop_bytes,fast_retransmit_triggers,\
rto_fires,final_cwnd_bytes,final_ssthresh_bytes,final_rto_ns";
    const AGGREGATE: &str = "aggregate,,,,8192,8589934592,0,0,1000362708,8589934592,5898983,8589934592,616,\
894219,5899120,235964800,549,698963,479,696163,70,2800,317,2,,,";

    let text = std::fs::read_to_string(p12_path(GOLDEN_VECTORS))
        .expect("E5 protocol golden vectors must be committed");
    let records = text
        .lines()
        .filter(|line| !line.starts_with('#'))
        .collect::<Vec<_>>();
    assert_eq!(records.len(), FLOW_COUNT + 2, "header + flows + aggregate");
    assert_eq!(records[0], HEADER);
    assert_eq!(records.last().copied(), Some(AGGREGATE));
    let mut sums = [0_u128; 27];
    let mut last_completion = 0_u64;
    for (flow_id, line) in records[1..=FLOW_COUNT].iter().enumerate() {
        let fields = line.split(',').collect::<Vec<_>>();
        assert_eq!(fields.len(), 27, "flow row {flow_id} width changed");
        assert_eq!(fields[0], "flow");
        assert_eq!(fields[1].parse::<usize>(), Ok(flow_id));
        assert_eq!(fields[5].parse::<u64>(), Ok(FLOW_BYTES));
        assert_eq!(fields[6].parse::<u64>(), Ok(0));
        assert_eq!(fields[7].parse::<u64>(), Ok(0));
        assert!(
            !fields[8].is_empty(),
            "flow {flow_id} completion is missing"
        );
        last_completion = last_completion.max(fields[8].parse().expect("numeric completion"));
        assert_eq!(fields[9].parse::<u64>(), Ok(FLOW_BYTES));
        for index in [
            5_usize, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
        ] {
            sums[index] += fields[index]
                .parse::<u128>()
                .unwrap_or_else(|error| panic!("flow {flow_id} field {index}: {error}"));
        }
        assert!(
            !fields[24].is_empty(),
            "flow {flow_id} final cwnd is missing"
        );
        assert!(
            !fields[25].is_empty(),
            "flow {flow_id} final ssthresh is missing"
        );
        assert!(
            !fields[26].is_empty(),
            "flow {flow_id} final RTO is missing"
        );
    }
    assert_eq!(sums[5], TOTAL_BYTES);
    assert_eq!(sums[9], TOTAL_BYTES);
    assert_eq!(sums[10], FROZEN_PRIMARY_EXPLICIT_ORIGINALS);
    assert_eq!(sums[11], TOTAL_BYTES);
    assert_eq!(sums[12], FROZEN_PRIMARY_EXPLICIT_RETRANSMISSIONS);
    assert_eq!(sums[13], FROZEN_PRIMARY_RETRANSMIT_DATA_BYTES);
    assert_eq!(sums[14], FROZEN_PRIMARY_ACK_PACKETS);
    assert_eq!(sums[15], FROZEN_PRIMARY_ACK_BYTES);
    assert_eq!(sums[16], FROZEN_PRIMARY_DROPS);
    assert_eq!(sums[17], FROZEN_PRIMARY_DROP_BYTES);
    assert_eq!(sums[18], FROZEN_PRIMARY_DATA_DROPS);
    assert_eq!(sums[19], FROZEN_PRIMARY_DATA_DROP_BYTES);
    assert_eq!(sums[20], FROZEN_PRIMARY_ACK_DROPS);
    assert_eq!(sums[21], FROZEN_PRIMARY_ACK_DROPS * u128::from(ACK_BYTES));
    assert_eq!(sums[22], FROZEN_PRIMARY_FAST_RETRANSMIT_TRIGGERS);
    assert_eq!(sums[23], FROZEN_PRIMARY_RTO_FIRES);
    assert_eq!(sums[16], sums[18] + sums[20]);
    assert_eq!(sums[17], sums[19] + sums[21]);
    assert_eq!(sums[10] + sums[12], sums[14] + sums[18]);
    assert_eq!(last_completion, PRIMARY_COMPLETION_NS);
}

#[test]
fn e5_fixtures_pin_the_frozen_contract() {
    for (fixture, capacity, controller) in [
        (PRIMARY_FIXTURE, 200_i64, "TCPReno"),
        (CUBIC_FIXTURE, 200, "CUBIC"),
        (BACKUP_FIXTURE, 256, "TCPReno"),
    ] {
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
            Some(controller)
        );
        if fixture == CUBIC_FIXTURE {
            assert_eq!(
                flow_set["traffic"]["tcp"]["cubic"]["beta"].as_float(),
                Some(0.7)
            );
            assert_eq!(
                flow_set["traffic"]["tcp"]["cubic"]["c"].as_float(),
                Some(0.4)
            );
            assert_eq!(
                flow_set["traffic"]["tcp"]["cubic"]["fast_convergence"].as_bool(),
                Some(true)
            );
        }

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
                    assert_eq!(
                        matches!(tcp.control, TcpCongestionControl::Cubic(_)),
                        fixture == CUBIC_FIXTURE
                    );
                }
            }
        }
        assert_eq!(tcp_flows, FLOW_COUNT);
    }
}

#[test]
fn e5_projected_device_plans_are_derived_and_fit_boston() {
    for fixture in [PRIMARY_FIXTURE, CUBIC_FIXTURE, BACKUP_FIXTURE] {
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

    for (fixture, pre_o212_bytes) in [
        (PRIMARY_FIXTURE, FROZEN_PRE_O212_PRIMARY_PLAN_BYTES),
        (CUBIC_FIXTURE, FROZEN_PRE_O212_CUBIC_PLAN_BYTES),
        (BACKUP_FIXTURE, FROZEN_PRE_O212_BACKUP_PLAN_BYTES),
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
        let remote_staging_words = report
            .planes
            .iter()
            .find(|plane| plane.name == "remote_staging")
            .expect("E5 Metal plan must report remote staging")
            .words as u128;
        assert_eq!(remote_staging_words % 12, 0);
        let compact_reduction_bytes = (report.event_arenas.channel_stream_event_slots as u128 * 3
            + report.event_arenas.service_stream_event_slots as u128 * 9
            + report.event_arenas.generator_stream_event_slots as u128 * 4
            + remote_staging_words / 12 * 2)
            * 8;
        let expected_bytes = pre_o212_bytes
            .checked_sub(compact_reduction_bytes)
            .expect("compact planes must fit the prior frozen plan");
        println!(
            "E5 exact capped Metal plan {fixture}: bytes={derived_total} gib={:.9} \
             channel_slots={} service_slots={} generator_slots={} remote_staging_words={} \
             compact_reduction_bytes={compact_reduction_bytes} headroom_bytes={}",
            derived_total as f64 / GIB_BYTES as f64,
            report.event_arenas.channel_stream_event_slots,
            report.event_arenas.service_stream_event_slots,
            report.event_arenas.generator_stream_event_slots,
            remote_staging_words,
            DEVICE_CAP_BYTES - derived_total
        );
        assert_eq!(derived_total, expected_bytes);
        assert!(derived_total <= DEVICE_CAP_BYTES);
        let tcp_state_bytes = report
            .planes
            .iter()
            .find(|plane| plane.name == "tcp_state")
            .expect("E5 Metal plan must report tcp_state")
            .bytes as u128;
        assert_eq!(tcp_state_bytes % 8, 0);
        let tcp_words_per_flow = (tcp_state_bytes / 8) / FLOW_COUNT as u128;
        let ledger_words_per_flow = tcp_words_per_flow
            .checked_sub(E5_TCP_FIXED_WORDS_PER_FLOW)
            .expect("TCP fixed state must fit its plane");
        assert_eq!(ledger_words_per_flow % TCP_LEDGER_RECORD_WORDS, 0);
        assert_eq!(
            ledger_words_per_flow / TCP_LEDGER_RECORD_WORDS,
            FROZEN_PRIMARY_LEDGER_RECORDS_PER_FLOW_CAP as u128,
            "{fixture} capped plan's per-flow ledger capacity moved"
        );
        assert_eq!(
            (derived_total * 1_000 + GIB_BYTES / 2) / GIB_BYTES,
            (expected_bytes * 1_000 + GIB_BYTES / 2) / GIB_BYTES
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
    // the design study's inferred data-attempt-excess definition.
    let original_segments = FLOW_COUNT as u128 * u128::from(FLOW_BYTES.div_ceil(MSS_BYTES));
    let data_attempts = run
        .result
        .summary
        .sourced_packets
        .saturating_sub(run.result.summary.received_packets);
    let data_attempt_excess = data_attempts.saturating_sub(original_segments);
    let last_round = run
        .rounds
        .last()
        .expect("E5 must execute at least one round");
    let anchor = fingerprint(&run.result);

    println!(
        "E5 PRIMARY flow-set diagnostic: completed={completed}/{FLOW_COUNT} demanded={demanded} \
         acked={acked} drops={} dataAttemptExcess={data_attempt_excess} rounds={} \
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
        data_attempt_excess, FROZEN_PRIMARY_DATA_ATTEMPT_EXCESS,
        "E5 flow-set fixture does not reproduce the frozen inferred data-attempt excess; \
         stop and report"
    );
    assert_eq!(run.rounds.len(), FROZEN_PRIMARY_ROUNDS);
    assert_eq!(last_round.frontier_ns, PRIMARY_COMPLETION_NS);
    assert_eq!(transitions, FROZEN_PRIMARY_TRANSITIONS);
    assert_eq!(
        weighted_width_numerator,
        FROZEN_PRIMARY_WEIGHTED_WIDTH_NUMERATOR
    );
    assert_eq!(
        weighted_width_ceiling,
        FROZEN_PRIMARY_WEIGHTED_WIDTH_CEILING
    );
    assert_eq!(wide_transitions, FROZEN_PRIMARY_WIDE_TRANSITIONS);
    assert_eq!(
        wide_share_tenths_percent,
        FROZEN_PRIMARY_WIDE_SHARE_TENTHS_PERCENT
    );
    assert_eq!(
        last_round.exclusive_horizon_ns,
        FROZEN_PRIMARY_EXCLUSIVE_HORIZON_NS
    );
    assert_primary_anchor(anchor);
}

#[test]
#[ignore = "E5 CUBIC scalar trajectory/width gate: 8,192 x 1 MiB TCP flows to completion"]
fn e5_cubic_flow_set_fixture_reproduces_the_frozen_scalar_probe() {
    let image = lower(CUBIC_FIXTURE);
    let run = run_scalar_rounds_with_observations(&image, None, ObservationMode::Summary)
        .expect("CUBIC E5 scalar round run must succeed");

    // Report the complete candidate trajectory before comparing any frozen value. A mismatch is a
    // diagnostic stop, never authority to silently re-pin the sensitivity row.
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
    let original_segments = FLOW_COUNT as u128 * u128::from(FLOW_BYTES.div_ceil(MSS_BYTES));
    let data_attempts = run
        .result
        .summary
        .sourced_packets
        .saturating_sub(run.result.summary.received_packets);
    let data_attempt_excess = data_attempts.saturating_sub(original_segments);
    let last_round = run.rounds.last().expect("CUBIC E5 must execute");
    let anchor = fingerprint(&run.result);

    println!(
        "E5 CUBIC flow-set diagnostic: completed={completed}/{FLOW_COUNT} demanded={demanded} \
         acked={acked} drops={} dataAttemptExcess={data_attempt_excess} rounds={} \
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

    assert_eq!(completed, FLOW_COUNT, "every CUBIC E5 flow must complete");
    assert_eq!(demanded, TOTAL_BYTES, "CUBIC E5 demand changed");
    assert_eq!(acked, TOTAL_BYTES, "CUBIC E5 ACKed bytes changed");
    assert_eq!(bytes_in_flight, 0, "completed CUBIC E5 must have no flight");
    assert_eq!(armed_timers, 0, "completed CUBIC E5 must have no timer");
    assert!(
        run.result.resident_packets.is_empty(),
        "CUBIC E5 must drain"
    );
    assert!(run.result.pending_events.is_empty(), "CUBIC E5 must drain");
    assert_eq!(run.result.summary.dropped_packets, FROZEN_CUBIC_DROPS);
    assert_eq!(data_attempt_excess, FROZEN_CUBIC_DATA_ATTEMPT_EXCESS);
    assert_eq!(run.rounds.len(), FROZEN_CUBIC_ROUNDS);
    assert_eq!(last_round.frontier_ns, CUBIC_COMPLETION_NS);
    assert_eq!(transitions, FROZEN_CUBIC_TRANSITIONS);
    assert_eq!(
        weighted_width_numerator,
        FROZEN_CUBIC_WEIGHTED_WIDTH_NUMERATOR
    );
    assert_eq!(weighted_width_ceiling, FROZEN_CUBIC_WEIGHTED_WIDTH_CEILING);
    assert!(
        weighted_width_ceiling >= 10_000,
        "CUBIC E5 collapsed below the approved wide-row floor: {weighted_width_ceiling}"
    );
    assert_eq!(wide_transitions, FROZEN_CUBIC_WIDE_TRANSITIONS);
    assert_eq!(
        wide_share_tenths_percent,
        FROZEN_CUBIC_WIDE_SHARE_TENTHS_PERCENT
    );
    assert_eq!(
        last_round.exclusive_horizon_ns,
        FROZEN_CUBIC_EXCLUSIVE_HORIZON_NS
    );
    assert_cubic_anchor(anchor);
}

#[test]
#[ignore = "E5 CUBIC Full-observation explicit retransmission gate"]
fn e5_cubic_full_observation_retransmissions_are_frozen() {
    let run =
        run_scalar_rounds_with_observations(&lower(CUBIC_FIXTURE), None, ObservationMode::Full)
            .expect("CUBIC E5 Full-observation scalar run must succeed");
    let retransmissions = run
        .result
        .observed_packets
        .iter()
        .filter(|packet| {
            matches!(
                packet.kind,
                PacketKind::TcpData(header) if header.retransmission
            )
        })
        .count() as u128;
    let last_round = run.rounds.last().expect("CUBIC E5 must execute");
    let completed = run
        .result
        .host_states
        .iter()
        .flat_map(|host| &host.generators)
        .filter(|generator| {
            matches!(
                generator.kind,
                FlowGeneratorKind::Tcp(tcp) if tcp.highest_ack >= tcp.total_bytes
            )
        })
        .count();
    println!(
        "E5 CUBIC Full diagnostic: completed={completed}/{FLOW_COUNT} drops={} \
         explicitRetransmissions={retransmissions} rounds={} completionNs={}",
        run.result.summary.dropped_packets,
        run.rounds.len(),
        last_round.frontier_ns,
    );

    assert_eq!(completed, FLOW_COUNT);
    assert_eq!(run.result.summary.dropped_packets, FROZEN_CUBIC_DROPS);
    assert_eq!(retransmissions, FROZEN_CUBIC_EXPLICIT_RETRANSMISSIONS);
    assert_eq!(run.rounds.len(), FROZEN_CUBIC_ROUNDS);
    assert_eq!(last_round.frontier_ns, CUBIC_COMPLETION_NS);
}

#[test]
#[ignore = "E5 CUBIC completion anchor on CPU with 2 workers"]
fn e5_cubic_completion_anchor_is_identical_on_cpu_x2() {
    let image = lower(CUBIC_FIXTURE);
    validate(&image, Backend::Cpu { workers: 2 }).expect("E5 CUBIC CPU x2 validation failed");
    let run = run_cpu_with_observations(
        &image,
        None,
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        ObservationMode::Summary,
    )
    .expect("E5 CUBIC CPU x2 run failed");
    let anchor = fingerprint(&run.result);
    let last_round = run.rounds.last().expect("E5 CUBIC CPU run must execute");
    let transitions = run
        .rounds
        .iter()
        .map(|round| u128::from(round.semantic.events_processed))
        .sum::<u128>();
    println!(
        "E5 CUBIC CPU x2 anchor: bytes={} fnv1a64={:016x} rounds={} \
         transitions={transitions} completionNs={}",
        anchor.bytes,
        anchor.fnv1a64,
        run.rounds.len(),
        last_round.semantic.frontier_ns,
    );
    assert_cubic_anchor(anchor);
    assert_eq!(run.rounds.len(), FROZEN_CUBIC_ROUNDS);
    assert_eq!(transitions, FROZEN_CUBIC_TRANSITIONS);
    assert_eq!(last_round.semantic.frontier_ns, CUBIC_COMPLETION_NS);
}

#[test]
#[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
#[ignore = "E5 CUBIC completion anchor on production Metal with the capped device plan"]
fn e5_cubic_completion_anchor_is_identical_on_metal() {
    use days_executor::{MetalConfig, MetalExecutor};

    let image = lower(CUBIC_FIXTURE);
    validate(&image, Backend::Metal).expect("E5 CUBIC Metal validation failed");
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
        .expect("E5 CUBIC Metal completion run failed");
    let anchor = fingerprint(&run.result);
    println!(
        "E5 CUBIC Metal anchor: bytes={} fnv1a64={:016x} rounds={} transitions={} \
         capacityRetries={}",
        anchor.bytes,
        anchor.fnv1a64,
        run.rounds,
        run.transitions,
        run.capacity_retry_trace.len(),
    );
    assert_cubic_anchor(anchor);
    assert_eq!(run.rounds, FROZEN_CUBIC_ROUNDS as u64);
    assert_eq!(run.transitions, FROZEN_CUBIC_TRANSITIONS as u64);
    assert!(
        run.capacity_retry_trace.is_empty(),
        "E5 CUBIC's frozen capped plan must run without capacity repair: {:?}",
        run.capacity_retry_trace
    );
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
#[cfg(all(feature = "metal-test-hooks", target_vendor = "apple"))]
#[ignore = "E5 primary completion anchor on production Metal with the capped device plan"]
fn e5_primary_completion_anchor_is_identical_on_metal() {
    use days_executor::{
        ArenaOccupancyHighWater, MetalConfig, MetalExecutor,
        take_dominant_arena_high_water_for_testing,
    };

    fn assert_high_water(
        arena: &str,
        occupancy: &ArenaOccupancyHighWater,
        authored_cap: Option<u64>,
    ) {
        assert_eq!(
            occupancy.high_water.len(),
            occupancy.capacities.len(),
            "{arena}: every planned entity needs a high-water mark"
        );
        assert!(
            occupancy.high_water.iter().any(|observed| *observed > 0),
            "{arena}: E5 must exercise the arena"
        );
        for (entity, (&observed, &capacity)) in occupancy
            .high_water
            .iter()
            .zip(&occupancy.capacities)
            .enumerate()
        {
            assert!(
                observed <= capacity,
                "{arena} entity {entity}: high-water {observed} exceeded planned capacity {capacity}"
            );
            if let Some(cap) = authored_cap {
                assert!(
                    capacity <= cap,
                    "{arena} entity {entity}: final planned capacity {capacity} exceeded authored cap {cap}"
                );
            }
        }
        let (peak_entity, &peak) = occupancy
            .high_water
            .iter()
            .enumerate()
            .max_by_key(|(_, observed)| *observed)
            .expect("E5 arena must have entities");
        println!(
            "E5 PRIMARY capped Metal {arena} high-water: peak={peak} entity={peak_entity} \
             capacityAtPeak={}",
            occupancy.capacities[peak_entity]
        );
    }

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
    let high_water = take_dominant_arena_high_water_for_testing()
        .expect("completed E5 Metal attempt must publish arena high-water vectors");
    assert_eq!(
        high_water.stream_records.high_water.len(),
        image.channels.len() + image.nodes.len() + image.flows.len()
    );
    assert_eq!(
        high_water.remote_staging.high_water.len(),
        image.nodes.len()
    );
    assert_eq!(high_water.queue_records.high_water.len(), image.nodes.len());
    assert_high_water("stream_records", &high_water.stream_records, None);
    assert_high_water(
        "remote_staging",
        &high_water.remote_staging,
        E5_CAPACITY_CAPS
            .remote_staging_events_per_lp
            .map(|capacity| capacity as u64),
    );
    assert_high_water(
        "queue_records",
        &high_water.queue_records,
        E5_CAPACITY_CAPS
            .queue_packets_per_lp
            .map(|capacity| capacity as u64),
    );
    let channel_cap = E5_CAPACITY_CAPS
        .channel_events_per_stream
        .expect("E5 capped plan must cap channel streams") as u64;
    assert!(
        high_water.stream_records.capacities[..image.channels.len()]
            .iter()
            .all(|capacity| *capacity <= channel_cap),
        "E5 channel-stream plan exceeded the authored cap"
    );
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
    let data_attempt_excess = data_attempts.saturating_sub(original_segments);
    let last_round = run.rounds.last().expect("E5 backup must execute");
    let transitions = run
        .rounds
        .iter()
        .map(|round| u128::from(round.events_processed))
        .sum::<u128>();
    println!(
        "E5 BACKUP flow-set diagnostic: completed={completed}/{FLOW_COUNT} demanded={demanded} \
         acked={acked} drops={} dataAttemptExcess={data_attempt_excess} rounds={} transitions={} \
         finalFrontierNs={} finalExclusiveHorizonNs={} residentPackets={} pendingEvents={}",
        run.result.summary.dropped_packets,
        run.rounds.len(),
        transitions,
        last_round.frontier_ns,
        last_round.exclusive_horizon_ns,
        run.result.resident_packets.len(),
        run.result.pending_events.len(),
    );
    assert_eq!(completed, FLOW_COUNT);
    assert_eq!(demanded, TOTAL_BYTES);
    assert_eq!(acked, TOTAL_BYTES);
    assert_eq!(run.result.summary.dropped_packets, FROZEN_BACKUP_DROPS);
    assert_eq!(data_attempt_excess, FROZEN_BACKUP_DATA_ATTEMPT_EXCESS);
    assert_eq!(run.rounds.len(), FROZEN_BACKUP_ROUNDS);
    assert_eq!(transitions, FROZEN_BACKUP_TRANSITIONS);
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
