//! P18 ackfix: the window-throttle design's F1 (`days-gpu/evidence/P18/window-throttle/design.md`
//! §3.1, user-approved Oct 13, recorded as decision D16b): at a host's NIC, every RoCE ACK, RoCE
//! NACK and DCQCN CNP is served ahead of every other queued packet, non-preemptively and in
//! today's order among themselves. TCP ACKs, PFC frames and data keep the FIFO order.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use days::scenario::compile_config;
use days_executor::{
    ArrivalDisposition, Backend, CpuConfig, MechanismTransitionRecord, NodeId, ObservationMode,
    PacketKind, PayloadId, RoceTransitionRecord, RunResult, SimulationImage,
    run_cpu_with_observations, run_scalar_with_observations, serialization_time_ns, validate,
};

const ACK_FIRST: &str = "configs/p18/roce_ack_first.toml";
const CNP_FIRST: &str = "configs/p18/dcqcn_cnp_first.toml";

fn repo_path(relative: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(relative)
}

fn lower(relative: &str) -> SimulationImage {
    compile_config(repo_path(relative))
        .unwrap_or_else(|error| panic!("{relative} must lower: {error}"))
}

fn run_full(image: &SimulationImage) -> RunResult {
    run_scalar_with_observations(image, None, ObservationMode::Full).expect("the fixture runs")
}

/// One packet a host sourced into its NIC queue, as the full-observation records show it.
#[derive(Clone, Copy, Debug)]
struct Sourced {
    payload: PayloadId,
    /// The instant it entered the host queue: the sender transition that emitted it (data) or
    /// the receiver transition that answered with it (an ACK or NACK).
    enqueued_ns: u64,
    /// A RoCE ACK or NACK (F1's control class).
    control: bool,
    /// Its service start at the host link: departure minus serialization.
    started_ns: u64,
    departed_ns: u64,
}

/// Every RoCE data packet, ACK and NACK each host sourced, with its queue entry, service start and
/// departure.
fn host_timelines(image: &SimulationImage, result: &RunResult) -> BTreeMap<NodeId, Vec<Sourced>> {
    let records = &result
        .diagnostics
        .as_ref()
        .expect("full observation")
        .mechanism_transitions;
    // A packet departs once per hop; its first departure is from its source host.
    let mut departures = BTreeMap::<PayloadId, u64>::new();
    for departure in &result.departures {
        departures
            .entry(departure.payload)
            .and_modify(|time_ns| *time_ns = (*time_ns).min(departure.time_ns))
            .or_insert(departure.time_ns);
    }
    let packets = result
        .observed_packets
        .iter()
        .map(|packet| (packet.id, *packet))
        .collect::<BTreeMap<_, _>>();
    let mut timelines = BTreeMap::<NodeId, Vec<Sourced>>::new();
    for record in records {
        let MechanismTransitionRecord::Roce(record) = record else {
            continue;
        };
        let (node, time_ns, payload, control) = match record {
            RoceTransitionRecord::Sender(sender) => match sender.emitted {
                Some(emission) => (sender.node, sender.key.time_ns, emission.payload, false),
                None => continue,
            },
            RoceTransitionRecord::Receiver(receiver) => match receiver.feedback_payload {
                Some(payload) => (receiver.node, receiver.key.time_ns, payload, true),
                None => continue,
            },
        };
        let packet = packets[&payload];
        assert_eq!(
            control,
            matches!(
                packet.kind,
                PacketKind::RoceAck(_) | PacketKind::RoceNack(_)
            ),
            "{packet:?}"
        );
        let Some(&departed_ns) = departures.get(&payload) else {
            continue;
        };
        let link = image
            .links
            .iter()
            .find(|link| link.source == node)
            .expect("a host's egress link");
        let serialization = serialization_time_ns(packet.size_bytes, link.rate_bps).unwrap();
        timelines.entry(node).or_default().push(Sourced {
            payload,
            enqueued_ns: time_ns,
            control,
            started_ns: departed_ns - serialization,
            departed_ns,
        });
    }
    timelines
}

/// The `(control, data)` pairs of one host where the data entered the queue no later than the
/// control packet and had not started service when it did: F1 sends the control packet first.
fn overtaking_pairs(timeline: &[Sourced]) -> Vec<(Sourced, Sourced)> {
    let mut pairs = Vec::new();
    for control in timeline.iter().filter(|packet| packet.control) {
        for data in timeline.iter().filter(|packet| !packet.control) {
            if data.enqueued_ns <= control.enqueued_ns && data.started_ns > control.enqueued_ns {
                pairs.push((*control, *data));
            }
        }
    }
    pairs
}

#[test]
fn a_roce_ack_queued_behind_data_departs_first() {
    let image = lower(ACK_FIRST);
    let result = run_full(&image);
    let timelines = host_timelines(&image, &result);
    assert_eq!(timelines.len(), 2, "both hosts send data and acknowledge");
    let mut exercised = 0;
    for (node, timeline) in &timelines {
        // Non-preemptive: a packet in service when a control packet arrives still departs first,
        // so service intervals never overlap.
        let mut by_start = timeline.clone();
        by_start.sort_by_key(|packet| packet.started_ns);
        for pair in by_start.windows(2) {
            assert!(
                pair[0].departed_ns <= pair[1].started_ns,
                "{node:?}: overlapping service {pair:?}"
            );
        }
        for (control, data) in overtaking_pairs(timeline) {
            assert!(
                control.departed_ns < data.departed_ns,
                "{node:?}: the ACK {:?} queued at {} waits behind data {:?} queued at {} that \
                 had not started (it starts at {})",
                control.payload,
                control.enqueued_ns,
                data.payload,
                data.enqueued_ns,
                data.started_ns
            );
            exercised += 1;
        }
    }
    assert!(
        exercised > 0,
        "the fixture queues an ACK while its host's data waits"
    );
    let finished = result
        .host_states
        .iter()
        .flat_map(|state| &state.generators)
        .filter(|generator| {
            generator.next_emission.status == days_executor::GeneratorStatus::Finished
        })
        .count();
    assert_eq!(finished, 2, "both queue pairs complete");
}

#[test]
fn the_ack_first_fixture_matches_cpu_at_one_to_four_workers() {
    let image = lower(ACK_FIRST);
    let scalar = run_full(&image);
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
        .unwrap_or_else(|error| panic!("CPU with {workers} workers: {error}"));
        assert!(cpu.result == scalar, "CPU with {workers} workers differs");
    }
}

/// The checkpoint's resumed image: the prefix's state and pending work over the original image.
fn resumed_from(image: &SimulationImage, prefix: &RunResult) -> SimulationImage {
    let mut resumed = image.clone();
    resumed.host_states.clone_from(&prefix.host_states);
    resumed.switch_states.clone_from(&prefix.switch_states);
    resumed.initial_packets.clone_from(&prefix.resident_packets);
    resumed.initial_events.clone_from(&prefix.pending_events);
    resumed
}

/// A checkpoint taken while a host queue holds an ACK ahead of data that was queued before it
/// (restored packets lose their source instants, design note §3.1): the resumed run equals the
/// uninterrupted one on Scalar and on CPU at one to four workers, and the checkpoint validates.
#[test]
fn a_checkpoint_with_an_ack_queued_ahead_of_data_resumes_exactly() {
    let image = lower(ACK_FIRST);
    let full = run_full(&image);
    let uninterrupted =
        run_scalar_with_observations(&image, None, ObservationMode::Summary).expect("runs");
    let timelines = host_timelines(&image, &full);
    // The first overtake of data queued strictly before the ACK, whose ACK still waits after it
    // was queued: at a horizon at its service start, the queue holds the ACK and the older data
    // behind it.
    let (node, control, data) = timelines
        .iter()
        .flat_map(|(node, timeline)| {
            overtaking_pairs(timeline)
                .into_iter()
                .map(move |(control, data)| (*node, control, data))
        })
        .filter(|(_, control, data)| {
            data.enqueued_ns < control.enqueued_ns && control.started_ns > control.enqueued_ns
        })
        .min_by_key(|(_, control, data)| (control.started_ns, control.payload, data.payload))
        .expect("an ACK waits in a queue that holds older data");
    assert!(
        control.started_ns < data.started_ns,
        "the ACK {control:?} is served before the older data {data:?}"
    );
    let horizon = control.started_ns;
    let prefix = run_scalar_with_observations(&image, Some(horizon), ObservationMode::Summary)
        .expect("the prefix runs");
    let slot = image
        .nodes
        .iter()
        .find(|descriptor| descriptor.id == node)
        .unwrap()
        .state_slot as usize;
    let queue = prefix.host_states[slot]
        .queue
        .iter()
        .copied()
        .collect::<Vec<_>>();
    let position = |payload| {
        queue
            .iter()
            .position(|queued| *queued == payload)
            .unwrap_or_else(|| panic!("{payload:?} is queued at the horizon: {queue:?}"))
    };
    assert!(
        position(control.payload) < position(data.payload),
        "the ACK is queued ahead of the older data: {queue:?}"
    );
    let resumed = resumed_from(&image, &prefix);
    validate(&resumed, Backend::Scalar).expect("the checkpoint validates");
    let finished = run_scalar_with_observations(&resumed, None, ObservationMode::Summary)
        .expect("the checkpoint runs");
    assert_eq!(finished.host_states, uninterrupted.host_states);
    assert_eq!(finished.switch_states, uninterrupted.switch_states);
    assert_eq!(finished.pending_events, uninterrupted.pending_events);
    assert_eq!(finished.resident_packets, uninterrupted.resident_packets);
    for workers in 1..=4 {
        let cpu = run_cpu_with_observations(
            &resumed,
            None,
            CpuConfig {
                workers,
                ..CpuConfig::default()
            },
            ObservationMode::Summary,
        )
        .unwrap_or_else(|error| panic!("CPU with {workers} workers: {error}"));
        assert!(
            cpu.result == finished,
            "CPU with {workers} workers differs from Scalar after the checkpoint"
        );
    }
}

/// A DCQCN CNP is a control packet too: queued while its host's data waits, it departs ahead of
/// every packet that had not started service. Its queue entry is the delivery of the data packet
/// that triggered it; a host allocates payload sequences in sourcing order, so a smaller payload
/// of the same host was queued earlier.
#[test]
fn a_dcqcn_cnp_queued_behind_data_departs_first() {
    let image = lower(CNP_FIRST);
    let result = run_full(&image);
    let node_count = image.nodes.len() as u64;
    let mut first_departure = BTreeMap::<PayloadId, u64>::new();
    for departure in &result.departures {
        first_departure
            .entry(departure.payload)
            .and_modify(|time_ns| *time_ns = (*time_ns).min(departure.time_ns))
            .or_insert(departure.time_ns);
    }
    let delivered = result
        .arrivals
        .iter()
        .filter(|arrival| arrival.disposition == ArrivalDisposition::Delivered)
        .map(|arrival| (arrival.payload, arrival.time_ns))
        .collect::<BTreeMap<_, _>>();
    // (payload, control, queued at for a CNP, service start, departure) per source host.
    let mut hosts = BTreeMap::<u64, Vec<(PayloadId, Option<u64>, u64, u64)>>::new();
    for packet in &result.observed_packets {
        let Some(&departed_ns) = first_departure.get(&packet.id) else {
            continue;
        };
        let host = packet.id.0 % node_count;
        let link = image
            .links
            .iter()
            .find(|link| link.source == NodeId(host))
            .expect("a source host's egress link");
        let started_ns =
            departed_ns - serialization_time_ns(packet.size_bytes, link.rate_bps).unwrap();
        let queued_ns = match packet.kind {
            PacketKind::DcqcnCnp(header) => Some(delivered[&header.trigger_payload]),
            _ => None,
        };
        hosts
            .entry(host)
            .or_default()
            .push((packet.id, queued_ns, started_ns, departed_ns));
    }
    let mut exercised = 0;
    let mut cnps = 0;
    for (host, packets) in &hosts {
        for &(cnp, queued_ns, _, cnp_departed_ns) in packets {
            let Some(queued_ns) = queued_ns else {
                continue;
            };
            cnps += 1;
            for &(other, other_queued, started_ns, departed_ns) in packets {
                if other_queued.is_some() || started_ns <= queued_ns {
                    continue;
                }
                assert!(
                    cnp_departed_ns < departed_ns,
                    "host {host}: the CNP {cnp:?} queued at {queued_ns} departs after {other:?}, \
                     which starts service at {started_ns}"
                );
                exercised += usize::from(other < cnp);
            }
        }
    }
    assert!(cnps > 0, "the incast marks and host 0 sends CNPs");
    assert!(
        exercised > 0,
        "some CNP is queued while older data of its host waits"
    );
}

#[test]
fn the_cnp_first_fixture_matches_cpu_at_one_to_four_workers() {
    let image = lower(CNP_FIRST);
    let scalar = run_full(&image);
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
        .unwrap_or_else(|error| panic!("CPU with {workers} workers: {error}"));
        assert!(cpu.result == scalar, "CPU with {workers} workers differs");
    }
}
