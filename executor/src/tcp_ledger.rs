use std::collections::{BTreeMap, BTreeSet};

use crate::{
    EventKind, FlowGeneratorKind, FlowId, PacketDescriptor, PacketKind, PayloadId, SimulationImage,
};

pub(crate) type TcpSegmentLedger = BTreeMap<FlowId, BTreeMap<u64, PacketDescriptor>>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TcpSegmentConflict {
    pub flow: FlowId,
    pub sequence: u64,
    pub original_size_bytes: u64,
    pub replacement_size_bytes: u64,
}

pub(crate) fn initial_live_payloads(image: &SimulationImage) -> BTreeSet<PayloadId> {
    image
        .initial_events
        .iter()
        .filter(|event| event.kind != EventKind::RetransmissionTimeout)
        .map(|event| event.payload)
        .chain(
            image
                .host_states
                .iter()
                .flat_map(|state| state.queue.iter().copied().chain(state.in_service)),
        )
        .chain(image.switch_states.iter().flat_map(|state| {
            state
                .queues
                .iter()
                .flat_map(|queue| queue.queue.iter().copied().chain(queue.in_service))
        }))
        .collect()
}

pub(crate) fn seed_image(image: &SimulationImage) -> Result<TcpSegmentLedger, TcpSegmentConflict> {
    let mut segments = TcpSegmentLedger::new();
    for packet in image.initial_packets.iter().copied() {
        seed_segment(&mut segments, packet)?;
    }
    normalize_image(&mut segments, image)?;
    Ok(segments)
}

pub(crate) fn seed_packets(
    image: &SimulationImage,
    packets: impl IntoIterator<Item = PacketDescriptor>,
) -> Result<TcpSegmentLedger, TcpSegmentConflict> {
    let mut segments = TcpSegmentLedger::new();
    for packet in packets {
        seed_segment(&mut segments, packet)?;
    }
    normalize_image(&mut segments, image)?;
    Ok(segments)
}

fn normalize_image(
    segments: &mut TcpSegmentLedger,
    image: &SimulationImage,
) -> Result<(), TcpSegmentConflict> {
    // `acknowledge_segments` is a total no-op on a flow the ledger does not carry, so an empty
    // ledger cannot be changed by any generator. Skipping the whole-image generator walk in that
    // case keeps the per-LP seeding of a TCP-free image out of `O(nodes x generators)`.
    if segments.is_empty() {
        return Ok(());
    }
    for generator in image.host_states.iter().flat_map(|state| &state.generators) {
        if let FlowGeneratorKind::Tcp(tcp) = generator.kind {
            acknowledge_segments(segments, generator.flow, tcp.highest_ack)?;
        }
    }
    Ok(())
}

pub(crate) fn seed_segment(
    segments: &mut TcpSegmentLedger,
    packet: PacketDescriptor,
) -> Result<(), TcpSegmentConflict> {
    let PacketKind::TcpData(header) = packet.kind else {
        return Ok(());
    };
    let flow_segments = segments.entry(packet.flow).or_default();
    if let Some(original) = flow_segments.insert(header.sequence, packet) {
        if original.size_bytes != packet.size_bytes {
            return Err(TcpSegmentConflict {
                flow: packet.flow,
                sequence: header.sequence,
                original_size_bytes: original.size_bytes,
                replacement_size_bytes: packet.size_bytes,
            });
        }
    }
    Ok(())
}

pub(crate) fn acknowledge_segments(
    segments: &mut TcpSegmentLedger,
    flow: FlowId,
    acknowledgment: u64,
) -> Result<(), TcpSegmentConflict> {
    let Some(flow_segments) = segments.get_mut(&flow) else {
        return Ok(());
    };
    let mut unacknowledged = flow_segments.split_off(&acknowledgment);
    if let Some((sequence, mut packet)) = flow_segments.pop_last() {
        let acknowledged_bytes = acknowledgment - sequence;
        if acknowledged_bytes < packet.size_bytes {
            packet.size_bytes -= acknowledged_bytes;
            let PacketKind::TcpData(ref mut header) = packet.kind else {
                unreachable!("TCP segment ledgers contain only TCP data")
            };
            header.sequence = acknowledgment;
            if let Some(original) = unacknowledged.get(&acknowledgment) {
                if original.size_bytes != packet.size_bytes {
                    return Err(TcpSegmentConflict {
                        flow,
                        sequence: acknowledgment,
                        original_size_bytes: original.size_bytes,
                        replacement_size_bytes: packet.size_bytes,
                    });
                }
            } else {
                unacknowledged.insert(acknowledgment, packet);
            }
        }
    }
    *flow_segments = unacknowledged;
    if flow_segments.is_empty() {
        segments.remove(&flow);
    }
    Ok(())
}
