//! P14 T4(a): a TCP ring all-reduce whose shared link drops data completes through
//! retransmission, identically on Scalar and CPU, and its stage completions still follow the
//! acknowledgement and in-order-frontier rules.

#[path = "collective_tcp.rs"]
#[allow(dead_code)]
mod tcp;

use days_executor::{
    ArrivalDisposition, CollectiveActivationCause, FlowGeneratorKind, GeneratorStatus, HostState,
    PacketKind, TcpTransitionInput,
};

/// Hosts 0 and 1 hang off switch 4, hosts 2 and 3 off switch 5. The ring order 0 -> 2 -> 1 ->
/// 3 -> 0 sends two ring hops across each direction of the 4-5 link, so their line-rate windows
/// collide in a four-packet TailDrop queue.
fn lossy_ring_config() -> String {
    tcp::tcp_collective_config("RingAllReduce", 4, 20_000, 4)
        .replace("duration = 0.05", "duration = 5.0")
        .replace(
            "edges = [[0, 4], [1, 4], [2, 4], [3, 4]]",
            "edges = [[0, 4], [1, 4], [2, 5], [3, 5], [4, 5]]",
        )
        .replace("sources = [0, 1, 2, 3]", "sources = [0, 2, 1, 3]")
        .replace("sinks = [1, 2, 3, 0]", "sinks = [2, 1, 3, 0]")
}

#[test]
fn lossy_tcp_ring_completes_identically_through_retransmission() {
    let image = tcp::compile_text("lossy-ring", &lossy_ring_config());
    let totals = tcp::tcp_total_bytes(&image);
    let result = tcp::run_everywhere(&image, "lossy ring");

    let packets = result
        .observed_packets
        .iter()
        .map(|packet| (packet.id, *packet))
        .collect::<std::collections::BTreeMap<_, _>>();
    let dropped_data = result
        .arrivals
        .iter()
        .filter(|arrival| {
            arrival.disposition == ArrivalDisposition::Dropped
                && matches!(packets[&arrival.payload].kind, PacketKind::TcpData(_))
        })
        .count();
    let retransmissions = result
        .observed_packets
        .iter()
        .filter(
            |packet| matches!(packet.kind, PacketKind::TcpData(header) if header.retransmission),
        )
        .count();
    let diagnostics = result.diagnostics.as_ref().unwrap();
    let timeouts = diagnostics
        .tcp_transitions
        .iter()
        .filter(|record| matches!(record.input, TcpTransitionInput::Timeout { .. }))
        .count();
    let duplicate_acks = diagnostics
        .tcp_transitions
        .iter()
        .filter(|record| matches!(record.input, TcpTransitionInput::DuplicateAck { .. }))
        .count();
    assert!(dropped_data > 0, "the shared link must drop ring data");
    assert!(
        retransmissions >= dropped_data,
        "every lost segment is resent"
    );
    assert!(
        timeouts > 0 && duplicate_acks > 0,
        "both recovery paths run"
    );

    assert!(result.pending_events.is_empty());
    for (generator, stage) in result
        .host_states
        .iter()
        .flat_map(HostState::generators_with_stages)
    {
        assert_eq!(generator.next_emission.status, GeneratorStatus::Finished);
        let FlowGeneratorKind::Tcp(tcp) = generator.kind else {
            unreachable!()
        };
        assert_eq!(tcp.highest_ack, tcp.total_bytes);
        assert!(stage.unwrap().activated);
    }

    // Retransmitted and duplicate bytes never advance the inbound count: every inbound row adds
    // exactly the frontier advance, and completion happens when the replayed frontier first
    // reaches the chunk.
    // Replay the receiver's in-order frontier after every delivered data arrival.
    let mut frontier_after = std::collections::BTreeMap::<(days_executor::FlowId, u64), u64>::new();
    let mut ranges = std::collections::BTreeMap::<days_executor::FlowId, Vec<(u64, u64)>>::new();
    let mut out_of_order_arrivals = 0;
    for arrival in &result.arrivals {
        if arrival.disposition != ArrivalDisposition::Delivered {
            continue;
        }
        let packet = packets[&arrival.payload];
        let PacketKind::TcpData(header) = packet.kind else {
            continue;
        };
        let flow_ranges = ranges.entry(packet.flow).or_default();
        let before = flow_ranges.iter().fold(0, |frontier, &(start, end)| {
            if start <= frontier {
                frontier.max(end)
            } else {
                frontier
            }
        });
        flow_ranges.push((header.sequence, header.sequence + packet.size_bytes));
        flow_ranges.sort_unstable();
        let after = flow_ranges.iter().fold(0, |frontier, &(start, end)| {
            if start <= frontier {
                frontier.max(end)
            } else {
                frontier
            }
        });
        if after == before {
            out_of_order_arrivals += 1;
        }
        frontier_after.insert((packet.flow, arrival.time_ns), after);
    }
    assert!(
        out_of_order_arrivals > 0,
        "loss must produce arrivals that do not advance the frontier"
    );

    let acknowledged = tcp::acknowledged_at(&result, &totals);
    let delivered = tcp::delivered_at(&result, &totals);
    let rows = tcp::progress(&result);
    for row in &rows {
        match row.cause {
            CollectiveActivationCause::LocalCompletion => {
                assert_eq!(row.key.time_ns, acknowledged[&row.cause_flow]);
            }
            CollectiveActivationCause::InboundArrival => {
                assert_eq!(
                    row.after_inbound_bytes,
                    row.before_inbound_bytes + row.arrival_bytes
                );
                assert_eq!(
                    row.after_inbound_bytes,
                    frontier_after[&(row.cause_flow, row.key.time_ns)],
                    "inbound progress tracks the in-order frontier, not raw arrivals"
                );
                if row.after_inbound_complete {
                    assert_eq!(row.key.time_ns, delivered[&row.cause_flow]);
                }
            }
        }
        if row.activated {
            let local = row.local_predecessor.map_or(0, |flow| acknowledged[&flow]);
            let inbound = row.inbound_predecessor.map_or(0, |flow| delivered[&flow]);
            assert_eq!(row.key.time_ns, local.max(inbound));
        }
    }
    assert_eq!(rows.iter().filter(|row| row.activated).count(), 20);
}
