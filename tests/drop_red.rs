#![cfg(feature = "test")]

use std::time::Duration;

use log::info;
use nexosim::ports::EventSlot;
use nexosim::simulation::{Mailbox, SimInit};
use nexosim::time::MonotonicTime;

use daytone::flows::flow::FlowType;
use daytone::flows::sink::PacketSink;
use daytone::flows::source::PacketSource;
use daytone::flows::{DistributionInfo, TrafficCharacteristics};
use daytone::schedulers::drop::{CapacityUnit, DropStrategy};
use daytone::schedulers::port::Port;
use daytone::utils::logger::CsvLogger;

/// In this test, RED (Random Early Detection) is configured so that
/// it can begin dropping packets at certain average queue sizes —— even
/// when the buffer hasn’t reached its max capacity. We use a larger
/// buffer and more frequent packet arrivals so we see early drops.
#[test]
fn test_drop_strategy_red_early_drop() {
    let log_path = "logs/drop_test_red_early_drop";
    if let Err(e) = CsvLogger::get_instance().init(log_path) {
        panic!("Failed to initialize CsvLogger ({}): {}", log_path, e);
    }

    // faster arrivals (0.05s) so that we can fill the queue sufficiently
    // for RED to trigger early drops
    let mut source = PacketSource::new(
        0,
        Vec::new(),
        FlowType::PacketDistribution,
        TrafficCharacteristics::new(
            0.0,       // no initial delay
            Some(5.0), // duration
            None,
            DistributionInfo::Uniform {
                low: 0.05,
                high: 0.05,
            },
            DistributionInfo::DiscreteUniform {
                low: 1000,
                high: 1000,
            },
            None,
        ),
        0,
    );

    // larger capacity (e.g., 10 packets) so pure TailDrop wouldn't drop
    // many packets this early; RED's early-drop mechanism should kick in
    // once average queue size rises above min_threshold
    let mut port = Port::new(
        10_000.0, // link rate
        10,       // 10-packet capacity
        CapacityUnit::Packets,
        DropStrategy::RED,
    );

    let mut sink = PacketSink::new(&source);

    // sets up mailboxes and connects source -> port -> sink
    let source_mbox = Mailbox::new();
    let port_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let sink_addr = sink_mbox.address();
    let sink_id = sink.id();

    source.output().connect(Port::packet_received, &port_mbox);
    port.output.connect(PacketSink::packet_received, &sink_mbox);

    let mut sink_statistics = EventSlot::new();
    sink.statistics().connect_sink(&sink_statistics);

    let t0 = MonotonicTime::EPOCH;
    match SimInit::new()
        .add_model(source, source_mbox, "REDSource")
        .add_model(port, port_mbox, "REDPort")
        .add_model(sink, sink_mbox, "REDSink")
        .init(t0)
    {
        Ok((mut sim, _)) => {
            let _ = sim.step_until(Duration::from_secs(10));
            let _ = sim.process_event(PacketSink::report, sink_id, &sink_addr);

            let packets_sent = CsvLogger::get_instance().total_packets_sent();
            if let Some(statistics) = sink_statistics.next() {
                info!("{:#.3}", statistics);
                // We expect that RED has dropped packets even before queue is truly "full".
                assert!(
                    packets_sent > statistics.packets.len(),
                    "RED test: expected some packets to be dropped via early detection."
                );
            } else {
                panic!("No statistics were reported by the sink for RED test.");
            }

            info!(
                "RED test completed at time {:.3}.",
                sim.time().duration_since(t0).as_secs_f64()
            );
        }
        Err(_) => panic!("Failed to initialize the simulation for RED test"),
    }
}
