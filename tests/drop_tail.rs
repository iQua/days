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

/// In this test, we force TailDrop to discard packets by
/// using a very small buffer and a high packet rate.
/// TailDrop only drops packets when the buffer is completely full.
#[test]
fn test_drop_strategy_taildrop_small_buffer() {
    let log_path = "logs/drop_test_taildrop_small_buffer";
    if let Err(e) = CsvLogger::get_instance().init(log_path) {
        panic!("Failed to initialize CsvLogger ({}): {}", log_path, e);
    }

    // high rate traffic (uniform inter-arrival at 0.1s) and moderately sized
    // packets (1000 bytes)
    let mut source = PacketSource::new(
        0,
        Vec::new(),
        FlowType::PacketDistribution,
        TrafficCharacteristics::new(
            0.0,       // no initial delay
            Some(5.0), // run 5 seconds
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
        None,
    );

    // a low-capacity (2 packets) TailDrop queue—will only drop when full
    let mut port = Port::new(
        150_000.0, // link rate (bits per second)
        2,         // 2-packet capacity
        CapacityUnit::Packets,
        DropStrategy::TailDrop,
    );

    let mut sink = PacketSink::new(&source);

    // sets up mailboxes and connect source -> port -> sink
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
        .add_model(source, source_mbox, "TailDropSource")
        .add_model(port, port_mbox, "TailDropPort")
        .add_model(sink, sink_mbox, "TailDropSink")
        .init(t0)
    {
        Ok((mut sim, _)) => {
            let _ = sim.step_until(Duration::from_secs(10));
            let _ = sim.process_event(PacketSink::report, sink_id, &sink_addr);

            let packets_sent = CsvLogger::get_instance().total_packets_sent();
            if let Some(statistics) = sink_statistics.next() {
                info!("{:#.3}", statistics);
                // we expect at least one packet drop once the buffer is full
                assert!(
                    packets_sent > statistics.packets.len(),
                    "TailDrop test: expected some packets to be dropped, but none were."
                );
            } else {
                panic!("No statistics were reported by the sink for TailDrop test.");
            }

            info!(
                "TailDrop test completed at time {:.3}.",
                sim.time().duration_since(t0).as_secs_f64()
            );
        }
        Err(_) => panic!("Failed to initialize the simulation for TailDrop test."),
    }
}
