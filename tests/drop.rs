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

#[test]
fn test_drop_strategy() {
    let _ = env_logger::builder().is_test(true).try_init();

    // initializes the logger
    if let Err(e) = CsvLogger::get_instance().init("logs/drop_test") {
        panic!("Failed to initialize CsvLogger: {}", e);
    }

    // creates packet source with high rate
    let mut source = PacketSource::new(
        0,
        Vec::new(),
        FlowType::PacketDistribution,
        TrafficCharacteristics::new(
            0.0,       // no initial delay
            Some(5.0), // duration
            None,
            DistributionInfo::Uniform {
                low: 0.1,
                high: 0.1,
            },
            DistributionInfo::DiscreteUniform {
                low: 1000,
                high: 1000,
            },
            None,
        ),
        0,
    );

    // creates FIFO port with limited capacity
    let mut port = Port::new(
        10000.0, // source rate is 80,000 bits/second
        2,       // small capacity
        CapacityUnit::Packets,
        DropStrategy::TailDrop,
    );

    let mut sink = PacketSink::new(&source);
    let source_mbox = Mailbox::new();
    let port_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let sink_addr = sink_mbox.address();
    let sink_id = sink.id();

    // connects source to port and port to sink
    source.output().connect(Port::packet_received, &port_mbox);
    port.output.connect(PacketSink::packet_received, &sink_mbox);

    let mut sink_statistics = EventSlot::new();
    sink.statistics().connect_sink(&sink_statistics);

    // initializes simulation
    let t0 = MonotonicTime::EPOCH;
    match SimInit::new()
        .add_model(source, source_mbox, "Source")
        .add_model(port, port_mbox, "FIFO")
        .add_model(sink, sink_mbox, "Sink")
        .init(t0)
    {
        Ok((mut sim, _)) => {
            // runs the simulation for 5 seconds
            let _ = sim.step_until(Duration::from_secs(10));

            // requests statistics report
            let _ = sim.process_event(PacketSink::report, sink_id, &sink_addr);

            // obtains the total number of packets sent
            let packets_sent = CsvLogger::get_instance().total_packets_sent();

            if let Some(statistics) = sink_statistics.next() {
                info!("{:#.3}", statistics);

                // Verify that some packets were dropped due to capacity limit
                assert!(
                    packets_sent > statistics.packets.len(),
                    "Expected packets to be dropped due to capacity limit."
                );
            } else {
                panic!("No statistics were reported by the sink.");
            }

            info!(
                "Simulation completed at time {:.3}.",
                sim.time().duration_since(t0).as_secs_f64()
            );
        }
        Err(_) => panic!("Failed to initialize the simulation."),
    }
}
