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

fn run_drop_test(strategy: DropStrategy, log_path: &str) {
    // initializes the logger for this test; each test uses a different log directory
    if let Err(e) = CsvLogger::get_instance().init(log_path) {
        panic!("Failed to initialize CsvLogger ({}): {}", log_path, e);
    }

    // creates a packet source with high rate
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

    // creates a port with small capacity and the specified drop strategy
    // using 2-packet capacity to force drops in both tests.
    let mut port = Port::new(
        10_000.0, // link rate, in bits per second
        2,        // small buffer capacity
        CapacityUnit::Packets,
        strategy,
    );

    // creates a sink and connect source -> port -> sink
    let mut sink = PacketSink::new(&source);
    let source_mbox = Mailbox::new();
    let port_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let sink_addr = sink_mbox.address();
    let sink_id = sink.id();

    // connects the components
    source.output().connect(Port::packet_received, &port_mbox);
    port.output.connect(PacketSink::packet_received, &sink_mbox);

    // EventSlot for collecting statistics from the sink
    let mut sink_statistics = EventSlot::new();
    sink.statistics().connect_sink(&sink_statistics);

    // initializes and runs the simulation
    let t0 = MonotonicTime::EPOCH;
    match SimInit::new()
        .add_model(source, source_mbox, "Source")
        .add_model(port, port_mbox, "Port")
        .add_model(sink, sink_mbox, "Sink")
        .init(t0)
    {
        Ok((mut sim, _)) => {
            // runs the simulation for up to 10 seconds
            let _ = sim.step_until(Duration::from_secs(10));

            // asks the sink for a statistics report
            let _ = sim.process_event(PacketSink::report, sink_id, &sink_addr);

            // checks how many packets were sent in total
            let packets_sent = CsvLogger::get_instance().total_packets_sent();

            if let Some(statistics) = sink_statistics.next() {
                info!("{:#.3}", statistics);

                // verifies that some packets got dropped (small buffer)
                assert!(
                    packets_sent > statistics.packets.len(),
                    "Expected some packets to be dropped, but none were."
                );
            } else {
                panic!("No statistics were reported by the sink.");
            }

            info!(
                "Simulation (strategy={:?}) completed at time {:.3}.",
                strategy,
                sim.time().duration_since(t0).as_secs_f64()
            );
        }
        Err(_) => panic!(
            "Failed to initialize the simulation for strategy: {:?}",
            strategy
        ),
    }
}

#[test]
fn test_drop_strategy_taildrop() {
    run_drop_test(DropStrategy::TailDrop, "logs/drop_test_taildrop");
}

#[test]
fn test_drop_strategy_red() {
    run_drop_test(DropStrategy::RED, "logs/drop_test_red");
}
