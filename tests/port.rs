#![cfg(feature = "test")]

use std::time::Duration;

use log::info;
use nexosim::ports::EventSlot;
use nexosim::simulation::{Mailbox, SimInit};
use nexosim::time::MonotonicTime;

use days::flows::flow::FlowType;
use days::flows::sink::PacketSink;
use days::flows::source::PacketSource;
use days::flows::{DistributionInfo, TrafficCharacteristics};
use days::schedulers::drop::{CapacityUnit, DropStrategy};
use days::schedulers::port::Port;
use days::utils::logger::CsvLogger;

#[test]
fn test_fifo_scheduling() {
    let _ = env_logger::builder().is_test(true).try_init();

    // initializes the logger
    if let Err(e) = CsvLogger::get_instance().init("logs/port_test") {
        panic!("Failed to initialize CsvLogger: {}", e);
    }

    // creates packet sources with different rates
    let mut source_1 = PacketSource::new(
        0,
        Vec::new(),
        FlowType::PacketDistribution,
        TrafficCharacteristics::new(
            1.0,        // initial delay
            Some(10.0), // duration
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
        0,
        None,
    );

    let mut source_2 = PacketSource::new(
        1,
        Vec::new(),
        FlowType::PacketDistribution,
        TrafficCharacteristics::new(
            1.0,        // initial delay
            Some(10.0), // duration
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
        0,
        None,
    );

    // creates the FIFO port scheduler
    let mut port = Port::new(
        160000.0, // 160,000 bits/second
        100,      // capacity
        CapacityUnit::Packets,
        DropStrategy::TailDrop,
    );

    let mut sink = PacketSink::new(&source_1);
    let source_1_mbox = Mailbox::new();
    let source_2_mbox = Mailbox::new();
    let port_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let sink_addr = sink_mbox.address();
    let sink_id = sink.id();

    // connects sources to port and port to sink
    source_1.output().connect(Port::packet_received, &port_mbox);
    source_2.output().connect(Port::packet_received, &port_mbox);
    port.output.connect(PacketSink::packet_received, &sink_mbox);

    let mut sink_statistics = EventSlot::new();
    sink.statistics().connect_sink(&sink_statistics);

    // initializes the simulation
    let t0 = MonotonicTime::EPOCH;
    match SimInit::new()
        .add_model(source_1, source_1_mbox, "Source_1")
        .add_model(source_2, source_2_mbox, "Source_2")
        .add_model(port, port_mbox, "FIFO")
        .add_model(sink, sink_mbox, "Sink")
        .init(t0)
    {
        Ok((mut sim, _)) => {
            // runs simulation for 20 seconds
            let _ = sim.step_until(Duration::from_secs(20));

            // requests statistics report
            let _ = sim.process_event(PacketSink::report, sink_id, &sink_addr);

            // obtains the total number of packets sent
            let packets_sent = CsvLogger::get_instance().total_packets_sent();

            if let Some(statistics) = sink_statistics.next() {
                info!("{:#.3}", statistics);

                // verifies the FIFO behavior: packets should be processed in order of arrival
                let mut prev_arrival = 0.0;
                let packets_received = statistics.packets.len();

                for packet in statistics.packets {
                    assert!(
                        (packet.time - prev_arrival) < 1e-7 || packet.time > prev_arrival,
                        "Packets not processed in FIFO order."
                    );

                    prev_arrival = packet.time;
                }

                println!("packets sent: {}", packets_sent);
                println!("packets received: {}", packets_received);

                // verifies that all packets were processed
                assert!(packets_received > 0, "No packets were processed.");
                assert!(
                    packets_sent == packets_received,
                    "Packets were dropped unexpectedly."
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
