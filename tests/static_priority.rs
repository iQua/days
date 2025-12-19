#![cfg(feature = "test")]

use std::sync::Arc;
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
use days::schedulers::sp::SPServer;
use days::utils::logger::CsvLogger;

#[test]
fn test_static_priority_scheduler() {
    let _ = env_logger::builder().is_test(true).try_init();

    // initializes the singleton of the logger of reports
    if let Err(e) = CsvLogger::get_instance().init("logs/sp_test") {
        panic!("Failed to initialize CsvLogger: {}", e);
    }

    // instantiates models and their mailboxes
    let mut source_1 = PacketSource::new(
        0,
        Vec::new(),
        FlowType::PacketDistribution,
        TrafficCharacteristics::new(
            1.5,
            Some(10.0),
            None,
            DistributionInfo::Uniform {
                low: 1.5,
                high: 1.5,
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
            2.0,
            Some(10.0),
            None,
            DistributionInfo::Uniform {
                low: 2.0,
                high: 2.0,
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

    let mut sp = SPServer::new(
        8000.0,
        100,
        CapacityUnit::Packets,
        Arc::new(|flow_id| flow_id),
        DropStrategy::TailDrop,
        0.0,
        vec![1, 2],
    );

    let mut sink = PacketSink::new(&source_1);
    let source_1_mbox = Mailbox::new();
    let source_2_mbox = Mailbox::new();
    let sp_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let sink_addr = sink_mbox.address();
    let sink_id = sink.id();

    // connects the output of packet sources to the input of the Static Priority
    // scheduler
    source_1
        .output()
        .connect(SPServer::packet_received, &sp_mbox);
    source_2
        .output()
        .connect(SPServer::packet_received, &sp_mbox);
    sp.output.connect(PacketSink::packet_received, &sink_mbox);

    let mut sink_statistics = EventSlot::new();
    sink.statistics().connect_sink(&sink_statistics);

    // instantiates the simulator
    let t0 = MonotonicTime::EPOCH;
    match SimInit::new()
        .add_model(source_1, source_1_mbox, "Source_1")
        .add_model(source_2, source_2_mbox, "Source_2")
        .add_model(sp, sp_mbox, "SP")
        .add_model(sink, sink_mbox, "Sink")
        .init(t0)
    {
        Ok((mut sim, _)) => {
            // starts the simulation
            let _ = sim.step_until(Duration::from_secs(10));

            // requests the packet sink to report statistics
            let _ = sim.process_event(PacketSink::report, sink_id, &sink_addr);

            if let Some(statistics) = sink_statistics.next() {
                info!("{:#.3}", statistics);

                // Ground truth: (packet_id, flow_id)
                let ground_truth = vec![
                    (0, 0),
                    (0, 1),
                    (1, 0),
                    (1, 1),
                    (2, 0),
                    (3, 0),
                    (2, 1),
                    (3, 1),
                    (4, 0),
                    (5, 0),
                ];
                let mut ground_truth_iter = ground_truth.iter();

                for packet in statistics.packets {
                    let priority = packet.flow_id; // flow_id is used as priority in this test

                    info!(
                        "Packet ID: {}, Flow ID: {}, Priority: {}",
                        packet.packet_id, packet.flow_id, priority
                    );

                    // Get the next expected packet
                    let (expected_packet_id, expected_flow_id) = ground_truth_iter
                        .next()
                        .expect("Received more packets than expected in the test's ground truth.");

                    assert_eq!(packet.packet_id, *expected_packet_id);
                    assert_eq!(packet.flow_id, *expected_flow_id);
                }
            } else {
                panic!("No statistics were reported by the sink.");
            }

            info!(
                "Simulation completed at time {:.3}.",
                sim.time().duration_since(t0).as_secs_f64()
            );
            assert!(true);

            // generates three CSV files containing statistics of this simulation run
            CsvLogger::get_instance().flush_reports();
        }
        Err(_) => panic!("Failed to initialize the simulation."),
    }
}
