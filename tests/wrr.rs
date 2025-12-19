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
use days::schedulers::wrr::WRRServer;
use days::seed_from_config;
use days::utils::logger::CsvLogger;

/// This integration test creates two packet sources and sends traffic to a
/// Weighted Round Robin (WRR) scheduler. The WRR has two classes with weights
/// 1:2. We expect the second flow (weight 2) to receive approximately twice
/// the throughput of the first (weight 1).
#[test]
fn test_weighted_round_robin() {
    let _ = env_logger::builder().is_test(true).try_init();

    // Fix the random seed to make the stochastic traffic patterns reproducible.
    let _ = seed_from_config("tests/wrr_seed.toml");

    // Initialize the logger
    if let Err(e) = CsvLogger::get_instance().init("logs/wrr_test") {
        panic!("Failed to initialize CsvLogger: {}", e);
    }

    // Create two traffic sources with different packet-size distributions,
    // but same flow durations and inter-packet times.
    let mut source_1 = PacketSource::new(
        0, // flow_id
        Vec::new(),
        FlowType::PacketDistribution,
        TrafficCharacteristics::new(
            1.0,         // initial delay before sending begins
            Some(100.0), // total sending duration
            None,
            DistributionInfo::Uniform {
                low: 0.05,
                high: 0.1,
            },
            DistributionInfo::DiscreteUniform {
                low: 500,
                high: 1500,
            },
            None,
        ),
        0,
        0, // route_id, if relevant
        None,
    );

    let mut source_2 = PacketSource::new(
        1, // flow_id
        Vec::new(),
        FlowType::PacketDistribution,
        TrafficCharacteristics::new(
            1.0,         // initial delay
            Some(100.0), // duration
            None,
            DistributionInfo::Uniform {
                low: 0.05,
                high: 0.1,
            },
            DistributionInfo::DiscreteUniform {
                low: 500,
                high: 1500,
            },
            None,
        ),
        0,
        0,
        None,
    );

    // Create Weighted Round Robin scheduler with two classes having weights 1:2.
    // The flow_classes closure simply uses the flow_id as the class_id, so
    // flow_id=0 -> class 0, flow_id=1 -> class 1.
    let mut wrr = WRRServer::new(
        8000.0, // link rate in bits per second (8 Mbps)
        100,    // buffer capacity in "packets"
        CapacityUnit::Packets,
        Arc::new(|flow_id| flow_id), // each flow_id corresponds to a distinct class
        DropStrategy::TailDrop,
        0.0,
        vec![1, 2], // weight of class 0 is 1, weight of class 1 is 2
    );

    // Create a sink to receive all packets sent out of the WRR.
    let mut sink = PacketSink::new(&source_1);

    // Each model (source, wrr, sink) needs a mailbox to receive events.
    let source_1_mbox = Mailbox::new();
    let source_2_mbox = Mailbox::new();
    let wrr_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let sink_addr = sink_mbox.address();
    let sink_id = sink.id();

    // Connect the sources to the WRR scheduler and the WRR output to the sink.
    source_1
        .output()
        .connect(WRRServer::packet_received, &wrr_mbox);
    source_2
        .output()
        .connect(WRRServer::packet_received, &wrr_mbox);
    wrr.output.connect(PacketSink::packet_received, &sink_mbox);

    // We will capture the sink statistics (List of forwarded packets, etc.)
    let mut sink_statistics = EventSlot::new();
    sink.statistics().connect_sink(&sink_statistics);

    // Initialize simulation starting at time = 0.
    let t0 = MonotonicTime::EPOCH;
    match SimInit::new()
        .add_model(source_1, source_1_mbox, "Source_1")
        .add_model(source_2, source_2_mbox, "Source_2")
        .add_model(wrr, wrr_mbox, "WRR_Scheduler")
        .add_model(sink, sink_mbox, "Sink")
        .init(t0)
    {
        Ok((mut sim, _)) => {
            // Run the simulation until 50 seconds to allow enough time
            // for both sources to send a good number of packets.
            let _ = sim.step_until(Duration::from_secs(50));

            // Request a statistics report from the sink.
            let _ = sim.process_event(PacketSink::report, sink_id, &sink_addr);

            // Retrieve the statistics from the sink, if any were reported.
            if let Some(statistics) = sink_statistics.next() {
                info!("{:#.3}", statistics);

                // Count total bytes per flow_id. We expect flow_id=1
                // to receive about twice as many bytes as flow_id=0.
                let mut flow0_bytes = 0;
                let mut flow1_bytes = 0;

                for packet in statistics.packets {
                    match packet.flow_id {
                        0 => flow0_bytes += packet.size,
                        1 => flow1_bytes += packet.size,
                        _ => panic!("Unexpected flow ID for this test"),
                    }
                }

                // Calculate ratio of flow1 throughput vs flow0 throughput
                let ratio = flow1_bytes as f64 / flow0_bytes.max(1) as f64;
                info!("Weighted Round Robin ratio: flow1 / flow0 = {:.2}", ratio);

                // We allow some tolerance because the schedule is stochastic.
                // A broad range around 2.0 is acceptable.
                assert!(
                    ratio >= 1.5 && ratio <= 2.5,
                    "Flow 1 expected ~2x throughput of flow 0, but ratio = {:.2}",
                    ratio
                );
            } else {
                panic!("No statistics from sink—something went wrong in the simulation.");
            }

            info!(
                "Simulation completed at time {:.3}",
                sim.time().duration_since(t0).as_secs_f64()
            );

            // Write out periodic CSV files (if any events were logged).
            CsvLogger::get_instance().flush_reports();
        }
        Err(_) => panic!("Failed to initialize the simulation."),
    }
}
