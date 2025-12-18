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
use days::schedulers::drr::DRRServer;
use days::utils::logger::CsvLogger;

#[test]
fn test_deficit_round_robin() {
    let _ = env_logger::builder().is_test(true).try_init();

    // Initialize the logger
    if let Err(e) = CsvLogger::get_instance().init("logs/drr_test") {
        panic!("Failed to initialize CsvLogger: {}", e);
    }

    // Create packet sources with different rates and packet sizes
    let mut source_1 = PacketSource::new(
        0,
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

    let mut source_2 = PacketSource::new(
        1,
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

    // Create DRR scheduler with weights 1:2
    let mut drr = DRRServer::new(
        8000.0, // 8 Mbps
        100,
        CapacityUnit::Packets,
        Arc::new(|flow_id| flow_id),
        DropStrategy::TailDrop,
        vec![1, 2], // weights
    );

    let mut sink = PacketSink::new(&source_1);
    let source_1_mbox = Mailbox::new();
    let source_2_mbox = Mailbox::new();
    let drr_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let sink_addr = sink_mbox.address();
    let sink_id = sink.id();

    // Connect sources to scheduler and scheduler to sink
    source_1
        .output()
        .connect(DRRServer::packet_received, &drr_mbox);
    source_2
        .output()
        .connect(DRRServer::packet_received, &drr_mbox);
    drr.output.connect(PacketSink::packet_received, &sink_mbox);

    let mut sink_statistics = EventSlot::new();
    sink.statistics().connect_sink(&sink_statistics);

    // Initialize simulation
    let t0 = MonotonicTime::EPOCH;
    println!("t0 = {:?}", t0);
    match SimInit::new()
        .add_model(source_1, source_1_mbox, "Source_1")
        .add_model(source_2, source_2_mbox, "Source_2")
        .add_model(drr, drr_mbox, "DRR")
        .add_model(sink, sink_mbox, "Sink")
        .init(t0)
    {
        Ok((mut sim, _)) => {
            // Run simulation for 50 seconds to allow scheduler to stabilize
            let _ = sim.step_until(Duration::from_secs(50));

            // Request statistics report
            let _ = sim.process_event(PacketSink::report, sink_id, &sink_addr);

            if let Some(statistics) = sink_statistics.next() {
                info!("{:#.3}", statistics);

                // Ground truth based on DRR behavior:
                // Flow 0 (weight 1) and Flow 1 (weight 2) should receive
                // packets in a 1:2 ratio
                let mut flow0_traffic = 0;
                let mut flow1_traffic = 0;

                for packet in statistics.packets {
                    match packet.flow_id {
                        0 => flow0_traffic += packet.size,
                        1 => flow1_traffic += packet.size,
                        _ => panic!("Unexpected flow ID"),
                    }
                }

                // Verify ratio is approximately 1:2 with a wider tolerance
                let ratio = flow1_traffic as f64 / flow0_traffic as f64;

                assert!(
                    ratio >= 1.5 && ratio <= 2.5,
                    "Expected ratio ~2:1, got {}:1",
                    ratio
                );
            } else {
                panic!("No statistics were reported by the sink.");
            }

            info!(
                "Simulation completed at time {:.3}.",
                sim.time().duration_since(t0).as_secs_f64()
            );

            // Generate CSV files
            CsvLogger::get_instance().flush_reports();
        }
        Err(_) => panic!("Failed to initialize the simulation."),
    }
}
