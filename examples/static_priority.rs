//! An example of connecting two packet sources into one Static Priority scheduler.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use log::info;

use nexosim::ports::EventSlot;
use nexosim::simulation::{Mailbox, SimInit};
use nexosim::time::MonotonicTime;

use day::flows::flow::FlowType;
use day::flows::sink::PacketSink;
use day::flows::source::PacketSource;
use day::flows::{DistributionInfo, TrafficCharacteristics};
use day::schedulers::drop::{CapacityUnit, DropStrategy};
use day::schedulers::sp::SPServer;

fn main() {
    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

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
    );

    let mut sp = SPServer::new(
        8000.0,
        100,
        CapacityUnit::Packets,
        Arc::new(|flow_id| flow_id),
        DropStrategy::TailDrop,
        HashMap::from([(0, 1), (1, 2)]),
    );

    let mut sink = PacketSink::new(&source_1);
    let source_1_mbox = Mailbox::new();
    let source_2_mbox = Mailbox::new();
    let sp_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let sink_addr = sink_mbox.address();

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
        .add_model(source_1, source_1_mbox, "Source1")
        .add_model(source_2, source_2_mbox, "Source2")
        .add_model(sp, sp_mbox, "SP")
        .add_model(sink, sink_mbox, "Sink")
        .init(t0)
    {
        Ok((mut sim, _)) => {
            // starts the simulation
            let _ = sim.step_until(Duration::from_secs(100));

            // requests the packet sink to report statistics
            let _ = sim.process_event(PacketSink::report, 2, &sink_addr);

            if let Some(statistics) = sink_statistics.next() {
                info!("{:#.3}", statistics);
            }

            info!(
                "Simulation completed at time {:.3}.",
                sim.time().duration_since(t0).as_secs_f64()
            );
        }
        Err(e) => {
            info!("Simulation failed: {e}");
        }
    }
}
