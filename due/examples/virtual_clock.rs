//! An example of connecting two packet sources into one Virtual Clock scheduler.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use log::info;

use asynchronix::simulation::{Mailbox, SimInit};
use asynchronix::time::MonotonicTime;

use due::flows::sink::PacketSink;
use due::flows::source::PacketSource;
use due::flows::{DistributionInfo, TrafficCharacteristics};
use due::schedulers::drop::{CapacityUnit, DropStrategy};
use due::schedulers::virtual_clock::VirtualClockServer;

fn main() {
    let env = env_logger::Env::default();
    env_logger::init_from_env(env);

    // instantiates models and their mailboxes
    let mut source_1 = PacketSource::new(
        0,
        TrafficCharacteristics::new(
            2.5,
            10.0,
            DistributionInfo::DiscreteUniform { low: 2, high: 2 },
            DistributionInfo::DiscreteUniform {
                low: 2000,
                high: 2000,
            },
        ),
        0,
    );

    let mut source_2 = PacketSource::new(
        1,
        TrafficCharacteristics::new(
            2.0,
            10.0,
            DistributionInfo::DiscreteUniform { low: 1, high: 1 },
            DistributionInfo::DiscreteUniform {
                low: 1000,
                high: 1000,
            },
        ),
        0,
    );

    let mut vc = VirtualClockServer::new(
        8000.0,
        100,
        CapacityUnit::Packets,
        Arc::new(|flow_id| flow_id),
        DropStrategy::TailDrop,
        HashMap::from([(0, 1), (1, 2)]),
    );

    let mut sink = PacketSink::new(2);
    let source_1_mbox = Mailbox::new();
    let source_2_mbox = Mailbox::new();
    let vc_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let sink_addr = sink_mbox.address();

    // connects the output of packet sources to the input of the Virtual Clock scheduler
    source_1
        .output
        .connect(VirtualClockServer::packet_received, &vc_mbox);
    source_2
        .output
        .connect(VirtualClockServer::packet_received, &vc_mbox);
    vc.output.connect(PacketSink::packet_received, &sink_mbox);
    let mut sink_statistics = sink.statistics.connect_slot().0;

    // instantiates the simulator
    let t0 = MonotonicTime::EPOCH;
    let mut sim = SimInit::new()
        .add_model(source_1, source_1_mbox)
        .add_model(source_2, source_2_mbox)
        .add_model(vc, vc_mbox)
        .add_model(sink, sink_mbox)
        .init(t0);

    // starts the simulation
    sim.step_by(Duration::from_secs(100));

    // requests the packet sink to report statistics
    sim.send_event(PacketSink::report, 2, &sink_addr);

    if let Some(statistics) = sink_statistics.take() {
        info!("{:#.3}", statistics);
    }

    info!(
        "Simulation completed at time {:.3}.",
        sim.time().duration_since(t0).as_secs_f64()
    );
}
