//! The main program for running a simulation using a specific configuration.

use std::sync::Arc;
use std::time::Duration;

use log::info;

use asynchronix::simulation::{Mailbox, SimInit};
use asynchronix::time::MonotonicTime;

use due::endpoints::drop::{CapacityUnit, DropStrategy};
use due::endpoints::drr::DRRServer;
use due::endpoints::sink::PacketSink;
use due::endpoints::source::PacketSource;
use due::flows::flow::DistributionInfo;

fn main() {
    let env = env_logger::Env::default();
    env_logger::init_from_env(env);

    let seed = 1;

    // Instantiates models and their mailboxes.
    let mut source = PacketSource::new(
        0,
        1.0,
        10.0,
        DistributionInfo::Uniform { low: 1, high: 1 },
        DistributionInfo::Uniform {
            low: 1000,
            high: 1000,
        },
        seed,
    );
    let mut drr = DRRServer::new(
        1000.0,
        100,
        CapacityUnit::Packets,
        Arc::new(|flow_id| flow_id),
        DropStrategy::TailDrop,
        vec![1],
    );
    let mut sink = PacketSink::new(0, 10.0);
    let source_mbox = Mailbox::new();
    let drr_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let source_addr = source_mbox.address();
    let sink_addr = sink_mbox.address();

    // Connects the output of packet source to the input of packet sink.
    source.output.connect(DRRServer::packet_received, &drr_mbox);
    drr.output.connect(PacketSink::packet_received, &sink_mbox);
    let mut sink_statistics = sink.statistics.connect_slot().0;

    // Instantiates the simulator.
    let t0 = MonotonicTime::EPOCH;
    let mut sim = SimInit::new()
        .add_model(source, source_mbox)
        .add_model(drr, drr_mbox)
        .add_model(sink, sink_mbox)
        .init(t0);

    sim.send_event(PacketSource::run, (), &source_addr);

    sim.step_by(Duration::from_secs(20));

    sim.send_event(PacketSink::report, 1, &sink_addr);
    if let Some(statistics) = sink_statistics.take() {
        info!("{:#.3}", statistics);
    }

    info!(
        "Simulation completed at time {:.3}.",
        sim.time().duration_since(t0).as_secs_f64()
    );
}
