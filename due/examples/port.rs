//! The main program for running a simulation using a specific configuration.

use std::time::Duration;

use log::info;

use asynchronix::simulation::{Mailbox, SimInit};
use asynchronix::time::MonotonicTime;

use due::endpoints::drop::{CapacityUnit, DropStrategy};
use due::endpoints::flow::DistributionInfo;
use due::endpoints::port::Port;
use due::endpoints::sink::PacketSink;
use due::endpoints::source::PacketSource;

fn main() {
    let env = env_logger::Env::default();
    env_logger::init_from_env(env);

    // instantiates models and their mailboxes
    let mut source_1 = PacketSource::new(
        0,
        1.0,
        10.0,
        DistributionInfo::Uniform { low: 1, high: 1 },
        DistributionInfo::Uniform {
            low: 1000,
            high: 1000,
        },
    );

    let mut source_2 = PacketSource::new(
        1,
        2.0,
        10.0,
        DistributionInfo::Uniform { low: 1, high: 1 },
        DistributionInfo::Uniform {
            low: 1000,
            high: 1000,
        },
    );

    let mut port = Port::new(4000.0, 100, CapacityUnit::Packets, DropStrategy::TailDrop);

    let mut sink = PacketSink::new(2);
    let source_1_mbox = Mailbox::new();
    let source_2_mbox = Mailbox::new();
    let port_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let sink_addr = sink_mbox.address();

    // connects the output of packet sources to the input of the DRR scheduler
    source_1.output.connect(Port::packet_received, &port_mbox);
    source_2.output.connect(Port::packet_received, &port_mbox);
    port.output.connect(PacketSink::packet_received, &sink_mbox);
    let mut sink_statistics = sink.statistics.connect_slot().0;

    // instantiates the simulator
    let t0 = MonotonicTime::EPOCH;
    let mut sim = SimInit::new()
        .add_model(source_1, source_1_mbox)
        .add_model(source_2, source_2_mbox)
        .add_model(port, port_mbox)
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
