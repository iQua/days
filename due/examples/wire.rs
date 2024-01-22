//! An example of connecting a packet source to a network wire, and then to a
//! packet sink.

use std::time::Duration;

use log::info;

use asynchronix::simulation::{Mailbox, SimInit};
use asynchronix::time::MonotonicTime;

use due::flows::sink::PacketSink;
use due::flows::source::PacketSource;
use due::flows::wire::Wire;
use due::flows::{DistributionInfo, TrafficCharacteristics};

fn main() {
    let env = env_logger::Env::default();
    env_logger::init_from_env(env);

    // instantiates models and their mailboxes
    let mut source = PacketSource::new(
        0,
        TrafficCharacteristics::new(
            1.1,
            Some(10.0),
            Some(4000),
            DistributionInfo::Uniform {
                low: 0.1,
                high: 0.1,
            },
            DistributionInfo::DiscreteUniform {
                low: 1000,
                high: 1000,
            },
        ),
        0,
    );

    let mut wire = Wire::new(
        0,
        DistributionInfo::Uniform {
            low: 0.05,
            high: 0.05,
        },
    );

    let mut sink = PacketSink::new(0);

    let source_mbox = Mailbox::new();
    let wire_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let sink_addr = sink_mbox.address();

    // connects the output of packet source to the input of the wire
    source.output.connect(Wire::packet_received, &wire_mbox);
    wire.output.connect(PacketSink::packet_received, &sink_mbox);
    let mut sink_statistics = sink.statistics.connect_slot().0;

    // instantiates the simulator
    let t0 = MonotonicTime::EPOCH;
    let mut sim = SimInit::new()
        .add_model(source, source_mbox)
        .add_model(wire, wire_mbox)
        .add_model(sink, sink_mbox)
        .init(t0);

    // starts the simulation
    sim.step_by(Duration::from_secs(100));

    // requests the packet sink to report statistics
    sim.send_event(PacketSink::report, 1, &sink_addr);

    if let Some(statistics) = sink_statistics.take() {
        info!("{:#.3}", statistics);
    }

    info!(
        "Simulation completed at time {:.3}.",
        sim.time().duration_since(t0).as_secs_f64()
    );
}
