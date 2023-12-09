//! An example of connecting one packet source to one packet sink.

use std::time::Duration;

use log::info;

use asynchronix::simulation::{Mailbox, SimInit};
use asynchronix::time::MonotonicTime;

use due::endpoints::flow::DistributionInfo;
use due::endpoints::sink::PacketSink;
use due::endpoints::source::PacketSource;

fn main() {
    let env = env_logger::Env::default();
    env_logger::init_from_env(env);

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
    );
    let mut sink = PacketSink::new(0);
    let source_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let sink_addr = sink_mbox.address();

    // Connects the output of packet source to the input of packet sink.
    source
        .output
        .connect(PacketSink::packet_received, &sink_mbox);
    let mut sink_statistics = sink.statistics.connect_slot().0;

    // Instantiates the simulator.
    let t0 = MonotonicTime::EPOCH;
    let mut sim = SimInit::new()
        .add_model(source, source_mbox)
        .add_model(sink, sink_mbox)
        .init(t0);

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
