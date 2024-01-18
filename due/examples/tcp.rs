//! An example of connecting a TCP packet source to a TCP packet sink.

use std::time::Duration;

use log::info;

use asynchronix::simulation::{Mailbox, SimInit};
use asynchronix::time::MonotonicTime;

use due::flows::cc::CCAlgorithm::TCPReno;
use due::flows::tcp_sink::TCPPacketSink;
use due::flows::tcp_source::TCPPacketSource;
use due::flows::{DistributionInfo, TrafficCharacteristics};

fn main() {
    let env = env_logger::Env::default();
    env_logger::init_from_env(env);

    // instantiates models and their mailboxes
    let mut source = TCPPacketSource::new(
        0,
        TrafficCharacteristics::new(
            0.1,
            Some(10.0),
            Some(2014),
            DistributionInfo::Uniform {
                low: 0.1,
                high: 0.1,
            },
            DistributionInfo::DiscreteUniform {
                low: 512,
                high: 512,
            },
        ),
        TCPReno,
        0.5,
        0,
    );
    let source_mbox = Mailbox::new();
    let mut sink = TCPPacketSink::new(0);
    let sink_mbox = Mailbox::new();
    let sink_addr = sink_mbox.address();

    // connects the output of packet source to the input of packet sink
    source
        .output
        .connect(TCPPacketSink::packet_received, &sink_mbox);
    // connects the output of packet sink to the input of packet source to
    // receive acknowledgment
    sink.output
        .connect(TCPPacketSource::ack_packet_received, &source_mbox);
    let mut sink_statistics = sink.statistics.connect_slot().0;

    // instantiates the simulator
    let t0 = MonotonicTime::EPOCH;
    let mut sim = SimInit::new()
        .add_model(source, source_mbox)
        .add_model(sink, sink_mbox)
        .init(t0);

    sim.step_by(Duration::from_secs(20));

    sim.send_event(TCPPacketSink::report, 1, &sink_addr);
    if let Some(statistics) = sink_statistics.take() {
        info!("{:#.3}", statistics);
    }

    info!(
        "Simulation completed at time {:.3}.",
        sim.time().duration_since(t0).as_secs_f64()
    );
}
