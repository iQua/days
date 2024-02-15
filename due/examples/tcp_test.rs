//! An example of connecting a TCP packet source to a TCP packet sink.

use std::time::Duration;

use log::info;

use asynchronix::simulation::{Mailbox, SimInit};
use asynchronix::time::MonotonicTime;

use due::flows::cc::CCAlgorithm::TCPReno;
use due::flows::flow::FlowType;
use due::flows::sink::PacketSink;
use due::flows::source::PacketSource;
use due::flows::wire::Wire;
use due::flows::{DistributionInfo, TCPCharacteristics, TrafficCharacteristics};

fn main() {
    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    // instantiates models
    let mut source = PacketSource::new(
        0,
        FlowType::TCP,
        TrafficCharacteristics::new(
            0.0,
            None,
            Some(2014),
            DistributionInfo::Uniform {
                low: 0.1,
                high: 0.1,
            },
            DistributionInfo::DiscreteUniform {
                low: 512,
                high: 512,
            },
            Some(TCPCharacteristics {
                cc_algorithm: TCPReno,
            }),
        ),
        0.01,
        0,
    );

    let mut wire = Wire::new(
        0,
        DistributionInfo::Uniform {
            low: 0.1,
            high: 0.1,
        },
    );

    let mut sink = PacketSink::new(&source);

    // instantiates models' mailboxes
    let source_mbox = Mailbox::new();
    let wire_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let sink_addr = sink_mbox.address();

    // connects TCP packet source -> wire -> TCP packet sink
    source.output().connect(Wire::packet_received, &wire_mbox);
    wire.output.connect(PacketSink::packet_received, &sink_mbox);

    // connects TCP packet sink -> TCP packet source for sending acknowledgments
    sink.output()
        .connect(PacketSource::packet_received, &source_mbox);

    // instantiates the simulator
    let t0 = MonotonicTime::EPOCH;
    let mut sim = SimInit::new()
        .add_model(source, source_mbox)
        .add_model(wire, wire_mbox)
        .add_model(sink, sink_mbox)
        .init(t0);

    sim.step_by(Duration::from_secs(10));

    sim.send_event(PacketSink::report, 2, &sink_addr);

    info!(
        "Simulation completed at time {:.3}.",
        sim.time().duration_since(t0).as_secs_f64()
    );
}
