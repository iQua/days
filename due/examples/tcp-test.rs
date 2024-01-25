use std::time::Duration;

use log::info;

use asynchronix::simulation::{Mailbox, SimInit};
use asynchronix::time::MonotonicTime;

use due::flows::cc::CCAlgorithm::TCPReno;
use due::flows::tcp_sink::TCPPacketSink;
use due::flows::tcp_source::TCPPacketSource;
use due::flows::wire::Wire;
use due::flows::{DistributionInfo, TCPCharacteristics, TrafficCharacteristics};

fn main() {
    let env = env_logger::Env::default();
    env_logger::init_from_env(env);

    // Instantiates models and their mailboxes.
    let mut source = TCPPacketSource::new(
        0,
        TrafficCharacteristics::new(
            0.0,
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
            Some(TCPCharacteristics {
                cc_algorithm: TCPReno,
                rtt_estimate: 0.5,
            }),
        ),
        0,
    );

    let mut wire = Wire::new(
        0,
        DistributionInfo::Uniform {
            low: 0.1,
            high: 0.1,
        },
    );

    let mut sink = TCPPacketSink::new(0);

    let source_mbox = Mailbox::new();
    let wire_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let sink_addr = sink_mbox.address();

    // Connects the output of packet source to the input of packet sink.
    source.output.connect(Wire::packet_received, &wire_mbox);

    wire.output
        .connect(TCPPacketSink::packet_received, &sink_mbox);

    sink.output
        .connect(TCPPacketSource::ack_packet_received, &source_mbox);

    // Instantiates the simulator.
    let t0 = MonotonicTime::EPOCH;
    let mut sim = SimInit::new()
        .add_model(source, source_mbox)
        .add_model(wire, wire_mbox)
        .add_model(sink, sink_mbox)
        .init(t0);

    sim.step_by(Duration::from_secs(10));

    sim.send_event(TCPPacketSink::report, 1, &sink_addr);

    info!(
        "Simulation completed at time {:.3}.",
        sim.time().duration_since(t0).as_secs_f64()
    );
}
