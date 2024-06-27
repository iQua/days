//! An example of connecting a TCP packet source to a TCP packet sink in a
//! simple two-hop network.

use std::sync::Arc;
use std::time::Duration;

use log::info;

use asynchronix::simulation::{Mailbox, SimInit};
use asynchronix::time::MonotonicTime;

use day::flows::flow::FlowType;
//use day::flows::cc::CCAlgorithm::TCPReno;
use day::flows::cc::CCAlgorithm::TCPCubic;
use day::flows::sink::PacketSink;
use day::flows::source::PacketSource;
use day::flows::wire::Wire;
use day::flows::{DistributionInfo, TCPCharacteristics, TrafficCharacteristics};
use day::schedulers::drop::{CapacityUnit, DropStrategy};
use day::schedulers::drr::DRRServer;

fn main() {
    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    // instantiates models and their mailboxes
    let mut source = PacketSource::new(
        0,
        Vec::new(),
        FlowType::TCP,
        TrafficCharacteristics::new(
            0.0,
            None,
            Some(3014),
            DistributionInfo::Uniform {
                low: 0.1,
                high: 0.1,
            },
            DistributionInfo::DiscreteUniform {
                low: 512,
                high: 512,
            },
            Some(TCPCharacteristics {
                cc_algorithm: TCPCubic,
            }),
        ),
        0,
    );

    // initializes a DRR server
    let mut server = DRRServer::new(
        512.0 / 0.2,
        100,
        CapacityUnit::Packets,
        Arc::new(|flow_id| flow_id),
        DropStrategy::TailDrop,
        vec![1],
    );

    let mut wire = Wire::new(
        0,
        DistributionInfo::Uniform {
            low: 0.1,
            high: 0.1,
        },
    );

    let mut sink = PacketSink::new(&source);

    let source_mbox = Mailbox::new();
    let server_mbox = Mailbox::new();
    let wire_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let sink_addr = sink_mbox.address();

    // connects components
    source
        .output()
        .connect(DRRServer::packet_received, &server_mbox);
    server.output.connect(Wire::packet_received, &wire_mbox);
    wire.output.connect(PacketSink::packet_received, &sink_mbox);
    sink.output()
        .connect(PacketSource::packet_received, &source_mbox);

    let mut sink_statistics = sink.statistics().connect_slot().0;

    // instantiates the simulator
    let t0 = MonotonicTime::EPOCH;
    let mut sim = SimInit::new()
        .add_model(source, source_mbox)
        .add_model(server, server_mbox)
        .add_model(wire, wire_mbox)
        .add_model(sink, sink_mbox)
        .init(t0);

    // starts the simulation
    sim.step_by(Duration::from_secs(20));

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
