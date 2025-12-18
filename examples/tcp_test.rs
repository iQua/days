//! An example of connecting a TCP packet source to a TCP packet sink.

use std::time::Duration;

use log::info;

use nexosim::simulation::{Mailbox, SimInit};
use nexosim::time::MonotonicTime;

use days::flows::cc::CCAlgorithm::TCPReno;
use days::flows::flow::FlowType;
use days::flows::sink::PacketSink;
use days::flows::source::PacketSource;
use days::flows::wire::Wire;
use days::flows::{DistributionInfo, TCPCharacteristics, TrafficCharacteristics};
use days::utils::logger::CsvLogger;

fn main() {
    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    // initializes the singleton of the logger of reports
    if let Err(e) = CsvLogger::get_instance().init("logs/tcp_test") {
        panic!("Failed to initialize CsvLogger: {}", e);
    }

    // instantiates models
    let mut source = PacketSource::new(
        0,
        Vec::new(),
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
        0,
        0,
        None,
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
    match SimInit::new()
        .add_model(source, source_mbox, "Source")
        .add_model(wire, wire_mbox, "Wire")
        .add_model(sink, sink_mbox, "Sink")
        .init(t0)
    {
        Ok((mut sim, _)) => {
            let _ = sim.step_until(Duration::from_secs(10));

            let _ = sim.process_event(PacketSink::report, 2, &sink_addr);

            info!(
                "Simulation completed at time {:.3}.",
                sim.time().duration_since(t0).as_secs_f64()
            );

            // generates three CSV files containing statistics of this simulation run
            CsvLogger::get_instance().flush_reports();
        }
        Err(e) => {
            info!("Simulation failed: {e}");
        }
    }
}
