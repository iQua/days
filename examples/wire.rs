//! An example of connecting a packet source to a network wire, and then to a
//! packet sink.

use std::time::Duration;

use log::info;

use nexosim::ports::EventSlot;
use nexosim::simulation::{Mailbox, SimInit};
use nexosim::time::MonotonicTime;

use days::flows::flow::FlowType;
use days::flows::sink::PacketSink;
use days::flows::source::PacketSource;
use days::flows::wire::Wire;
use days::flows::{DistributionInfo, TrafficCharacteristics};
use days::utils::logger::CsvLogger;

fn main() {
    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    // initializes the singleton of the logger of reports
    if let Err(e) = CsvLogger::get_instance().init("logs/wire") {
        panic!("Failed to initialize CsvLogger: {}", e);
    }

    // instantiates models and their mailboxes
    let mut source = PacketSource::new(
        0,
        Vec::new(),
        FlowType::PacketDistribution,
        TrafficCharacteristics::new(
            0.0,
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
            None,
        ),
        0,
        None,
    );

    let mut wire = Wire::new(
        0,
        DistributionInfo::Uniform {
            low: 0.2,
            high: 0.2,
        },
    );

    let mut sink = PacketSink::new(&source);

    let source_mbox = Mailbox::new();
    let wire_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let sink_addr = sink_mbox.address();

    // connects the output of packet source to the input of the wire
    source.output().connect(Wire::packet_received, &wire_mbox);
    wire.output.connect(PacketSink::packet_received, &sink_mbox);

    let mut sink_statistics = EventSlot::new();
    sink.statistics().connect_sink(&sink_statistics);

    // instantiates the simulator
    let t0 = MonotonicTime::EPOCH;
    match SimInit::new()
        .add_model(source, source_mbox, "Source")
        .add_model(wire, wire_mbox, "Wire")
        .add_model(sink, sink_mbox, "Sink")
        .init(t0)
    {
        Ok((mut sim, _)) => {
            // starts the simulation
            let _ = sim.step_until(Duration::from_secs(100));

            // requests the packet sink to report statistics
            let _ = sim.process_event(PacketSink::report, 1, &sink_addr);

            if let Some(statistics) = sink_statistics.next() {
                info!("{:#.3}", statistics);
            }

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
