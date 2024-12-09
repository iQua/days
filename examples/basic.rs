//! A basic example of connecting one packet source to one packet sink.

use std::time::Duration;

use log::info;

use nexosim::ports::EventSlot;
use nexosim::simulation::{Mailbox, SimInit};
use nexosim::time::MonotonicTime;

use day::flows::flow::FlowType;
use day::flows::sink::PacketSink;
use day::flows::source::PacketSource;
use day::flows::{DistributionInfo, TrafficCharacteristics};
use day::utils::logger::CsvLogger;

fn main() {
    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    // initializes the singleton of the logger of reports
    CsvLogger::get_instance().init("logs/basic");

    // instantiates models and their mailboxes
    let mut source = PacketSource::new(
        0,
        Vec::new(),
        FlowType::PacketDistribution,
        TrafficCharacteristics::new(
            0.0,
            Some(10.0),
            None,
            DistributionInfo::DiscreteUniform { low: 1, high: 1 },
            DistributionInfo::DiscreteUniform {
                low: 1000,
                high: 1000,
            },
            None,
        ),
        0,
    );

    let mut sink = PacketSink::new(&source);

    let source_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let sink_addr = sink_mbox.address();

    // connects the output of packet source to the input of packet sink
    source
        .output()
        .connect(PacketSink::packet_received, &sink_mbox);

    let mut sink_statistics = EventSlot::new();
    sink.statistics().connect_sink(&sink_statistics);

    // instantiates the simulator
    let t0 = MonotonicTime::EPOCH;
    match SimInit::new()
        .add_model(source, source_mbox, "Source")
        .add_model(sink, sink_mbox, "Sink")
        .init(t0)
    {
        Ok((mut sim, _)) => {
            let _ = sim.step_until(Duration::from_secs(20));

            let _ = sim.process_event(PacketSink::report, 1, &sink_addr);
            if let Some(statistics) = sink_statistics.next() {
                info!("{:#.3}", statistics);
            }

            info!(
                "Simulation completed at time {:.3}.",
                sim.time().duration_since(t0).as_secs_f64()
            );

            // generates three CSV files containing statistics of this simulation run
            CsvLogger::flush_reports();
        }
        Err(e) => {
            info!("Simulation failed: {e}");
        }
    }
}
