//! An example of connecting a packet switch.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use log::info;

use nexosim::ports::EventSlot;
use nexosim::simulation::{Mailbox, SimInit};
use nexosim::time::MonotonicTime;

use daytone::flows::flow::FlowType;
use daytone::flows::sink::PacketSink;
use daytone::flows::source::PacketSource;
use daytone::flows::{DistributionInfo, TrafficCharacteristics};
use daytone::schedulers::drop::{CapacityUnit, DropStrategy};
use daytone::schedulers::drr::DRRServer;
use daytone::switches::switch::PacketSwitch;
use daytone::utils::logger::CsvLogger;

fn main() {
    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    // initializes the singleton of the logger of reports
    if let Err(e) = CsvLogger::get_instance().init("logs/switch") {
        panic!("Failed to initialize CsvLogger: {}", e);
    }

    // instantiates models
    let mut source_1 = PacketSource::new(
        0,
        Vec::new(),
        FlowType::PacketDistribution,
        TrafficCharacteristics::new(
            1.0,
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

    let mut source_2 = PacketSource::new(
        1,
        Vec::new(),
        FlowType::PacketDistribution,
        TrafficCharacteristics::new(
            1.0,
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

    let mut fib = HashMap::new();
    fib.insert(0, 2);
    fib.insert(1, 2);
    let mut switch: PacketSwitch = PacketSwitch::new(fib.clone(), fib);

    let mut drr = DRRServer::new(
        8000.0,
        100,
        CapacityUnit::Packets,
        Arc::new(|flow_id| flow_id),
        DropStrategy::TailDrop,
        vec![1, 1],
    );

    let mut sink = PacketSink::new(&source_1);

    // instantiates models' mailboxes
    let source_1_mbox = Mailbox::new();
    let source_2_mbox = Mailbox::new();
    let switch_mbox = Mailbox::new();
    let drr_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let sink_addr = sink_mbox.address();

    // connects the output of packet sources to the input of the switch
    source_1
        .output()
        .connect(PacketSwitch::packet_received, &switch_mbox);
    source_2
        .output()
        .connect(PacketSwitch::packet_received, &switch_mbox);

    // connects the output of the switch to the DRR scheduler
    let switch_output = switch.outputs.get_mut(&2).unwrap();
    switch_output.connect(DRRServer::packet_received, &drr_mbox);

    // connects the DRR scheduler to the packet sink
    drr.output.connect(PacketSink::packet_received, &sink_mbox);

    let mut sink_statistics = EventSlot::new();
    sink.statistics().connect_sink(&sink_statistics);

    // instantiates the simulator
    let t0 = MonotonicTime::EPOCH;

    // connects to the packet sink with an switch id of 2
    match SimInit::new()
        .add_model(source_1, source_1_mbox, "Source1")
        .add_model(source_2, source_2_mbox, "Source2")
        .add_model(switch, switch_mbox, "Switch")
        .add_model(drr, drr_mbox, "DRR")
        .add_model(sink, sink_mbox, "Sink")
        .init(t0)
    {
        Ok((mut sim, _)) => {
            // starts the simulation
            let _ = sim.step_until(Duration::from_secs(100));

            // requests the packet sink to report statistics
            let _ = sim.process_event(PacketSink::report, 2, &sink_addr);

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
