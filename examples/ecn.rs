//! An example of ECN with TCP congestion control using RED_ECN on a port.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use log::{info, warn};

use nexosim::model::{Context, Model};
use nexosim::simulation::{Mailbox, SimInit};
use nexosim::time::MonotonicTime;

use days::flows::cc::CCAlgorithm::TCPCubic;
use days::flows::flow::FlowType;
use days::flows::packet::EcnField;
use days::flows::sink::PacketSink;
use days::flows::source::PacketSource;
use days::flows::{DistributionInfo, TCPCharacteristics, TrafficCharacteristics};
use days::schedulers::drop::{CapacityUnit, DropStrategy};
use days::schedulers::port::Port;
use days::utils::logger::CsvLogger;

struct EcnCounter {
    ce_count: Arc<Mutex<usize>>,
    ece_count: Arc<Mutex<usize>>,
    count_data: bool,
}

impl EcnCounter {
    fn new(ce_count: Arc<Mutex<usize>>, ece_count: Arc<Mutex<usize>>, count_data: bool) -> Self {
        Self {
            ce_count,
            ece_count,
            count_data,
        }
    }

    async fn packet_received(&mut self, packet: days::flows::packet::Packet, _: &Context<Self>) {
        if self.count_data {
            if matches!(packet.ecn, EcnField::Ce) {
                *self.ce_count.lock().unwrap() += 1;
            }
        } else if let Some(ack) = packet.ack {
            if ack.ece {
                *self.ece_count.lock().unwrap() += 1;
            }
        }
    }
}

impl Model for EcnCounter {
    type Env = ();
}

fn main() {
    let env = env_logger::Env::default().filter_or("RUST_LOG", "info");
    env_logger::init_from_env(env);

    if let Err(e) = CsvLogger::get_instance().init("logs/ecn") {
        panic!("Failed to initialize CsvLogger: {}", e);
    }

    let mut source = PacketSource::new(
        0,
        Vec::new(),
        FlowType::TCP,
        TrafficCharacteristics::new(
            0.0,
            None,
            Some(1000 * 512),
            DistributionInfo::Uniform {
                low: 0.004,
                high: 0.004,
            },
            DistributionInfo::DiscreteUniform {
                low: 512,
                high: 512,
            },
            Some(TCPCharacteristics {
                cc_algorithm: TCPCubic,
                ecn: true,
                cubic: None,
            }),
        ),
        0,
        0,
        None,
    );

    let mut port = Port::new(
        500_000.0, // moderate link rate to build queue but allow more delivery
        5,
        CapacityUnit::Packets,
        DropStrategy::RedEcn,
        0.0,
    );

    let mut sink = PacketSink::new(&source);

    let source_mbox = Mailbox::new();
    let port_mbox = Mailbox::new();
    let sink_mbox = Mailbox::new();
    let sink_addr = sink_mbox.address();
    let sink_id = sink.id();

    let ce_count = Arc::new(Mutex::new(0));
    let ece_count = Arc::new(Mutex::new(0));
    let data_counter = EcnCounter::new(ce_count.clone(), ece_count.clone(), true);
    let ack_counter = EcnCounter::new(ce_count.clone(), ece_count.clone(), false);
    let data_counter_mbox = Mailbox::new();
    let ack_counter_mbox = Mailbox::new();

    source.output().connect(Port::packet_received, &port_mbox);
    port.output.connect(PacketSink::packet_received, &sink_mbox);
    port.output
        .connect(EcnCounter::packet_received, &data_counter_mbox);
    sink.output()
        .connect(PacketSource::packet_received, &source_mbox);
    sink.output()
        .connect(EcnCounter::packet_received, &ack_counter_mbox);

    let mut sink_statistics = nexosim::ports::EventSlot::new();
    sink.statistics().connect_sink(sink_statistics.writer());

    let t0 = MonotonicTime::EPOCH;
    match SimInit::new()
        .add_model(source, source_mbox, "ECNSource")
        .add_model(port, port_mbox, "ECNPort")
        .add_model(sink, sink_mbox, "ECNSink")
        .add_model(data_counter, data_counter_mbox, "ECNDataCounter")
        .add_model(ack_counter, ack_counter_mbox, "ECNAckCounter")
        .init(t0)
    {
        Ok(mut sim) => {
            let _ = sim.step_until(Duration::from_secs(10));

            let _ = sim.process_event_fn(PacketSink::report, sink_id, &sink_addr);
            if let Some(statistics) = sink_statistics.next() {
                info!("{:#.3}", statistics);
            }

            let ce_count = *ce_count.lock().unwrap();
            let ece_count = *ece_count.lock().unwrap();

            info!("Observed CE-marked data packets: {}", ce_count);
            info!("Observed ECE-marked ACKs: {}", ece_count);
            if ce_count == 0 || ece_count == 0 {
                warn!("No ECN marks observed; try lowering the link rate or buffer size.");
            }

            info!(
                "Simulation completed at time {:.3}.",
                sim.time().duration_since(t0).as_secs_f64()
            );

            CsvLogger::get_instance().flush_reports();
        }
        Err(e) => {
            info!("Simulation failed: {e}");
        }
    }
}
