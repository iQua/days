//! A basic example that four packet generators sends packets to a sink via two
//! ports, where their queues are limited by bytes and packet numbers,
//! respectively.

use rand::{rngs::SmallRng, SeedableRng};
use rand_distr::Uniform;
use std::cell::RefCell;

use ns::packets::dist_generator::DistPacketGenerator;
use ns::packets::sink::PacketSink;
use ns::packets::packet::Packet;
use ns::ports::port::Port;
use ns::Shared;
use ns::utils::utils::FixedDistribution;
use sim::{Process, RandomVar, SimContext};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    let (sender_0, receiver_0) = unbounded_channel();
    let (sender_1, receiver_1) = unbounded_channel();

    for i in 0..2 {
        let mut generator = DistPacketGenerator::new(
            i,
            1.0,
            Box::new(|| FixedDistribution(1.0)),
            Box::new(|| Uniform::new(1000, 1001)),
        );
        generator.sender = sender_0.clone();
        sim.activate(generator.run(sim));
    }

    let mut port = Port::new(0, (1000 * 8) as f64, 100, false);
    let mut sink = PacketSink::new(0);

    port.receiver = receiver_0;
    sink.receiver = receiver_1;
    port.sender = sender_1;

    sim.activate(port.run(sim));
    sim.activate(sink.run(sim));

    // waiting for the end of this simulation
    sim.advance(sim.shared().duration).await;
}

fn main() {
    let outcome = sim::simulation(
        Shared {
            rng: RefCell::new(SmallRng::seed_from_u64(SEED)),
            packet_size: RandomVar::new(),
            duration: 5.,
        },
        |sim| Process::new(sim, network_sim(sim)),
    );

    println!(
        "Statistics on packet sizes in this simulation: {:#.3}",
        outcome.packet_size
    );
}
