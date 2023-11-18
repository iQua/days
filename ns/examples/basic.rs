//! A basic example where two packet generators sends packets to a sink.

use rand::{rngs::SmallRng, SeedableRng};
use rand_distr::{Exp, Uniform};
use std::cell::RefCell;

use ns::packets::dist_generator::DistPacketGenerator;
use ns::packets::sink::PacketSink;
use ns::ports::wire::Wire;
use ns::utils::distribution::FixedDistribution;
use ns::Shared;
use sim::{Process, RandomVar, SimContext};
use tokio::sync::mpsc::unbounded_channel;

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    let mut sink = PacketSink::new(2);
    let mut wire = Wire::new(1, Box::new(|| FixedDistribution(2.00)));

    let (sender_1, receiver_1) = unbounded_channel();
    let (sender_2, receiver_2) = unbounded_channel();

    // Creating a collection of packet generators
    for i in 0..2 {
        let mut generator = DistPacketGenerator::new(
            i,
            1.0,
            Box::new(|| Exp::new(1.).unwrap()),
            Box::new(|| Uniform::new(1000, 1500)),
        );
        generator.sender = sender_1.clone();
        sim.activate(generator.run(sim));
    }

    wire.receiver = receiver_1;
    wire.sender = sender_2;
    sink.receiver = receiver_2;

    sim.activate(wire.run(sim));
    sim.activate(sink.run(sim));

    // waiting for the end of this simulation
    sim.advance(sim.shared().duration + 100.).await;
}

fn main() {
    let outcome = sim::simulation(
        Shared {
            rng: RefCell::new(SmallRng::seed_from_u64(SEED)),
            packet_size: RandomVar::new(),
            duration: 10.,
        },
        |sim| Process::new(sim, network_sim(sim)),
    );

    println!(
        "Statistics on packet sizes in this simulation: {:#.3}",
        outcome.packet_size
    );
}
