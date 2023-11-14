//! A basic example where two packet generators sends packets to a sink.

use rand::{rngs::SmallRng, SeedableRng};
use rand_distr::{Exp, Uniform};
use std::cell::RefCell;

use ns::packets::dist_generator::DistPacketGenerator;
use ns::packets::sink::PacketSink;
use ns::Shared;
use ns::port::port::Port;
use sim::{channel, Process, RandomVar, SimContext};

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    let mut generator = DistPacketGenerator::new(
        0,
        1.0,
        Box::new(|| Exp::new(1.).unwrap()),
        Box::new(|| Uniform::new(1000, 1001)),
    );
    let mut port = Port::new(1, (1000*8) as f64);
    let mut sink = PacketSink::new(2);

    let (sender1, receiver1) = channel();
    let (sender2, receiver2) = channel();
    generator.sender = sender1;
    port.receiver = receiver1;
    port.sender = sender2;
    sink.receiver = receiver2;


    sim.activate(generator.run(sim));
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
            duration: 10.,
        },
        |sim| Process::new(sim, network_sim(sim)),
    );

    println!(
        "Statistics on packet sizes in this simulation: {:#.3}",
        outcome.packet_size
    );
}
