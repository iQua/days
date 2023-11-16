//! TODO

use rand::{rngs::SmallRng, SeedableRng};
use rand_distr::{Exp, Uniform};
use std::{cell::RefCell, collections::HashMap};

use ns::packets::dist_generator::DistPacketGenerator;
use ns::packets::packet::Packet;
use ns::packets::sink::PacketSink;
use ns::schedulers::drr::DRRServer;
use ns::Shared;
use sim::{channel, Process, RandomVar, Receiver, Sender, SimContext};

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    let mut senders: Vec<Sender<Packet>> = Vec::new();
    let mut receivers: Vec<Receiver<Packet>> = Vec::new();
    for _ in 0..2 {
        let (sender, receiver) = channel();
        senders.push(sender);
        receivers.push(receiver);
    }

    for i in 0..2 {
        let mut generator = DistPacketGenerator::new(
            i,
            1.0,
            Box::new(|| Exp::new(1.).unwrap()),
            Box::new(|| Uniform::new(1000, 1001)),
        );
        generator.sender = senders[0].clone();
        sim.activate(generator.run(sim));
    }
    let mut weights = HashMap::new();
    weights.insert(0, 1);
    weights.insert(1, 2);
    let mut drr_server = DRRServer::new(0, (1000 * 8) as f64, weights);
    let mut sink = PacketSink::new(0);

    drr_server.receiver = receivers[0].clone();
    sink.receiver = receivers[1].clone();

    drr_server.sender = senders[1].clone();

    sim.activate(drr_server.run(sim));
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
