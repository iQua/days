//! TODO

use rand::{rngs::SmallRng, SeedableRng};
use rand_distr::Uniform;
use std::{cell::RefCell, collections::HashMap};

use ns::packets::dist_generator::DistPacketGenerator;
use ns::packets::packet::Packet;
use ns::packets::sink::PacketSink;
use ns::schedulers::drr::DRRServer;
use ns::Shared;
use ns::utils::utils::FixedDistribution;
use ns::utils::splitter::Splitter;
use sim::{channel, Process, RandomVar, Receiver, Sender, SimContext};

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {

    // initializes channels
    let mut senders: Vec<Sender<Packet>> = Vec::new();
    let mut receivers: Vec<Receiver<Packet>> = Vec::new();
    for _ in 0..6 {
        let (sender, receiver) = channel();
        senders.push(sender);
        receivers.push(receiver);
    }

    // initializes packet generators
    let mut pg1 = DistPacketGenerator::new(
        0,
        0.0,
        Box::new(|| FixedDistribution(1.75)),
        Box::new(|| Uniform::new(1000, 1001)),
    );
    let mut pg2 = DistPacketGenerator::new(
        1,
        10.0,
        Box::new(|| FixedDistribution(1.75)),
        Box::new(|| Uniform::new(1000, 1001)),
    );

    // initializes packet sinks
    let mut ps: PacketSink = PacketSink::new(0);
    let mut sink_1: PacketSink = PacketSink::new(1);
    let mut sink_2: PacketSink = PacketSink::new(2);

    // initializes the DRR server
    let mut weights = HashMap::new();
    weights.insert(0, 1);
    weights.insert(1, 2);
    let mut drr_server = DRRServer::new(0, (1000 * 8) as f64 / 1.75, weights);
    
    // initializes splitters
    let mut splitter_1 = Splitter::new();
    let mut splitter_2 = Splitter::new();

    // connects packet generators and splitters
    pg1.sender = senders[0].clone();
    pg2.sender = senders[1].clone();
    splitter_1.receiver = receivers[0].clone();
    splitter_2.receiver = receivers[1].clone();

    // connects splitters and the DRR server
    splitter_1.sender_1 = senders[2].clone();
    splitter_2.sender_1 = senders[2].clone();
    drr_server.receiver = receivers[2].clone();

    // connects the DRR server and the packet sink
    drr_server.sender = senders[3].clone();
    ps.receiver = receivers[3].clone();

    // connects splitters and packet sinks
    splitter_1.sender_2 = senders[4].clone();
    splitter_2.sender_2 = senders[5].clone();
    sink_1.receiver = receivers[4].clone();
    sink_2.receiver = receivers[5].clone();

    // activates all network components
    sim.activate(pg1.run(sim));
    sim.activate(pg2.run(sim));
    sim.activate(splitter_1.run());
    sim.activate(splitter_2.run());
    sim.activate(drr_server.run(sim));
    sim.activate(ps.run(sim));
    sim.activate(sink_1.run(sim));
    sim.activate(sink_2.run(sim));

    // waiting for the end of this simulation
    sim.advance(sim.shared().duration).await;
}

fn main() {
    let outcome = sim::simulation(
        Shared {
            rng: RefCell::new(SmallRng::seed_from_u64(SEED)),
            packet_size: RandomVar::new(),
            duration: 20.,
        },
        |sim| Process::new(sim, network_sim(sim)),
    );

    println!(
        "Statistics on packet sizes in this simulation: {:#.3}",
        outcome.packet_size
    );
}
