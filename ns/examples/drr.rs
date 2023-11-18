//! TODO

use rand::{rngs::SmallRng, SeedableRng};
use rand_distr::Uniform;
use std::{cell::RefCell, collections::HashMap};

use ns::packets::dist_generator::DistPacketGenerator;
use ns::packets::sink::PacketSink;
use ns::schedulers::drr::DRRServer;
use ns::utils::distribution::FixedDistribution;
use ns::utils::splitter::Splitter;
use ns::Shared;
use sim::{Process, RandomVar, SimContext};
use tokio::sync::mpsc::unbounded_channel;

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    // initializes channels

    let (sender_0, receiver_0) = unbounded_channel();
    let (sender_1, receiver_1) = unbounded_channel();
    let (sender_2, receiver_2) = unbounded_channel();
    let (sender_3, receiver_3) = unbounded_channel();
    let (sender_4, receiver_4) = unbounded_channel();
    let (sender_5, receiver_5) = unbounded_channel();

    // initializes packet generators
    let mut pg1 = DistPacketGenerator::new(
        0,
        0.0,
        Box::new(|| FixedDistribution(1.75)),
        Box::new(|| Uniform::new(1000, 1001)),
    );
    let mut pg2 = DistPacketGenerator::new(
        1,
        1.0,
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
    weights.insert(1, 1);
    let mut drr_server = DRRServer::new(0, (1000 * 8) as f64 / 1.75, weights);

    // initializes splitters
    let mut splitter_1 = Splitter::new(3);
    let mut splitter_2 = Splitter::new(4);

    // connects packet generators and splitters
    pg1.sender = sender_0;
    pg2.sender = sender_1;
    splitter_1.receiver = receiver_0;
    splitter_2.receiver = receiver_1;

    // connects splitters and the DRR server
    splitter_1.sender_1 = sender_2.clone();
    splitter_2.sender_1 = sender_2.clone();
    drr_server.receiver = receiver_2;

    // connects the DRR server and the packet sink
    drr_server.sender = sender_3;
    ps.receiver = receiver_3;

    // connects splitters and packet sinks
    splitter_1.sender_2 = sender_4;
    splitter_2.sender_2 = sender_5;
    sink_1.receiver = receiver_4;
    sink_2.receiver = receiver_5;

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
    sim.advance(sim.shared().duration + 100.).await;
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
