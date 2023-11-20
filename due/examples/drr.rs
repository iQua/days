//! This example shows a simple example that uses a Deficit Round Robin (DRR)
//! server.

use std::cell::RefCell;

use rand::{rngs::SmallRng, SeedableRng};
use statrs::distribution::{DiscreteUniform, Uniform};
use tokio::sync::mpsc::unbounded_channel;

use due::packets::dist_generator::DistPacketGenerator;
use due::packets::sink::PacketSink;
use due::schedulers::drr::DRRServer;
use due::sim::{simulation, Process, RandomVar, SimContext};
use due::utils::splitter::Splitter;
use due::{connect_pair, Shared};

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    // initializes packet generators
    let mut pg1 = DistPacketGenerator::new(
        0,
        0.0,
        Box::new(|| Uniform::new(1.0, 1.0).unwrap()),
        Box::new(|| DiscreteUniform::new(1000, 1000).unwrap()),
    );
    let mut pg2 = DistPacketGenerator::new(
        1,
        1.0,
        Box::new(|| Uniform::new(1.0, 1.0).unwrap()),
        Box::new(|| DiscreteUniform::new(1000, 1000).unwrap()),
    );

    // initializes packet sinks
    let mut ps: PacketSink = PacketSink::new(0);
    let mut sink_1: PacketSink = PacketSink::new(1);
    let mut sink_2: PacketSink = PacketSink::new(2);

    // initializes the DRR server
    let mut weights = Vec::new();
    weights.push(1);
    weights.push(2);
    let mut drr_server = DRRServer::new(0, (1000 * 8) as f64 / 1.75, weights, false);

    // initializes splitters
    let mut splitter_1 = Splitter::new(1);
    let mut splitter_2 = Splitter::new(2);

    // connects packet generators and splitters
    let (sender_0, receiver_0) = unbounded_channel();
    let (sender_1, receiver_1) = unbounded_channel();
    pg1.sender = sender_0;
    pg2.sender = sender_1;
    splitter_1.receiver = receiver_0;
    splitter_2.receiver = receiver_1;

    // connects splitters and the DRR server
    let (sender_2, receiver_2) = unbounded_channel();
    splitter_1.sender_1 = sender_2.clone();
    splitter_2.sender_1 = sender_2.clone();
    drr_server.receiver = receiver_2;
    drop(sender_2);

    // connects the DRR server and the packet sink
    connect_pair(&mut drr_server, &mut ps);

    // connects splitters and packet sinks
    let (sender_4, receiver_4) = unbounded_channel();
    let (sender_5, receiver_5) = unbounded_channel();
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
    let outcome = simulation(
        Shared {
            rng: RefCell::new(SmallRng::seed_from_u64(SEED)),
            queueing_delay: RandomVar::new(),
            duration: 20.,
        },
        |sim| Process::new(sim, network_sim(sim)),
    );

    println!(
        "Statistics on queueing delay in this simulation: {:#.3}",
        outcome.queueing_delay
    );
}
