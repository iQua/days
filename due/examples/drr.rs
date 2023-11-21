//! This example shows a simple example that uses a Deficit Round Robin (DRR)
//! server.

use std::cell::RefCell;

use rand::{rngs::SmallRng, SeedableRng};
use statrs::distribution::{DiscreteUniform, Uniform};
use tokio::sync::mpsc::unbounded_channel;

use due::packets::dist_generator::DistPacketGenerator;
use due::packets::sink::PacketSink;
use due::packets::splitter::Splitter;
use due::schedulers::drop::{CapacityUnit, DropStrategy};
use due::schedulers::drr::DRRServer;
use due::sim::{simulation, Process, RandomVar, SimContext};
use due::{connect_1_n, connect_pair, Element, Shared};

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
    let weights = vec![1, 2];
    let mut drr_server = DRRServer::new(
        0,
        100,
        CapacityUnit::Packets,
        (1000 * 8) as f64,
        DropStrategy::TailDrop,
        weights,
    );

    // initializes splitters
    let mut splitter_1 = Splitter::new(1);
    let mut splitter_2 = Splitter::new(2);

    // connects packet generators and splitters
    connect_pair(&mut pg1, &mut splitter_1);
    connect_pair(&mut pg2, &mut splitter_2);

    // connects splitters and the DRR server
    let mut downstreams: Vec<Box<dyn Element>> = vec![Box::new(drr_server)];
    downstreams.push(Box::new(sink_1));
    connect_1_n(&mut splitter_1, &mut downstreams);

    // connects the DRR server and the packet sink
    connect_pair(&mut drr_server, &mut ps);

    let (sender_0, receiver_0) = unbounded_channel();
    let (sender_1, receiver_1) = unbounded_channel();
    let (sender_2, receiver_2) = unbounded_channel();
    let senders_splitter_1 = vec![sender_0.clone(), sender_1];
    let senders_splitter_2 = vec![sender_0, sender_2];

    // connects splitters to packet sinks and the DRR server
    splitter_1.connect_senders(senders_splitter_1);
    splitter_2.connect_senders(senders_splitter_2);
    drr_server.connect_receiver(receiver_0);
    sink_1.connect_receiver(receiver_1);
    sink_2.connect_receiver(receiver_2);

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
