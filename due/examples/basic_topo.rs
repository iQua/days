//! This example shows how to create a basic network where two packet sources
//! send packets to a wire that adds propagation delays according to a random
//! distribution, and then to a packet sink.

use std::cell::RefCell;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use petgraph::graph::UnGraph;
use rand::{rngs::SmallRng, SeedableRng};
use statrs::distribution::{DiscreteUniform, Exp, Uniform};

use due::packets::sink::PacketSink;
use due::packets::source::PacketSource;
use due::packets::wire::Wire;
use due::sim::{simulation, Process, RandomVar, SimContext};
use due::topos;
use due::{Shared, Sink, Source};

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    let graph = UnGraph::<i32, ()>::from_edges(&[(0, 1)]);

    let mut sources: Vec<Box<dyn Source>> = Vec::new();
    let mut sinks: Vec<Box<dyn Sink>> = Vec::new();

    let arr_interval_dist = Arc::new(|| Exp::new(1.0).unwrap());
    let packet_size_dist = Arc::new(|| DiscreteUniform::new(1000, 1500).unwrap());

    // creates a collection of packet sources
    for _ in 0..2 {
        let mut source =
            PacketSource::new(0, 1.0, arr_interval_dist.clone(), packet_size_dist.clone());
        sources.push(Box::new(source));
    }

    // creates a sink
    let mut sink = PacketSink::default();
    sinks.push(Box::new(sink));

    // creates a wire that can be used to connect elements in the network
    let mut wire = Wire::new(0, Box::new(|| Uniform::new(2.0, 2.0).unwrap()));

    // constructs the network graph with the (optionally provided) wire
    topology = topos::construct(graph, &mut wire);

    // attaches sources and sinks to hosts in the network graph
    topology.attach(sources, sinks);

    // waits for the end of this simulation
    sim.advance(sim.shared().duration + 100.).await;
}

fn main() {
    let outcome = simulation(
        Shared {
            rng: RefCell::new(SmallRng::seed_from_u64(SEED)),
            queueing_delay: RandomVar::new(),
            duration: 10.,
            next_id: AtomicUsize::new(0),
        },
        |sim| Process::new(sim, network_sim(sim)),
    );

    println!(
        "Statistics on queueing delay in this simulation: {:#.3}",
        outcome.queueing_delay
    );
}
