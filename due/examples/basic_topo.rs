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
use due::switches::switch::PacketSwitch;
use due::switches::SchedulingDiscipline;
use due::topos::Topology;
use due::{Element, EndPoint, Shared};

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    let graph = UnGraph::<i32, ()>::from_edges(&[(0, 1)]);

    let arr_interval_dist = Arc::new(|| Exp::new(1.0).unwrap());
    let packet_size_dist = Arc::new(|| DiscreteUniform::new(1000, 1500).unwrap());

    let mut endpoints: Vec<Box<dyn EndPoint>> = Vec::new();
    // creates a collection of packet sources
    for _ in 0..2 {
        let source = PacketSource::new(0, 1.0, arr_interval_dist.clone(), packet_size_dist.clone());
        endpoints.push(Box::new(source));
    }

    // creates a sink
    let sink = PacketSink::default();
    endpoints.push(Box::new(sink));

    // creates a wire that can be used to connect elements in the network
    let wire = Wire::new(0, Box::new(|| Uniform::new(2.0, 2.0).unwrap()));

    // initializes a packet switch
    let weights = vec![1, 1];
    let fib = vec![0];

    let mut switch_1 = PacketSwitch::new(
        1,
        (1000 * 8) as f64,
        100,
        weights.clone(),
        fib.clone(),
        SchedulingDiscipline::FIFO,
        Arc::new(|flow_id| flow_id),
    );

    let mut switch_2 = PacketSwitch::new(
        1,
        (1000 * 8) as f64,
        100,
        weights.clone(),
        fib.clone(),
        SchedulingDiscipline::FIFO,
        Arc::new(|flow_id| flow_id),
    );

    let mut topology = Topology::new(graph);

    // constructs the network graph with network elements
    let mut elements: Vec<Box<&mut dyn Element>> =
        vec![Box::new(&mut switch_1), Box::new(&mut switch_2)];
    topology.construct(&mut elements);

    // attaches sources and sinks to hosts in the network graph
    topology.attach(elements, endpoints, vec![0, 0, 1]);

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
