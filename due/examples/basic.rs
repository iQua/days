//! This example shows how to create a basic network where two packet sources
//! send packets to a wire that adds propagation delays according to a random
//! distribution, and then to a packet sink.

use std::cell::RefCell;
use std::sync::Arc;

use petgraph::graph::UnGraph;
use rand::{rngs::SmallRng, SeedableRng};

use due::packets::sink::PacketSink;
use due::packets::source::PacketSource;
use due::sim::{simulation, Process, RandomVar, SimContext};
use due::switches::switch::PacketSwitch;
use due::switches::SchedulingDiscipline;
use due::topos::topology::Topology;
use due::{Element, EndPoint, Shared};

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    // element ids in a network graph start from 0
    let graph = UnGraph::<usize, ()>::from_edges(&[(0, 1)]);
    // packet sources and sinks are endpoints
    let mut endpoints: Vec<EndPoint> = Vec::new();

    // creates a collection of packet sources
    let source = PacketSource::new(1.0);
    endpoints.push(EndPoint::PacketSource(source));

    // creates a sink
    let sink = PacketSink::default();
    endpoints.push(EndPoint::PacketSink(sink));

    // initializes a packet switch only one outbound port (#0)
    let weights = vec![1];

    let switch_1 = PacketSwitch::new(
        (1000 * 8) as f64,
        100,
        weights.clone(),
        vec![1],
        SchedulingDiscipline::FIFO,
        Arc::new(|flow_id| flow_id),
    );

    let switch_2 = PacketSwitch::new(
        (1000 * 8) as f64,
        100,
        weights.clone(),
        vec![usize::MAX],
        SchedulingDiscipline::FIFO,
        Arc::new(|flow_id| flow_id),
    );

    let elements: Vec<Element> = vec![
        Element::PacketSwitch(switch_1),
        Element::PacketSwitch(switch_2),
    ];
    let hosts = vec![0, 1];
    let mut topology = Topology::new(graph, hosts, elements, endpoints);

    // constructs the network graph with network elements
    topology.connect();
    // attaches sources and sinks to hosts in the network graph
    topology.attach(vec![0, 1]);
    // runs the topology
    topology.run(sim);

    // waits for the end of this simulation
    sim.advance(sim.shared().duration + 100.).await;
}

fn main() {
    let outcome = simulation(
        Shared {
            rng: RefCell::new(SmallRng::seed_from_u64(SEED)),
            queueing_delay: RandomVar::new(),
            duration: 10.,
        },
        |sim| Process::new(sim, network_sim(sim)),
    );

    println!(
        "Statistics on queueing delay in this simulation: {:#.3}",
        outcome.queueing_delay
    );
}
