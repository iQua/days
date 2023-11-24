//! This example shows an example to simulate networks of fat tree datacenter
//! topology.

use std::cell::RefCell;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use due::packets::sink::Sink;
use due::packets::source::Source;
use rand::Rng;
use rand::{rngs::SmallRng, SeedableRng};
use statrs::distribution::{DiscreteUniform, Uniform};

use due::sim::{simulation, Process, RandomVar, SimContext};
use due::switches::SchedulingDiscipline;
use due::topos::fattree::{get_fibs, get_path, FatTree};
use due::Shared;

const SEED: u64 = 1000;

async fn network_sim(k: usize, sim: SimContext<'_, Shared>) {
    assert!(k > 0 && k % 2 == 0, "Invalid k!");

    let n_classes_per_port: usize = 4;

    // sets up parameters for packet generators and switches
    let port_rate = (4000 * 8) as f64;
    let capacity = 100;
    let arr_interval_dist = Arc::new(|| Uniform::new(1.0, 1.0).unwrap());
    let packet_size_dist = Arc::new(|| DiscreteUniform::new(1000, 1000).unwrap());

    // initializes flow_classes and weights for all DRRServer inside switches
    let flow_classes = Arc::new(move |flow_id| flow_id % n_classes_per_port);
    let weights = (1..=n_classes_per_port).collect::<Vec<_>>();
    let num_edge_switches = k.pow(2) / 2;
    let num_hosts = num_edge_switches * k / 2;
    let fib: Vec<_> = (0..=3).cycle().take(num_hosts).collect();

    // sets a generator and a sink
    let generator = Source::new(0., arr_interval_dist, packet_size_dist);
    let sink = Sink::default();

    // constructs the FatTree topology
    let fattree = FatTree::new(
        k,
        port_rate,
        capacity,
        flow_classes,
        weights,
        fib,
        SchedulingDiscipline::DRR,
        generator,
        sink,
    );

    // TODO: initializes paths and fibs for all flows!!!
    let mut paths = Vec::new();
    for gen_idx in 0..fattree.generators.len() {
        // first randomly set the destination of the flow
        let sink_idx = sim.shared().rng.borrow_mut().gen_range(0..num_hosts);
        paths.push(get_path(k, gen_idx, sink_idx, &sim.shared()))
    }
    println!("\n\nlength of flows: {}\n{:?}\n\n", paths.len(), paths);

    // The order of senders in switches.
    // 1. for edge-layer switches: sink1, sink2, agg1, agg2
    // 2. for agg-layer switches: edge1, edge2, core1, core2
    // 3. for core-layer switches: agg1, agg2, agg3, agg4
    // Next step is to generate a fib (Vec<HashMap<>>).
    // Given paths, for each path (starts from a pg, ends with a sink), iterate
    // all hops, and generate fib for each switch.
    // Lastly, assign fibs to all switches.
    let fibs = get_fibs(k, paths);
    println!("\n\nlength of fibs: {}\n{:?}\n\n", fibs.len(), fibs);

    // constructs, connects and activates all elements
    fattree.activate(sim);

    // waits for the end of this simulation
    sim.advance(sim.shared().duration + 1.).await;
}

fn main() {
    let outcome = simulation(
        Shared {
            rng: RefCell::new(SmallRng::seed_from_u64(SEED)),
            queueing_delay: RandomVar::new(),
            duration: 1.,
            next_id: (0..3).map(|_| AtomicUsize::new(0)).collect(),
        },
        |sim| Process::new(sim, network_sim(4, sim)),
    );

    println!(
        "Statistics on queueing delay in this simulation: {:#.3}",
        outcome.queueing_delay
    );
}
