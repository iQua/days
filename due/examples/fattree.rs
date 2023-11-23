//! This example shows an example to simulate networks of fat tree datacenter
//! topology.

use std::cell::RefCell;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use rand::Rng;
use rand::{rngs::SmallRng, SeedableRng};
use statrs::distribution::{DiscreteUniform, Uniform};

use due::sim::{simulation, Process, RandomVar, SimContext};
use due::switches::switch::PacketSwitch;
use due::switches::SchedulingDiscipline;
use due::topos::fattree::{get_path, FatTree};
use due::Shared;

const SEED: u64 = 1000;

async fn network_sim(k: usize, sim: SimContext<'_, Shared>) {
    assert!(k > 0 && k % 2 == 0, "Invalid k!");

    let n_classes_per_port: usize = 4;

    // initializes number of elements for all layers
    let num_core_switches = (k / 2).pow(2);
    let num_agg_switches = (k.pow(2)) / 2;
    let num_edge_switches = (k.pow(2)) / 2;
    let num_hosts = num_edge_switches * k / 2;

    // initializes Vecs for all switches
    let mut edge_switches = Vec::new();
    let mut agg_switches = Vec::new();
    let mut core_switches = Vec::new();

    // sets up parameters for packet generators and switches
    let port_rate = (4000 * 8) as f64;
    let capacity = 100;
    let arr_interval_dist = Arc::new(|| Uniform::new(1.0, 1.0).unwrap());
    let packet_size_dist = Arc::new(|| DiscreteUniform::new(1000, 1000).unwrap());

    // constructs the fattree topology
    let mut fattree = FatTree::new(k, 0., arr_interval_dist, packet_size_dist);

    // initializes flow_classes and weights for all DRRServer inside switches
    let flow_classes = Arc::new(move |flow_id| flow_id % n_classes_per_port);
    let weights = (1..=n_classes_per_port).collect::<Vec<_>>();

    // TODO: initializes paths and fibs for all flows!!!
    let mut paths = Vec::new();
    for (pg_idx, generator) in fattree.generators.iter().enumerate() {
        // first randomly set the destination of the flow
        let sink_idx = sim.shared().rng.borrow_mut().gen_range(0..num_hosts);
        paths.push(get_path(k, pg_idx, sink_idx))
    }

    let fib: Vec<_> = (0..=3).cycle().take(num_hosts).collect();

    // initializes switches in the edge layer
    for _ in 0..num_edge_switches {
        let switch = PacketSwitch::new(
            k,
            port_rate,
            capacity,
            weights.clone(),
            fib.clone(),
            SchedulingDiscipline::DRR,
            flow_classes.clone(),
        );
        edge_switches.push(switch);
    }

    // initializes switches in the aggregation layer
    for _ in 0..num_agg_switches {
        let switch = PacketSwitch::new(
            k,
            port_rate,
            capacity,
            weights.clone(),
            fib.clone(),
            SchedulingDiscipline::DRR,
            flow_classes.clone(),
        );
        agg_switches.push(switch);
    }

    // initializes switches in the core layer
    for _ in 0..num_core_switches {
        let switch = PacketSwitch::new(
            k,
            port_rate,
            capacity,
            weights.clone(),
            fib.clone(),
            SchedulingDiscipline::DRR,
            flow_classes.clone(),
        );
        core_switches.push(switch);
    }

    // sets switches for the fattree topology
    fattree.set_switches(edge_switches, agg_switches, core_switches);

    // connects and activates all elements
    fattree.run(sim);

    // waits for the end of this simulation
    sim.advance(sim.shared().duration + 100.).await;
}

fn main() {
    let outcome = simulation(
        Shared {
            rng: RefCell::new(SmallRng::seed_from_u64(SEED)),
            queueing_delay: RandomVar::new(),
            duration: 10.,
            next_id: (0..3).map(|_| AtomicUsize::new(0)).collect(),
        },
        |sim| Process::new(sim, network_sim(4, sim)),
    );

    println!(
        "Statistics on queueing delay in this simulation: {:#.3}",
        outcome.queueing_delay
    );
}
