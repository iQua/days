//! This example shows an example to simulate networks of fat tree datacenter
//! topology.

use std::cell::RefCell;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use rand::{rngs::SmallRng, SeedableRng};
use statrs::distribution::{DiscreteUniform, Uniform};

use due::packets::dist_generator::DistPacketGenerator;
use due::sim::{simulation, Process, RandomVar, SimContext};
use due::switches::switch::PacketSwitch;
use due::switches::SchedulingDiscipline;
use due::topos::fattree::FatTree;
use due::{get_id, Shared};

const SEED: u64 = 1000;

async fn network_sim(k: usize, sim: SimContext<'_, Shared>) {
    assert!(k > 0 && k % 2 == 0, "Invalid k!");

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

    // initializes one generator
    let generator = DistPacketGenerator::new(
        usize::MAX,
        usize::MAX,
        0.,
        arr_interval_dist.clone(),
        packet_size_dist.clone(),
    );

    // constructs the fattree topology
    let mut fattree = FatTree::new(k, generator);

    // TODO: modify weights, fib, flow_to_classes, and add dst for flows!
    let weights: Vec<_> = (1..=4).cycle().take(num_hosts).collect();
    let fib: Vec<_> = (0..=3).cycle().take(num_hosts).collect();

    // initializes switches in the edge layer
    for _ in 0..num_edge_switches {
        let switch = PacketSwitch::new(
            get_id(),
            k,
            port_rate,
            capacity,
            weights.clone(),
            fib.clone(),
            SchedulingDiscipline::DRR,
        );
        edge_switches.push(switch);
    }

    // initializes switches in the aggregation layer
    for _ in 0..num_agg_switches {
        let switch = PacketSwitch::new(
            get_id(),
            k,
            port_rate,
            capacity,
            weights.clone(),
            fib.clone(),
            SchedulingDiscipline::DRR,
        );
        agg_switches.push(switch);
    }

    // initializes switches in the core layer
    for _ in 0..num_core_switches {
        let switch = PacketSwitch::new(
            get_id(),
            k,
            port_rate,
            capacity,
            weights.clone(),
            fib.clone(),
            SchedulingDiscipline::DRR,
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
