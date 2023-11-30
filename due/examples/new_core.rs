//! The main program for running a simulation using a specific configuration.

use std::cell::RefCell;
use std::sync::{Arc, Mutex};
// use std::env;

use log::{debug, info, warn};
use petgraph::graph::UnGraph;
use rand::{rngs::SmallRng, SeedableRng};

use due::flows::flow::Flow;
use due::sim::{simulation, Process, RandomVar, Scheduler as Sched, SimContext};
use due::topos::topo::Topology;
use due::{get_seed, Shared};
use tokio::sync::mpsc::unbounded_channel;
use tokio::sync::Semaphore;

async fn network_sim(config_path: &str, sim: SimContext<'_, Shared>) {
    let file_path = config_path;

    let graph = UnGraph::<usize, ()>::from_edges([(0, 1)]);
    let hosts = vec![0, 1];
    info!("The network graph has been initialized: {:?}", graph);

    let flows = Flow::flows_from_config(file_path);
    debug!(
        "A total of {} network flows has been initialized.",
        flows.len()
    );

    // initializes the topology
    let topology = Topology::new(file_path, graph, hosts, flows);

    // runs the topology
    topology.run(sim);

    // waits for the end of this simulation
    sim.advance(sim.shared().duration + 100.).await;
}

#[tokio::main]
async fn main() {
    let env = env_logger::Env::default();
    env_logger::init_from_env(env);

    let path = "configs/simple.toml";
    let seed = get_seed(&path);

    // initializes the UnboundedChannel and the SemaPhore
    let (tx, rx) = unbounded_channel();
    let semaphore = Arc::new(Semaphore::new(10));

    let shared = Shared {
        rng: Mutex::new(SmallRng::seed_from_u64(seed)),
        queueing_delay: RandomVar::new(),
        duration: 1.5,
    };

    let permit = semaphore.clone().acquire_owned().await.unwrap();
    warn!("After owned a permit in main function.");
    let sched = Sched::new(shared);
    let sim = SimContext { handle: &sched };

    tokio::spawn(async move {
        let outcome = network_sim(path, sim).await;
        tx.send(outcome).unwrap();
        drop(permit);
    });

    while let Some(outcome) = rx.recv().await {
        println!(
            "Statistics on queueing delay in this simulation: {:#.3}",
            outcome.queueing_delay
        );
    }
}
