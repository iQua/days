//! The main program for running a simulation using a specific configuration.

use std::cell::RefCell;
use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex, RwLock};

use log::{debug, info, warn};
use petgraph::graph::UnGraph;
use rand::{rngs::SmallRng, SeedableRng};

use due::flows::flow::Flow;
use due::sim::{RandomVar, Time, NewProcess, NewSimContext, NewNextEvent};
use due::topos::topo::Topology;
use due::{get_seed, Shared};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::Semaphore;

async fn network_sim(config_path: &str, sim: NewSimContext<Shared>, tx: UnboundedSender<usize>) {
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

    // initialization
    let now: Arc<RwLock<Time>> = Arc::new(RwLock::new(Time::default()));
    let calendar: RefCell<BinaryHeap<NewNextEvent<Shared>>> = RefCell::default();
    let shared = Shared {
        rng: Mutex::new(SmallRng::seed_from_u64(seed)),
        queueing_delay: RandomVar::new(),
        duration: 1.5,
    };
    let num_threads = 0;

    // initializes the UnboundedChannel
    let (tx, rx) = unbounded_channel::<usize>();
    
    let sim = NewSimContext{now, shared};
    let root = NewProcess::new(sim, network_sim(path, sim, tx.clone()));
    
    // Now we do not add root to the calendar, but spawn it directly
    let active: RefCell<NewProcess<Shared>> = RefCell::new(root.clone());
    num_threads += 1;

    tokio::spawn(root);



    // let semaphore = Arc::new(Semaphore::new(10));
    // let permit = semaphore.clone().acquire_owned().await.unwrap();
    // warn!("After owned a permit in main function.");

}
