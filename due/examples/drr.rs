//! This example shows a simple example that uses a Deficit Round Robin (DRR)
//! server.

use std::cell::RefCell;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use rand::{rngs::SmallRng, SeedableRng};
use statrs::distribution::{DiscreteUniform, Uniform};

use due::packets::sink::PacketSink;
use due::packets::source::PacketSource;
use due::packets::splitter::Splitter;
use due::schedulers::drop::{CapacityUnit, DropStrategy};
use due::schedulers::drr::DRRServer;
use due::sim::{simulation, Process, RandomVar, SimContext};
use due::topos::{connect_n_m, connect_pair};
use due::{Element, Shared};

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    let arr_interval_dist = Arc::new(|| Uniform::new(1.0, 1.0).unwrap());
    let packet_size_dist = Arc::new(|| DiscreteUniform::new(1000, 1000).unwrap());

    // initializes packet generators
    let mut generator_1 =
        PacketSource::new(0.0, arr_interval_dist.clone(), packet_size_dist.clone());
    let mut generator_2 = PacketSource::new(0, arr_interval_dist.clone(), packet_size_dist.clone());

    // initializes splitters
    let mut splitter_1 = Splitter::default();
    let mut splitter_2 = Splitter::default();

    // initializes the DRR server
    let weights = vec![1, 2];
    let mut drr_server = DRRServer::new(
        0,
        (1000 * 8) as f64,
        100,
        CapacityUnit::Packets,
        Arc::new(|flow_id| flow_id),
        DropStrategy::TailDrop,
        weights,
    );
    let drr_server_id = drr_server.id();

    // initializes packet sinks
    let mut sink: PacketSink = Sink::default();
    let mut sink_1: Sink = Sink::default();
    let sink_1_id = sink_1.id();
    let mut sink_2: Sink = Sink::default();
    let sink_2_id = sink_2.id();

    // connects packet generators and splitters
    connect_pair(&mut generator_1, &mut splitter_1);
    connect_pair(&mut generator_2, &mut splitter_2);

    // connects splitters and the DRR server
    let mut upstreams: Vec<Box<&mut dyn Element>> =
        vec![Box::new(&mut splitter_1), Box::new(&mut splitter_2)];

    let mut downstreams: Vec<Box<&mut dyn Element>> = vec![
        Box::new(&mut sink_1),
        Box::new(&mut drr_server),
        Box::new(&mut sink_2),
    ];

    connect_n_m(
        &mut upstreams,
        &mut downstreams,
        vec![
            vec![sink_1_id, drr_server_id],
            vec![drr_server_id, sink_2_id],
        ],
    );

    // connects the DRR server and the packet sink
    connect_pair(&mut drr_server, &mut sink);

    // activates all network components
    sim.activate(generator_1.run(sim));
    sim.activate(generator_2.run(sim));
    sim.activate(splitter_1.run());
    sim.activate(splitter_2.run());
    sim.activate(drr_server.run(sim));
    sim.activate(sink.run(sim));
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
            next_id: AtomicUsize::new(0),
        },
        |sim| Process::new(sim, network_sim(sim)),
    );

    println!(
        "Statistics on queueing delay in this simulation: {:#.3}",
        outcome.queueing_delay
    );
}
