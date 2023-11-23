//! In this example, four packet generators send packets to a sink via two
//! ports, where their queues are limited by bytes and packet numbers,
//! respectively.

use rand::{rngs::SmallRng, SeedableRng};
use statrs::distribution::Uniform;
use std::cell::RefCell;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use due::packets::dist_generator::DistPacketGenerator;
use due::packets::sink::PacketSink;
use due::schedulers::drop::{CapacityUnit, DropStrategy};
use due::schedulers::port::Port;
use due::sim::{simulation, Process, RandomVar, SimContext};
use due::topos::{connect_n_1_homo, connect_pair};
use due::Shared;

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    let mut generators = Vec::new();
    let arr_interval_dist = Arc::new(|| Uniform::new(1.0, 1.0).unwrap());
    let packet_size_dist = Arc::new(|| Uniform::new(1000.0, 1000.0).unwrap());

    for _ in 0..2 {
        let generator =
            DistPacketGenerator::new(1.0, arr_interval_dist.clone(), packet_size_dist.clone());
        generators.push(generator);
    }

    let mut port = Port::new(
        (1000 * 8) as f64,
        2,
        CapacityUnit::Packets,
        DropStrategy::TailDrop,
    );
    let mut sink = PacketSink::default();

    // connects the generators to the port
    connect_n_1_homo(&mut generators, &mut port);

    // connects the port to the sink
    connect_pair(&mut port, &mut sink);

    for generator in generators {
        sim.activate(generator.run(sim));
    }
    sim.activate(port.run(sim));
    sim.activate(sink.run(sim));

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
        |sim| Process::new(sim, network_sim(sim)),
    );

    println!(
        "Statistics on queueing delay in this simulation: {:#.3}",
        outcome.queueing_delay
    );
}
