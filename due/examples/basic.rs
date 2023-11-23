//! This example shows how to create a basic network where two packet generators
//! send packets to a wire that adds propagation delays according to a random
//! distribution, and then to a packet sink.

use std::cell::RefCell;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use rand::{rngs::SmallRng, SeedableRng};
use statrs::distribution::{DiscreteUniform, Exp, Uniform};

use due::packets::dist_generator::DistPacketGenerator;
use due::packets::sink::PacketSink;
use due::packets::wire::Wire;
use due::sim::{simulation, Process, RandomVar, SimContext};
use due::topos::{connect_n_1_homo, connect_pair};
use due::Shared;

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    let mut sink = PacketSink::new(2);
    let mut wire = Wire::new(1, Box::new(|| Uniform::new(2.0, 2.0).unwrap()));
    let arr_interval_dist = Arc::new(|| Exp::new(1.0).unwrap());
    let packet_size_dist = Arc::new(|| DiscreteUniform::new(1000, 1500).unwrap());

    // creates a collection of packet generators
    let mut generators = Vec::new();
    for i in 0..2 {
        let generator =
            DistPacketGenerator::new(i, 1.0, arr_interval_dist.clone(), packet_size_dist.clone());
        generators.push(generator);
    }

    // connects the generators to the wire
    connect_n_1_homo(&mut generators, &mut wire);
    // connects the wire to the sink
    connect_pair(&mut wire, &mut sink);

    for generator in generators {
        sim.activate(generator.run(sim));
    }
    sim.activate(wire.run(sim));
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
