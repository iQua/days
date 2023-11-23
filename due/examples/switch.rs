//! This example shows a simple example that uses a fair packet switch.

use std::cell::RefCell;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use rand::{rngs::SmallRng, SeedableRng};
use statrs::distribution::{DiscreteUniform, Uniform};

use due::packets::dist_generator::DistPacketGenerator;
use due::packets::sink::PacketSink;
use due::sim::{simulation, Process, RandomVar, SimContext};
use due::switches::switch::PacketSwitch;
use due::switches::SchedulingDiscipline;
use due::topos::{connect_1_n, connect_n_1_homo};
use due::{get_flow_id, get_id, Shared};

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    let mut generators = Vec::new();
    let mut sinks = Vec::new();
    let arr_interval_dist = Arc::new(|| Uniform::new(1.0, 1.0).unwrap());
    let packet_size_dist = Arc::new(|| DiscreteUniform::new(1000, 1000).unwrap());

    // initializes packet generators and packet sinks
    for _ in 0..2 {
        let generator = DistPacketGenerator::new(
            get_id(),
            get_flow_id(),
            0.,
            arr_interval_dist.clone(),
            packet_size_dist.clone(),
        );
        let sink = PacketSink::new(get_id());
        generators.push(generator);
        sinks.push(sink);
    }

    // initializes the fair packet switch
    let weights = vec![1, 2];
    let fib = vec![0, 1];
    let mut switch = PacketSwitch::new(
        get_id(),
        2,
        (1000 * 8) as f64,
        100,
        weights,
        fib,
        SchedulingDiscipline::DRR,
    );

    // connects packet generators and the switch
    connect_n_1_homo(&mut generators, &mut switch);
    // connects the switch to packet sinks
    connect_1_n(&mut switch, &mut sinks);

    // activates all elements
    for generator in generators {
        sim.activate(generator.run(sim));
    }
    for sink in sinks {
        sim.activate(sink.run(sim));
    }
    sim.activate(switch.run(sim));

    // waits for the end of this simulation
    sim.advance(sim.shared().duration + 100.).await
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
