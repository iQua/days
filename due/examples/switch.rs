//! This example shows a simple example that uses a fair packet switch.

use std::cell::RefCell;

use rand::{rngs::SmallRng, SeedableRng};
use statrs::distribution::{DiscreteUniform, Uniform};

use due::packets::dist_generator::DistPacketGenerator;
use due::packets::sink::PacketSink;
use due::sim::{simulation, Process, RandomVar, SimContext};
use due::switches::switch::PacketSwitch;
use due::switches::SchedulingDiscipline;
use due::{connect_1_n, connect_n_1, Shared};

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    // initializes packet generators and packet sinks
    let mut generators = Vec::new();
    let mut sinks = Vec::new();

    for i in 0..2 {
        let generator = DistPacketGenerator::new(
            i,
            0.,
            Box::new(|| Uniform::new(1.0, 1.0).unwrap()),
            Box::new(|| DiscreteUniform::new(1000, 1000).unwrap()),
        );
        let sink = PacketSink::new(i);
        generators.push(generator);
        sinks.push(sink);
    }

    // initializes the fair packet switch
    let weights = vec![1, 2];
    let fib = vec![0, 1];
    let mut switch = PacketSwitch::new(
        0,
        2,
        (1000 * 8) as f64,
        100,
        weights,
        fib,
        SchedulingDiscipline::DRR,
    );

    // connects packet generators and the switch
    connect_n_1(&mut generators, &mut switch);
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
        },
        |sim| Process::new(sim, network_sim(sim)),
    );

    println!(
        "Statistics on queueing delay in this simulation: {:#.3}",
        outcome.queueing_delay
    );
}
