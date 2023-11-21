//! This example shows a simple example that uses a fair packet switch.

use std::cell::RefCell;

use due::switches::fair::FairPacketSwitch;
use rand::{rngs::SmallRng, SeedableRng};
use statrs::distribution::{DiscreteUniform, Uniform};
use tokio::sync::mpsc::unbounded_channel;

use due::packets::dist_generator::DistPacketGenerator;
use due::packets::sink::PacketSink;
use due::sim::{simulation, Process, RandomVar, SimContext};
use due::{connect, connect_to_many, Shared};

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
    let mut weights = Vec::new();
    let mut fib = Vec::new();
    for i in 0..2 {
        fib.push(i);
        weights.push(i + 1);
    }
    let mut fair_packet_switch = FairPacketSwitch::new(0, 2, (1000 * 8) as f64, 100, weights, fib);

    // connects packet generators and the switch
    connect(&mut generators, &mut fair_packet_switch);

    // initializes senders and receivers between the switch and packet sinks
    // Note: this should be removed later as we do not want to initialize
    // channels in the network_sim function!!!
    let mut senders = Vec::new();
    let mut receivers = Vec::new();
    for _ in 0..2 {
        let (sender, receiver) = unbounded_channel();
        senders.push(sender);
        receivers.push(receiver);
    }

    // connects the switch to packet sinks
    connect_to_many(&mut fair_packet_switch, &mut sinks, senders, receivers);

    // activates all elements
    for generator in generators {
        sim.activate(generator.run(sim));
    }
    for sink in sinks {
        sim.activate(sink.run(sim));
    }
    sim.activate(fair_packet_switch.run(sim));

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
