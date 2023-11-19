//! A basic example where two packet generators sends packets to a sink.

use ns::packets::dist_generator::DistPacketGenerator;
use ns::packets::sink::PacketSink;
use ns::ports::wire::Wire;
use ns::{connect, Shared};

use rand::{rngs::SmallRng, SeedableRng};
use sim::{Process, RandomVar, SimContext};
use statrs::distribution::{DiscreteUniform, Exp, Uniform};
use std::cell::RefCell;

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    let mut sink = PacketSink::new(2);
    let mut wire = Wire::new(1, Box::new(|| Uniform::new(2.0, 2.0).unwrap()));

    // creates a collection of packet generators
    let mut generators = Vec::new();
    for i in 0..2 {
        let generator = DistPacketGenerator::new(
            i,
            1.0,
            Box::new(|| Exp::new(1.).unwrap()),
            Box::new(|| DiscreteUniform::new(1000, 1500).unwrap()),
        );
        generators.push(generator);
    }

    // connects the generators to the wire
    connect(&mut generators, &mut wire);

    // connects the wire to the sink
    let mut wires = Vec::new();
    wires.push(wire);
    connect(&mut wires, &mut sink);

    for generator in generators {
        sim.activate(generator.run(sim));
    }
    for wire in wires {
        sim.activate(wire.run(sim));
    }
    sim.activate(sink.run(sim));

    // waits for the end of this simulation
    sim.advance(sim.shared().duration + 100.).await;
}

fn main() {
    let outcome = sim::simulation(
        Shared {
            rng: RefCell::new(SmallRng::seed_from_u64(SEED)),
            packet_size: RandomVar::new(),
            duration: 10.,
        },
        |sim| Process::new(sim, network_sim(sim)),
    );

    println!(
        "Statistics on packet sizes in this simulation: {:#.3}",
        outcome.packet_size
    );
}
