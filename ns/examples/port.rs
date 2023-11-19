//! A basic example that four packet generators sends packets to a sink via two
//! ports, where their queues are limited by bytes and packet numbers,
//! respectively.

use rand::{rngs::SmallRng, SeedableRng};
use statrs::distribution::Uniform;
use std::cell::RefCell;

use ns::packets::dist_generator::DistPacketGenerator;
use ns::packets::sink::PacketSink;
use ns::ports::port::Port;
use ns::{connect, Shared};
use sim::{Process, RandomVar, SimContext};

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    let mut generators = Vec::new();

    for i in 0..2 {
        let generator = DistPacketGenerator::new(
            i,
            1.0,
            Box::new(|| Uniform::new(1.0, 1.0).unwrap()),
            Box::new(|| Uniform::new(1000.0, 1000.0).unwrap()),
        );
        generators.push(generator);
    }

    let mut port = Port::new(0, (1000 * 8) as f64, 100, false);
    let mut sink = PacketSink::new(0);

    // Connecting the generators to the port
    connect(&mut generators, &mut port);

    // Connecting the port to the sink
    let mut ports = Vec::new();
    ports.push(port);
    connect(&mut ports, &mut sink);

    for generator in generators {
        sim.activate(generator.run(sim));
    }
    for port in ports {
        sim.activate(port.run(sim));
    }
    sim.activate(sink.run(sim));

    // waiting for the end of this simulation
    sim.advance(sim.shared().duration + 100.).await;
}

fn main() {
    let outcome = sim::simulation(
        Shared {
            rng: RefCell::new(SmallRng::seed_from_u64(SEED)),
            packet_size: RandomVar::new(),
            duration: 5.,
        },
        |sim| Process::new(sim, network_sim(sim)),
    );

    println!(
        "Statistics on packet sizes in this simulation: {:#.3}",
        outcome.packet_size
    );
}
