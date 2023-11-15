//! A basic example that four packet generators sends packets to a sink via two
//! ports, where their queues are limited by bytes and packet numbers,
//! respectively.

use rand::{rngs::SmallRng, SeedableRng};
use rand_distr::{Exp, Uniform};
use std::cell::RefCell;

use ns::packets::packet::Packet;
use ns::packets::dist_generator::DistPacketGenerator;
use ns::packets::sink::PacketSink;
use ns::ports::port::Port;
use ns::Shared;
use sim::{channel, Process, RandomVar, SimContext, Sender, Receiver};

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    let mut senders: Vec<Sender<Packet>> = Vec::new();
    let mut receivers: Vec<Receiver<Packet>> = Vec::new();
    for _ in 0..3 {
        let (sender, receiver) = channel();
        senders.push(sender);
        receivers.push(receiver);
    }

    for i in 0..4 {
        let mut generator = DistPacketGenerator::new(
            i,
            1.0,
            Box::new(|| Exp::new(1.).unwrap()),
            Box::new(|| Uniform::new(1000, 1001)),
        );
        generator.sender = senders[i as usize/2].clone();
        sim.activate(generator.run(sim));
    }
    
    let mut port_on_packets = Port::new(0, (1000*8) as f64, 1, false);
    let mut port_on_bytes = Port::new(1, (1000*8) as f64, 1000, true);
    let mut sink = PacketSink::new(0);

    port_on_packets.receiver = receivers[0].clone();
    port_on_bytes.receiver = receivers[1].clone();
    sink.receiver = receivers[2].clone();

    port_on_packets.sender = senders[2].clone();
    port_on_bytes.sender = senders[2].clone();
    
    sim.activate(port_on_packets.run(sim));
    sim.activate(port_on_bytes.run(sim));
    sim.activate(sink.run(sim));

    // waiting for the end of this simulation
    sim.advance(sim.shared().duration).await;
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
