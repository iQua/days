//! Implements a packet generator that simulates the sending of packets with a
//!  specified inter-arrival time distribution and a packet size distribution.
use crate::packets::packet::Packet;
use crate::Shared;

use rand_distr::Distribution;
use sim::{channel, Sender, SimContext, Time};

pub struct DistPacketGenerator<A, B>
where
    A: Distribution<Time>,
    B: Distribution<u32>,
{
    element_id: u32,
    initial_delay: Time,
    arr_interval_dist: Box<dyn Fn() -> A>,
    packet_size_dist: Box<dyn Fn() -> B>,
    packets_sent: u32,
    pub sender: Sender<Packet>,
}

impl<A, B> DistPacketGenerator<A, B>
where
    A: Distribution<Time>,
    B: Distribution<u32>,
{
    pub fn new(
        element_id: u32,
        initial_delay: Time,
        arr_interval_dist: Box<dyn Fn() -> A>,
        packet_size_dist: Box<dyn Fn() -> B>,
    ) -> DistPacketGenerator<A, B> {
        DistPacketGenerator {
            element_id,
            initial_delay,
            arr_interval_dist,
            packet_size_dist,
            packets_sent: 0,
            sender: channel().0,
        }
    }

    fn packet_sent(&mut self, sim: SimContext<'_, Shared>, packet: Packet) {
        self.packets_sent += 1;

        // Update global statistics about packet sizes
        sim.shared().packet_size.tabulate(packet.size);

        println!(
            "DistPacketGenerator {} sent packet {} ({} bytes) at time {:.3}. {} packets sent.",
            self.element_id,
            packet.packet_id,
            packet.size,
            sim.now(),
            self.packets_sent,
        );
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        println!(
            "DistPacketGenerator {} will be waiting for {:.3} sec(s) at the beginning.",
            self.element_id, self.initial_delay
        );

        sim.advance(self.initial_delay).await;

        while sim.now() < sim.shared().duration {
            let packet_size = (self.packet_size_dist)().sample(&mut *sim.shared().rng.borrow_mut());

            let packet = Packet {
                production_time: sim.now(),
                time: sim.now(),
                size: packet_size,
                flow_id: self.element_id,
                packet_id: self.packets_sent,
                src: "source".to_string(),
                dst: "destination".to_string(),
            };

            self.sender
                .send(packet.clone())
                .await
                .expect("no receiving element in the simulation");

            self.packet_sent(sim, packet);

            let interval = (self.arr_interval_dist)().sample(&mut *sim.shared().rng.borrow_mut());
            sim.advance(interval).await;
        }
    }
}
