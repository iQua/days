//! A simple wire component.

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use statrs::statistics::Distribution;
use sim::{SimContext, Time};

use crate::packets::packet::Packet;
use crate::Shared;

pub struct Wire<A>
where
    A: Distribution<Time>,
{
    element_id: u32,
    /// the packet delay distribution
    delay_dist: Box<dyn Fn() -> A>,
    /// the time of the last sent packet, used to calculate the delay of the
    /// next packet
    last_sent: Time,
    /// the packet queue of the wire
    pub sender: UnboundedSender<Packet>,
    /// a receiver for receiving incoming packets
    pub receiver: UnboundedReceiver<Packet>,
}

impl<A> Wire<A>
where
    A: Distribution<Time>,
{
    pub fn new(element_id: u32, delay_dist: Box<dyn Fn() -> A>) -> Wire<A> {
        Wire {
            element_id,
            delay_dist,
            last_sent: 0.,
            sender: unbounded_channel().0,
            receiver: unbounded_channel().1,
        }
    }

    async fn forward_packet(&mut self, mut packet: Packet, sim: SimContext<'_, Shared>) {
        println!(
            "Wire {} received packet {} ({} bytes) from flow {} at time {:.3}.",
            self.element_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            sim.now(),
        );

        let delay = (self.delay_dist)().sample(&mut *sim.shared().rng.borrow_mut());

        if self.last_sent == 0. {
            self.last_sent = packet.time;
        }

        packet.time += delay;
        sim.advance(packet.time - self.last_sent).await;

        match self.sender.send(packet.clone()) {
            Ok(_) => {
                self.last_sent = sim.now();

                println!(
                    "Wire {} sent packet {} ({} bytes) from flow {} with a packet time of {:.3} at time {:.3}.",
                    self.element_id,
                    packet.packet_id,
                    packet.size,
                    packet.flow_id,
                    packet.time,
                    sim.now(),
                );
            }
            Err(_) => {
                panic!(
                    "Wire {}: a downstream element may have closed its channel.",
                    self.element_id
                );
            }
        }
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        while let Some(packet) = self.receiver.recv().await {
            self.forward_packet(packet, sim).await;
        }
    }
}
