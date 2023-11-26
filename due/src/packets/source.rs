//! Implements a packet generator that simulates the sending of packets with a
//!  specified inter-arrival time distribution and a packet size distribution.

use rand::distributions::Distribution;
use statrs::distribution::{DiscreteUniform, Exp};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::packets::packet::Packet;
use crate::sim::{SimContext, Time};
use crate::{next_endpoint_id, Shared};

pub struct PacketSource {
    endpoint_id: usize,
    initial_delay: Time,
    packets_sent: usize,
    sender: UnboundedSender<Packet>,
    receiver: UnboundedReceiver<Packet>,
}

impl Clone for PacketSource {
    fn clone(&self) -> Self {
        PacketSource {
            endpoint_id: next_endpoint_id(),
            initial_delay: self.initial_delay,
            packets_sent: 0,
            sender: unbounded_channel().0,
            receiver: unbounded_channel().1,
        }
    }
}

impl PacketSource {
    pub fn new(initial_delay: Time) -> PacketSource {
        PacketSource {
            endpoint_id: next_endpoint_id(),
            initial_delay,
            packets_sent: 0,
            sender: unbounded_channel().0,
            receiver: unbounded_channel().1,
        }
    }

    pub fn id(&self) -> usize {
        self.endpoint_id
    }

    pub fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        self.sender = sender;
    }

    pub fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        self.receiver = receiver;
    }

    fn packet_sent(&mut self, now: Time, packet: Packet) {
        self.packets_sent += 1;

        println!(
            "PacketSource {} sent packet {} ({} bytes) at time {:.3}. {} packets sent.",
            self.endpoint_id, packet.packet_id, packet.size, now, self.packets_sent,
        );
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        println!(
            "PacketSource {} will be waiting for {:.3} sec(s) at the beginning.",
            self.endpoint_id, self.initial_delay
        );

        sim.advance(self.initial_delay).await;

        while sim.now() < sim.shared().duration {
            let interval = Exp::new(1.0)
                .unwrap()
                .sample(&mut *sim.shared().rng.borrow_mut());
            sim.advance(interval).await;
            let packet_size = DiscreteUniform::new(1000, 1500)
                .unwrap()
                .sample(&mut *sim.shared().rng.borrow_mut()) as usize;

            let mut packet = Packet::new(
                packet_size,
                self.packets_sent,
                "PacketSource".to_string(),
                "destination".to_string(),
                self.endpoint_id,
                sim.now(),
            );

            packet.send(sim.now());
            let _ = self.sender.send(packet.clone());

            self.packet_sent(sim.now(), packet);
        }

        println!(
            "PacketSource {} finished running at time {}.",
            self.endpoint_id,
            sim.now()
        );
    }
}
