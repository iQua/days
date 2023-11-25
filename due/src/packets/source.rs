//! Implements a packet generator that simulates the sending of packets with a
//!  specified inter-arrival time distribution and a packet size distribution.

use statrs::statistics::Distribution;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::packets::packet::Packet;
use crate::sim::{SimContext, Time};
use crate::{Shared, Source};

pub struct PacketSource<A, B>
where
    A: Distribution<Time>,
    B: Distribution<f64>,
{
    flow_id: usize,
    initial_delay: Time,
    arr_interval_dist: Arc<dyn Fn() -> A>,
    packet_size_dist: Arc<dyn Fn() -> B>,
    packets_sent: usize,
    sender: UnboundedSender<Packet>,
    receiver: UnboundedReceiver<Packet>,
}

impl<A, B> Source for PacketSource<A, B>
where
    A: Distribution<Time>,
    B: Distribution<f64>,
{
    fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        self.sender = sender;
    }

    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        self.receiver = receiver;
    }
}

impl<A, B> Clone for PacketSource<A, B>
where
    A: Distribution<Time>,
    B: Distribution<f64>,
{
    fn clone(&self) -> Self {
        PacketSource {
            flow_id: self.flow_id + 1,
            initial_delay: self.initial_delay,
            arr_interval_dist: self.arr_interval_dist.clone(),
            packet_size_dist: self.packet_size_dist.clone(),
            packets_sent: 0,
            sender: unbounded_channel().0,
            receiver: unbounded_channel().1,
        }
    }
}

impl<A, B> PacketSource<A, B>
where
    A: Distribution<Time>,
    B: Distribution<f64>,
{
    pub fn new(
        flow_id: usize,
        initial_delay: Time,
        arr_interval_dist: Arc<dyn Fn() -> A>,
        packet_size_dist: Arc<dyn Fn() -> B>,
    ) -> PacketSource<A, B> {
        PacketSource {
            flow_id,
            initial_delay,
            arr_interval_dist,
            packet_size_dist,
            packets_sent: 0,
            sender: unbounded_channel().0,
            receiver: unbounded_channel().1,
        }
    }

    pub fn flow_id(&self) -> usize {
        self.flow_id
    }

    fn packet_sent(&mut self, sim: SimContext<'_, Shared>, packet: Packet) {
        self.packets_sent += 1;

        println!(
            "PacketSource {} sent packet {} ({} bytes) at time {:.3}. {} packets sent.",
            self.flow_id,
            packet.packet_id,
            packet.size,
            sim.now(),
            self.packets_sent,
        );
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        println!(
            "PacketSource {} will be waiting for {:.3} sec(s) at the beginning.",
            self.flow_id, self.initial_delay
        );

        sim.advance(self.initial_delay).await;

        while sim.now() < sim.shared().duration {
            let interval = (self.arr_interval_dist)().sample(&mut *sim.shared().rng.borrow_mut());
            sim.advance(interval).await;
            let packet_size =
                (self.packet_size_dist)().sample(&mut *sim.shared().rng.borrow_mut()) as usize;

            let mut packet = Packet::new(
                packet_size,
                self.packets_sent,
                "PacketSource".to_string(),
                "destination".to_string(),
                self.flow_id,
                sim.now(),
            );

            packet.send(sim.now());
            let _ = self.sender.send(packet.clone());

            self.packet_sent(sim, packet);
        }

        println!(
            "PacketSource {} finished running at time {}.",
            self.flow_id,
            sim.now()
        );
    }
}
