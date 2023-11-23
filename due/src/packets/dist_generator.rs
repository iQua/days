//! Implements a packet generator that simulates the sending of packets with a
//!  specified inter-arrival time distribution and a packet size distribution.

use statrs::statistics::Distribution;
use std::sync::Arc;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::packets::packet::Packet;
use crate::sim::{SimContext, Time};
use crate::{get_flow_id, get_id, Element, Shared};

pub struct DistPacketGenerator<A, B>
where
    A: Distribution<Time>,
    B: Distribution<f64>,
{
    element_id: usize,
    flow_id: usize,
    initial_delay: Time,
    arr_interval_dist: Arc<dyn Fn() -> A>,
    packet_size_dist: Arc<dyn Fn() -> B>,
    packets_sent: usize,
    sender: UnboundedSender<Packet>,
    receiver: UnboundedReceiver<Packet>,
}

impl<A, B> Element for DistPacketGenerator<A, B>
where
    A: Distribution<Time>,
    B: Distribution<f64>,
{
    fn id(&mut self) -> usize {
        self.element_id
    }

    fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        self.sender = sender;
    }

    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        self.receiver = receiver;
    }
}

impl<A, B> Clone for DistPacketGenerator<A, B>
where
    A: Distribution<Time>,
    B: Distribution<f64>,
{
    fn clone(&self) -> Self {
        DistPacketGenerator {
            element_id: get_id(),
            flow_id: get_flow_id(),
            initial_delay: self.initial_delay,
            arr_interval_dist: self.arr_interval_dist.clone(),
            packet_size_dist: self.packet_size_dist.clone(),
            packets_sent: 0,
            sender: unbounded_channel().0,
            receiver: unbounded_channel().1,
        }
    }
}

impl<A, B> DistPacketGenerator<A, B>
where
    A: Distribution<Time>,
    B: Distribution<f64>,
{
    pub fn new(
        initial_delay: Time,
        arr_interval_dist: Arc<dyn Fn() -> A>,
        packet_size_dist: Arc<dyn Fn() -> B>,
    ) -> DistPacketGenerator<A, B> {
        DistPacketGenerator {
            element_id: get_id(),
            flow_id: get_flow_id(),
            initial_delay,
            arr_interval_dist,
            packet_size_dist,
            packets_sent: 0,
            sender: unbounded_channel().0,
            receiver: unbounded_channel().1,
        }
    }

    /// Samples a DistPacketGenerator, which will not occupy ids.
    pub fn sample(
        initial_delay: Time,
        arr_interval_dist: Arc<dyn Fn() -> A>,
        packet_size_dist: Arc<dyn Fn() -> B>,
    ) -> DistPacketGenerator<A, B> {
        DistPacketGenerator {
            element_id: usize::MAX,
            flow_id: usize::MAX,
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
            let interval = (self.arr_interval_dist)().sample(&mut *sim.shared().rng.borrow_mut());
            sim.advance(interval).await;
            let packet_size =
                (self.packet_size_dist)().sample(&mut *sim.shared().rng.borrow_mut()) as usize;

            let mut packet = Packet::new(
                packet_size,
                self.packets_sent,
                "source".to_string(),
                "destination".to_string(),
                self.flow_id,
                sim.now(),
            );

            packet.send(sim.now());
            let _ = self.sender.send(packet.clone());

            self.packet_sent(sim, packet);
        }

        println!(
            "DistPacketGenerator {} finished running at time {}.",
            self.element_id,
            sim.now()
        );
    }
}
