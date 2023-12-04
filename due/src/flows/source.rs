//! Implements a packet generator that simulates the sending of packets with a
//!  specified inter-arrival time distribution and a packet size distribution.

use std::sync::Arc;

use log::debug;
use rand::distributions::Distribution;
use statrs::distribution::{DiscreteUniform, Exp};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::flows::flow::DistributionInfo;
use crate::flows::packet::Packet;
use crate::sim::{Simulator, Time};
use crate::{next_endpoint_id, Shared};

#[derive(Debug)]
pub struct PacketSource {
    endpoint_id: usize,
    flow_id: usize,
    initial_delay: Time,
    arr_dist: DistributionInfo,
    pkt_size_dist: DistributionInfo,
    packets_sent: usize,
    sender: UnboundedSender<Packet>,
    receiver: UnboundedReceiver<Packet>,
}

impl Clone for PacketSource {
    fn clone(&self) -> Self {
        PacketSource {
            endpoint_id: next_endpoint_id(),
            flow_id: self.flow_id,
            initial_delay: self.initial_delay,
            arr_dist: self.arr_dist,
            pkt_size_dist: self.pkt_size_dist,
            packets_sent: 0,
            sender: unbounded_channel().0,
            receiver: unbounded_channel().1,
        }
    }
}

impl PacketSource {
    pub fn new(
        flow_id: usize,
        initial_delay: Time,
        arr_dist: DistributionInfo,
        pkt_size_dist: DistributionInfo,
    ) -> PacketSource {
        PacketSource {
            endpoint_id: next_endpoint_id(),
            flow_id,
            initial_delay,
            arr_dist,
            pkt_size_dist,
            packets_sent: 0,
            sender: unbounded_channel().0,
            receiver: unbounded_channel().1,
        }
    }

    pub fn id(&self) -> usize {
        self.endpoint_id
    }

    pub fn flow_id(&self) -> usize {
        self.flow_id
    }

    pub fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        self.sender = sender;
    }

    pub fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        self.receiver = receiver;
    }

    pub fn connect_switch(
        &mut self,
        sender: UnboundedSender<Packet>,
        receiver: UnboundedReceiver<Packet>,
    ) {
        self.sender = sender;
        self.receiver = receiver;
    }

    fn packet_sent(&mut self, now: Time, packet: Packet) {
        self.packets_sent += 1;

        debug!(
            "PacketSource {} sent packet {} ({} bytes) at time {:.3}. {} packets sent.",
            self.endpoint_id, packet.packet_id, packet.size, now, self.packets_sent,
        );
    }

    pub async fn run(mut self, sim: Arc<Simulator<Shared>>) {
        debug!(
            "PacketSource {} will be waiting for {:.3} sec(s) at the beginning.",
            self.endpoint_id, self.initial_delay
        );
        let mut rng = sim.get_rng().await;

        sim.advance(self.initial_delay).await;

        while sim.now().await < sim.read_shared().await.duration {
            let interval = match self.arr_dist {
                DistributionInfo::Exp { lambda } => Exp::new(lambda).unwrap().sample(&mut rng),
                DistributionInfo::Uniform { low, high } => {
                    DiscreteUniform::new(low, high).unwrap().sample(&mut rng)
                }
            };
            sim.advance(interval).await;

            let packet_size = match self.pkt_size_dist {
                DistributionInfo::Exp { lambda } => {
                    Exp::new(lambda).unwrap().sample(&mut rng) as usize
                }
                DistributionInfo::Uniform { low, high } => {
                    DiscreteUniform::new(low, high).unwrap().sample(&mut rng) as usize
                }
            };

            let now = sim.now().await;

            let mut packet = Packet::new(
                packet_size,
                self.packets_sent,
                "PacketSource".to_string(),
                "destination".to_string(),
                self.flow_id(),
                now,
            );

            packet.send(now);
            self.packet_sent(now, packet.clone());
            let _ = sim.send(&self.sender, packet).await;
        }

        debug!(
            "PacketSource {} finished running at time {}.",
            self.endpoint_id,
            sim.now().await
        );

        sim.terminate().await;
    }
}
