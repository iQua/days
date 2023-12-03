//! The wire element adds a propagation delay to packets.

use log::{debug, info};
use rand::distributions::Distribution;
use rand::rngs::SmallRng;
use statrs::distribution::Uniform;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::flows::packet::Packet;
use crate::sim::{Simulator, Time};
use crate::Shared;

#[derive(Debug)]
pub struct Wire {
    wire_id: usize,
    /// the time of the last sent packet, used to calculate the delay of the
    /// next packet
    last_sent: Time,
    /// the sender for sending outbound packets
    sender: UnboundedSender<Packet>,
    /// a receiver for receiving inbound packets
    receiver: UnboundedReceiver<Packet>,
}

impl Wire {
    pub fn new(wire_id: usize) -> Wire {
        Wire {
            wire_id,
            last_sent: 0.,
            sender: unbounded_channel().0,
            receiver: unbounded_channel().1,
        }
    }

    async fn forward_packet(
        &mut self,
        mut packet: Packet,
        sim: Simulator<Shared>,
        mut rng: SmallRng,
    ) {
        debug!(
            "Wire {} received packet {} ({} bytes) from flow {} at time {:.3}.",
            self.wire_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            sim.now().await,
        );

        let delay_dist = Uniform::new(2.0, 2.0).unwrap();
        let delay = delay_dist.sample(&mut rng);

        if self.last_sent == 0. {
            self.last_sent = packet.time;
        }

        // updates the packet's time and advance the simulation to that time
        // before sending the packet, whose queueing delay remains unchanged
        packet.time += delay;
        sim.advance(packet.time - self.last_sent).await;

        match self.sender.send(packet.clone()) {
            Ok(_) => {
                self.last_sent = sim.now().await;

                debug!(
                    "Wire {} sent packet {} ({} bytes) from flow {} with a packet time of {:.3} at time {:.3}.",
                    self.wire_id,
                    packet.packet_id,
                    packet.size,
                    packet.flow_id,
                    packet.time,
                    self.last_sent,
                );
            }
            Err(_) => {
                panic!(
                    "Wire {}: a downstream element may have closed its channel.",
                    self.wire_id
                );
            }
        }
    }

    pub async fn run(mut self, sim: Simulator<Shared>) {
        let rng = sim.get_rng().await;
        while let Some(packet) = sim.recv_with_permit(&mut self.receiver).await {
            self.forward_packet(packet, sim.clone(), rng.clone()).await;
        }

        info!(
            "Wire {} finished running at time {}.",
            self.wire_id,
            sim.now().await
        );
    }
}
