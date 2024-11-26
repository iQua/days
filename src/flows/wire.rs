//! The wire element adds a propagation delay to packets.

use log::debug;
use rand::distributions::Distribution;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use statrs::distribution::{DiscreteUniform, Exp, Uniform};
use std::time::Duration;

use nexosim::model::{Context, Model};
use nexosim::ports::Output;
use nexosim::time::MonotonicTime;

use crate::flows::packet::Packet;
use crate::flows::DistributionInfo;
use crate::get_seed;

#[derive(Debug)]
pub struct Wire {
    wire_id: usize,
    delay_dist: DistributionInfo,
    rng: SmallRng,

    pub output: Output<Packet>,
}

impl Wire {
    pub fn new(wire_id: usize, delay_dist: DistributionInfo) -> Wire {
        let seed = get_seed();
        let rng = match seed {
            1.. => SmallRng::seed_from_u64(seed as u64),
            _ => SmallRng::from_entropy(),
        };

        Wire {
            wire_id,
            delay_dist,
            rng,
            output: Output::default(),
        }
    }

    pub async fn packet_received(&mut self, mut packet: Packet, cx: &Context<Self>) {
        let now = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

        debug!(
            "Wire {} received packet {} ({} bytes) from flow {} at time {:.3}.",
            self.wire_id, packet.packet_id, packet.size, packet.flow_id, now,
        );

        let delay = match self.delay_dist {
            DistributionInfo::DiscreteUniform { low, high } => DiscreteUniform::new(low, high)
                .unwrap()
                .sample(&mut self.rng),
            DistributionInfo::Exp { lambda } => Exp::new(lambda).unwrap().sample(&mut self.rng),
            DistributionInfo::Uniform { low, high } => {
                if low == high {
                    low
                } else {
                    Uniform::new(low, high).unwrap().sample(&mut self.rng)
                }
            }
        };

        // updates the packet's time and advances the simulation to that time
        // before sending the packet, whose queueing delay remains unchanged
        packet.time += delay;

        if packet.time > now {
            cx.schedule_event(
                Duration::from_secs_f64(packet.time - now),
                Self::forward_packet,
                packet.clone(),
            )
            .unwrap();
        } else {
            self.forward_packet(packet).await;
        }
    }

    async fn forward_packet(&mut self, packet: Packet) {
        debug!(
            "Wire {} sent packet {} ({} bytes) from flow {} at time {:.3}.",
            self.wire_id, packet.packet_id, packet.size, packet.flow_id, packet.time,
        );

        self.output.send(packet).await;
    }
}

impl Model for Wire {}
