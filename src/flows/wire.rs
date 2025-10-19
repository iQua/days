//! Implements a wire element that adds a propagation delay to packets.

use std::time::Duration;

use log::debug;
use rand::SeedableRng;
use rand::distr::Distribution;
use rand::distr::Uniform;
use rand::rngs::SmallRng;
use rand_distr::Exp;
use tracing::instrument;

use nexosim::model::{Context, Model};
use nexosim::ports::Output;

use crate::flows::DistributionInfo;
use crate::flows::packet::Packet;
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
            _ => SmallRng::from_os_rng(),
        };

        Wire {
            wire_id,
            delay_dist,
            rng,
            output: Output::default(),
        }
    }

    #[instrument(skip(self, cx))]
    pub async fn packet_received(&mut self, mut packet: Packet, cx: &mut Context<Self>) {
        #[cfg(feature = "test")]
        {
            use nexosim::time::MonotonicTime;

            let global_time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

            // makes sure that the current simulation time can be correctly retrieved from
            // the packet itself
            assert!(
                (packet.time - global_time).abs() <= 1e-7,
                "Timing mismatch: packet.time = {}, global_time = {}",
                packet.time,
                global_time
            );
        }

        let now = packet.time;

        debug!(
            "Wire {} received packet {} ({} bytes) from flow {} at time {:.3}.",
            self.wire_id, packet.packet_id, packet.size, packet.flow_id, now,
        );

        let delay = match self.delay_dist {
            DistributionInfo::DiscreteUniform { low, high } => {
                let dist = Uniform::new_inclusive(low, high).unwrap();
                dist.sample(&mut self.rng) as f64
            }
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
                packet,
            )
            .unwrap();
        } else {
            self.forward_packet(packet).await;
        }
    }

    #[instrument(skip(self))]
    async fn forward_packet(&mut self, packet: Packet) {
        debug!(
            "Wire {} sent packet {} ({} bytes) from flow {} at time {:.3}.",
            self.wire_id, packet.packet_id, packet.size, packet.flow_id, packet.time,
        );

        self.output.send(packet).await;
    }
}

impl Model for Wire {}
