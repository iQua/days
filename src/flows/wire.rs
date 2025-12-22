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

#[inline]
fn s_to_ns_round(t_s: f64) -> u64 {
    (t_s * 1e9).round().max(0.0) as u64
}

#[inline]
fn ns_to_s(t_ns: u64) -> f64 {
    (t_ns as f64) * 1e-9
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

        let now_ns = s_to_ns_round(packet.time);
        let now = ns_to_s(now_ns);

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

        let delay_ns = s_to_ns_round(delay);
        let arrival_ns = now_ns.saturating_add(delay_ns);
        let arrival_s = ns_to_s(arrival_ns);

        // updates the packet's time and advances the simulation to that time
        // before sending the packet, whose queueing delay remains unchanged
        packet.time = arrival_s;

        let delta_ns = arrival_ns.saturating_sub(now_ns);
        if delta_ns > 0 {
            cx.schedule_event(
                Duration::from_nanos(delta_ns),
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
