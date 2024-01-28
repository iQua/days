//! Implements a packet source that simulates the sending of packets with
//! specific distributions of inter-arrival times and packet sizes.

use std::time::Duration;

use log::debug;
use rand::distributions::Distribution;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use statrs::distribution::{DiscreteUniform, Exp, Uniform};

use asynchronix::model::{Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::flows::packet::Packet;
use crate::flows::{DistributionInfo, TrafficCharacteristics};
use crate::{get_seed, next_endpoint_id};

#[derive(Debug)]
pub struct DistPacketSource {
    pub endpoint_id: usize,
    pub flow_id: usize,
    pub traffic: TrafficCharacteristics,
    packets_sent: usize,
    pub sent_size: usize,
    rng: SmallRng,

    pub output: Output<Packet>,
}

impl DistPacketSource {
    pub fn new(flow_id: usize, traffic: TrafficCharacteristics, seed: usize) -> DistPacketSource {
        let global_seed = get_seed();
        let rng = match global_seed {
            1.. => SmallRng::seed_from_u64((global_seed + seed) as u64),
            _ => SmallRng::from_entropy(),
        };

        DistPacketSource {
            endpoint_id: next_endpoint_id(),
            flow_id,
            traffic,
            packets_sent: 0,
            sent_size: 0,
            rng,
            output: Output::default(),
        }
    }

    pub fn packet_sent(&mut self, packet: &Packet, now: f64) -> (bool, Duration) {
        self.packets_sent += 1;
        self.sent_size += packet.size;

        debug!(
            "DistPacketSource {} sent packet {} ({} bytes) at time {:.3}. {} packets sent.",
            self.endpoint_id, packet.packet_id, packet.size, now, self.packets_sent,
        );

        (false, Duration::default())
    }

    pub fn packet_received(&mut self, packet: Packet, scheduler: &Scheduler<Self>) {
        let now = scheduler.time();
        let arrival_time = now.duration_since(MonotonicTime::EPOCH).as_secs_f64();

        debug!(
            "DistPacketSource {} received packet {} ({} bytes) from flow {} at time {:.3}.",
            self.endpoint_id, packet.packet_id, packet.size, packet.flow_id, arrival_time,
        );
    }

    pub fn produce_packet(&mut self, now: f64) -> (Packet, Duration) {
        let interval = match self.traffic.arr_dist {
            DistributionInfo::DiscreteUniform { low, high } => DiscreteUniform::new(low, high)
                .unwrap()
                .sample(&mut self.rng),
            DistributionInfo::Exp { lambda } => Exp::new(lambda).unwrap().sample(&mut self.rng),
            DistributionInfo::Uniform { low, high } => {
                Uniform::new(low, high).unwrap().sample(&mut self.rng)
            }
        };

        let packet_size = match self.traffic.pkt_size_dist {
            DistributionInfo::DiscreteUniform { low, high } => DiscreteUniform::new(low, high)
                .unwrap()
                .sample(&mut self.rng)
                as usize,
            DistributionInfo::Exp { lambda } => {
                Exp::new(lambda).unwrap().sample(&mut self.rng) as usize
            }
            DistributionInfo::Uniform { low, high } => {
                Uniform::new(low, high).unwrap().sample(&mut self.rng) as usize
            }
        };

        let mut packet = Packet::new(packet_size, self.packets_sent, self.flow_id, now);
        packet.time += interval;

        (packet, Duration::from_secs_f64(interval))
    }

    pub fn schedule_next_run(&self, now: f64) -> (bool, Duration) {
        let (packet, interval) = self.produce_packet(now);
        (true, interval)
    }

    pub fn traffic_exceeded(&self, now: f64) -> bool {
        self.traffic.size.exceeded(self.sent_size, now)
    }
}

impl Model for DistPacketSource {}
