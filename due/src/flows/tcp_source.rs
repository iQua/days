//! Implements a packet source that simulates the TCP protocol, including
//! support for various congestion control mechanisms.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use log::{debug, info};
use rand::distributions::Distribution;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use statrs::distribution::{DiscreteUniform, Exp, Uniform};

use asynchronix::model::{InitializedModel, Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::flows::packet::Packet;
use crate::flows::{DistributionInfo, TrafficCharacteristics};
use crate::{get_seed, next_endpoint_id};

#[derive(Debug)]
pub struct TCPPacketSource {
    endpoint_id: usize,
    flow_id: usize,
    traffic: TrafficCharacteristics,
    packets_sent: usize,
    sent_size: usize,
    rng: SmallRng,

    pub output: Output<Packet>,
}

impl TCPPacketSource {
    pub fn new(flow_id: usize, traffic: TrafficCharacteristics, seed: usize) -> TCPPacketSource {
        let global_seed = get_seed();
        let rng = match global_seed {
            1.. => SmallRng::seed_from_u64((global_seed + seed) as u64),
            _ => SmallRng::from_entropy(),
        };

        TCPPacketSource {
            endpoint_id: next_endpoint_id(),
            flow_id,
            traffic,
            packets_sent: 0,
            sent_size: 0,
            rng,
            output: Output::default(),
        }
    }

    pub fn id(&self) -> usize {
        self.endpoint_id
    }

    pub fn flow_id(&self) -> usize {
        self.flow_id
    }

    fn packet_sent(&mut self, now: Duration, packet: Packet) {
        self.packets_sent += 1;
        self.sent_size += packet.size;

        debug!(
            "TCPPacketSource {} sent packet {} ({} bytes) at time {:.3}. {} packets sent.",
            self.endpoint_id,
            packet.packet_id,
            packet.size,
            now.as_secs_f64(),
            self.packets_sent,
        );
    }

    pub fn packet_received(&mut self, packet: Packet, scheduler: &Scheduler<Self>) {
        let now = scheduler.time();
        let arrival_time = now.duration_since(MonotonicTime::EPOCH).as_secs_f64();

        debug!(
            "TCPPacketSource {} received packet {} ({} bytes) from flow {} at time {:.3}.",
            self.endpoint_id, packet.packet_id, packet.size, packet.flow_id, arrival_time,
        );
    }

    fn produce_packet(&mut self, now: f64) -> (Packet, Duration) {
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

        let packet = Packet::new(packet_size, self.packets_sent, self.flow_id(), now);
        (packet, Duration::from_secs_f64(interval))
    }

    pub fn run<'a>(
        &'a mut self,
        _: (),
        scheduler: &'a Scheduler<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let current_time = scheduler.time().duration_since(MonotonicTime::EPOCH);
            let now = current_time.as_secs_f64();
            let (packet, interval) = self.produce_packet(now);

            // sends the packet out to the next element now
            self.output.send(packet.clone()).await;
            self.packet_sent(current_time, packet);

            if (self.sent_size < self.traffic.size)
                & (now + interval.as_secs_f64() <= self.traffic.duration)
            {
                scheduler.schedule_event(interval, Self::run, ()).unwrap();
            } else {
                info!(
                    "TCPPacketSource {} of Flow {} finished running at {:.3}.",
                    self.endpoint_id, self.flow_id, now
                );
            }
        }
    }
}

impl Model for TCPPacketSource {
    fn init(
        self,
        scheduler: &Scheduler<Self>,
    ) -> Pin<Box<dyn Future<Output = InitializedModel<Self>> + Send + '_>> {
        Box::pin(async move {
            if self.traffic.initial_delay > 0.0 {
                scheduler
                    .schedule_event(
                        Duration::from_secs_f64(self.traffic.initial_delay),
                        Self::run,
                        (),
                    )
                    .unwrap();
            } else {
                panic!(
                    "TCPPacketSource {}'s initial delay must be positive.",
                    self.endpoint_id
                )
            }

            self.into()
        })
    }
}
