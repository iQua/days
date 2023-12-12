//! Implements a packet source that simulates the sending of packets with
//! specific distributions of inter-arrival times and packet sizes.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use log::{debug, info};
use rand::distributions::Distribution;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use statrs::distribution::{DiscreteUniform, Exp};

use asynchronix::model::{InitializedModel, Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::flows::flow::DistributionInfo;
use crate::flows::packet::Packet;
use crate::{get_seed, next_endpoint_id};

#[derive(Debug)]
pub struct PacketSource {
    endpoint_id: usize,
    flow_id: usize,
    initial_delay: f64,
    duration: f64,
    arr_dist: DistributionInfo,
    pkt_size_dist: DistributionInfo,
    packets_sent: usize,
    rng: SmallRng,

    pub output: Output<Packet>,
}

impl Clone for PacketSource {
    fn clone(&self) -> Self {
        PacketSource {
            endpoint_id: next_endpoint_id(),
            flow_id: self.flow_id,
            initial_delay: self.initial_delay,
            duration: self.duration,
            arr_dist: self.arr_dist,
            pkt_size_dist: self.pkt_size_dist,
            packets_sent: 0,
            rng: self.rng.clone(),
            output: Output::default(),
        }
    }
}

impl PacketSource {
    pub fn new(
        flow_id: usize,
        initial_delay: f64,
        duration: f64,
        arr_dist: DistributionInfo,
        pkt_size_dist: DistributionInfo,
    ) -> PacketSource {
        let seed = get_seed();
        let rng = match seed {
            1.. => SmallRng::seed_from_u64(seed as u64),
            _ => SmallRng::from_entropy(),
        };

        PacketSource {
            endpoint_id: next_endpoint_id(),
            flow_id,
            initial_delay,
            duration,
            arr_dist,
            pkt_size_dist,
            packets_sent: 0,
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

        debug!(
            "PacketSource {} sent packet {} ({} bytes) at time {:.3}. {} packets sent.",
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
            "PacketSource {} received packet {} ({} bytes) from flow {} at time {:.3}.",
            self.endpoint_id, packet.packet_id, packet.size, packet.flow_id, arrival_time,
        );
    }

    fn produce_packet(&mut self, now: f64) -> (Packet, Duration) {
        let interval = match self.arr_dist {
            DistributionInfo::Exp { lambda } => Exp::new(lambda).unwrap().sample(&mut self.rng),
            DistributionInfo::Uniform { low, high } => DiscreteUniform::new(low, high)
                .unwrap()
                .sample(&mut self.rng),
        };

        let packet_size = match self.pkt_size_dist {
            DistributionInfo::Exp { lambda } => {
                Exp::new(lambda).unwrap().sample(&mut self.rng) as usize
            }
            DistributionInfo::Uniform { low, high } => DiscreteUniform::new(low, high)
                .unwrap()
                .sample(&mut self.rng)
                as usize,
        };

        let src = format!("source-{}", self.endpoint_id);
        let dst = format!("destination-{}", self.endpoint_id);

        let mut packet = Packet::new(
            packet_size,
            self.packets_sent,
            src,
            dst,
            self.flow_id(),
            now,
        );

        packet.update(now);

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

            if now + interval.as_secs_f64() <= self.duration {
                scheduler.schedule_event(interval, Self::run, ()).unwrap();
            } else {
                info!(
                    "PacketSource {} finished running at {:.3}.",
                    self.endpoint_id, now
                );
            }
        }
    }
}

impl Model for PacketSource {
    fn init(
        self,
        scheduler: &Scheduler<Self>,
    ) -> Pin<Box<dyn Future<Output = InitializedModel<Self>> + Send + '_>> {
        Box::pin(async move {
            if self.initial_delay > 0.0 {
                scheduler
                    .schedule_event(Duration::from_secs_f64(self.initial_delay), Self::run, ())
                    .unwrap();
            } else {
                panic!(
                    "PacketSource {}'s initial delay must be positive.",
                    self.endpoint_id
                )
            }

            self.into()
        })
    }
}
