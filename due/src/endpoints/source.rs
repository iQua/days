//! Implements a packet source that simulates the sending of packets with
//! specific distributions of inter-arrival times and packet sizes.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use log::debug;
use rand::distributions::Distribution;
use rand::{rngs::SmallRng, SeedableRng};
use statrs::distribution::{DiscreteUniform, Exp};

use asynchronix::model::{InitializedModel, Model, Output};
use asynchronix::time::Scheduler;

use crate::flows::flow::DistributionInfo;
use crate::flows::packet::Packet;
use crate::next_endpoint_id;

#[derive(Debug)]
pub struct PacketSource {
    endpoint_id: usize,
    flow_id: usize,
    initial_delay: f64,
    duration: f64,
    arr_dist: DistributionInfo,
    pkt_size_dist: DistributionInfo,
    packets_sent: usize,
    seed: u64,
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
            seed: self.seed,
            rng: SmallRng::seed_from_u64(self.seed),
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
        seed: u64,
    ) -> PacketSource {
        PacketSource {
            endpoint_id: next_endpoint_id(),
            flow_id,
            initial_delay,
            duration,
            arr_dist,
            pkt_size_dist,
            packets_sent: 0,
            seed,
            rng: SmallRng::seed_from_u64(seed),
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

        packet.send(now);

        (packet, Duration::from_secs_f64(interval))
    }

    /// Sends a packet to the next element.
    async fn send(&mut self, packet: Packet) {
        self.output.send(packet).await;
    }

    async fn run(&mut self, scheduler: &Scheduler<Self>) {
        debug!(
            "PacketSource {} will be waiting for {:.3} sec(s) at the beginning.",
            self.endpoint_id, self.initial_delay
        );

        let mut current_time = Duration::from_secs_f64(self.initial_delay);

        let (packet, _) = self.produce_packet(current_time.as_secs_f64());
        self.packet_sent(current_time, packet.clone());
        scheduler
            .schedule_event(current_time, Self::send, packet)
            .unwrap();

        while current_time <= Duration::from_secs_f64(self.duration) {
            let (packet, interval) = self.produce_packet(current_time.as_secs_f64());

            current_time += interval;
            self.packet_sent(current_time, packet.clone());

            scheduler
                .schedule_event(current_time, Self::send, packet)
                .unwrap();
        }

        debug!(
            "PacketSource {} finished running at time {}.",
            self.endpoint_id,
            current_time.as_secs_f64()
        );
    }
}

impl Model for PacketSource {
    fn init(
        mut self,
        scheduler: &Scheduler<Self>,
    ) -> Pin<Box<dyn Future<Output = InitializedModel<Self>> + Send + '_>> {
        Box::pin(async move {
            Self::run(&mut self, scheduler).await;

            self.into()
        })
    }
}
