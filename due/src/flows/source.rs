//! Implements a packet source that simulates the sending of packets with
//! specific distributions of inter-arrival times and packet sizes.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use rand::rngs::SmallRng;
use rand::SeedableRng;

use asynchronix::model::{InitializedModel, Model, Output};
use asynchronix::time::Scheduler;

use crate::flows::dist_source::DistPacketSource;
use crate::flows::packet::Packet;
use crate::flows::tcp_source::TCPPacketSource;
use crate::flows::TrafficCharacteristics;
use crate::get_seed;

#[derive(Debug)]
pub enum PacketSource {
    DistPacketSource(DistPacketSource),
    TCPPacketSource(TCPPacketSource),
}

impl PacketSource {
    pub fn new(flow_id: usize, traffic: TrafficCharacteristics, seed: usize) -> Self {
        let global_seed = get_seed();
        let rng = match global_seed {
            1.. => SmallRng::seed_from_u64((global_seed + seed) as u64),
            _ => SmallRng::from_entropy(),
        };

        if traffic.tcp.is_some() {
            PacketSource::TCPPacketSource(TCPPacketSource::new(flow_id, traffic, seed))
        } else {
            PacketSource::DistPacketSource(DistPacketSource::new(flow_id, traffic, seed))
        }
    }

    pub fn output(&self) -> Output<Packet> {
        match self {
            PacketSource::DistPacketSource(source) => source.output,
            PacketSource::TCPPacketSource(source) => source.output,
        }
    }

    pub fn id(&self) -> usize {
        match self {
            PacketSource::DistPacketSource(source) => source.endpoint_id,
            PacketSource::TCPPacketSource(source) => source.endpoint_id,
        }
    }

    pub fn flow_id(&self) -> usize {
        match self {
            PacketSource::DistPacketSource(source) => source.flow_id,
            PacketSource::TCPPacketSource(source) => source.flow_id,
        }
    }

    fn traffic_exceeded(&self, now: f64) -> bool {
        match self {
            PacketSource::DistPacketSource(source) => {
                source.traffic.size.exceeded(source.sent_size, now)
            }
            PacketSource::TCPPacketSource(source) => {
                source.traffic.size.exceeded(source.next_seq, now)
            }
        }
    }

    fn packet_sent(&mut self, now: f64, packet: Packet) {
        match self {
            PacketSource::DistPacketSource(source) => source.packet_sent(now, packet),
            PacketSource::TCPPacketSource(source) => source.packet_sent(now, packet),
        }
    }

    pub async fn packet_received(&mut self, packet: Packet, scheduler: &Scheduler<Self>) {
        match self {
            PacketSource::DistPacketSource(source) => source.packet_received(packet, scheduler),
            PacketSource::TCPPacketSource(source) => {
                source.ack_packet_received(packet, scheduler).await
            }
        }
    }

    pub async fn run(&mut self, _: (), scheduler: &Scheduler<Self>) {
        match self {
            PacketSource::DistPacketSource(source) => source.run((), scheduler).await,
            PacketSource::TCPPacketSource(source) => source.run((), scheduler).await,
        }
    }
}

impl Model for PacketSource {
    fn init(
        mut self,
        scheduler: &Scheduler<Self>,
    ) -> Pin<Box<dyn Future<Output = InitializedModel<Self>> + Send + '_>> {
        Box::pin(async move {
            let initial_delay = match self {
                PacketSource::DistPacketSource(source) => source.traffic.initial_delay,
                PacketSource::TCPPacketSource(source) => source.traffic.initial_delay,
            };

            if initial_delay > 0.0 {
                scheduler
                    .schedule_event(Duration::from_secs_f64(initial_delay), Self::run, ())
                    .unwrap();
            } else {
                self.run((), scheduler).await;
            }

            self.into()
        })
    }
}
