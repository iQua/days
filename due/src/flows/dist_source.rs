//! Implements a packet source that simulates the sending of packets with
//! specific distributions of inter-arrival times and packet sizes.

use std::time::Duration;

use log::debug;
use rand::distributions::Distribution;
use rand::rngs::SmallRng;
use statrs::distribution::{DiscreteUniform, Exp, Uniform};

use asynchronix::model::{Model, Output};

use crate::flows::packet::Packet;
use crate::flows::source::PacketSourceReport;
use crate::flows::{DistributionInfo, TrafficCharacteristics};
use crate::next_endpoint_id;
use crate::utils::logger::{Report, ReportLogger};
use crate::utils::progress::FinishMsg;

#[derive(Debug)]
pub struct DistPacketSource {
    pub endpoint_id: usize,
    pub flow_id: usize,
    pub traffic: TrafficCharacteristics,
    packets_sent: usize,
    sent_size: usize,
    rng: SmallRng,

    pub output: Output<Packet>,
    pub finish_msg_output: Output<FinishMsg>,

    /// the report of a report interval
    pub report: PacketSourceReport,
}

impl DistPacketSource {
    pub fn new(flow_id: usize, traffic: TrafficCharacteristics, rng: SmallRng) -> DistPacketSource {
        let endpoint_id = next_endpoint_id();

        DistPacketSource {
            endpoint_id,
            flow_id,
            traffic,
            packets_sent: 0,
            sent_size: 0,
            rng,
            output: Output::default(),
            finish_msg_output: Output::default(),
            report: PacketSourceReport::new(endpoint_id as u32, 0.0),
        }
    }

    pub fn packet_sent(&mut self, packet: &Packet, now: f64) {
        self.packets_sent += 1;
        self.sent_size += packet.size;

        self.report.update(&packet);

        debug!(
            "DistPacketSource {} sent packet {} ({} bytes) at time {:.3}. {} packets sent.",
            self.endpoint_id, packet.packet_id, packet.size, now, self.packets_sent,
        );
    }

    pub fn packet_received(&mut self, packet: Packet, now: f64) {
        debug!(
            "DistPacketSource {} received packet {} ({} bytes) from flow {} at time {:.3}.",
            self.endpoint_id, packet.packet_id, packet.size, packet.flow_id, now,
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

        let packet = Packet::new(packet_size, self.packets_sent, self.flow_id, now);

        (packet, Duration::from_secs_f64(interval))
    }

    pub async fn send_packet(&mut self, now: f64) -> Duration {
        let (packet, interval) = self.produce_packet(now);

        self.output.send(packet.clone()).await;

        self.packet_sent(&packet, now);

        interval
    }

    pub fn traffic_exceeded(&self, now: f64) -> bool {
        self.traffic.size.exceeded(self.sent_size, now)
    }

    pub fn log_report(&mut self, now: f64) {
        self.report.last_update(now, 0);
        ReportLogger::log_report(Report::PacketSourceReport(self.report.clone()));
        debug!(
            "DistPacketSource {} logged a periodic report at time {:.3}.",
            self.endpoint_id, now
        );

        // resets the report
        self.report = PacketSourceReport::new(self.endpoint_id as u32, now);
    }
}

impl Model for DistPacketSource {}
