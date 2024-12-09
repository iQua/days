//! Implements a packet source that simulates the sending of packets with
//! specific distributions of inter-arrival times and packet sizes.

use std::collections::HashSet;
use std::time::Duration;

use log::debug;
use rand::distributions::Distribution;
use rand::rngs::SmallRng;
use statrs::distribution::{DiscreteUniform, Exp, Uniform};

use nexosim::model::Model;
use nexosim::ports::Output;

use crate::flows::packet::Packet;
use crate::flows::source::PacketSourceReport;
use crate::flows::{DistributionInfo, TrafficCharacteristics};
use crate::next_endpoint_id;
use crate::utils::logger::CsvLogger;
use crate::utils::ui::{Report, ReportTiming};

#[derive(Debug)]
pub struct DistPacketSource {
    pub endpoint_id: usize,
    pub flow_id: usize,
    pub flow_start_after: HashSet<usize>,
    pub flow_start_time: f64,
    pub traffic: TrafficCharacteristics,
    packets_sent: usize,
    sent_size: usize,
    sent_size_in_period: usize,
    rng: SmallRng,

    pub output: Output<Packet>,
    pub report_output: Output<Report>,

    pub report_start_time: f64,
}

impl DistPacketSource {
    pub fn new(
        flow_id: usize,
        flow_start_after: Vec<usize>,
        traffic: TrafficCharacteristics,
        rng: SmallRng,
    ) -> DistPacketSource {
        DistPacketSource {
            endpoint_id: next_endpoint_id(),
            flow_id,
            flow_start_after: HashSet::from_iter(flow_start_after.iter().cloned()),
            flow_start_time: 0.0,
            traffic,
            packets_sent: 0,
            sent_size: 0,
            sent_size_in_period: 0,
            rng,
            output: Output::default(),
            report_output: Output::default(),
            report_start_time: 0.0,
        }
    }

    pub fn packet_sent(&mut self, packet: &Packet, now: f64) {
        self.packets_sent += 1;
        self.sent_size += packet.size;
        self.sent_size_in_period += packet.size;

        debug!(
            "DistPacketSource {} of flow {} sent packet {} ({} bytes) at time {:.3}. {} packets sent.",
            self.endpoint_id, self.flow_id, packet.packet_id, packet.size, now, self.packets_sent,
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
            DistributionInfo::DiscreteUniform { low, high } => {
                let dist = DiscreteUniform::new(low, high).unwrap();
                dist.sample(&mut self.rng)
            }
            DistributionInfo::Exp { lambda } => {
                let dist = Exp::new(lambda).unwrap();
                dist.sample(&mut self.rng)
            }
            DistributionInfo::Uniform { low, high } => {
                if low == high {
                    low
                } else {
                    Uniform::new(low, high).unwrap().sample(&mut self.rng)
                }
            }
        };

        let packet_size = match self.traffic.pkt_size_dist {
            DistributionInfo::DiscreteUniform { low, high } => {
                let dist = DiscreteUniform::new(low, high).unwrap();
                dist.sample(&mut self.rng)
            }
            DistributionInfo::Exp { lambda } => {
                let dist = Exp::new(lambda).unwrap();
                dist.sample(&mut self.rng)
            }
            DistributionInfo::Uniform { low, high } => {
                if low == high {
                    low
                } else {
                    Uniform::new(low, high).unwrap().sample(&mut self.rng)
                }
            }
        };

        // Ensure that the packet size is non-negative and at least 1 byte
        let rounded_packet_size = packet_size.round().max(1.0) as usize;
        let mut packet = Packet::new(rounded_packet_size, self.packets_sent, self.flow_id, now);

        if self.traffic.size.exceeded(
            self.sent_size + rounded_packet_size,
            self.flow_start_time,
            now + interval,
        ) {
            packet.last_packet = true;
        }

        (packet, Duration::from_secs_f64(interval))
    }

    pub async fn send_packet(&mut self, now: f64) -> Duration {
        let (packet, interval) = self.produce_packet(now);

        self.output.send(packet.clone()).await;

        self.packet_sent(&packet, now);

        interval
    }

    pub fn traffic_exceeded(&self, now: f64) -> bool {
        self.traffic
            .size
            .exceeded(self.sent_size, self.flow_start_time, now)
    }

    pub fn log_report(&mut self, now: f64, timing: ReportTiming) {
        let report = PacketSourceReport {
            id: self.endpoint_id,
            flow_id: self.flow_id,
            start_time: self.report_start_time,
            end_time: now,
            sent_packets: self.packets_sent,
            packet_sizes: self.sent_size_in_period,
            ack_bytes: 0,
        };
        CsvLogger::log_report(Report::PacketSourceReport(report), timing);

        debug!(
            "DistPacketSource {} logged a periodic report at time {:.3}.",
            self.endpoint_id, now
        );

        // resets the statistics of report
        self.report_start_time = now;
        self.packets_sent = 0;
        self.sent_size_in_period = 0;
    }
}

impl Model for DistPacketSource {}
