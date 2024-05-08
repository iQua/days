//! Implements a basic packet sink for receiving packets and recording
//! statisctis.

use log::debug;
use std::fmt::Debug;

use asynchronix::model::{Model, Output};

use crate::flows::packet::Packet;
use crate::flows::sink::{PacketSinkReport, PacketStatistics};
use crate::next_endpoint_id;
use crate::utils::logger::{Report, ReportLogger};

#[derive(Debug)]
pub struct BasicPacketSink {
    pub endpoint_id: usize,
    /// the statistics of received packets
    pub packet_statistics: PacketStatistics,
    /// output: packet statistics
    pub statistics: Output<PacketStatistics>,
    /// output: outbound to packet switches
    pub output: Output<Packet>,
    /// the statistics of a preiodic report
    report_start_time: f64,
    received_packets: usize,
    received_sizes: usize,
    queueing_delay_mean: f64,
    one_way_delay_mean: f64,
}

impl BasicPacketSink {
    pub fn new() -> Self {
        let endpoint_id = next_endpoint_id();
        let sink_name = format!("PacketSink {endpoint_id}");
        BasicPacketSink {
            endpoint_id,
            packet_statistics: PacketStatistics::new(sink_name),
            statistics: Output::default(),
            output: Output::default(),
            report_start_time: 0.0,
            received_packets: 0,
            received_sizes: 0,
            queueing_delay_mean: 0.0,
            one_way_delay_mean: 0.0,
        }
    }

    pub fn update_report_stats(&mut self, packet: &Packet, now: f64) {
        let num_packets = self.received_packets as f64;
        self.queueing_delay_mean =
            (self.queueing_delay_mean * num_packets + packet.queueing_delay) / (num_packets + 1.0);
        self.one_way_delay_mean = (self.one_way_delay_mean * num_packets + now
            - packet.creation_time)
            / (num_packets + 1.0);
        self.received_packets += 1;
        self.received_sizes += packet.size;
    }

    pub fn log_report(&mut self, now: f64) {
        let report = PacketSinkReport {
            id: self.endpoint_id,
            start_time: self.report_start_time,
            end_time: now,
            received_packets: self.received_packets,
            received_sizes: self.received_sizes,
            queueing_delay_mean: self.queueing_delay_mean,
            one_way_delay_mean: self.one_way_delay_mean,
        };

        ReportLogger::log_report(Report::PacketSinkReport(report));
        debug!(
            "PacketSink {} logged a periodic report at time {:.3}.",
            self.endpoint_id, now
        );

        // resets the statistics of report
        self.report_start_time = now;
        self.received_packets = 0;
        self.received_sizes = 0;
    }
}

impl Model for BasicPacketSink {}
