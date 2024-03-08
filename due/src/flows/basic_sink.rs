//! Implements a basic packet sink for receiving packets and recording
//! statisctis.

use log::debug;
use std::fmt::Debug;

use asynchronix::model::{Model, Output};

use crate::flows::logger::{PeriodicLogger, Report};
use crate::flows::packet::Packet;
use crate::flows::sink::{PacketSinkReport, PacketStatistics};
use crate::next_endpoint_id;

#[derive(Debug)]
pub struct BasicPacketSink {
    pub endpoint_id: usize,
    /// the statistics of received packets
    pub packet_statistics: PacketStatistics,
    /// output: packet statistics
    pub statistics: Output<PacketStatistics>,
    /// output: outbound to packet switches
    pub output: Output<Packet>,
    /// the report of a report interval
    pub report: PacketSinkReport,
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
            report: PacketSinkReport::new(endpoint_id as u32, 0.0),
        }
    }

    pub fn log_report(&mut self, now: f64) {
        self.report.end_time = now;

        PeriodicLogger::log_report(Report::PacketSinkReport(self.report.clone()));
        debug!(
            "PacketSink {} logged a periodic report at time {:.3}.",
            self.endpoint_id, now
        );

        // resets the report
        self.report = PacketSinkReport::new(self.endpoint_id as u32, now);
    }
}

impl Model for BasicPacketSink {}
