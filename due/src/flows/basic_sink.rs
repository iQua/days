//! Implements a basic packet sink for receiving packets and recording
//! statisctis.

use std::fmt::Debug;

use asynchronix::model::{Model, Output};

use crate::flows::packet::Packet;
use crate::flows::progress::Report;
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
    /// the interval of sending a periodic report to the progress coroutine
    pub report_interval: f64,
    /// the sender for sending periodic reports
    pub report_output: Output<Report>,
}

impl BasicPacketSink {
    pub fn new(report_interval: f64) -> Self {
        let endpoint_id = next_endpoint_id();
        let sink_name = format!("PacketSink {endpoint_id}");
        BasicPacketSink {
            endpoint_id,
            packet_statistics: PacketStatistics::new(sink_name),
            statistics: Output::default(),
            output: Output::default(),
            report: PacketSinkReport::new(endpoint_id as u32, 0.0),
            report_interval,
            report_output: Output::default(),
        }
    }
}

impl Model for BasicPacketSink {}
