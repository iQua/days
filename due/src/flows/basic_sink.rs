//! Implements a basic packet sink for receiving packets and recording
//! statisctis.

use std::fmt::Debug;

use asynchronix::model::{Model, Output};

use crate::flows::packet::Packet;
use crate::flows::progress::Report;
use crate::flows::sink::PacketSinkStatistics;
use crate::next_endpoint_id;

#[derive(Debug)]
pub struct BasicPacketSink {
    pub endpoint_id: usize,
    /// the statistics of received packets
    pub packet_statistics: PacketSinkStatistics,
    /// output: packet statistics
    pub statistics: Output<PacketSinkStatistics>,
    /// output: outbound to packet switches
    pub output: Output<Packet>,
    /// the interval of sending a periodic report to the progress coroutine
    pub report_interval: f64,
    /// the sender for sedning reports
    pub report_output: Output<Report>,
}

impl BasicPacketSink {
    pub fn new(report_interval: f64) -> Self {
        let endpoint_id = next_endpoint_id();
        let sink_name = format!("PacketSink {endpoint_id}");
        BasicPacketSink {
            endpoint_id,
            packet_statistics: PacketSinkStatistics::new(sink_name),
            statistics: Output::default(),
            output: Output::default(),
            report_interval,
            report_output: Output::default(),
        }
    }
}

impl Model for BasicPacketSink {}
