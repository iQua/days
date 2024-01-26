//! Implements a basic packet sink for receiving packets and recording
//! statisctis.

use std::fmt::Debug;

use asynchronix::model::{Model, Output};

use crate::flows::packet::Packet;
use crate::flows::sink::PacketStatistics;
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
}

impl BasicPacketSink {
    pub fn new() -> Self {
        BasicPacketSink {
            endpoint_id: next_endpoint_id(),
            packet_statistics: PacketStatistics::new(),
            statistics: Output::default(),
            output: Output::default(),
        }
    }
}

impl Model for BasicPacketSink {}
