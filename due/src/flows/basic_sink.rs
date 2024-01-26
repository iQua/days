//! Implements a basic packet sink for receiving packets and recording
//! statisctis.

use std::fmt::Debug;

use log::debug;

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

    pub async fn report(&mut self, endpoint_id: usize) {
        assert_eq!(endpoint_id, self.endpoint_id);
        debug!("BasicPacketSink {} reporting upon request.", endpoint_id);
        self.statistics.send(self.packet_statistics.clone()).await;
    }
}

impl Model for BasicPacketSink {}
