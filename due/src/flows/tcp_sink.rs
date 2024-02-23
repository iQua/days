//! Implements a TCPSink, designed to send acknowledgement packets back to
//! TCPPacketSource.

use std::fmt::Debug;

use log::debug;

use asynchronix::model::{Model, Output};

use crate::flows::packet::{Packet, TCPAck};
use crate::flows::progress::Report;
use crate::flows::sink::{PacketSinkReport, PacketSinkStatistics};
use crate::next_endpoint_id;

#[derive(Debug)]
pub struct TCPPacketSink {
    pub endpoint_id: usize,
    /// the statistics of received packets
    pub packet_statistics: PacketSinkStatistics,
    /// the receive buffer, which is a priority queue that is sorted based on
    /// the sequence number of the packet (packet_id)
    recv_buffer: Vec<(usize, usize)>,
    /// the next sequence number expected to be received
    next_seq_expected: usize,
    /// output: packet statistics
    pub statistics: Output<PacketSinkStatistics>,
    /// output: outbound to packet switches
    pub output: Output<Packet>,
    /// the report of a report interval
    pub report: PacketSinkReport,
    /// the interval of sending a periodic report to the progress coroutine
    pub report_interval: f64,
    /// the sender for sedning reports
    pub report_output: Output<Report>,
}

impl TCPPacketSink {
    pub fn new(report_interval: f64) -> TCPPacketSink {
        let endpoint_id = next_endpoint_id();
        let sink_name = format!("TCPPacketSink {endpoint_id}");
        TCPPacketSink {
            endpoint_id,
            packet_statistics: PacketSinkStatistics::new(sink_name),
            recv_buffer: Vec::new(),
            next_seq_expected: 0,
            statistics: Output::default(),
            output: Output::default(),
            report: PacketSinkReport::new(endpoint_id as u32, 0.0),
            report_interval,
            report_output: Output::default(),
        }
    }

    pub async fn wrap_up(&mut self, packet: Packet, now: f64) {
        let sequence_num = packet.packet_id;

        // inserts the packet into the receive buffer and sorts based on the
        // sequence number of the packet (packet_id)
        self.recv_buffer
            .push((sequence_num, sequence_num + packet.size));
        self.recv_buffer.sort();

        let mut merged_stats: Vec<(usize, usize)> = Vec::new();
        for (start, end) in self.recv_buffer.iter() {
            if merged_stats.last().is_some() & (start <= &merged_stats.last().unwrap_or(&(0, 0)).1)
            {
                let last = merged_stats.last_mut().unwrap();
                *last = (last.0, *end.max(&last.1));
            } else {
                merged_stats.push((*start, *end));
            }
        }

        self.recv_buffer = merged_stats;

        self.next_seq_expected = self.recv_buffer[0].1;

        let acknowledgment = Packet {
            time: packet.time,
            creation_time: packet.creation_time,
            size: 40,
            packet_id: packet.packet_id,
            flow_id: packet.flow_id,
            queueing_delay: packet.queueing_delay,
            ack: Some(TCPAck {
                sequence_num: self.next_seq_expected,
            }),
        };

        // sends the acknowledgment packet out to the TCPPacketSource now
        self.output.send(acknowledgment.clone()).await;

        debug!(
            "TCPPacketSink {} sent ack packet {} ({} bytes) at time {:.3}.",
            self.endpoint_id, acknowledgment.packet_id, acknowledgment.size, now,
        );
    }
}

impl Model for TCPPacketSink {}
