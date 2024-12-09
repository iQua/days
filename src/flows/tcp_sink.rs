//! Implements a TCPSink, designed to send acknowledgement packets back to
//! TCPPacketSource.

use log::debug;
use std::fmt::Debug;

use nexosim::model::Model;
use nexosim::ports::Output;

use crate::flows::packet::{Packet, TCPAck};
use crate::flows::sink::{PacketSinkReport, PacketStatistics};
use crate::flows::FlowFinishMsg;
use crate::next_endpoint_id;
use crate::utils::ui::{Report, ReportTiming};

#[derive(Debug)]
pub struct TCPPacketSink {
    pub endpoint_id: usize,
    flow_id: usize,
    /// the statistics of received packets
    pub packet_statistics: PacketStatistics,
    /// the receive buffer, which is a priority queue that is sorted based on
    /// the sequence number of the packet (packet_id)
    recv_buffer: Vec<(usize, usize)>,
    /// the next sequence number expected to be received
    next_seq_expected: usize,
    /// output: packet statistics
    pub statistics: Output<PacketStatistics>,
    /// output: outbound to packet switches
    pub output: Output<Packet>,
    /// output: outbound to the user interface
    pub report_output: Output<Report>,
    /// outputs: outbounds to packet sources of flows wait for this flow to
    /// finish
    pub flow_finish_outputs: Vec<Output<FlowFinishMsg>>,
    /// the statistics of a preiodic report
    report_start_time: f64,
    received_packets: usize,
    received_sizes: usize,
    queueing_delay_mean: f64,
    one_way_delay_mean: f64,
}

impl TCPPacketSink {
    pub fn new(flow_id: usize) -> TCPPacketSink {
        let endpoint_id = next_endpoint_id();
        let sink_name = format!("TCPPacketSink {endpoint_id}");
        TCPPacketSink {
            endpoint_id,
            flow_id,
            packet_statistics: PacketStatistics::new(sink_name),
            recv_buffer: Vec::new(),
            next_seq_expected: 0,
            statistics: Output::default(),
            output: Output::default(),
            report_output: Output::default(),
            flow_finish_outputs: Vec::new(),
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

    pub async fn update_ui(&mut self, now: f64, timing: ReportTiming) {
        let report = PacketSinkReport {
            id: self.endpoint_id,
            flow_id: self.flow_id,
            start_time: self.report_start_time,
            end_time: now,
            received_packets: self.received_packets,
            received_sizes: self.received_sizes,
            queueing_delay_mean: self.queueing_delay_mean,
            one_way_delay_mean: self.one_way_delay_mean,
            timing,
        };

        self.report_output
            .send(Report::PacketSinkReport(report))
            .await;

        debug!(
            "TCPPacketSink {} logged a periodic report at time {:.3}.",
            self.endpoint_id, now
        );

        // resets the statistics of report
        self.report_start_time = now;
        self.received_packets = 0;
        self.received_sizes = 0;
    }

    pub async fn produce_ack(&mut self, packet: Packet, now: f64) {
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
            last_packet: false,
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

    pub async fn process(&mut self, packet: Packet, now: f64) {
        self.packet_statistics.update(&packet, now);
        self.update_report_stats(&packet, now);
        self.produce_ack(packet, now).await;
    }
}

impl Model for TCPPacketSink {}
