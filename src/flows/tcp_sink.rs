//! Implements a TCPSink, designed to send acknowledgement packets back to
//! TCPPacketSource.

use std::fmt::Debug;

use log::debug;
use tracing::instrument;

use nexosim::model::Model;
use nexosim::ports::Output;

use crate::flows::FlowFinishMsg;
use crate::flows::packet::{EcnField, Packet, TCPAck};
use crate::flows::sink::{PacketSinkReport, PacketStatistics};
use crate::next_endpoint_id;
use crate::utils::logger::CsvLogger;
use crate::utils::logger::{Report, ReportTiming};

#[derive(Debug)]
pub struct TCPPacketSink {
    /// the current simulation time, maintained locally. This is useful for reducing the competition
    /// for access the global simulation clock, which will only be accessed when absolutely necessary
    pub time: f64,

    pub endpoint_id: usize,
    flow_id: usize,
    /// the statistics of received packets
    pub packet_statistics: PacketStatistics,
    /// the receive buffer, which is a priority queue that is sorted based on
    /// the sequence number of the packet (packet_id)
    recv_buffer: Vec<(usize, usize)>,
    /// the next sequence number expected to be received
    next_seq_expected: usize,
    /// whether the receiver is echoing ECN congestion (ECE)
    ecn_echo: bool,
    /// output: packet statistics
    pub statistics: Output<PacketStatistics>,
    /// output: outbound to packet switches
    pub output: Output<Packet>,
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
            time: 0.0,
            endpoint_id,
            flow_id,
            packet_statistics: PacketStatistics::new(sink_name),
            recv_buffer: Vec::new(),
            next_seq_expected: 0,
            ecn_echo: false,
            statistics: Output::default(),
            output: Output::default(),
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

    pub fn log_report(&mut self, now: f64, timing: ReportTiming) {
        let report = PacketSinkReport {
            id: self.endpoint_id,
            flow_id: self.flow_id,
            start_time: self.report_start_time,
            end_time: now,
            received_packets: self.received_packets,
            received_sizes: self.received_sizes,
            queueing_delay_mean: self.queueing_delay_mean,
            one_way_delay_mean: self.one_way_delay_mean,
        };
        CsvLogger::log_report(Report::PacketSinkReport(report), timing);

        debug!(
            "TCPPacketSink {} logged a periodic report at time {:.3}.",
            self.endpoint_id, now
        );

        // resets the statistics of report
        self.report_start_time = now;
        self.received_packets = 0;
        self.received_sizes = 0;
    }

    fn update_ecn_echo(&mut self, packet: &Packet) {
        if packet.cwr {
            self.ecn_echo = false;
        }
        if packet.ecn == EcnField::Ce {
            self.ecn_echo = true;
        }
    }

    #[instrument(skip(self))]
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
            priority: packet.priority,
            ack: Some(TCPAck {
                sequence_num: self.next_seq_expected,
                acknowledged_size: packet.size,
                ece: self.ecn_echo,
            }),
            control: None,
            ecn: EcnField::NotEct,
            cwr: false,
        };

        // sends the acknowledgment packet out to the TCPPacketSource now
        let ack_size = acknowledgment.size;
        let packet_id = acknowledgment.packet_id;
        self.output.send(acknowledgment).await;

        debug!(
            "TCPPacketSink {} sent ack packet {} ({} bytes) at time {:.3}.",
            self.endpoint_id, packet_id, ack_size, now,
        );
    }

    pub async fn process(&mut self, packet: Packet, now: f64) {
        // updates the locally maintained simulation time

        if packet.packet_id < self.next_seq_expected {
            log::debug!(
                "Duplicate packet received: TCP sink={} flow={} pkt_id={} now={:.3}",
                self.endpoint_id,
                packet.flow_id,
                packet.packet_id,
                now
            );
        }
        self.time = now;

        self.update_ecn_echo(&packet);
        self.packet_statistics.update(&packet, now);
        self.update_report_stats(&packet, now);
        self.produce_ack(packet, now).await;
    }
}

impl Model for TCPPacketSink {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ece_echo_latches_and_clears_on_cwr() {
        let mut sink = TCPPacketSink::new(0);

        let mut ce_packet = Packet::new(512, 0, 0, 0.0);
        ce_packet.ecn = EcnField::Ce;
        sink.update_ecn_echo(&ce_packet);
        assert!(sink.ecn_echo);

        let mut cwr_packet = Packet::new(512, 1, 0, 0.0);
        cwr_packet.cwr = true;
        sink.update_ecn_echo(&cwr_packet);
        assert!(!sink.ecn_echo);
    }
}
