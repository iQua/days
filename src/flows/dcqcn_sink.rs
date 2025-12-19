//! Implements a DCQCN sink that generates CNP packets on CE-marked traffic.

use log::debug;

use nexosim::model::Model;
use nexosim::ports::Output;

use crate::flows::packet::{ControlPacket, EcnField, Packet};
use crate::flows::sink::{PacketSinkReport, PacketStatistics};
use crate::flows::{FlowFinishMsg, TrafficCharacteristics};
use crate::next_endpoint_id;
use crate::utils::logger::CsvLogger;
use crate::utils::logger::{Report, ReportTiming};

#[derive(Debug)]
pub struct DcqcnPacketSink {
    /// the current simulation time, maintained locally. This is useful for reducing the competition
    /// for access the global simulation clock, which will only be accessed when absolutely necessary
    pub time: f64,

    pub endpoint_id: usize,
    pub flow_id: usize,
    pub packet_statistics: PacketStatistics,
    pub statistics: Output<PacketStatistics>,
    pub output: Output<Packet>,
    pub flow_finish_outputs: Vec<Output<FlowFinishMsg>>,

    report_start_time: f64,
    received_packets: usize,
    received_sizes: usize,
    queueing_delay_mean: f64,
    one_way_delay_mean: f64,

    last_cnp_time: f64,
    cnp_interval: f64,
    cnp_priority: u8,
}

impl DcqcnPacketSink {
    pub fn new(flow_id: usize, traffic: &TrafficCharacteristics) -> Self {
        let endpoint_id = next_endpoint_id();
        let sink_name = format!("DCQCNPacketSink {endpoint_id}");
        let dcqcn = traffic
            .dcqcn
            .as_ref()
            .expect("DCQCN traffic requires DCQCN characteristics");

        let cnp_interval = dcqcn.cnp_interval_ns.unwrap_or(50_000.0) * 1e-9;
        let cnp_priority = dcqcn.cnp_priority.unwrap_or(0);

        DcqcnPacketSink {
            time: 0.0,
            endpoint_id,
            flow_id,
            packet_statistics: PacketStatistics::new(sink_name),
            statistics: Output::default(),
            output: Output::default(),
            flow_finish_outputs: Vec::new(),
            report_start_time: 0.0,
            received_packets: 0,
            received_sizes: 0,
            queueing_delay_mean: 0.0,
            one_way_delay_mean: 0.0,
            last_cnp_time: f64::NEG_INFINITY,
            cnp_interval,
            cnp_priority,
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
            "DCQCN sink {} logged a periodic report at time {:.3}.",
            self.endpoint_id, now
        );

        self.report_start_time = now;
        self.received_packets = 0;
        self.received_sizes = 0;
    }

    async fn maybe_send_cnp(&mut self, packet: &Packet, now: f64) {
        if packet.ecn != EcnField::Ce {
            return;
        }
        if now - self.last_cnp_time < self.cnp_interval {
            return;
        }
        self.last_cnp_time = now;

        let mut cnp = Packet::new(64, packet.packet_id, self.flow_id, now);
        cnp.control = Some(ControlPacket::DcqcnCnp);
        cnp.priority = self.cnp_priority;
        cnp.ecn = EcnField::NotEct;
        cnp.cwr = false;
        cnp.last_packet = false;
        cnp.queueing_delay = packet.queueing_delay;

        self.output.send(cnp).await;

        debug!(
            "DCQCN sink {} sent CNP for flow {} at time {:.3}.",
            self.endpoint_id, self.flow_id, now
        );
    }

    pub async fn process(&mut self, packet: Packet, now: f64) {
        self.time = now;

        self.packet_statistics.update(&packet, now);
        self.update_report_stats(&packet, now);
        self.maybe_send_cnp(&packet, now).await;

        if packet.last_packet {
            self.notify_pending_sources(now).await;
        }
    }

    /// Notifies pending sources that are waiting for this flow to end
    pub async fn notify_pending_sources(&mut self, now: f64) {
        if !self.flow_finish_outputs.is_empty() {
            for output in self.flow_finish_outputs.iter_mut() {
                output
                    .send(FlowFinishMsg {
                        flow_id: self.flow_id,
                    })
                    .await;
            }
            debug!(
                "DCQCN sink {} of flow {} notified {} flow(s) to start at time {:.3}.",
                self.endpoint_id,
                self.flow_id,
                self.flow_finish_outputs.len(),
                now
            );
        }
    }
}

impl Model for DcqcnPacketSink {}
