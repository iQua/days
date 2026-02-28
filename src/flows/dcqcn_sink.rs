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

#[cfg(all(feature = "lean", feature = "dcqcn"))]
use crate::utils::logger::{DcqcnEventKind, DcqcnEventRow, DcqcnLoggedEcnField};

#[cfg(all(feature = "lean", feature = "dcqcn"))]
fn to_ns(time_s: f64) -> u64 {
    (time_s.max(0.0) * 1e9).round() as u64
}

#[cfg(all(feature = "lean", feature = "dcqcn"))]
fn to_ppb(v: f64) -> u64 {
    (v.max(0.0) * 1e9).round() as u64
}

#[cfg(all(feature = "lean", feature = "dcqcn"))]
fn to_bps(v: f64) -> u64 {
    v.max(0.0).round() as u64
}

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

    // Stored for `dcqcn_events.csv` logging under the `lean` feature.
    g: f64,
    mi_factor: f64,
    init_rate_bps: f64,
    min_rate_bps: f64,
    max_rate_bps: f64,
    ai_rate_bps: f64,
    hai_rate_bps: f64,
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

        let init_rate_bps = dcqcn.rate_gbps * 1e9;
        let min_rate_bps = dcqcn.min_rate_gbps * 1e9;
        let max_rate_bps = dcqcn.max_rate_gbps * 1e9;
        let ai_rate_bps = dcqcn.ai_rate_gbps * 1e9;
        let hai_rate_bps = dcqcn.hai_rate_gbps * 1e9;

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
            g: dcqcn.g,
            mi_factor: dcqcn.mi_factor,
            init_rate_bps,
            min_rate_bps,
            max_rate_bps,
            ai_rate_bps,
            hai_rate_bps,
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

        #[cfg(all(feature = "lean", feature = "dcqcn"))]
        {
            let event = DcqcnEventRow {
                time_ns: to_ns(now),
                event_id: CsvLogger::next_dcqcn_event_id(),
                kind: DcqcnEventKind::CnpSent,
                endpoint_id: self.endpoint_id as u64,
                flow_id: self.flow_id as u64,
                pkt_id: Some(packet.packet_id as u64),
                pkt_flow_id: Some(packet.flow_id as u64),
                trigger_ecn: Some(DcqcnLoggedEcnField::from(packet.ecn)),
                cnp_priority: Some(cnp.priority),
                cnp_size_b: Some(cnp.size as u64),
                cnp_ecn: Some(DcqcnLoggedEcnField::from(cnp.ecn)),
                cnp_cwr: Some(cnp.cwr),
                cnp_last_packet: Some(cnp.last_packet),
                cnp_interval_ns: to_ns(self.cnp_interval),
                g_ppb: to_ppb(self.g),
                mi_ppb: to_ppb(self.mi_factor),
                init_rate_bps: to_bps(self.init_rate_bps),
                min_rate_bps: to_bps(self.min_rate_bps),
                max_rate_bps: to_bps(self.max_rate_bps),
                ai_rate_bps: to_bps(self.ai_rate_bps),
                hai_rate_bps: to_bps(self.hai_rate_bps),
                alpha_ppb: None,
                rate_bps: None,
                cnp_seen: None,
                last_cnp_ns: Some(to_ns(self.last_cnp_time)),
            };
            CsvLogger::try_log_report(Report::DcqcnEventRow(event), ReportTiming::InProgress);
        }

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

impl Model for DcqcnPacketSink {
    type Env = ();
}
