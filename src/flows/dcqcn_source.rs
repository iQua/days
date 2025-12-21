//! Implements a packet source that simulates DCQCN rate control.

use std::collections::HashSet;

use log::debug;
use rand::distr::{Distribution, Uniform};
use rand::rngs::SmallRng;
use rand_distr::Exp;

use nexosim::model::Model;
use nexosim::ports::Output;

use crate::flows::packet::{ControlPacket, EcnField, Packet};
use crate::flows::source::PacketSourceReport;
use crate::flows::{DistributionInfo, FlowFinishMsg, TrafficCharacteristics};
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

#[cfg(all(feature = "lean", feature = "dcqcn"))]
fn to_last_ns(time_s: f64) -> Option<u64> {
    time_s.is_finite().then(|| to_ns(time_s))
}

#[derive(Debug)]
pub struct DcqcnPacketSource {
    /// the current simulation time, maintained locally. This is useful for reducing the competition
    /// for access the global simulation clock, which will only be accessed when absolutely necessary
    pub time: f64,

    pub endpoint_id: usize,
    pub flow_id: usize,
    pub priority: u8,
    pub flow_start_after: HashSet<usize>,
    pub flow_start_time: f64,
    pub traffic: TrafficCharacteristics,
    pub traffic_exceeded: bool,

    rate_bps: f64,
    init_rate_bps: f64,
    min_rate_bps: f64,
    max_rate_bps: f64,
    alpha: f64,
    g: f64,
    ai_rate_bps: f64,
    hai_rate_bps: f64,
    mi_factor: f64,
    rtt: f64,
    cnp_interval: f64,
    pacing_interval: f64,
    last_cnp_time: f64,
    cnp_seen: bool,

    rng: SmallRng,

    packets_sent: usize,
    sent_size: usize,
    sent_size_in_period: usize,

    pub output: Output<Packet>,
    /// output: outbound to the user interface
    pub ui_output: Output<FlowFinishMsg>,
    /// outputs: outbounds to packet sources of flows waiting for this flow to finish
    pub flow_finish_outputs: Vec<Output<FlowFinishMsg>>,
    sent_flow_finish_msg: bool,

    pub report_start_time: f64,
}

impl DcqcnPacketSource {
    pub fn new(
        flow_id: usize,
        flow_start_after: Vec<usize>,
        traffic: TrafficCharacteristics,
        priority: u8,
        rng: SmallRng,
    ) -> DcqcnPacketSource {
        let dcqcn = traffic
            .dcqcn
            .clone()
            .expect("DCQCN traffic requires DCQCN characteristics");

        let rate_bps = dcqcn.rate_gbps * 1e9;
        let min_rate_bps = dcqcn.min_rate_gbps * 1e9;
        let max_rate_bps = dcqcn.max_rate_gbps * 1e9;
        let ai_rate_bps = dcqcn.ai_rate_gbps * 1e9;
        let hai_rate_bps = dcqcn.hai_rate_gbps * 1e9;

        let rtt = dcqcn.rtt_ns.unwrap_or(100_000.0) * 1e-9;
        let cnp_interval = dcqcn.cnp_interval_ns.unwrap_or(50_000.0) * 1e-9;
        let pacing_interval = dcqcn.pacing_interval_ns.unwrap_or(1_000.0) * 1e-9;
        DcqcnPacketSource {
            time: 0.0,
            endpoint_id: next_endpoint_id(),
            flow_id,
            priority,
            flow_start_after: HashSet::from_iter(flow_start_after.iter().cloned()),
            flow_start_time: 0.0,
            traffic,
            traffic_exceeded: false,
            rate_bps,
            init_rate_bps: rate_bps,
            min_rate_bps,
            max_rate_bps,
            alpha: 0.0,
            g: dcqcn.g,
            ai_rate_bps,
            hai_rate_bps,
            mi_factor: dcqcn.mi_factor,
            rtt: rtt.max(1e-9),
            cnp_interval: cnp_interval.max(0.0),
            pacing_interval: pacing_interval.max(0.0),
            last_cnp_time: f64::NEG_INFINITY,
            cnp_seen: false,
            rng,
            packets_sent: 0,
            sent_size: 0,
            sent_size_in_period: 0,
            output: Output::default(),
            ui_output: Output::default(),
            flow_finish_outputs: Vec::new(),
            sent_flow_finish_msg: false,
            report_start_time: 0.0,
        }
    }

    pub fn timer_interval(&self) -> f64 {
        self.rtt
    }

    pub fn current_rate_bps(&self) -> f64 {
        self.rate_bps
    }

    pub fn current_alpha(&self) -> f64 {
        self.alpha
    }

    pub fn packet_received(&mut self, packet: Packet, now: f64) {
        self.time = now;
        if packet.control == Some(ControlPacket::DcqcnCnp) {
            self.on_cnp(now);

            #[cfg(all(feature = "lean", feature = "dcqcn"))]
            {
                let event = DcqcnEventRow {
                    time_ns: to_ns(now),
                    event_id: CsvLogger::next_dcqcn_event_id(),
                    kind: DcqcnEventKind::CnpRecv,
                    endpoint_id: self.endpoint_id as u64,
                    flow_id: self.flow_id as u64,
                    pkt_id: Some(packet.packet_id as u64),
                    pkt_flow_id: Some(packet.flow_id as u64),
                    trigger_ecn: None,
                    cnp_priority: Some(packet.priority),
                    cnp_size_b: Some(packet.size as u64),
                    cnp_ecn: Some(DcqcnLoggedEcnField::from(packet.ecn)),
                    cnp_cwr: Some(packet.cwr),
                    cnp_last_packet: Some(packet.last_packet),
                    cnp_interval_ns: to_ns(self.cnp_interval),
                    g_ppb: to_ppb(self.g),
                    mi_ppb: to_ppb(self.mi_factor),
                    init_rate_bps: to_bps(self.init_rate_bps),
                    min_rate_bps: to_bps(self.min_rate_bps),
                    max_rate_bps: to_bps(self.max_rate_bps),
                    ai_rate_bps: to_bps(self.ai_rate_bps),
                    hai_rate_bps: to_bps(self.hai_rate_bps),
                    alpha_ppb: Some(to_ppb(self.alpha)),
                    rate_bps: Some(to_bps(self.rate_bps)),
                    cnp_seen: Some(self.cnp_seen),
                    last_cnp_ns: to_last_ns(self.last_cnp_time),
                };
                CsvLogger::try_log_report(Report::DcqcnEventRow(event), ReportTiming::InProgress);
            }
        }
    }

    fn on_cnp(&mut self, now: f64) {
        if now - self.last_cnp_time < self.cnp_interval {
            return;
        }
        self.last_cnp_time = now;
        self.cnp_seen = true;

        self.alpha = (1.0 - self.g) * self.alpha + self.g;
        let decrease = 1.0 - self.mi_factor * self.alpha;
        let decreased_rate = self.rate_bps * decrease;
        self.rate_bps = decreased_rate.max(self.min_rate_bps);

        debug!(
            "DCQCN source {} received CNP at {:.3e}: alpha {:.3} rate {:.3} Gbps",
            self.endpoint_id,
            now,
            self.alpha,
            self.rate_bps / 1e9
        );
    }

    pub fn timer_tick(&mut self, now: f64) {
        self.time = now;
        if !self.cnp_seen {
            self.alpha *= 1.0 - self.g;
            let inc = if self.alpha < 0.1 {
                self.hai_rate_bps
            } else {
                self.ai_rate_bps
            };
            self.rate_bps = (self.rate_bps + inc).min(self.max_rate_bps);
        }
        self.cnp_seen = false;

        #[cfg(all(feature = "lean", feature = "dcqcn"))]
        {
            let event = DcqcnEventRow {
                time_ns: to_ns(now),
                event_id: CsvLogger::next_dcqcn_event_id(),
                kind: DcqcnEventKind::TimerTick,
                endpoint_id: self.endpoint_id as u64,
                flow_id: self.flow_id as u64,
                pkt_id: None,
                pkt_flow_id: None,
                trigger_ecn: None,
                cnp_priority: None,
                cnp_size_b: None,
                cnp_ecn: None,
                cnp_cwr: None,
                cnp_last_packet: None,
                cnp_interval_ns: to_ns(self.cnp_interval),
                g_ppb: to_ppb(self.g),
                mi_ppb: to_ppb(self.mi_factor),
                init_rate_bps: to_bps(self.init_rate_bps),
                min_rate_bps: to_bps(self.min_rate_bps),
                max_rate_bps: to_bps(self.max_rate_bps),
                ai_rate_bps: to_bps(self.ai_rate_bps),
                hai_rate_bps: to_bps(self.hai_rate_bps),
                alpha_ppb: Some(to_ppb(self.alpha)),
                rate_bps: Some(to_bps(self.rate_bps)),
                cnp_seen: Some(self.cnp_seen),
                last_cnp_ns: to_last_ns(self.last_cnp_time),
            };
            CsvLogger::try_log_report(Report::DcqcnEventRow(event), ReportTiming::InProgress);
        }
    }

    fn sample_packet_size(&mut self) -> usize {
        let packet_size = match &self.traffic.pkt_size_dist {
            DistributionInfo::DiscreteUniform { low, high } => {
                let dist = Uniform::new_inclusive(*low, *high).unwrap();
                dist.sample(&mut self.rng) as f64
            }
            DistributionInfo::Exp { lambda } => {
                let dist = Exp::new(*lambda).unwrap();
                dist.sample(&mut self.rng)
            }
            DistributionInfo::Uniform { low, high } => {
                if (low - high).abs() < f64::EPSILON {
                    *low
                } else {
                    Uniform::new(*low, *high).unwrap().sample(&mut self.rng)
                }
            }
        };
        packet_size.round().max(1.0) as usize
    }

    pub fn packet_sent(&mut self, packet: &Packet, now: f64) {
        self.packets_sent += 1;
        self.sent_size += packet.size;
        self.sent_size_in_period += packet.size;

        debug!(
            "DCQCN source {} of flow {} sent packet {} ({} bytes) at time {:.3}.",
            self.endpoint_id, self.flow_id, packet.packet_id, packet.size, now
        );
    }

    pub fn traffic_exceeded(&self, now: f64) -> bool {
        if self.traffic_exceeded {
            return true;
        }
        self.traffic
            .size
            .exceeded(self.sent_size, self.flow_start_time, now)
    }

    pub async fn send_packet(&mut self, now: f64) -> Option<f64> {
        if self.traffic_exceeded(now) {
            return None;
        }

        let packet_size = self.sample_packet_size();
        let raw_interval = (packet_size as f64 * 8.0) / self.rate_bps.max(1.0);
        let interval = raw_interval.max(self.pacing_interval);

        let mut packet = Packet::new(packet_size, self.packets_sent, self.flow_id, now);
        packet.set_priority(self.priority);
        packet.ecn = EcnField::Ect0;
        if self.traffic.size.exceeded(
            self.sent_size + packet_size,
            self.flow_start_time,
            now + interval,
        ) {
            packet.last_packet = true;
            self.traffic_exceeded = true;
        }

        self.output.send(packet.clone()).await;
        self.packet_sent(&packet, now);

        if self.traffic_exceeded {
            return None;
        }

        Some(interval)
    }

    pub fn log_report(&mut self, now: f64, timing: ReportTiming) {
        let report = PacketSourceReport {
            id: self.endpoint_id,
            flow_id: self.flow_id,
            start_time: self.report_start_time,
            end_time: now,
            sent_packets: self.packets_sent,
            packet_sizes: self.sent_size_in_period,
            ack_bytes: 0,
        };
        CsvLogger::log_report(Report::PacketSourceReport(report), timing);

        debug!(
            "DCQCN source {} logged a periodic report at time {:.3}.",
            self.endpoint_id, now
        );

        self.report_start_time = now;
        self.packets_sent = 0;
        self.sent_size_in_period = 0;
    }

    /// Notifies sources that wait for this flow to end.
    pub async fn wrap_up(&mut self, now: f64) {
        if !self.sent_flow_finish_msg && !self.flow_finish_outputs.is_empty() {
            for output in self.flow_finish_outputs.iter_mut() {
                output
                    .send(FlowFinishMsg {
                        flow_id: self.flow_id,
                    })
                    .await;
            }
            debug!(
                "DCQCN source {} of flow {} notified {} flow(s) to start at time {:.3}.",
                self.endpoint_id,
                self.flow_id,
                self.flow_finish_outputs.len(),
                now
            );
            self.sent_flow_finish_msg = true;
        }
    }
}

impl Model for DcqcnPacketSource {}
