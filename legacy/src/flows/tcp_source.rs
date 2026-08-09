//! Implements a packet source that simulates the TCP protocol, including
//! support for various congestion control mechanisms.
use std::cmp::Ordering;
use std::cmp::min;
use std::collections::{BinaryHeap, HashMap, HashSet};

use core::fmt;
use log::debug;
use rand::rngs::SmallRng;

use nexosim::model::Model;
use nexosim::ports::Output;
use nexosim::simulation::EventKey;

use crate::flows::app_source::AppSourceBufferHandle;
use crate::flows::bbr::TCPBBR;
use crate::flows::cc::{AckEvent, CCAlgorithm, CongestionControl, CongestionEvent, RateSample};
use crate::flows::cubic::TCPCubic;
use crate::flows::dist_source::DistPacketSource;
use crate::flows::packet::{EcnField, Packet};
use crate::flows::reno::TCPReno;
use crate::flows::source::PacketSourceReport;
use crate::flows::{FlowFinishMsg, FlowSize, TrafficCharacteristics};
use crate::next_endpoint_id;
use crate::utils::exact_time::{behavior_delay_ns, seconds_view};
use crate::utils::logger::CsvLogger;
use crate::utils::logger::{Report, ReportTiming, TcpMetricsReport};

#[cfg(feature = "lean")]
use crate::utils::logger::{CubicEventKind, CubicEventRow};

#[cfg(feature = "lean")]
fn to_ns(time_s: f64) -> u64 {
    (time_s.max(0.0) * 1e9).round() as u64
}

#[cfg(feature = "lean")]
fn to_ppb(v: f64) -> u64 {
    (v.max(0.0) * 1e9).round() as u64
}

#[derive(Debug, Clone)]
pub struct PacketTimeout {
    pub packet_id: usize,
    pub rto_ns: u64,
    pub deadline_ns: u64,
}

#[derive(Debug, Clone)]
struct SentPacketMeta {
    sent_time: f64,
    first_sent_time: f64,
    delivered_at_send: usize,
    delivered_time_at_send: f64,
    prior_inflight: usize,
    is_app_limited: bool,
}

pub struct SyntheticDataSource {
    dist: DistPacketSource,
}

impl SyntheticDataSource {
    fn new(flow_id: usize, traffic: TrafficCharacteristics, rng: SmallRng) -> Self {
        Self {
            dist: DistPacketSource::new(flow_id, Vec::new(), traffic, 0, rng),
        }
    }

    pub fn set_flow_start_time(&mut self, flow_start_time: f64) {
        self.dist.flow_start_time = flow_start_time;
    }

    pub fn produce_data(&mut self, now: f64) -> (Packet, f64) {
        let (packet, interval) = self.dist.produce_packet(now);
        self.dist.packet_sent(&packet, now);
        (packet, interval)
    }

    pub fn traffic_exceeded(&self, now: f64) -> bool {
        self.dist.traffic_exceeded(now)
    }
}

impl PartialOrd for PacketTimeout {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for PacketTimeout {
    fn eq(&self, other: &Self) -> bool {
        self.deadline_ns == other.deadline_ns && self.packet_id == other.packet_id
    }
}

impl Ord for PacketTimeout {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.deadline_ns, self.packet_id)
            .cmp(&(other.deadline_ns, other.packet_id))
            .reverse()
    }
}

impl Eq for PacketTimeout {}

pub struct TCPPacketSource {
    /// the current simulation time, maintained locally. This is useful for reducing the competition
    /// for access the global simulation clock, which will only be accessed when absolutely necessary
    pub time: f64,

    pub endpoint_id: usize,
    pub flow_id: usize,
    pub priority: u8,
    pub flow_start_after: HashSet<usize>,
    pub traffic: TrafficCharacteristics,
    pub traffic_exceeded: bool,
    /// the congestion controller
    congestion_control: Box<dyn CongestionControl + Send + Sync>,
    /// maximum segment size, in bytes
    pub mss: usize,
    /// the next sequence number to be sent, in bytes
    pub next_seq: usize,
    /// the maximum sequence number in the in-transit data buffer
    pub send_buffer: usize,
    /// the sequence number of the segment that is last acknowledged
    pub last_ack: usize,
    /// the count of duplicate acknolwedgments
    dupack: usize,
    /// deviation of the RTT
    rtt_var: f64,
    /// smoothed RTT
    smoothed_rtt: f64,
    /// the retransmission timeout
    pub rto: f64,
    /// the in-flight packets (segments)
    sent_packets: HashMap<usize, Packet>,
    sent_packet_meta: HashMap<usize, SentPacketMeta>,
    /// min-heap of in-flight packets, where packets are sorted according to
    /// their timeout
    timeout_queue: BinaryHeap<PacketTimeout>,

    pub app_source: Option<AppSourceBufferHandle>,
    synthetic_source: Option<SyntheticDataSource>,
    /// the source is considered busy retrieving the current packet from flow
    /// until this time
    pub busy_until_ns: u64,

    packets_sent: usize,
    total_original_packets: usize,
    final_original_segment_bytes: usize,
    sent_size: usize,
    sent_size_in_period: usize,
    retransmissions: usize,
    retransmitted_bytes: usize,
    timer_ticks: usize,
    completion_time_ns: Option<u64>,
    e5_metrics_enabled: bool,
    periodic_timer_key: Option<EventKey>,
    timer_cancelled: bool,

    pub output: Output<Packet>,
    /// output: outbound to the user interface
    pub ui_output: Output<FlowFinishMsg>,
    /// outputs: outbounds to packet sources of flows waiting for this flow to
    /// finish
    pub flow_finish_outputs: Vec<Output<FlowFinishMsg>>,
    sent_flow_finish_msg: bool,

    pub report_start_time: f64,

    /// Clock granularity in seconds for RTO calculation
    clock_granularity: f64,
    /// Minimum RTO value in seconds
    min_rto: f64,
    /// Maximum RTO value in seconds
    max_rto: f64,
    remaining_bytes: usize,
    app_limited: bool,
    pending_lost_bytes: usize,
    pending_ecn_marked: bool,
    ecn_enabled: bool,
    cwr_pending: bool,
    ecn_reduction_in_flight: bool,
    delivered: usize,
    delivered_time: f64,
    first_sent_time: f64,
}

impl fmt::Debug for TCPPacketSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("")
            .field(&self.endpoint_id)
            .field(&self.flow_id)
            .finish()
    }
}

impl TCPPacketSource {
    const MIN_PACING_INTERVAL: f64 = 1e-9;

    pub fn synthetic_source_mut(&mut self) -> Option<&mut SyntheticDataSource> {
        self.synthetic_source.as_mut()
    }

    pub fn has_synthetic_source(&self) -> bool {
        self.synthetic_source.is_some()
    }

    pub fn new(
        flow_id: usize,
        flow_start_after: Vec<usize>,
        traffic: TrafficCharacteristics,
        priority: u8,
        app_source: Option<AppSourceBufferHandle>,
        rng: SmallRng,
    ) -> TCPPacketSource {
        let tcp_config = traffic
            .tcp
            .as_ref()
            .expect("TCP traffic requires TCP characteristics");
        let cc_algorithm = tcp_config.cc_algorithm;
        let cubic_config = tcp_config.cubic.as_ref();
        let mss = traffic
            .tcp_mss()
            .expect("TCP traffic requires a positive fixed integral MSS");

        let congestion_control: Box<dyn CongestionControl + Send + Sync> = match cc_algorithm {
            CCAlgorithm::TCPReno => Box::new(TCPReno::with_mss(mss)),
            CCAlgorithm::TCPCubic => {
                let mut cubic = TCPCubic::with_mss(mss);
                if let Some(config) = cubic_config {
                    cubic.apply_config(config);
                }
                Box::new(cubic)
            }
            CCAlgorithm::TCPBBR => Box::new(TCPBBR::with_mss(mss)),
        };
        let ecn_enabled = traffic.tcp.as_ref().map(|tcp| tcp.ecn).unwrap_or(false);
        let (synthetic_source, remaining_bytes) = match (&app_source, &traffic.size) {
            (Some(handle), _) => (None, handle.get_total_size().unwrap_or(usize::MAX)),
            (None, FlowSize::Bytes(bytes)) => (None, *bytes),
            (None, FlowSize::Duration(_)) => (
                Some(SyntheticDataSource::new(flow_id, traffic.clone(), rng)),
                usize::MAX,
            ),
        };
        TCPPacketSource {
            time: 0.0,
            endpoint_id: next_endpoint_id(),
            flow_id,
            priority,
            flow_start_after: HashSet::from_iter(flow_start_after.iter().cloned()),
            traffic,
            traffic_exceeded: false,
            congestion_control,
            mss,
            next_seq: 0,
            send_buffer: 0,
            last_ack: 0,
            dupack: 0,
            rtt_var: 0.0,
            smoothed_rtt: 0.0,
            rto: 1.0,
            sent_packets: HashMap::new(),
            sent_packet_meta: HashMap::new(),
            timeout_queue: BinaryHeap::new(),
            app_source,
            synthetic_source,
            remaining_bytes,
            busy_until_ns: 0,
            packets_sent: 0,
            total_original_packets: 0,
            final_original_segment_bytes: 0,
            sent_size: 0,
            sent_size_in_period: 0,
            retransmissions: 0,
            retransmitted_bytes: 0,
            timer_ticks: 0,
            completion_time_ns: None,
            e5_metrics_enabled: false,
            periodic_timer_key: None,
            timer_cancelled: false,
            output: Output::default(),
            ui_output: Output::default(),
            flow_finish_outputs: Vec::new(),
            sent_flow_finish_msg: false,
            report_start_time: 0.0,
            clock_granularity: 0.001, // 1 ms granularity
            min_rto: 1.0,             // 1 second minimum as per RFC 6298
            max_rto: 60.0,            // 60 seconds maximum (commonly used value)
            app_limited: false,
            pending_lost_bytes: 0,
            pending_ecn_marked: false,
            ecn_enabled,
            cwr_pending: false,
            ecn_reduction_in_flight: false,
            delivered: 0,
            delivered_time: 0.0,
            first_sent_time: 0.0,
        }
    }

    /// Pull data from the app source and create packets with proper TCP metadata.
    /// Converts raw bytes from the application layer into TCP segments.
    /// This function is typically called:
    /// - after receiving new ACKs (to refill the window)
    /// - before sending packets (to populate the send buffer)
    pub async fn pull_from_appsource(&mut self, _now: f64) {
        // stop if all flow data has been sent
        if self.remaining_bytes == 0 {
            self.app_limited = true;
            return;
        }
        self.app_limited = false;

        // compute available sending window (cwnd - unacked data)
        let cwnd = self.congestion_control.get_cwnd();
        let window_end = self.last_ack.saturating_add(cwnd);

        let buffered_end = self.send_buffer.max(self.next_seq);
        if buffered_end >= window_end {
            return;
        }

        let win_left = window_end - buffered_end;

        if win_left == 0 {
            return;
        }

        // pull at most min(available window, remaining bytes)
        let pull_size = win_left.min(self.remaining_bytes);

        if let Some(ref mut handle) = self.app_source {
            // Pull raw bytes from application layer
            let data = handle.pull(pull_size).await;

            // Buffer bytes for paced sending
            let mut offset = 0;
            while offset < data.len() {
                let chunk_size = self.mss.min(data.len() - offset);

                // ensure we do not exceed the current window
                if self
                    .send_buffer
                    .checked_add(chunk_size)
                    .map(|next| next > window_end)
                    .unwrap_or(true)
                {
                    break;
                }

                self.send_buffer = self.send_buffer.saturating_add(chunk_size);
                self.remaining_bytes = self.remaining_bytes.saturating_sub(chunk_size);
                offset += chunk_size;
            }
        } else if self.synthetic_source.is_none() {
            self.send_buffer = self.send_buffer.saturating_add(pull_size);
            self.remaining_bytes -= pull_size;
        }

        // if all bytes have been sent and acknowledged, complete the flow
        if self.remaining_bytes == 0 {
            self.traffic_exceeded = true;
            self.app_limited = true;
        }
    }

    /// Returns whether PacketSource should call run() after TCPPacketSource
    /// handles an acknowledgment.
    pub async fn ack_packet_received(&mut self, ack_packet: Packet, now_ns: u64) -> bool {
        let now = seconds_view(now_ns);
        // updates the locally maintained simulation time
        self.time = now;

        // the received packet must be an acknowledgment
        assert!(ack_packet.ack.is_some());

        debug!(
            "TCPPacketSource {} received ack of packet {} ({} bytes) from flow {} at time {:.3}.",
            self.endpoint_id, ack_packet.packet_id, ack_packet.size, ack_packet.flow_id, now,
        );

        if self.sent_packets.contains_key(&ack_packet.packet_id) {
            self.sent_packets.remove(&ack_packet.packet_id);
            self.sent_packet_meta.remove(&ack_packet.packet_id);
            self.timeout_queue
                .retain(|packet| packet.packet_id != ack_packet.packet_id);
        }

        let ack = ack_packet.ack.unwrap();
        if self.ecn_enabled && ack.ece {
            self.pending_ecn_marked = true;
            if !self.ecn_reduction_in_flight {
                let flight_size_bytes = self.bytes_in_flight();
                self.congestion_control
                    .ecn_congestion_event(CongestionEvent {
                        now,
                        flight_size_bytes,
                    });
                self.ecn_reduction_in_flight = true;
                self.cwr_pending = true;
                #[cfg(feature = "lean")]
                self.log_cubic_event(
                    CubicEventKind::Congestion,
                    None,
                    None,
                    now,
                    Some(flight_size_bytes),
                );
            }
        } else if !ack.ece {
            self.ecn_reduction_in_flight = false;
        }
        if ack.sequence_num == self.last_ack {
            self.dupack += 1;
        } else {
            // fast recovery in RFC 2001 and TCP Reno
            if self.dupack > 0 {
                self.congestion_control.dupack_over();
                self.dupack = 0;
                #[cfg(feature = "lean")]
                self.log_cubic_event(CubicEventKind::RecoveryExit, None, None, now, None);
            }
        }

        if self.dupack >= 3 {
            if self.dupack == 3 {
                let loss_size = self
                    .sent_packets
                    .get(&ack.sequence_num)
                    .map(|pkt| pkt.size)
                    .unwrap_or(self.mss);
                self.pending_lost_bytes = self.pending_lost_bytes.saturating_add(loss_size);
                let flight_size_bytes = self.bytes_in_flight();
                self.congestion_control.congestion_event(CongestionEvent {
                    now,
                    flight_size_bytes,
                });
                #[cfg(feature = "lean")]
                self.log_cubic_event(
                    CubicEventKind::Congestion,
                    None,
                    None,
                    now,
                    Some(flight_size_bytes),
                );
            }

            if let Some(resent_pkt) = self.sent_packets.get_mut(&ack.sequence_num) {
                resent_pkt.time = now;
                Self::apply_ecn_on_retransmit(resent_pkt);
                self.output.send(resent_pkt.clone()).await;
                self.retransmissions += 1;
                self.retransmitted_bytes += resent_pkt.size;

                debug!(
                    "Due to dupack, TCPPacketSource {} resent packet {} ({} bytes) from flow {} at time {:.3}.",
                    self.endpoint_id,
                    resent_pkt.packet_id,
                    resent_pkt.size,
                    resent_pkt.flow_id,
                    now,
                );
            }

            if self.dupack > 3 {
                self.congestion_control.more_dupacks_received();

                // transmits a new packet, if allowed by the new value of cwnd
                let cwnd_limit = self.last_ack + self.congestion_control.get_cwnd();
                let send_size = self.sendable_bytes(cwnd_limit);
                if send_size > 0 {
                    debug!(
                        "TCPPacketSource {} will send packet {} ({} bytes) at time {:.3} as dupack > 3.",
                        self.endpoint_id, self.next_seq, send_size, now,
                    );

                    let mut packet = Packet::new(send_size, self.next_seq, self.flow_id, now);
                    packet.set_priority(self.priority);
                    self.apply_ecn_on_new_data(&mut packet);
                    self.output.send(packet.clone()).await;
                    self.packet_sent(&packet, now_ns);
                }
            }
        }

        if self.dupack == 0 {
            // new acknowledgment received, updates the RTT estimate and the
            // retransmission timeout
            let sample_rtt = now - ack_packet.creation_time;

            // Authoritative sources for RTO calculation

            // RFC 6298: Computing TCP's Retransmission Timer

            // This RFC specifically focuses on the RTO algorithm and updates
            // the way RTO is calculated. It obsoletes the RTO calculation
            // described in RFC 2988. The updated algorithm is commonly referred
            // to as the "Karn/Partridge Algorithm."

            // calculates the deviation (RTTVAR) of the RTT to account for
            // variations in the network
            if self.rtt_var == 0.0 {
                self.rtt_var = sample_rtt / 2.0;
                self.smoothed_rtt = sample_rtt;
                // Initial RTO as per RFC 6298
                self.rto = f64::max(
                    self.min_rto,
                    self.smoothed_rtt + f64::max(self.clock_granularity, 4.0 * self.rtt_var),
                );
            } else {
                let beta = 0.25;
                let alpha = 0.125;

                // Update RTTVAR first using the old SRTT as per RFC 6298
                self.rtt_var =
                    (1.0 - beta) * self.rtt_var + beta * (self.smoothed_rtt - sample_rtt).abs();

                // Then update the smoothed round-trip time (SRTT)
                // computes a smoothed round-trip time (SRTT)
                if self.smoothed_rtt == 0.0 {
                    self.smoothed_rtt = sample_rtt;
                } else {
                    self.smoothed_rtt = (1.0 - alpha) * self.smoothed_rtt + alpha * sample_rtt;
                }

                // Calculate new RTO with bounds
                self.rto = f64::min(
                    self.max_rto,
                    f64::max(
                        self.min_rto,
                        self.smoothed_rtt + f64::max(self.clock_granularity, 4.0 * self.rtt_var),
                    ),
                );
            }

            let prev_delivered = self.delivered;
            let delivered_bytes = if ack.sequence_num > prev_delivered {
                ack.sequence_num - prev_delivered
            } else {
                ack.acknowledged_size
            };
            if delivered_bytes > 0 {
                self.delivered = ack.sequence_num;
                self.delivered_time = now;
            }

            let sample_packet_id = ack.sequence_num.saturating_sub(ack.acknowledged_size);
            let mut rate_sample = RateSample {
                delivered: delivered_bytes,
                interval: sample_rtt,
                ack_elapsed: sample_rtt,
                send_elapsed: sample_rtt,
                rtt: sample_rtt,
                acked: ack.acknowledged_size,
                lost: self.pending_lost_bytes,
                ecn_marked: self.pending_ecn_marked,
                ..RateSample::default()
            };
            if let Some(meta) = self.sent_packet_meta.get(&sample_packet_id) {
                let ack_elapsed = (now - meta.delivered_time_at_send).max(0.0);
                let send_elapsed = (meta.sent_time - meta.first_sent_time).max(0.0);
                let interval = ack_elapsed.max(send_elapsed);
                if interval > 0.0 {
                    rate_sample.interval = interval;
                    rate_sample.ack_elapsed = ack_elapsed;
                    rate_sample.send_elapsed = send_elapsed;
                }
                if self.delivered >= meta.delivered_at_send {
                    rate_sample.delivered = self.delivered - meta.delivered_at_send;
                }
                rate_sample.prior_inflight = meta.prior_inflight;
                rate_sample.is_app_limited = meta.is_app_limited;
            }

            self.last_ack = ack.sequence_num;
            self.congestion_control.ack_received(AckEvent {
                ack_seq: ack.sequence_num,
                rtt: sample_rtt,
                now,
                bytes_acked: ack.acknowledged_size,
                rate_sample,
            });
            #[cfg(feature = "lean")]
            {
                let acked_bytes = ack.acknowledged_size.max(1);
                let acked_segs = acked_bytes.saturating_add(self.mss.saturating_sub(1)) / self.mss;
                let rtt_ns = to_ns(sample_rtt);
                let rtt_s = (rtt_ns as f64) * 1e-9;
                self.log_cubic_event(
                    CubicEventKind::Ack,
                    Some(acked_segs),
                    Some(rtt_s),
                    now,
                    None,
                );
            }
            self.pending_lost_bytes = 0;
            self.pending_ecn_marked = false;

            debug!(
                "TCPPacketSource {} received ack till sequence number {} at time {:.3}.",
                self.endpoint_id, ack.sequence_num, now,
            );

            debug!(
                "TCPPacketSource {} congestion window size = {:.3}, last ack {}.",
                self.endpoint_id,
                self.congestion_control.get_cwnd(),
                self.last_ack,
            );

            // this acknowledgment should acknowledge all the intermediate
            // segments sent between the lost packet and the receipt of the
            // first duplicate ACK, if any
            self.sent_packets
                .retain(|&packet_id, _| packet_id >= ack.sequence_num);
            self.sent_packet_meta
                .retain(|&packet_id, _| packet_id >= ack.sequence_num);
            self.timeout_queue
                .retain(|packet| packet.packet_id >= ack.sequence_num);

            if self.is_complete() && self.completion_time_ns.is_none() {
                self.completion_time_ns = Some(now_ns);
            }

            if now_ns >= self.busy_until_ns {
                return true;
            }

            self.pull_from_appsource(now).await;
        }

        false
    }

    pub fn get_cwnd_limit(&self) -> usize {
        self.last_ack + self.congestion_control.get_cwnd()
    }

    /// Returns true only after the full byte budget has been sent and cumulatively acknowledged.
    pub fn is_complete(&self) -> bool {
        self.traffic_exceeded && self.next_seq >= self.send_buffer && self.next_seq == self.last_ack
    }

    pub fn enable_e5_metrics(&mut self, enabled: bool) {
        self.e5_metrics_enabled = enabled;
    }

    pub fn set_periodic_timer_key(&mut self, key: EventKey) {
        self.periodic_timer_key = Some(key);
    }

    pub fn cancel_periodic_timer(&mut self) {
        if let Some(key) = self.periodic_timer_key.take() {
            key.cancel();
            self.timer_cancelled = true;
        }
    }

    fn bytes_in_flight(&self) -> usize {
        self.sent_packets.values().map(|packet| packet.size).sum()
    }

    #[cfg(feature = "lean")]
    fn log_cubic_event(
        &mut self,
        kind: CubicEventKind,
        acked_segs: Option<usize>,
        rtt_s: Option<f64>,
        now: f64,
        flight_size_bytes: Option<usize>,
    ) {
        let cubic = match self
            .congestion_control
            .as_any_mut()
            .downcast_mut::<TCPCubic>()
        {
            Some(cubic) => cubic,
            None => return,
        };
        let snap = cubic.snapshot();
        let event = CubicEventRow {
            time_ns: to_ns(now),
            event_id: CsvLogger::next_cubic_event_id(),
            kind,
            endpoint_id: self.endpoint_id as u64,
            flow_id: self.flow_id as u64,
            acked_segs: acked_segs.map(|v| v as u64),
            rtt_ns: rtt_s.map(to_ns),
            mss_bytes: snap.mss as u64,
            beta_ppb: to_ppb(snap.beta),
            c_ppb: to_ppb(snap.c),
            tcp_friendly: snap.tcp_friendliness,
            fast_convergence: snap.fast_convergence,
            init_cwnd_bytes: snap.init_cwnd_bytes as u64,
            init_ssthresh_bytes: snap.init_ssthresh_bytes as u64,
            flight_size_bytes: flight_size_bytes.map(|v| v as u64),
            cwnd_bytes: snap.cwnd_bytes as u64,
            ssthresh_bytes: snap.ssthresh_bytes as u64,
            w_max_bytes: snap.w_max_bytes as u64,
            w_last_max_bytes: snap.w_last_max_bytes as u64,
            epoch_start_ns: snap.epoch_start.map(to_ns),
        };
        CsvLogger::try_log_report(Report::CubicEventRow(event), ReportTiming::InProgress);
    }

    pub fn packet_sent(&mut self, packet: &Packet, now_ns: u64) {
        let now = seconds_view(now_ns);
        self.packets_sent += 1;
        self.total_original_packets += 1;
        self.final_original_segment_bytes = packet.size;
        self.sent_size += packet.size;
        self.sent_size_in_period += packet.size;

        let prior_inflight = self.next_seq.saturating_sub(self.last_ack);
        if prior_inflight == 0 {
            self.first_sent_time = now;
        }
        let is_app_limited = self.app_limited || self.remaining_bytes == 0 || self.traffic_exceeded;
        self.sent_packet_meta.insert(
            packet.packet_id,
            SentPacketMeta {
                sent_time: now,
                first_sent_time: self.first_sent_time,
                delivered_at_send: self.last_ack,
                delivered_time_at_send: self.delivered_time,
                prior_inflight,
                is_app_limited,
            },
        );

        debug!(
            "TCPPacketSource {} sent packet {} ({} bytes) at time {:.3}. {} packets sent.",
            self.endpoint_id, packet.packet_id, packet.size, now, self.packets_sent,
        );

        self.sent_packets.insert(packet.packet_id, packet.clone());

        self.next_seq += packet.size;

        self.congestion_control.packet_sent(packet.size, now);

        let rto_ns = behavior_delay_ns(self.rto, "TCP retransmission timeout")
            .expect("TCP retransmission timeout must be finite and positive");
        let deadline_ns = now_ns
            .checked_add(rto_ns)
            .expect("TCP retransmission deadline exceeds the u64 nanosecond clock range");
        self.timeout_queue.push(PacketTimeout {
            packet_id: packet.packet_id,
            rto_ns,
            deadline_ns,
        });

        debug!(
            "TCPPacketSource {} set a timer for packet {} with an RTO of {:.3} and expiry time of {:.3}.",
            self.endpoint_id,
            packet.packet_id,
            self.rto,
            seconds_view(deadline_ns)
        );
    }

    fn apply_ecn_on_new_data(&mut self, packet: &mut Packet) {
        if self.ecn_enabled {
            packet.ecn = EcnField::Ect0;
        }
        if self.cwr_pending {
            packet.cwr = true;
            self.cwr_pending = false;
        }
    }

    fn apply_ecn_on_retransmit(packet: &mut Packet) {
        packet.ecn = EcnField::NotEct;
        packet.cwr = false;
    }

    fn sendable_bytes(&self, cwnd_limit: usize) -> usize {
        let send_limit = min(self.send_buffer, cwnd_limit);
        let available = send_limit.saturating_sub(self.next_seq);
        min(self.mss, available)
    }

    /// Checks if any sent packet reached timeout at regularly occurring intervals.
    pub async fn timer_tick(&mut self, now_ns: u64) {
        if self.is_complete() {
            return;
        }
        self.timer_ticks += 1;
        let now = seconds_view(now_ns);
        while !self.timeout_queue.is_empty() {
            let deadline_ns = self.timeout_queue.peek().unwrap().deadline_ns;
            if deadline_ns <= now_ns {
                let packet_timeout = self.timeout_queue.pop().unwrap();
                debug!(
                    "TCPPacketSource {}'s sent packet {} reached timeout at time {:.3}, \
                    with a current RTO of {:.3}.",
                    self.endpoint_id,
                    packet_timeout.packet_id,
                    seconds_view(packet_timeout.deadline_ns),
                    seconds_view(packet_timeout.rto_ns),
                );

                if let Some(lost_pkt) = self.sent_packets.get(&packet_timeout.packet_id) {
                    self.pending_lost_bytes = self.pending_lost_bytes.saturating_add(lost_pkt.size);
                }

                let flight_size_bytes = self.bytes_in_flight();
                self.congestion_control.timeout_event(CongestionEvent {
                    now,
                    flight_size_bytes,
                });
                #[cfg(feature = "lean")]
                self.log_cubic_event(
                    CubicEventKind::Timeout,
                    None,
                    None,
                    now,
                    Some(flight_size_bytes),
                );

                // retransmits the segment
                let resent_pkt = self
                    .sent_packets
                    .get_mut(&packet_timeout.packet_id)
                    .unwrap();

                resent_pkt.departure_update(now);
                Self::apply_ecn_on_retransmit(resent_pkt);

                self.output.send(resent_pkt.clone()).await;
                self.retransmissions += 1;
                self.retransmitted_bytes += resent_pkt.size;

                debug!(
                    "Due to timeout, TCPPacketSource {} resent packet {} ({} bytes) from flow {} at time {:.3}.",
                    self.endpoint_id,
                    resent_pkt.packet_id,
                    resent_pkt.size,
                    resent_pkt.flow_id,
                    now,
                );

                let max_rto_ns = behavior_delay_ns(self.max_rto, "maximum TCP RTO")
                    .expect("maximum TCP RTO must be finite and positive");
                let revised_rto_ns = packet_timeout.rto_ns.saturating_mul(2).min(max_rto_ns);

                let revised_timeout = PacketTimeout {
                    packet_id: packet_timeout.packet_id,
                    rto_ns: revised_rto_ns,
                    deadline_ns: packet_timeout
                        .deadline_ns
                        .checked_add(revised_rto_ns)
                        .expect("TCP retransmission deadline exceeds the u64 nanosecond range"),
                };

                self.timeout_queue.push(revised_timeout);

                debug!(
                    "TCPPacketSource {} reset a timer for packet {} with a RTO of {:.3}.",
                    self.endpoint_id,
                    packet_timeout.packet_id,
                    seconds_view(revised_rto_ns)
                );
            } else {
                return;
            }
        }
    }

    pub async fn send_packet(&mut self, now_ns: u64) -> Option<f64> {
        let now = seconds_view(now_ns);
        // Attempt to pull fresh packets from the application layer before sending, to ensure there is data ready within the current congestion window.
        self.pull_from_appsource(now).await;

        let cwnd_limit = self.last_ack + self.congestion_control.get_cwnd();
        let send_size = self.sendable_bytes(cwnd_limit);
        if send_size == 0 {
            return None;
        }

        let pacing_rate = self.congestion_control.get_pacing_rate();
        let pacing_interval = if pacing_rate > 0.0 {
            send_size as f64 / pacing_rate
        } else {
            0.0
        };

        if !pacing_interval.is_finite() || pacing_interval <= 0.0 {
            // No pacing rate yet; send as much as the window allows.
            loop {
                let send_size = self.sendable_bytes(cwnd_limit);
                if send_size == 0 {
                    break;
                }

                let mut packet = Packet::new(send_size, self.next_seq, self.flow_id, now);
                packet.set_priority(self.priority);
                self.apply_ecn_on_new_data(&mut packet);

                self.output.send(packet.clone()).await;
                self.packet_sent(&packet, now_ns);
            }
            return None;
        }

        if now_ns < self.busy_until_ns {
            return Some(seconds_view(self.busy_until_ns - now_ns));
        }

        let mut packet = Packet::new(send_size, self.next_seq, self.flow_id, now);
        packet.set_priority(self.priority);
        self.apply_ecn_on_new_data(&mut packet);

        self.output.send(packet.clone()).await;
        self.packet_sent(&packet, now_ns);

        let pacing_interval = pacing_interval.max(Self::MIN_PACING_INTERVAL);
        let pacing_interval_ns = behavior_delay_ns(pacing_interval, "TCP pacing interval")
            .expect("TCP pacing interval must be finite and positive");
        self.busy_until_ns = now_ns
            .checked_add(pacing_interval_ns)
            .expect("TCP pacing deadline exceeds the u64 nanosecond range");

        let next_can_send = self.sendable_bytes(cwnd_limit) > 0;
        if next_can_send {
            return Some(pacing_interval);
        }

        None
    }

    pub fn log_report(&mut self, now: f64, timing: ReportTiming) {
        let report = PacketSourceReport {
            id: self.endpoint_id,
            flow_id: self.flow_id,
            start_time: self.report_start_time,
            end_time: now,
            sent_packets: self.packets_sent,
            packet_sizes: self.sent_size_in_period,
            ack_bytes: self.last_ack,
        };

        CsvLogger::log_report(Report::PacketSourceReport(report), timing);
        if timing == ReportTiming::Final && self.e5_metrics_enabled {
            CsvLogger::log_report(
                Report::TcpMetricsReport(TcpMetricsReport {
                    flow_id: self.flow_id,
                    original_packets: self.total_original_packets,
                    original_bytes: self.sent_size,
                    final_original_segment_bytes: self.final_original_segment_bytes,
                    retransmissions: self.retransmissions,
                    retransmitted_bytes: self.retransmitted_bytes,
                    acked_bytes: self.last_ack,
                    completed: self.is_complete(),
                    completion_time_ns: self.completion_time_ns,
                    outstanding_bytes: self.bytes_in_flight(),
                    pending_timeouts: self.timeout_queue.len(),
                    timer_ticks: self.timer_ticks,
                    timer_cancelled: self.timer_cancelled,
                }),
                timing,
            );
        }

        debug!(
            "TCPPacketSource {} logged a periodic report at time {:.3}.",
            self.endpoint_id, now
        );

        // resets the statistics of report
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
                "TCPPacketSource {} of flow {} notified {} flow(s) to start at time {:.3}.",
                self.endpoint_id,
                self.flow_id,
                self.flow_finish_outputs.len(),
                now,
            );
            self.sent_flow_finish_msg = true;
        }
    }
}

impl Model for TCPPacketSource {
    type Env = ();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::flows::app_source::AppDataSource;
    use crate::flows::packet::TCPAck;
    use crate::flows::{DistributionInfo, TCPCharacteristics};
    use futures::executor::block_on;
    use futures::join;
    use rand::SeedableRng;
    use std::collections::BTreeSet;

    fn make_source(ecn: bool) -> TCPPacketSource {
        let traffic = TrafficCharacteristics::new(
            0.0,
            Some(1.0),
            None,
            DistributionInfo::Uniform {
                low: 0.1,
                high: 0.1,
            },
            DistributionInfo::DiscreteUniform {
                low: 512,
                high: 512,
            },
            Some(TCPCharacteristics {
                cc_algorithm: CCAlgorithm::TCPReno,
                ecn,
                cubic: None,
            }),
        );
        let mut rng = rand::rng();
        let rng = SmallRng::from_rng(&mut rng);
        TCPPacketSource::new(0, Vec::new(), traffic, 0, None, rng)
    }

    fn make_ack(flow_id: usize, seq: usize, acked: usize, ece: bool, now: f64) -> Packet {
        Packet {
            time: now,
            creation_time: 0.0,
            size: 40,
            packet_id: seq,
            flow_id,
            queueing_delay: 0.0,
            priority: 0,
            last_packet: false,
            ack: Some(TCPAck {
                sequence_num: seq,
                acknowledged_size: acked,
                ece,
            }),
            control: None,
            ecn: EcnField::NotEct,
            cwr: false,
        }
    }

    #[test]
    fn test_ecn_sets_cwr_on_next_data() {
        let mut source = make_source(true);
        let ack = make_ack(source.flow_id, source.mss, source.mss, true, 1.0);

        let _ = block_on(source.ack_packet_received(ack, 1_000_000_000));
        assert!(source.cwr_pending);
        assert!(source.ecn_reduction_in_flight);

        let mut packet = Packet::new(source.mss, 0, source.flow_id, 1.0);
        source.apply_ecn_on_new_data(&mut packet);
        assert_eq!(packet.ecn, EcnField::Ect0);
        assert!(packet.cwr);
        assert!(!source.cwr_pending);
    }

    #[test]
    fn test_ecn_reduction_clears_on_non_ece_ack() {
        let mut source = make_source(true);
        let ack_ece = make_ack(source.flow_id, source.mss, source.mss, true, 1.0);
        let _ = block_on(source.ack_packet_received(ack_ece, 1_000_000_000));
        assert!(source.ecn_reduction_in_flight);

        let ack_no_ece = make_ack(source.flow_id, source.mss * 2, source.mss, false, 2.0);
        let _ = block_on(source.ack_packet_received(ack_no_ece, 2_000_000_000));
        assert!(!source.ecn_reduction_in_flight);
    }

    #[test]
    fn test_retransmit_is_not_ect() {
        let mut packet = Packet::new(512, 0, 0, 0.0);
        packet.ecn = EcnField::Ect0;
        packet.cwr = true;
        TCPPacketSource::apply_ecn_on_retransmit(&mut packet);
        assert_eq!(packet.ecn, EcnField::NotEct);
        assert!(!packet.cwr);
    }

    #[test]
    fn test_dupack_counter_increments_and_resets() {
        let mut source = make_source(false);
        source.last_ack = 100;
        source.dupack = 2;

        let ack_dup = make_ack(source.flow_id, 100, source.mss, false, 1.0);
        let _ = block_on(source.ack_packet_received(ack_dup, 1_000_000_000));
        assert_eq!(source.dupack, 3);

        let ack_new = make_ack(source.flow_id, 200, source.mss, false, 2.0);
        let _ = block_on(source.ack_packet_received(ack_new, 2_000_000_000));
        assert_eq!(source.dupack, 0);
    }

    #[test]
    fn test_dupack_retransmit_marks_pending_lost_bytes() {
        let mut source = make_source(false);
        source.last_ack = 100;
        source.dupack = 2;

        let mut packet = Packet::new(100, 100, source.flow_id, 0.0);
        packet.time = 0.5;
        source.sent_packets.insert(100, packet);

        let ack_dup = make_ack(source.flow_id, 100, source.mss, false, 1.0);
        let _ = block_on(source.ack_packet_received(ack_dup, 1_000_000_000));

        assert_eq!(source.dupack, 3);
        assert_eq!(source.pending_lost_bytes, source.mss);
    }

    #[test]
    fn test_send_packet_sends_buffered_sub_mss_segment() {
        let mut source = make_source(false);
        source.send_buffer = 128;
        source.traffic_exceeded = true;

        let next_interval = block_on(source.send_packet(0));

        assert_eq!(next_interval, None);
        assert_eq!(source.next_seq, 128);
        assert_eq!(source.packets_sent, 1);
        assert_eq!(source.sent_size, 128);

        let packet = source.sent_packets.get(&0).expect("short segment not sent");
        assert_eq!(packet.packet_id, 0);
        assert_eq!(packet.size, 128);
    }

    #[test]
    fn retransmission_deadlines_use_exact_integer_nanoseconds() {
        let mut source = make_source(false);
        source.send_buffer = source.mss;
        source.traffic_exceeded = true;
        let now_ns = (1_u64 << 24) * 1_000_000_000;

        block_on(source.send_packet(now_ns));

        let timeout = source.timeout_queue.peek().expect("missing packet timeout");
        assert_eq!(timeout.rto_ns, 1_000_000_000);
        assert_eq!(timeout.deadline_ns, now_ns + 1_000_000_000);

        block_on(source.timer_tick(now_ns + 999_999_999));
        assert_eq!(
            source.timeout_queue.peek().unwrap().deadline_ns,
            now_ns + 1_000_000_000
        );

        block_on(source.timer_tick(now_ns + 1_000_000_000));
        let backed_off = source.timeout_queue.peek().unwrap();
        assert_eq!(backed_off.rto_ns, 2_000_000_000);
        assert_eq!(backed_off.deadline_ns, now_ns + 3_000_000_000);
    }

    #[test]
    fn test_send_packet_sends_partial_tail_segment() {
        let mut source = make_source(false);
        source.next_seq = source.mss;
        source.last_ack = source.mss;
        source.send_buffer = 600;
        source.traffic_exceeded = true;

        let next_interval = block_on(source.send_packet(0));

        assert_eq!(next_interval, None);
        assert_eq!(source.next_seq, 600);
        assert_eq!(source.packets_sent, 1);
        assert_eq!(source.sent_size, 88);

        let packet = source
            .sent_packets
            .get(&source.mss)
            .expect("tail segment not sent");
        assert_eq!(packet.packet_id, source.mss);
        assert_eq!(packet.size, 88);
    }

    #[test]
    fn test_pull_from_appsource_accepts_sub_mss_window() {
        let mut source = make_source(false);
        let cwnd = source.congestion_control.get_cwnd();
        let mut data_src = AppDataSource::create_source_buffer(
            128,
            crate::flows::app_source::AppBufferConfig::default(),
        );
        let mut actor = data_src.take_actor().expect("missing app source actor");
        source.app_source = Some(data_src.handle());
        source.synthetic_source = None;
        source.remaining_bytes = 128;
        source.send_buffer = cwnd - 128;
        source.next_seq = source.send_buffer;

        block_on(async {
            let pull = source.pull_from_appsource(0.0);
            let respond = actor.respond_once_for_test();
            let (_, ()) = join!(pull, respond);
        });

        assert_eq!(source.send_buffer, cwnd);
        assert_eq!(source.remaining_bytes, 0);
        assert!(source.traffic_exceeded);
    }

    fn fixed_byte_trace(arrival_seconds: f64) -> Vec<(usize, usize)> {
        let traffic = TrafficCharacteristics::new(
            0.0,
            None,
            Some(3500),
            DistributionInfo::Uniform {
                low: arrival_seconds,
                high: arrival_seconds,
            },
            DistributionInfo::DiscreteUniform {
                low: 1460,
                high: 1460,
            },
            Some(TCPCharacteristics {
                cc_algorithm: CCAlgorithm::TCPReno,
                ecn: false,
                cubic: None,
            }),
        );
        let mut rng = rand::rng();
        let rng = SmallRng::from_rng(&mut rng);
        let mut source = TCPPacketSource::new(7, Vec::new(), traffic, 0, None, rng);

        assert_eq!(source.mss, 1460);
        assert_eq!(source.congestion_control.get_cwnd(), 2920);
        assert!(!source.has_synthetic_source());
        assert!(!source.is_complete());

        block_on(source.send_packet(0));
        let initial = source
            .sent_packets
            .values()
            .map(|packet| (packet.packet_id, packet.size))
            .collect::<BTreeSet<_>>();
        assert_eq!(initial, BTreeSet::from([(0, 1460), (1460, 1460)]));
        assert!(!source.is_complete());

        let mut ack = make_ack(source.flow_id, 1460, 1460, false, 0.000_001);
        ack.packet_id = 0;
        let _ = block_on(source.ack_packet_received(ack, 1_000));
        block_on(source.send_packet(1_000));
        assert_eq!(
            source.sent_packets.get(&2920).map(|packet| packet.size),
            Some(580)
        );
        assert_eq!(source.next_seq, 3500);
        assert_eq!(source.send_buffer, 3500);
        assert_eq!(source.sent_size, 3500);
        assert!(!source.is_complete());

        let mut final_ack = make_ack(source.flow_id, 3500, 580, false, 0.000_002);
        final_ack.packet_id = 2920;
        let _ = block_on(source.ack_packet_received(final_ack, 2_000));
        assert!(source.is_complete());

        vec![(0, 1460), (1460, 1460), (2920, 580)]
    }

    #[test]
    fn fixed_byte_tcp_uses_mss_and_sends_an_exact_tail_independent_of_arrival_distribution() {
        assert_eq!(fixed_byte_trace(1.0), fixed_byte_trace(0.000_000_001));
    }

    #[test]
    fn duration_tcp_keeps_the_legacy_synthetic_application_source() {
        assert!(make_source(false).has_synthetic_source());
    }

    #[test]
    fn three_duplicate_acks_fast_retransmit_the_hole_with_byte_flight_accounting() {
        let mut source = make_source(false);
        for sequence in [0, 512, 1024, 1536] {
            let packet = Packet::new(512, sequence, source.flow_id, 0.0);
            source.packet_sent(&packet, 0);
        }
        let reno = source
            .congestion_control
            .as_any()
            .downcast_ref::<TCPReno>()
            .expect("Reno controller");
        assert_eq!(reno.flight_accounting(), (2048, 2048));

        for (packet_id, now_ns) in [(512, 1_000), (1024, 2_000), (1536, 3_000)] {
            let mut ack = make_ack(source.flow_id, 0, 512, false, now_ns as f64 * 1e-9);
            ack.packet_id = packet_id;
            let _ = block_on(source.ack_packet_received(ack, now_ns));
        }

        assert_eq!(source.dupack, 3);
        assert_eq!(source.sent_packets[&0].time, 0.000_003);
        assert_eq!(source.pending_lost_bytes, 512);
        assert_eq!(source.congestion_control.get_cwnd(), 2560);
        assert_eq!(source.total_original_packets, 4);
        assert_eq!(source.sent_size, 2048);
        assert_eq!(source.retransmissions, 1);
    }

    #[test]
    fn rto_retransmission_occurs_on_the_exact_tick_and_can_complete() {
        let traffic = TrafficCharacteristics::new(
            0.0,
            None,
            Some(512),
            DistributionInfo::Uniform {
                low: 1.0,
                high: 1.0,
            },
            DistributionInfo::DiscreteUniform {
                low: 512,
                high: 512,
            },
            Some(TCPCharacteristics {
                cc_algorithm: CCAlgorithm::TCPReno,
                ecn: false,
                cubic: None,
            }),
        );
        let mut rng = rand::rng();
        let rng = SmallRng::from_rng(&mut rng);
        let mut source = TCPPacketSource::new(8, Vec::new(), traffic, 0, None, rng);
        block_on(source.send_packet(0));

        block_on(source.timer_tick(999_999_999));
        assert_eq!(source.sent_packets[&0].time, 0.0);
        block_on(source.timer_tick(1_000_000_000));
        assert_eq!(source.sent_packets[&0].time, 1.0);
        assert_eq!(source.pending_lost_bytes, 512);
        assert_eq!(source.total_original_packets, 1);
        assert_eq!(source.sent_size, 512);
        assert_eq!(source.retransmissions, 1);

        let ack = make_ack(source.flow_id, 512, 512, false, 1.000_001);
        let _ = block_on(source.ack_packet_received(ack, 1_000_001_000));
        assert!(source.is_complete());
    }
}
