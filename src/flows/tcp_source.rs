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

use crate::flows::app_source::AppSourceBufferHandle;
use crate::flows::bbr::TCPBBR;
use crate::flows::cc::{AckEvent, CCAlgorithm, CongestionControl, RateSample};
use crate::flows::cubic::TCPCubic;
use crate::flows::dist_source::DistPacketSource;
use crate::flows::packet::Packet;
use crate::flows::reno::TCPReno;
use crate::flows::source::PacketSourceReport;
use crate::flows::{FlowFinishMsg, TrafficCharacteristics};
use crate::next_endpoint_id;
use crate::utils::logger::CsvLogger;
use crate::utils::logger::{Report, ReportTiming};

#[derive(Debug, Clone)]
pub struct PacketTimeout {
    pub packet_id: usize,
    pub rto: f64,
    pub timeout: f64,
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
        self.timeout == other.timeout
    }
}

impl Ord for PacketTimeout {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timeout
            .partial_cmp(&other.timeout)
            .unwrap_or(Ordering::Equal)
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
    pub busy_until: f64,

    packets_sent: usize,
    sent_size: usize,
    sent_size_in_period: usize,

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
        let cc_algorithm = traffic
            .tcp
            .as_ref()
            .expect("TCP traffic requires TCP characteristics")
            .cc_algorithm;

        let congestion_control: Box<dyn CongestionControl + Send + Sync> = match cc_algorithm {
            CCAlgorithm::TCPReno => Box::new(TCPReno::new()),
            CCAlgorithm::TCPCubic => Box::new(TCPCubic::new()),
            CCAlgorithm::TCPBBR => Box::new(TCPBBR::new()),
        };
        let synthetic_source = if app_source.is_some() {
            None
        } else {
            Some(SyntheticDataSource::new(flow_id, traffic.clone(), rng))
        };
        let remaining_bytes = app_source
            .as_ref()
            .and_then(|h| h.get_total_size())
            .unwrap_or(usize::MAX);
        TCPPacketSource {
            time: 0.0,
            endpoint_id: next_endpoint_id(),
            flow_id,
            priority,
            flow_start_after: HashSet::from_iter(flow_start_after.iter().cloned()),
            traffic,
            traffic_exceeded: false,
            congestion_control,
            mss: 512,
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
            busy_until: 0.0,
            packets_sent: 0,
            sent_size: 0,
            sent_size_in_period: 0,
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

        // window too small to send a full segment, wait for ACKs
        if win_left < self.mss {
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
        }

        // if all bytes have been sent and acknowledged, complete the flow
        if self.remaining_bytes == 0 {
            self.traffic_exceeded = true;
            self.app_limited = true;
        }
    }

    /// Returns whether PacketSource should call run() after TCPPacketSource
    /// handles an acknowledgment.
    pub async fn ack_packet_received(&mut self, ack_packet: Packet, now: f64) -> bool {
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
        if ack.ecn_marked {
            self.pending_ecn_marked = true;
        }
        if ack.sequence_num == self.last_ack {
            self.dupack += 1;
        } else {
            // fast recovery in RFC 2001 and TCP Reno
            if self.dupack > 0 {
                self.congestion_control.dupack_over();
                self.dupack = 0;
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
                self.congestion_control.consecutive_dupacks_received();
            }

            if let Some(resent_pkt) = self.sent_packets.get_mut(&ack.sequence_num) {
                resent_pkt.time = now;
                self.output.send(resent_pkt.clone()).await;

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
                if self.last_ack + self.congestion_control.get_cwnd() >= ack.sequence_num
                    && self.next_seq < self.send_buffer
                {
                    debug!(
                        "TCPPacketSource {} will send packet {} ({} bytes) at time {:.3} as dupack > 3.",
                        self.endpoint_id, self.next_seq, self.mss, now,
                    );

                    let mut packet = Packet::new(self.mss, self.next_seq, self.flow_id, now);
                    packet.set_priority(self.priority);
                    self.output.send(packet.clone()).await;
                    self.packet_sent(&packet, now);
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

            let sample_packet_id = ack
                .sequence_num
                .saturating_sub(ack.acknowledged_size);
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
            self.congestion_control
                .ack_received(AckEvent {
                    ack_seq: ack.sequence_num,
                    rtt: sample_rtt,
                    now,
                    bytes_acked: ack.acknowledged_size,
                    rate_sample,
                });
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

            if now >= self.busy_until {
                return true;
            }

            self.pull_from_appsource(now).await;
        }

        false
    }

    pub fn get_cwnd_limit(&self) -> usize {
        self.last_ack + self.congestion_control.get_cwnd()
    }

    pub fn packet_sent(&mut self, packet: &Packet, now: f64) {
        self.packets_sent += 1;
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

        self.timeout_queue.push(PacketTimeout {
            packet_id: packet.packet_id,
            rto: self.rto,
            timeout: self.rto + now,
        });

        debug!(
            "TCPPacketSource {} set a timer for packet {} with an RTO of {:.3} and expiry time of {:.3}.",
            self.endpoint_id,
            packet.packet_id,
            self.rto,
            self.rto + now
        );
    }

    /// Checks if any sent packet reached timeout at regularly occurring intervals.
    pub async fn timer_tick(&mut self, now: f64) {
        while !self.timeout_queue.is_empty() {
            let timeout_time = self.timeout_queue.peek().unwrap().timeout;
            if timeout_time <= now {
                let packet_timeout = self.timeout_queue.pop().unwrap();
                debug!(
                    "TCPPacketSource {}'s sent packet {} reached timeout at time {:.3}, \
                    with a current RTO of {:.3}.",
                    self.endpoint_id,
                    packet_timeout.packet_id,
                    packet_timeout.timeout,
                    packet_timeout.rto,
                );

                if let Some(lost_pkt) = self.sent_packets.get(&packet_timeout.packet_id) {
                    self.pending_lost_bytes =
                        self.pending_lost_bytes.saturating_add(lost_pkt.size);
                }

                self.congestion_control.timer_expired();

                // retransmits the segment
                let resent_pkt = self
                    .sent_packets
                    .get_mut(&packet_timeout.packet_id)
                    .unwrap();

                resent_pkt.departure_update(packet_timeout.timeout);

                self.output.send(resent_pkt.clone()).await;

                debug!(
                    "Due to timeout, TCPPacketSource {} resent packet {} ({} bytes) from flow {} at time {:.3}.",
                    self.endpoint_id,
                    resent_pkt.packet_id,
                    resent_pkt.size,
                    resent_pkt.flow_id,
                    packet_timeout.timeout,
                );

                let revised_rto = f64::min(
                    self.max_rto,
                    packet_timeout.rto * 2.0, // Exponential backoff
                );

                let revised_timeout = PacketTimeout {
                    packet_id: packet_timeout.packet_id,
                    rto: revised_rto,
                    timeout: packet_timeout.timeout + revised_rto,
                };

                self.timeout_queue.push(revised_timeout);

                debug!(
                    "TCPPacketSource {} reset a timer for packet {} with a RTO of {:.3}.",
                    self.endpoint_id, packet_timeout.packet_id, revised_rto
                );
            } else {
                return;
            }
        }
    }

    pub async fn send_packet(&mut self, now: f64) -> Option<f64> {
        // Attempt to pull fresh packets from the application layer before sending, to ensure there is data ready within the current congestion window.
        self.pull_from_appsource(now).await;

        let cwnd_limit = self.last_ack + self.congestion_control.get_cwnd();
        let can_send = self.next_seq < self.send_buffer
            && self.next_seq + self.mss <= min(self.send_buffer, cwnd_limit);
        if !can_send {
            return None;
        }

        let pacing_rate = self.congestion_control.get_pacing_rate();
        let pacing_interval = if pacing_rate > 0.0 {
            self.mss as f64 / pacing_rate
        } else {
            0.0
        };

        if !pacing_interval.is_finite() || pacing_interval <= 0.0 {
            // No pacing rate yet; send as much as the window allows.
            while self.next_seq < self.send_buffer
                && self.next_seq + self.mss <= min(self.send_buffer, cwnd_limit)
            {
                let mut packet = Packet::new(self.mss, self.next_seq, self.flow_id, now);
                packet.set_priority(self.priority);

                self.output.send(packet.clone()).await;
                self.packet_sent(&packet, now);
            }
            return None;
        }

        if now < self.busy_until {
            let interval = (self.busy_until - now).max(Self::MIN_PACING_INTERVAL);
            return Some(interval);
        }

        let mut packet = Packet::new(self.mss, self.next_seq, self.flow_id, now);
        packet.set_priority(self.priority);

        self.output.send(packet.clone()).await;
        self.packet_sent(&packet, now);

        let pacing_interval = pacing_interval.max(Self::MIN_PACING_INTERVAL);
        self.busy_until = now + pacing_interval;

        let next_can_send = self.next_seq < self.send_buffer
            && self.next_seq + self.mss <= min(self.send_buffer, cwnd_limit);
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

impl Model for TCPPacketSource {}
