//! Implements a packet source that simulates the TCP protocol, including
//! support for various congestion control mechanisms.
use std::cmp::Ordering;
use std::cmp::min;
use std::collections::{BinaryHeap, HashMap, HashSet};

use core::fmt;
use log::debug;

use nexosim::model::Model;
use nexosim::ports::Output;
use rand::rngs::SmallRng;

use crate::flows::app_source::AppSourceHandle;
use crate::flows::bbr::TCPBBR;
use crate::flows::cc::{CCAlgorithm, CongestionControl};
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

pub(crate) struct LegacyAppDataSource {
    dist: DistPacketSource,
}

impl LegacyAppDataSource {
    pub(crate) fn new(flow_id: usize, traffic: TrafficCharacteristics, rng: SmallRng) -> Self {
        Self {
            dist: DistPacketSource::new(flow_id, Vec::new(), traffic, rng),
        }
    }

    pub(crate) fn set_flow_start_time(&mut self, flow_start_time: f64) {
        self.dist.flow_start_time = flow_start_time;
    }

    pub(crate) fn produce_data(&mut self, now: f64) -> (Packet, f64) {
        let (packet, interval) = self.dist.produce_packet(now);
        self.dist.packet_sent(&packet, now);
        (packet, interval)
    }

    pub(crate) fn traffic_exceeded(&self, now: f64) -> bool {
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
    /// min-heap of in-flight packets, where packets are sorted according to
    /// their timeout
    timeout_queue: BinaryHeap<PacketTimeout>,

    pub app_source: Option<AppSourceHandle>,
    legacy_source: Option<LegacyAppDataSource>,
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
    pub(crate) fn legacy_source_mut(&mut self) -> Option<&mut LegacyAppDataSource> {
        self.legacy_source.as_mut()
    }

    pub(crate) fn has_legacy_source(&self) -> bool {
        self.legacy_source.is_some()
    }

    pub fn new(
        flow_id: usize,
        flow_start_after: Vec<usize>,
        traffic: TrafficCharacteristics,
        app_source: Option<AppSourceHandle>,
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
        let legacy_source = if app_source.is_some() {
            None
        } else {
            Some(LegacyAppDataSource::new(flow_id, traffic.clone(), rng))
        };
        let remaining_bytes = app_source
            .as_ref()
            .and_then(|h| h.get_total_size())
            .unwrap_or(usize::MAX);
        TCPPacketSource {
            time: 0.0,
            endpoint_id: next_endpoint_id(),
            flow_id,
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
            timeout_queue: BinaryHeap::new(),
            app_source,
            legacy_source,
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
        }
    }

    /// Pull packets from the app source based on the size of the remaining congestion window
    /// (which is cwnd_limit - next_seq), using AppSourceHandle.
    /// This function is typically called:
    /// - after receiving new ACKs (to refill the window)
    /// - before sending packets (to populate the send buffer)
    pub async fn pull_from_appsource(&mut self, now: f64) {
        // stop if all flow data has been sent
        if self.remaining_bytes == 0 {
            return;
        }

        // compute available sending window (cwnd - unacked data)
        let cwnd = self.congestion_control.get_cwnd();
        let win_left = self.last_ack + cwnd - self.next_seq;

        // window too small to send a full segment, wait for ACKs
        if win_left < self.mss {
            return;
        }

        // pull at most min(available window, remaining bytes)
        let pull_size = win_left.min(self.remaining_bytes);

        if let Some(ref mut handle) = self.app_source {
            let pkts = handle.pull(pull_size).await;

            for mut pkt in pkts {
                // ensure we do not exceed the current window
                if self.next_seq + pkt.size > self.last_ack + cwnd {
                    break;
                }

                pkt.flow_id = self.flow_id;
                pkt.packet_id = self.next_seq;

                self.output.send(pkt.clone()).await;
                self.packet_sent(&pkt, now);

                self.remaining_bytes -= pkt.size;
            }
        }

        // if all bytes have been sent and acknowledged, complete the flow
        if self.remaining_bytes == 0 {
            self.traffic_exceeded = true;
        }
        if self.remaining_bytes == 0 && self.send_buffer == 0 {
            self.wrap_up(now).await;
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
            self.timeout_queue
                .retain(|packet| packet.packet_id != ack_packet.packet_id);
        }

        let ack = ack_packet.ack.unwrap();
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

                    let packet = Packet::new(self.mss, self.next_seq, self.flow_id, now);
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

            self.last_ack = ack.sequence_num;
            self.congestion_control.ack_received(
                ack.sequence_num,
                sample_rtt,
                now,
                ack.acknowledged_size,
            );

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

        debug!(
            "TCPPacketSource {} sent packet {} ({} bytes) at time {:.3}. {} packets sent.",
            self.endpoint_id, packet.packet_id, packet.size, now, self.packets_sent,
        );

        self.sent_packets.insert(packet.packet_id, packet.clone());

        self.next_seq += packet.size;

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

    pub async fn send_packet(&mut self, now: f64) {
        // Attempt to pull fresh packets from the application layer before sending, to ensure there is data ready within the current congestion window.
        self.pull_from_appsource(now).await;
        // the sender can transmit up to the size of the congestion window
        while self.next_seq < self.send_buffer
            && self.next_seq + self.mss
                <= min(
                    self.send_buffer,
                    self.last_ack + self.congestion_control.get_cwnd(),
                )
        {
            let packet = Packet::new(self.mss, self.next_seq, self.flow_id, now);

            self.output.send(packet.clone()).await;
            self.packet_sent(&packet, now);
        }
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
