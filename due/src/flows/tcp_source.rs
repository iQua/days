//! Implements a packet source that simulates the TCP protocol, including
//! support for various congestion control mechanisms.

use core::fmt;
use std::cmp::min;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::time::Duration;

use log::debug;
use rand::rngs::SmallRng;

use asynchronix::model::{Model, Output};

use crate::flows::cc::{CCAlgorithm, CongestionControl, TCPCubic, TCPReno};
use crate::flows::dist_source::DistPacketSource;
use crate::flows::packet::Packet;
use crate::flows::TrafficCharacteristics;
use crate::next_endpoint_id;

#[derive(Debug, Clone)]
pub struct PacketTimeout {
    pub packet_id: usize,
    pub timeout: f64,
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

/// An application packet source.
pub struct AppPacketSource {
    // currently implements the application packet source as a
    // distribution-based packet source, but it can be implemented as any type
    // of source later
    pub app_source: DistPacketSource,
}

impl AppPacketSource {
    pub fn new(flow_id: usize, traffic: TrafficCharacteristics, rng: SmallRng) -> AppPacketSource {
        AppPacketSource {
            app_source: DistPacketSource::new(flow_id, traffic, rng),
        }
    }
}

pub struct TCPPacketSource {
    pub endpoint_id: usize,
    pub flow_id: usize,
    pub traffic: TrafficCharacteristics,
    pub traffic_exceeded: bool,
    /// the congestion controller
    congestion_control: Box<dyn CongestionControl + Send + Sync>,
    /// maximum segment size, in bytes
    mss: usize,
    /// the next sequence number to be sent, in bytes
    pub next_seq: usize,
    /// the maximum sequence number in the in-transit data buffer
    pub send_buffer: usize,
    /// the sequence number of the segment that is last acknowledged
    last_ack: usize,
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

    pub app_packet_source: AppPacketSource,

    /// the source is considered busy retrieving the current packet from flow
    /// until this time
    pub busy_until: f64,

    packets_sent: usize,

    pub output: Output<Packet>,
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
    pub fn new(flow_id: usize, traffic: TrafficCharacteristics, rng: SmallRng) -> TCPPacketSource {
        let cc_algorithm = traffic.tcp.unwrap().cc_algorithm;

        let congestion_control: Box<dyn CongestionControl + Send + Sync> = match cc_algorithm {
            CCAlgorithm::TCPReno => Box::new(TCPReno::new()),
            CCAlgorithm::TCPCubic => Box::new(TCPCubic::new()),
        };

        TCPPacketSource {
            endpoint_id: next_endpoint_id(),
            flow_id,
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
            app_packet_source: AppPacketSource::new(flow_id, traffic, rng.clone()),
            busy_until: 0.0,
            packets_sent: 0,
            output: Output::default(),
        }
    }

    /// Returns whether PacketSource should call run() after TCPPacketSource
    /// handles an acknowledgment.
    pub async fn ack_packet_received(&mut self, ack_packet: Packet, now: f64) -> bool {
        // the received packet must be an acknowledgment
        assert!(ack_packet.ack.is_some());

        debug!(
            "TCPPacketSource {} received Ack of packet {} ({} bytes) from flow {} at time {:.3}.",
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

            let resent_pkt = self.sent_packets.get_mut(&ack.sequence_num).unwrap();
            resent_pkt.time = now;

            self.output.send(resent_pkt.clone()).await;

            debug!(
                "TCPPacketSource {} resent packet {} ({} bytes) from flow {} at time {:.3}.",
                self.endpoint_id, resent_pkt.packet_id, resent_pkt.size, resent_pkt.flow_id, now,
            );

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

                    let (packet, _) = self.produce_packet(now);

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

            let alpha = 0.125;
            let beta = 0.25;

            // calculates the deviation (RTTVAR) of the RTT to account for
            // variations in the network
            if self.rtt_var == 0.0 {
                self.rtt_var = sample_rtt / 2.0;
            } else {
                let deviation = self.smoothed_rtt - sample_rtt;
                self.rtt_var = (1.0 - beta) * self.rtt_var + beta * deviation.abs();
            }

            // computes a smoothed round-trip time (SRTT)
            if self.smoothed_rtt == 0.0 {
                self.smoothed_rtt = sample_rtt;
            } else {
                self.smoothed_rtt = (1.0 - alpha) * self.smoothed_rtt + alpha * sample_rtt;
            }
            self.rto = f64::max(1.0, self.smoothed_rtt + 4.0 * self.rtt_var);

            self.last_ack = ack.sequence_num;
            self.congestion_control.ack_received(sample_rtt, now);

            debug!(
                "TCPPacketSource {} received Ack till sequence number {} at time {:.3}.",
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
        }

        false
    }

    pub fn packet_sent(&mut self, packet: &Packet, now: f64) {
        self.packets_sent += 1;

        debug!(
            "TCPPacketSource {} sent packet {} ({} bytes) at time {:.3}. {} packets sent.",
            self.endpoint_id, packet.packet_id, packet.size, now, self.packets_sent,
        );

        self.sent_packets.insert(packet.packet_id, packet.clone());

        self.next_seq += packet.size;

        self.timeout_queue.push(PacketTimeout {
            packet_id: packet.packet_id,
            timeout: self.rto + now,
        });

        debug!(
            "TCPPacketSource {} set a timer for packet {} with an RTO of {:.3} and expiry time of {:.3}.",
            self.endpoint_id, packet.packet_id, self.rto, self.rto + now
        );
    }

    /// Checks if any sent packet reached timeout at regularly occurring
    /// intervals.
    pub async fn timer_tick(&mut self, now: f64) {
        while !self.timeout_queue.is_empty() {
            let timeout_time = self.timeout_queue.peek().unwrap().timeout;
            if timeout_time <= now {
                let timeout_packet = self.timeout_queue.pop().unwrap();
                debug!(
                    "TCPPacketSource {}'s sent packet {} reached timeout at time {:.3}.",
                    self.endpoint_id, timeout_packet.packet_id, timeout_packet.timeout,
                );

                self.congestion_control.timer_expired();

                // retransmits the segment
                let resent_pkt = self
                    .sent_packets
                    .get_mut(&timeout_packet.packet_id)
                    .unwrap();

                resent_pkt.departure_update(timeout_packet.timeout);

                self.output.send(resent_pkt.clone()).await;

                debug!(
                    "Due to timeout, TCPPacketSource {} resent packet {} ({} bytes) from flow {} at time {:.3}.",
                    self.endpoint_id,
                    resent_pkt.packet_id,
                    resent_pkt.size,
                    resent_pkt.flow_id,
                    timeout_packet.timeout, 
                );

                // doubles the retransmission timeout
                self.rto *= 2.0;

                self.timeout_queue.push(PacketTimeout {
                    packet_id: timeout_packet.packet_id,
                    timeout: self.rto + timeout_packet.timeout,
                });

                debug!(
                    "TCPPacketSource {} reset a timer for packet {} with an RTO of {:.3} and expiry time of {:.3}.",
                    self.endpoint_id, timeout_packet.packet_id, self.rto, self.rto + timeout_packet.timeout
                );
            } else {
                return;
            }
        }
    }

    pub fn should_produce_packet(&mut self) -> bool {
        // the sender can transmit up to the size of the congestion window
        self.next_seq + self.mss
            <= min(
                self.send_buffer,
                self.last_ack + self.congestion_control.get_cwnd(),
            )
            && self.next_seq < self.send_buffer
    }

    pub fn produce_packet(&mut self, now: f64) -> (Packet, Duration) {
        let packet = Packet::new(self.mss, self.next_seq, self.flow_id, now);

        (packet, Duration::default())
    }
}

impl Model for TCPPacketSource {}
