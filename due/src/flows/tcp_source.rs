//! Implements a packet source that simulates the TCP protocol, including
//! support for various congestion control mechanisms.

use core::fmt;
use std::collections::HashMap;
use std::time::Duration;

use log::debug;
use rand::distributions::Distribution;
use rand::rngs::SmallRng;
use statrs::distribution::{DiscreteUniform, Exp, Uniform};

use asynchronix::model::{Model, Output};
use asynchronix::time::EventKey;

use crate::flows::cc::{CCAlgorithm, CongestionControl, TCPCubic, TCPReno};
use crate::flows::packet::Packet;
use crate::flows::{DistributionInfo, TrafficCharacteristics};
use crate::next_endpoint_id;

/// Defines the action that PacketSource should take after TCPPacketSource
/// receives an acknowledgment.
pub struct AckAction {
    /// whether PacketSource should proceed another run()  
    pub proceed_run: bool,
    /// whether PacketSource should set a timer (schedule a timeout event)
    pub set_timer: bool,
    /// the packet id of the timer that will be set
    pub packet_id: Option<usize>,
}

pub struct TCPPacketSource {
    pub endpoint_id: usize,
    pub flow_id: usize,
    pub traffic: TrafficCharacteristics,
    /// the time when data last arrived from the flow
    last_arrival: f64,
    /// the congestion controller
    congestion_control: Box<dyn CongestionControl + Send + Sync>,
    /// maximum segment size, in bytes
    mss: usize,
    /// the next sequence number to be sent, in bytes
    next_seq: usize,
    /// the maximum sequence number in the in-transit data buffer
    send_buffer: usize,
    /// the sequence number of the segment that is last acknowledged
    last_ack: usize,
    /// the count of duplicate acknolwedgments
    dupack: usize,
    /// the RTT estimate
    rtt_estimate: f64,
    /// the retransmission timeout
    pub rto: f64,
    /// an estimate of the RTT deviation
    est_deviation: f64,
    /// the in-flight packets (segments)
    sent_packets: HashMap<usize, Packet>,
    /// the scheduled events of timeouts of in-flight packets (segments)
    timeout_events: HashMap<usize, EventKey>,

    /// the source is considered busy retrieving the current packet from flow
    /// until this time
    busy_until: f64,
    /// whether the source can send a packet before reaching the size of
    /// congestion window
    pub tcp_send_packet: bool,

    packets_sent: usize,
    rng: SmallRng,

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
        let rtt_estimate = traffic.tcp.unwrap().rtt_estimate;

        let congestion_control: Box<dyn CongestionControl + Send + Sync> = match cc_algorithm {
            CCAlgorithm::TCPReno => Box::new(TCPReno::new()),
            CCAlgorithm::TCPCubic => Box::new(TCPCubic::new()),
        };

        TCPPacketSource {
            endpoint_id: next_endpoint_id(),
            flow_id,
            traffic,
            last_arrival: 0.0,
            congestion_control,
            mss: 512,
            next_seq: 0,
            send_buffer: 0,
            last_ack: 0,
            dupack: 0,
            rtt_estimate,
            rto: rtt_estimate * 2.0,
            est_deviation: 0.0,
            sent_packets: HashMap::new(),
            timeout_events: HashMap::new(),
            busy_until: 0.0,
            packets_sent: 0,
            tcp_send_packet: false,
            rng,
            output: Output::default(),
        }
    }

    /// Returns the action that PacketSource should take after TCPPacketSource
    /// handles an acknowledgment.
    pub async fn ack_packet_received(&mut self, ack_packet: Packet, now: f64) -> AckAction {
        // the received packet must be an acknowledgment
        assert!(ack_packet.ack.is_some());

        debug!(
            "TCPPacketSource {} received Ack of packet {} ({} bytes) from flow {} at time {:.3}.",
            self.endpoint_id, ack_packet.packet_id, ack_packet.size, ack_packet.flow_id, now,
        );

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

        if self.dupack == 3 {
            self.congestion_control.consecutive_dupacks_received();

            let resent_pkt = self.sent_packets.get_mut(&ack.sequence_num).unwrap();
            resent_pkt.time = now;

            self.output.send(resent_pkt.clone()).await;

            debug!(
                "TCPPacketSource {} resent packet {} ({} bytes) from flow {} at time {:.3}.",
                self.endpoint_id, resent_pkt.packet_id, resent_pkt.size, resent_pkt.flow_id, now,
            );
        } else if self.dupack > 3 {
            self.congestion_control.more_dupacks_received();

            // transmits a new packet, if allowed by the new value of cwnd
            if self.last_ack as f64 + self.congestion_control.get_cwnd() >= ack.sequence_num as f64
                && !self.traffic.size.exceeded(self.next_seq, now)
            {
                debug!(
                    "TCPPacketSource {} will send packet {} ({} bytes) at time {:.3} as dupack > 3.",
                    self.endpoint_id, self.next_seq, self.mss, now,
                );

                let (packet, _) = self.produce_packet(now);

                self.output.send(packet.clone()).await;
                self.packet_sent(&packet, now);

                return AckAction {
                    proceed_run: false,
                    set_timer: true,
                    packet_id: Some(packet.packet_id),
                };
            }
        }

        if self.dupack == 0 {
            // new acknowledgment received, updates the RTT estimate and the
            // retransmission timeout
            let sample_rtt = now - ack_packet.creation_time;

            // Jacobsen '88: Congestion Avoidance and Control
            let sample_err = sample_rtt - self.rtt_estimate;
            self.rtt_estimate += 0.125 * sample_err;
            self.est_deviation += 0.25 * (sample_err.abs() - self.est_deviation);
            self.rto = self.rtt_estimate + 4.0 * self.est_deviation;

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

            // // this acknowledgment should acknowledge all the intermediate
            // // segments sent between the lost packet and the receipt of the
            // // first duplicate ACK, if any
            // for (packet_id, _) in self.sent_packets.iter_mut() {
            //     // cancels the events scheduled for the timeout of all the
            //     // intermediate segments
            //     if packet_id <= &ack_packet.packet_id {
            //         self.timeout_events
            //             .remove_entry(packet_id)
            //             .unwrap()
            //             .1
            //             .cancel();
            //     }
            // }
            self.sent_packets
                .retain(|&packet_id, _| packet_id > ack_packet.packet_id);

            if now >= self.busy_until {
                return AckAction {
                    proceed_run: true,
                    set_timer: false,
                    packet_id: None,
                };
            }
        }

        AckAction {
            proceed_run: false,
            set_timer: false,
            packet_id: None,
        }
    }

    pub fn packet_sent(&mut self, packet: &Packet, now: f64) {
        self.packets_sent += 1;

        debug!(
            "TCPPacketSource {} sent packet {} ({} bytes) at time {:.3}. {} packets sent.",
            self.endpoint_id, packet.packet_id, packet.size, now, self.packets_sent,
        );

        self.sent_packets.insert(packet.packet_id, packet.clone());

        self.next_seq += packet.size;
    }

    pub fn finish_wrap_up(&mut self, packet_id: usize, event_key: EventKey, now: f64) {
        self.timeout_events.insert(packet_id, event_key);

        debug!(
            "TCPPacketSource {} set a timer for packet {} with an RTO of {:.3} and expiry time of {:.3}.",
            self.endpoint_id, packet_id, self.rto, now + self.rto
        );
    }

    /// On a packet reaches timeout.
    pub async fn timer_expired(&mut self, packet_id: usize, now: f64) {
        debug!(
            "TCPPacketSource {}'s sent packet {} reached timeout at time {:.3}.",
            self.endpoint_id, packet_id, now
        );

        self.congestion_control.timer_expired();

        // retransmits the segment
        let resent_pkt = self.sent_packets.get_mut(&packet_id).unwrap();
        resent_pkt.time = now;

        self.output.send(resent_pkt.clone()).await;

        debug!(
            "TCPPacketSource {} resent packet {} ({} bytes) from flow {} at time {:.3}.",
            self.endpoint_id, resent_pkt.packet_id, resent_pkt.size, resent_pkt.flow_id, now,
        );

        // doubles the retransmission timeout
        self.rto *= 2.0;
    }

    /// Resets a timer for a packet that reached timeout.
    pub fn reset_timer(&mut self, packet_id: usize, event_key: EventKey, now: f64) {
        self.timeout_events.insert(packet_id, event_key);

        debug!(
                "TCPPacketSource {} reset a timer for packet {} with an RTO of {:.3} and expiry time of {:.3}.",
                self.endpoint_id, packet_id, self.rto, now + self.rto
            );
    }

    /// Retrieves packets from the (application-layer) flow.
    pub fn retrieve_packets_from_flow(&mut self, now: f64) -> (bool, Duration) {
        while self.next_seq >= self.send_buffer {
            let interval = match self.traffic.arr_dist {
                DistributionInfo::DiscreteUniform { low, high } => DiscreteUniform::new(low, high)
                    .unwrap()
                    .sample(&mut self.rng),
                DistributionInfo::Exp { lambda } => Exp::new(lambda).unwrap().sample(&mut self.rng),
                DistributionInfo::Uniform { low, high } => {
                    Uniform::new(low, high).unwrap().sample(&mut self.rng)
                }
            };

            let packet_size = match self.traffic.pkt_size_dist {
                DistributionInfo::DiscreteUniform { low, high } => DiscreteUniform::new(low, high)
                    .unwrap()
                    .sample(&mut self.rng)
                    as usize,
                DistributionInfo::Exp { lambda } => {
                    Exp::new(lambda).unwrap().sample(&mut self.rng) as usize
                }
                DistributionInfo::Uniform { low, high } => {
                    Uniform::new(low, high).unwrap().sample(&mut self.rng) as usize
                }
            };

            let wait_time = interval - (now - self.last_arrival);
            self.last_arrival = now;
            self.send_buffer += packet_size;

            // waits for the next arrival of the packet of the flow
            if wait_time > 0.0 {
                self.last_arrival += wait_time;
                self.busy_until = self.last_arrival;

                return (true, Duration::from_secs_f64(wait_time));
            }
        }

        (false, Duration::default())
    }

    pub fn should_produce_packet(&mut self) -> bool {
        // the sender can transmit up to the size of the congestion window
        self.tcp_send_packet = (self.next_seq + self.mss) as f64
            <= (self.send_buffer as f64)
                .min(self.last_ack as f64 + self.congestion_control.get_cwnd());

        self.tcp_send_packet
    }

    pub fn produce_packet(&mut self, now: f64) -> (Packet, Duration) {
        let packet = Packet::new(self.mss, self.next_seq, self.flow_id, now);

        (packet, Duration::default())
    }

    pub fn traffic_exceeded(&self, now: f64) -> bool {
        self.traffic.size.exceeded(self.next_seq, now)
    }
}

impl Model for TCPPacketSource {}
