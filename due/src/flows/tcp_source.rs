//! Implements a packet source that simulates the TCP protocol, including
//! support for various congestion control mechanisms.

use core::fmt;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use log::debug;
use rand::distributions::Distribution;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use statrs::distribution::{DiscreteUniform, Exp, Uniform};

use asynchronix::model::{InitializedModel, Model, Output};
use asynchronix::time::{EventKey, MonotonicTime, Scheduler};

use crate::flows::cc::{CCAlgorithm, CongestionControl, TCPCubic, TCPReno};
use crate::flows::packet::Packet;
use crate::flows::{DistributionInfo, TrafficCharacteristics};
use crate::{get_seed, next_endpoint_id};

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
    pub next_seq: usize,
    /// the maximum sequence number in the in-transit data buffer
    send_buffer: usize,
    /// the sequence number of the segment that is last acknowledged
    last_ack: usize,
    /// the count of duplicate acknolwedgments
    dupack: usize,
    /// the RTT estimate
    rtt_estimate: f64,
    /// the retransmission timeout
    rto: f64,
    /// an estimate of the RTT deviation
    est_deviation: f64,
    /// the in-flight packets (segments)
    sent_packets: HashMap<usize, Packet>,
    /// the scheduled events of timeouts of in-flight packets (segments)
    timeout_events: HashMap<usize, EventKey>,

    /// The source is considered busy retrieving the current packet from flow
    /// until this time
    busy_until: f64,

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
    pub fn new(flow_id: usize, traffic: TrafficCharacteristics, seed: usize) -> TCPPacketSource {
        let global_seed = get_seed();
        let rng = match global_seed {
            1.. => SmallRng::seed_from_u64((global_seed + seed) as u64),
            _ => SmallRng::from_entropy(),
        };

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
            rng,
            output: Output::default(),
        }
    }

    pub fn packet_sent(&mut self, now: f64, packet: Packet) {
        self.packets_sent += 1;

        debug!(
            "TCPPacketSource {} sent packet {} ({} bytes) at time {:.3}. {} packets sent.",
            self.endpoint_id, packet.packet_id, packet.size, now, self.packets_sent,
        );
    }

    /// On receiving an acknowledgment packet
    pub async fn ack_packet_received(&mut self, ack_packet: Packet, scheduler: &Scheduler<Self>) {
        // the received packet must be an acknowledgment
        assert!(ack_packet.ack.is_some());

        let now = scheduler
            .time()
            .duration_since(MonotonicTime::EPOCH)
            .as_secs_f64();

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

            return;
        } else if self.dupack > 3 {
            self.congestion_control.more_dupacks_received();

            if self.last_ack as f64 + self.congestion_control.get_cwnd() >= ack.sequence_num as f64
            {
                let resent_pkt = self.sent_packets.get_mut(&ack.sequence_num).unwrap();
                resent_pkt.time = now;

                self.output.send(resent_pkt.clone()).await;

                debug!(
                    "TCPPacketSource {} resent packet {} ({} bytes) from flow {} at time {:.3}.",
                    self.endpoint_id,
                    resent_pkt.packet_id,
                    resent_pkt.size,
                    resent_pkt.flow_id,
                    now,
                );
            }

            return;
        }

        if self.dupack == 0 {
            // new acknowledgment received, update the RTT estimate and the
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

            if self.sent_packets.contains_key(&ack_packet.packet_id) {
                self.sent_packets.remove(&ack_packet.packet_id);

                // cancels the event scheduled for the timeout of this packet
                self.timeout_events
                    .remove_entry(&ack_packet.packet_id)
                    .unwrap()
                    .1
                    .cancel();
            }

            if now >= self.busy_until {
                self.run((), scheduler).await;
            }
        }
    }

    fn timer_expired<'a>(
        &'a mut self,
        packet_id: usize,
        scheduler: &'a Scheduler<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let now = scheduler
                .time()
                .duration_since(MonotonicTime::EPOCH)
                .as_secs_f64();

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

            // schedule a new timeout event for this segment
            let event_key = scheduler
                .schedule_keyed_event(
                    Duration::from_secs_f64(self.rto),
                    Self::timer_expired,
                    packet_id,
                )
                .unwrap();

            self.timeout_events.insert(packet_id, event_key);

            debug!(
                "TCPPacketSource {} reset a timer for packet {} with an RTO of {:.3} and expiry time of {:.3}.",
                self.endpoint_id, packet_id, self.rto, now + self.rto
            );
        }
    }

    fn retrieve_packet_from_flow(&mut self, now: f64) -> (f64, usize) {
        // retrieves packet from the (application-layer) flow
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

        (interval - (now - self.last_arrival), packet_size)
    }

    pub fn run<'a>(
        &'a mut self,
        _: (),
        scheduler: &'a Scheduler<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let current_time = scheduler.time().duration_since(MonotonicTime::EPOCH);
            let now = current_time.as_secs_f64();

            while !self.traffic.size.exceeded(self.next_seq, now) {
                while self.next_seq >= self.send_buffer {
                    // retrieves more packets from the (application-layer) flow
                    let (wait_time, packet_size) = self.retrieve_packet_from_flow(now);

                    self.last_arrival = now;
                    self.send_buffer += packet_size;

                    // waits for the next arrival of the packet of the flow
                    if wait_time > 0.0 {
                        self.last_arrival += wait_time;
                        self.busy_until = self.last_arrival;

                        scheduler
                            .schedule_event(Duration::from_secs_f64(wait_time), Self::run, ())
                            .unwrap();

                        return;
                    }
                }

                // the sender can transmit up to the size of the congestion window
                if (self.next_seq + self.mss) as f64
                    <= (self.send_buffer as f64)
                        .min(self.last_ack as f64 + self.congestion_control.get_cwnd())
                {
                    let packet_id = self.next_seq;
                    let packet = Packet::new(self.mss, packet_id, self.flow_id, now);

                    // sends the packet out to the next element now
                    self.output.send(packet.clone()).await;
                    self.packet_sent(now, packet.clone());

                    self.sent_packets.insert(packet_id, packet.clone());

                    self.next_seq += packet.size;

                    // schedule a timeout event for this segment
                    let event_key = scheduler
                        .schedule_keyed_event(
                            Duration::from_secs_f64(self.rto),
                            Self::timer_expired,
                            packet_id,
                        )
                        .unwrap();

                    self.timeout_events.insert(packet_id, event_key);

                    debug!(
                            "TCPPacketSource {} set a timer for packet {} with an RTO of {:.3} and expiry time of {:.3}.",
                            self.endpoint_id, packet.packet_id, self.rto, now + self.rto
                        );
                } else {
                    return;
                }
            }
        }
    }
}

impl Model for TCPPacketSource {
    fn init(
        mut self,
        scheduler: &Scheduler<Self>,
    ) -> Pin<Box<dyn Future<Output = InitializedModel<Self>> + Send + '_>> {
        Box::pin(async move {
            if self.traffic.initial_delay > 0.0 {
                scheduler
                    .schedule_event(
                        Duration::from_secs_f64(self.traffic.initial_delay),
                        Self::run,
                        (),
                    )
                    .unwrap();
            } else {
                self.run((), scheduler).await;
            }

            self.into()
        })
    }
}
