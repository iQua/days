//! Implements a packet source that simulates the TCP protocol, including
//! support for various congestion control mechanisms.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use log::{debug, info};
use rand::distributions::Distribution;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use statrs::distribution::{DiscreteUniform, Exp, Uniform};

use asynchronix::model::{InitializedModel, Model, Output};
use asynchronix::time::{EventKey, MonotonicTime, Scheduler};

use crate::flows::cc::{CCAlgorithm, CongestionControl, TCPCubic, TCPReno};
use crate::flows::flow::Flow;
use crate::flows::packet::Packet;
use crate::flows::DistributionInfo;
use crate::{get_seed, next_endpoint_id};

pub struct TCPPacketSource {
    endpoint_id: usize,
    /// the flow that serves as the source
    flow: Flow,
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
    rto: f64,
    /// an estimate of the RTT deviation
    est_deviation: f64,
    /// the in-flight packets (segments)
    sent_packets: HashMap<usize, Packet>,
    /// the scheduled events of timeouts of in-flight packets (segments)
    timeout_events: HashMap<usize, EventKey>,

    packets_sent: usize,
    rng: SmallRng,

    pub output: Output<Packet>,
}

impl TCPPacketSource {
    pub fn new(
        flow: Flow,
        cc_algorithm: CCAlgorithm,
        rtt_estimate: f64,
        seed: usize,
    ) -> TCPPacketSource {
        let global_seed = get_seed();
        let rng = match global_seed {
            1.. => SmallRng::seed_from_u64((global_seed + seed) as u64),
            _ => SmallRng::from_entropy(),
        };

        let congestion_control: Box<dyn CongestionControl + Send + Sync> = match cc_algorithm {
            CCAlgorithm::TCPReno => Box::new(TCPReno::new()),
            CCAlgorithm::TCPCubic => Box::new(TCPCubic::new()),
        };

        TCPPacketSource {
            endpoint_id: next_endpoint_id(),
            flow,
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
            packets_sent: 0,
            rng,
            output: Output::default(),
        }
    }

    pub fn id(&self) -> usize {
        self.endpoint_id
    }

    pub fn flow_id(&self) -> usize {
        self.flow.id
    }

    fn packet_sent(&mut self, now: Duration, packet: Packet) {
        self.packets_sent += 1;

        debug!(
            "TCPPacketSource {} sent packet {} ({} bytes) at time {:.3}. {} packets sent.",
            self.endpoint_id,
            packet.packet_id,
            packet.size,
            now.as_secs_f64(),
            self.packets_sent,
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
        }
    }

    fn timeout_reached<'a>(
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
                    Self::timeout_reached,
                    packet_id,
                )
                .unwrap();

            self.timeout_events.insert(packet_id, event_key);
        }
    }

    fn retrieve_packet_from_flow(&mut self, now: f64) -> (f64, usize) {
        // retrieves packet from the (application-layer) flow
        let interval = match self.flow.traffic.arr_dist {
            DistributionInfo::DiscreteUniform { low, high } => DiscreteUniform::new(low, high)
                .unwrap()
                .sample(&mut self.rng),
            DistributionInfo::Exp { lambda } => Exp::new(lambda).unwrap().sample(&mut self.rng),
            DistributionInfo::Uniform { low, high } => {
                Uniform::new(low, high).unwrap().sample(&mut self.rng)
            }
        };

        let packet_size = match self.flow.traffic.pkt_size_dist {
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

            if !self.flow.traffic.size.exceeded(self.next_seq, now) {
                // waits for the next arrival of the packet of the flow
                let (wait_time, packet_size) = self.retrieve_packet_from_flow(now);
                self.last_arrival = now;
                if wait_time > 0.0 {
                    self.last_arrival += wait_time;
                    self.send_buffer += packet_size;
                    scheduler
                        .schedule_event(Duration::from_secs_f64(wait_time), Self::run, ())
                        .unwrap();
                }
                // the sender can transmit up to the size of the congestion window
                else if (self.next_seq + self.mss) as f64
                    <= (self.send_buffer as f64)
                        .min(self.last_ack as f64 + self.congestion_control.get_cwnd())
                {
                    let packet_id = self.next_seq;
                    let packet = Packet::new(self.mss, packet_id, self.flow_id(), now);

                    // sends the packet out to the next element now
                    self.output.send(packet.clone()).await;
                    self.packet_sent(current_time, packet.clone());

                    self.sent_packets.insert(packet_id, packet.clone());

                    self.next_seq += packet.size;

                    // schedule a timeout event for this segment
                    let event_key = scheduler
                        .schedule_keyed_event(
                            Duration::from_secs_f64(self.rto),
                            Self::timeout_reached,
                            packet_id,
                        )
                        .unwrap();

                    self.timeout_events.insert(packet_id, event_key);
                }
            } else {
                // source can be stopped when all its sent packets either
                // reached timeout or their acknowledgments were receieved
                if self.timeout_events.is_empty() {
                    info!(
                        "TCPPacketSource {} of Flow {} finished running at {:.3}.",
                        self.endpoint_id,
                        self.flow_id(),
                        now
                    );
                }
            }
        }
    }
}

impl Model for TCPPacketSource {
    fn init(
        self,
        scheduler: &Scheduler<Self>,
    ) -> Pin<Box<dyn Future<Output = InitializedModel<Self>> + Send + '_>> {
        Box::pin(async move {
            if self.flow.traffic.initial_delay > 0.0 {
                scheduler
                    .schedule_event(
                        Duration::from_secs_f64(self.flow.traffic.initial_delay),
                        Self::run,
                        (),
                    )
                    .unwrap();
            } else {
                panic!(
                    "The initial delay of TCPPacketSource {}'s flow must be positive.",
                    self.endpoint_id
                )
            }

            self.into()
        })
    }
}
