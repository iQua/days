//! Implements a packet source that simulates the TCP protocol, including
//! support for various congestion control mechanisms.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use asynchronix::simulation::Mailbox;
use log::{debug, info};
use rand::distributions::Distribution;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use statrs::distribution::{DiscreteUniform, Exp, Uniform};

use asynchronix::model::{InitializedModel, Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::flows::cc::{CCAlgorithm, CongestionControl, TCPCubic, TCPReno};
use crate::flows::flow::Flow;
use crate::flows::packet::Packet;
use crate::flows::{DistributionInfo, TrafficCharacteristics};
use crate::{get_seed, next_endpoint_id};

/// A simple timer that expires after a timeout value.
pub struct Timer {
    /// the id of this timer
    timer_id: usize,
    timeout: f64,
    output: Output<TimerExpiredMsg>,
}

/// The message that a timer sends to the TCPPacketSource when it expires.
#[derive(Clone)]
pub struct TimerExpiredMsg {
    timer_id: usize,
}

impl Timer {
    pub fn new(timer_id: usize, timeout: f64) -> Timer {
        Timer {
            timer_id,
            timeout,
            output: Output::default(),
        }
    }

    pub fn activate(&mut self, scheduler: &Scheduler<Self>) {
        scheduler
            .schedule_event(Duration::from_secs_f64(self.timeout), Self::send, ())
            .unwrap();
    }

    pub async fn send(&mut self) {
        self.output
            .send(TimerExpiredMsg {
                timer_id: self.timer_id,
            })
            .await;
    }
}

impl Model for Timer {}

#[derive(Debug)]
pub struct TCPPacketSource {
    endpoint_id: usize,
    /// the flow that serves as the source
    flow: Flow,
    /// the time when data last arrived from the flow
    last_arrival: f64,
    /// the congestion controller
    congestion_control: Box<dyn CongestionControl>,
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

    pub async fn send(&mut self, packet: Packet) {
        self.output.send(packet).await;
    }

    /// On receiving an acknowledgment packet.
    pub fn ack_packet_received(&mut self, ack_packet: Packet, scheduler: &Scheduler<Self>) {
        // the received packet must be an ack
        assert!(ack_packet.ack.is_some());

        let now = scheduler.time();
        let arrival_time = now.duration_since(MonotonicTime::EPOCH).as_secs_f64();

        debug!(
            "TCPPacketSource {} received packet {} ({} bytes) from flow {} at time {:.3}.",
            self.endpoint_id,
            ack_packet.packet_id,
            ack_packet.size,
            ack_packet.flow_id,
            arrival_time,
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
            resent_pkt.time = arrival_time;

            scheduler
                .schedule_event(Duration::from_secs_f64(0), Self::send, resent_pkt)
                .unwrap();

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
                resent_pkt.time = arrival_time;

                scheduler
                    .schedule_event(Duration::from_secs_f64(0), Self::send, resent_pkt)
                    .unwrap();

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
            // new ack received, update the RTT estimate and the retransmission timout

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
            }
        }
    }

    fn produce_packet(&mut self, now: f64) -> (Packet, Duration) {
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

        let packet = Packet::new(packet_size, self.packets_sent, self.flow_id(), now);
        (packet, Duration::from_secs_f64(interval))
    }

    pub fn run<'a>(
        &'a mut self,
        _: (),
        scheduler: &'a Scheduler<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let current_time = scheduler.time().duration_since(MonotonicTime::EPOCH);
            let now = current_time.as_secs_f64();
            let (packet, interval) = self.produce_packet(now);

            // sends the packet out to the next element now
            self.output.send(packet.clone()).await;
            self.packet_sent(current_time, packet);

            if (self.sent_size < self.traffic.size)
                & (now + interval.as_secs_f64() <= self.traffic.duration)
            {
                scheduler.schedule_event(interval, Self::run, ()).unwrap();
            } else {
                info!(
                    "TCPPacketSource {} of Flow {} finished running at {:.3}.",
                    self.endpoint_id, self.flow_id, now
                );
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
            if self.traffic.initial_delay > 0.0 {
                scheduler
                    .schedule_event(
                        Duration::from_secs_f64(self.traffic.initial_delay),
                        Self::run,
                        (),
                    )
                    .unwrap();
            } else {
                panic!(
                    "TCPPacketSource {}'s initial delay must be positive.",
                    self.endpoint_id
                )
            }

            self.into()
        })
    }
}
