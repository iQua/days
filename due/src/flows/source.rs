//! Implements a general packet source that provides interfaces of all kinds of
//! packet sources.

use std::borrow::BorrowMut;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use log::debug;
use rand::rngs::SmallRng;
use rand::SeedableRng;

use asynchronix::model::{InitializedModel, Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::flows::dist_source::DistPacketSource;
use crate::flows::flow::FlowType;
use crate::flows::packet::Packet;
use crate::flows::tcp_source::TCPPacketSource;
use crate::flows::TrafficCharacteristics;
use crate::get_seed;

#[derive(Debug)]
pub enum PacketSource {
    DistPacketSource(DistPacketSource),
    TCPPacketSource(TCPPacketSource),
}

impl std::fmt::Display for PacketSource {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PacketSource::DistPacketSource(_) => write!(f, "DistPacketSource {}", self.id()),
            PacketSource::TCPPacketSource(_) => write!(f, "TCPPacketSource {}", self.id()),
        }
    }
}

impl PacketSource {
    pub fn new(
        flow_id: usize,
        flow_type: FlowType,
        traffic: TrafficCharacteristics,
        seed: usize,
    ) -> Self {
        let global_seed = get_seed();
        let rng = match global_seed {
            1.. => SmallRng::seed_from_u64((global_seed + seed) as u64),
            _ => SmallRng::from_entropy(),
        };

        match flow_type {
            FlowType::PacketDistribution => {
                PacketSource::DistPacketSource(DistPacketSource::new(flow_id, traffic, rng))
            }
            FlowType::TCP => {
                PacketSource::TCPPacketSource(TCPPacketSource::new(flow_id, traffic, rng))
            }
        }
    }

    pub fn output(&mut self) -> &mut Output<Packet> {
        match self {
            PacketSource::DistPacketSource(source) => source.output.borrow_mut(),
            PacketSource::TCPPacketSource(source) => source.output.borrow_mut(),
        }
    }

    pub fn id(&self) -> usize {
        match self {
            PacketSource::DistPacketSource(source) => source.endpoint_id,
            PacketSource::TCPPacketSource(source) => source.endpoint_id,
        }
    }

    pub fn flow_id(&self) -> usize {
        match self {
            PacketSource::DistPacketSource(source) => source.flow_id,
            PacketSource::TCPPacketSource(source) => source.flow_id,
        }
    }

    fn traffic_exceeded(&self, now: f64) -> bool {
        match self {
            PacketSource::DistPacketSource(source) => source.traffic_exceeded(now),
            PacketSource::TCPPacketSource(source) => source.traffic_exceeded(now),
        }
    }

    pub async fn packet_received(&mut self, packet: Packet, scheduler: &Scheduler<Self>) {
        let now = scheduler
            .time()
            .duration_since(MonotonicTime::EPOCH)
            .as_secs_f64();
        match self {
            PacketSource::DistPacketSource(source) => source.packet_received(packet, now),
            PacketSource::TCPPacketSource(source) => {
                let action = source.ack_packet_received(packet, now).await;
                if action.proceed_run {
                    self.run((), scheduler).await;
                } else if action.set_timer {
                    let _packet_id = action.packet_id.unwrap();

                    // // schedules a timeout event for this packet
                    // let event_key = scheduler
                    //     .schedule_keyed_event(
                    //         Duration::from_secs_f64(source.rto),
                    //         Self::wrap_up_packet_event,
                    //         packet_id,
                    //     )
                    //     .unwrap();

                    // source.finish_wrap_up(packet_id, event_key, now);
                }
            }
        }
    }

    /// Returns whether PacketSource should return from the current while loop
    /// before producing a packet.
    pub fn early_return(&mut self, now: f64, scheduler: &Scheduler<Self>) -> bool {
        match self {
            PacketSource::DistPacketSource(_) => false,
            PacketSource::TCPPacketSource(source) => {
                let (should_return, interval) = source.retrieve_packets_from_flow(now);
                if should_return {
                    scheduler.schedule_event(interval, Self::run, ()).unwrap();
                }
                should_return
            }
        }
    }

    /// Returns whether PacketSource should produce a new packet at this point.
    fn should_produce_packet(&mut self) -> bool {
        match self {
            PacketSource::DistPacketSource(_) => true,
            PacketSource::TCPPacketSource(source) => source.should_produce_packet(),
        }
    }

    /// Returns a new packet and when to send out this packet.
    fn produce_packet(&mut self, now: f64) -> (Packet, Duration) {
        match self {
            PacketSource::DistPacketSource(source) => source.produce_packet(now),
            PacketSource::TCPPacketSource(source) => source.produce_packet(now),
        }
    }

    pub fn wrap_up_packet_event<'a>(
        &'a mut self,
        packet_id: usize,
        scheduler: &'a Scheduler<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            match self {
                // no wrap-up event after sending out a packet in
                // DistPacketSource
                PacketSource::DistPacketSource(_) => (),
                // the wrap-up event after sending out a packet in
                // TCPPacketSource is the timeout event scheduled for this
                // packet
                PacketSource::TCPPacketSource(source) => {
                    let now = scheduler
                        .time()
                        .duration_since(MonotonicTime::EPOCH)
                        .as_secs_f64();
                    source.timer_expired(packet_id, now).await;

                    // schedules a new timeout event for this packet
                    let event_key = scheduler
                        .schedule_keyed_event(
                            Duration::from_secs_f64(source.rto),
                            Self::wrap_up_packet_event,
                            packet_id,
                        )
                        .unwrap();

                    source.reset_timer(packet_id, event_key, now);
                }
            }
        }
    }

    /// Wraps up after sending out a packet.
    fn wrap_up(&mut self, packet: &Packet, now: f64, _scheduler: &Scheduler<Self>) {
        match self {
            PacketSource::DistPacketSource(source) => source.packet_sent(packet, now),
            PacketSource::TCPPacketSource(source) => {
                source.packet_sent(packet, now);

                // // schedules a timeout event for this packet
                // let event_key = scheduler
                //     .schedule_keyed_event(
                //         Duration::from_secs_f64(source.rto),
                //         Self::wrap_up_packet_event,
                //         packet.packet_id,
                //     )
                //     .unwrap();

                // source.finish_wrap_up(packet.packet_id, event_key, now);
            }
        }
    }

    async fn send_packet(&mut self, packet: Packet) {
        self.output().send(packet).await;
    }

    /// Returns whether PacketSource should return from the current while loop.
    fn wrap_up_run(&mut self, now: f64, scheduler: &Scheduler<Self>) -> bool {
        match self {
            PacketSource::DistPacketSource(_) => {
                let (_, interval) = self.produce_packet(now);
                scheduler.schedule_event(interval, Self::run, ()).unwrap();
                true
            }
            PacketSource::TCPPacketSource(source) => {
                if source.tcp_send_packet {
                    source.tcp_send_packet = false;
                    return false;
                }
                true
            }
        }
    }

    pub fn run<'a>(
        &'a mut self,
        _: (),
        scheduler: &'a Scheduler<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let now = scheduler
                .time()
                .duration_since(MonotonicTime::EPOCH)
                .as_secs_f64();

            while !self.traffic_exceeded(now) {
                if self.early_return(now, scheduler) {
                    return;
                }

                if self.should_produce_packet() {
                    let (packet, interval) = self.produce_packet(now);
                    if interval == Duration::default() {
                        // sends the packet now if interval is 0
                        self.send_packet(packet.clone()).await;
                    } else {
                        // schedules an event to send the packet if interval is
                        // more than 0
                        scheduler
                            .schedule_event(interval, Self::send_packet, packet.clone())
                            .unwrap();
                    }

                    self.wrap_up(&packet, now, scheduler);
                }

                if self.wrap_up_run(now, scheduler) {
                    return;
                }
            }

            if self.traffic_exceeded(now) {
                debug!("{} finished running at {:.3}.", format!("{self}"), now);
            }
        }
    }

    fn advance_initial_delay(&self) -> f64 {
        let initial_delay = match &self {
            PacketSource::DistPacketSource(source) => source.traffic.initial_delay,
            PacketSource::TCPPacketSource(source) => source.traffic.initial_delay,
        };

        debug!(
            "{} will be waiting for {:.3} sec(s) at the beginning.",
            format!("{self}"),
            initial_delay
        );

        initial_delay
    }
}

impl Model for PacketSource {
    fn init(
        mut self,
        scheduler: &Scheduler<Self>,
    ) -> Pin<Box<dyn Future<Output = InitializedModel<Self>> + Send + '_>> {
        Box::pin(async move {
            let initial_delay = self.advance_initial_delay();

            if initial_delay > 0.0 {
                scheduler
                    .schedule_event(Duration::from_secs_f64(initial_delay), Self::run, ())
                    .unwrap();
            } else {
                self.run((), scheduler).await;
            }

            self.into()
        })
    }
}
