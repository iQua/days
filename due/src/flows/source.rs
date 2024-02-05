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
                if source.ack_packet_received(packet, now).await {
                    self.run((), scheduler).await;
                }
            }
        }
    }

    fn prepare_run(&mut self, initial_delay: f64, scheduler: &Scheduler<Self>) {
        match self {
            PacketSource::DistPacketSource(_) => {}
            PacketSource::TCPPacketSource(source) => {
                // schedules a periodic timer to notify TCPPacketSource to
                // check if any of its sent packet reaches timeout
                scheduler
                    .schedule_event(
                        Duration::from_secs_f64(initial_delay + 0.05),
                        Self::periodic_timer_event,
                        (),
                    )
                    .unwrap();

                // schedules an application packet source to send packets to
                // TCPPacketSource
                let (packet, interval) = source
                    .app_packet_source
                    .app_source
                    .produce_packet(initial_delay);

                scheduler
                    .schedule_event(
                        interval + Duration::from_secs_f64(initial_delay),
                        Self::app_packet_arrive,
                        packet,
                    )
                    .unwrap();

                source.busy_until = interval.as_secs_f64() + initial_delay;
            }
        }
    }

    fn app_packet_arrive<'a>(
        &'a mut self,
        packet: Packet,
        scheduler: &'a Scheduler<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            match self {
                PacketSource::DistPacketSource(_) => (),
                PacketSource::TCPPacketSource(source) => {
                    let now = scheduler
                        .time()
                        .duration_since(MonotonicTime::EPOCH)
                        .as_secs_f64();

                    let (new_packet, interval) =
                        source.app_packet_source.app_source.produce_packet(now);

                    // lets TCPPacketSource retrieve this packet
                    source.send_buffer += packet.size;

                    if !source.app_packet_source.app_source.traffic_exceeded(now) {
                        // schedules the next packet from the
                        // (application-layer) flow
                        scheduler
                            .schedule_event(interval, Self::app_packet_arrive, new_packet)
                            .unwrap();
                    }

                    if source.next_seq < source.send_buffer {
                        // the TCPPacketSource could send new packet at this
                        // point, if the size of the congestion window
                        // allows
                        self.run((), scheduler).await;
                    } else {
                        // the TCPPacketSource is considered busy retrieving
                        // the next packet from the (application-layer) flow
                        source.busy_until = now + interval.as_secs_f64();
                    }
                }
            }
        }
    }

    /// Returns whether PacketSource should produce a new packet at this point.
    fn should_produce_packet(&mut self, now: f64) -> bool {
        if self.traffic_exceeded(now) {
            return false;
        }

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

    fn periodic_timer_event<'a>(
        &'a mut self,
        _: (),
        scheduler: &'a Scheduler<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            match self {
                PacketSource::DistPacketSource(_) => (),
                PacketSource::TCPPacketSource(source) => {
                    let now = scheduler
                        .time()
                        .duration_since(MonotonicTime::EPOCH)
                        .as_secs_f64();

                    source.timer_tick(now).await;

                    // schedules the next periodic timer event
                    scheduler
                        .schedule_event(
                            Duration::from_secs_f64(0.05),
                            Self::periodic_timer_event,
                            (),
                        )
                        .unwrap();
                }
            }
        }
    }

    async fn send_packet(&mut self, packet: Packet, scheduler: &Scheduler<Self>) {
        self.output().send(packet.clone()).await;

        let now = scheduler
            .time()
            .duration_since(MonotonicTime::EPOCH)
            .as_secs_f64();

        match self {
            PacketSource::DistPacketSource(source) => source.packet_sent(&packet, now),
            PacketSource::TCPPacketSource(source) => {
                source.packet_sent(&packet, now);
            }
        }
    }

    /// Returns whether PacketSource should return from the current while loop.
    fn wrap_up(&mut self, now: f64, scheduler: &Scheduler<Self>) -> bool {
        match self {
            PacketSource::DistPacketSource(_) => {
                let (_, interval) = self.produce_packet(now);
                scheduler.schedule_event(interval, Self::run, ()).unwrap();
                true
            }
            PacketSource::TCPPacketSource(_) => false,
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

            while self.should_produce_packet(now) {
                let (packet, interval) = self.produce_packet(now);
                if interval == Duration::default() {
                    // sends the packet now if interval is 0
                    self.send_packet(packet, scheduler).await;
                } else {
                    // schedules an event to send the packet if interval is more
                    // than 0
                    scheduler
                        .schedule_event(interval, Self::send_packet, packet)
                        .unwrap();
                }

                if self.wrap_up(now, scheduler) {
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

            self.prepare_run(initial_delay, scheduler);

            self.into()
        })
    }
}
