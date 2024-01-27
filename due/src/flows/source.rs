//! Implements a general packet source that provides interfaces of all kinds of
//! packet sources.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use log::debug;
use rand::rngs::SmallRng;
use rand::SeedableRng;

use asynchronix::model::{InitializedModel, Model, Output};
use asynchronix::time::{EventKey, MonotonicTime, Scheduler};

use crate::flows::dist_source::DistPacketSource;
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
    pub fn new(flow_id: usize, traffic: TrafficCharacteristics, seed: usize) -> Self {
        let global_seed = get_seed();
        let rng = match global_seed {
            1.. => SmallRng::seed_from_u64((global_seed + seed) as u64),
            _ => SmallRng::from_entropy(),
        };

        if traffic.tcp.is_some() {
            PacketSource::TCPPacketSource(TCPPacketSource::new(flow_id, traffic, seed))
        } else {
            PacketSource::DistPacketSource(DistPacketSource::new(flow_id, traffic, seed))
        }
    }

    pub fn output(&self) -> Output<Packet> {
        match self {
            PacketSource::DistPacketSource(source) => source.output,
            PacketSource::TCPPacketSource(source) => source.output,
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

    // pub async fn packet_received(&mut self, packet: Packet, scheduler: &Scheduler<Self>) {
    //     match self {
    //         PacketSource::DistPacketSource(source) => source.packet_received(packet, scheduler),
    //         PacketSource::TCPPacketSource(source) => {
    //             source.ack_packet_received(packet, scheduler).await
    //         }
    //     }
    // }

    pub fn schedule_next_run(&self, now: f64) -> (bool, Duration) {
        match self {
            PacketSource::DistPacketSource(source) => source.schedule_next_run(now),
            PacketSource::TCPPacketSource(source) => (false, Duration::default()),
        }
    }

    pub fn before_sending_packet(&self, now: f64) -> (bool, Duration) {
        match self {
            PacketSource::DistPacketSource(source) => (false, Duration::default()),
            PacketSource::TCPPacketSource(source) => source.before_sending_packet(now),
        }
    }

    pub fn packet_sent(&mut self, packet: &Packet, now: f64) -> (bool, Duration) {
        match self {
            PacketSource::DistPacketSource(source) => source.packet_sent(packet, now),
            PacketSource::TCPPacketSource(source) => source.packet_sent(packet, now),
        }
    }

    pub fn wrap_up_packet_event<'a>(
        &'a mut self,
        packet_id: usize,
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
                    let interval = source.timer_expired(packet_id, now).await;

                    // schedule a new timeout event for this segment
                    let event_key = scheduler
                        .schedule_keyed_event(interval, Self::wrap_up_packet_event, packet_id)
                        .unwrap();

                    source.reset_timer(packet_id, event_key, now);
                }
            }
        }
    }

    pub fn finish_wrap_up(&mut self, packet: Packet, event_key: EventKey, now: f64) {
        match self {
            PacketSource::DistPacketSource(source) => (),
            PacketSource::TCPPacketSource(source) => source.finish_wrap_up(packet, event_key, now),
        }
    }

    fn should_produce_packet(&self, now: f64) -> bool {
        match self {
            PacketSource::DistPacketSource(source) => true,
            PacketSource::TCPPacketSource(source) => source.should_produce_packet(now),
        }
    }

    fn produce_packet(&mut self, now: f64) -> (Packet, Duration) {
        match self {
            PacketSource::DistPacketSource(source) => source.produce_packet(now),
            PacketSource::TCPPacketSource(source) => source.produce_packet(now),
        }
    }

    async fn send_packet(&mut self, packet: Packet) {
        self.output().send(packet).await;
    }

    pub fn run<'a>(
        &'a mut self,
        _: (),
        scheduler: &'a Scheduler<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let current_time = scheduler.time().duration_since(MonotonicTime::EPOCH);
            let now = current_time.as_secs_f64();

            while !self.traffic_exceeded(now) {
                let (schedule_new_run, interval) = self.before_sending_packet(now);
                if schedule_new_run {
                    scheduler.schedule_event(interval, Self::run, ()).unwrap();

                    return;
                }

                if self.should_produce_packet(now) {
                    let (packet, interval) = self.produce_packet(now);
                    if interval == Duration::default() {
                        self.output().send(packet.clone()).await;
                    } else {
                        scheduler
                            .schedule_event(interval, Self::send_packet, packet.clone())
                            .unwrap();
                    }

                    let (wrap_up, interval) = self.packet_sent(&packet, now);
                    if wrap_up {
                        let event_key = scheduler
                            .schedule_keyed_event(
                                interval,
                                Self::wrap_up_packet_event,
                                packet.packet_id,
                            )
                            .unwrap();

                        self.finish_wrap_up(packet, event_key, now);
                    }
                }

                let (schedule_next_run, interval) = self.schedule_next_run(now);
                if schedule_next_run {
                    scheduler.schedule_event(interval, Self::run, ()).unwrap();
                    return;
                }
            }

            if self.traffic_exceeded(now) {
                debug!("{} finished running at {:.3}.", format!("{self}"), now);
            }
        }
    }
}

impl Model for PacketSource {
    fn init(
        mut self,
        scheduler: &Scheduler<Self>,
    ) -> Pin<Box<dyn Future<Output = InitializedModel<Self>> + Send + '_>> {
        Box::pin(async move {
            let initial_delay = match self {
                PacketSource::DistPacketSource(source) => source.traffic.initial_delay,
                PacketSource::TCPPacketSource(source) => source.traffic.initial_delay,
            };

            debug!(
                "{} will be waiting for {:.3} sec(s) at the beginning.",
                format!("{self}"),
                initial_delay
            );

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
