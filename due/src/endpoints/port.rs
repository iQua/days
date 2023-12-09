//! Implements a simple FIFO scheduler with only one queue.

use std::collections::VecDeque;
use std::future::Future;
use std::time::Duration;

use log::debug;

use asynchronix::model::{Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};
use tachyonix::{channel, Sender};

use crate::endpoints::drop::{CapacityUnit, DropStrategy, PacketDrop, TailDrop};
use crate::endpoints::packet::Packet;
use crate::next_scheduler_id;

pub struct Port {
    scheduler_id: usize,
    /// the bit rate of the port (0 for unlimited)
    rate: f64,
    /// a closure that determines whether an inbound packet should be dropped or not
    drop_strategy: Box<dyn PacketDrop + Send + Sync>,
    /// the number of packets received
    packets_received: usize,
    /// the number of dropped packets
    packets_dropped: usize,
    /// the total byte sizes in the queue
    bytes_in_queue: usize,
    /// the packet queue of the port
    queue: VecDeque<Packet>,
    /// The FIFO server is considered busy sending the current packet until this time
    busy_until: f64,
    pub sender: Sender<(Packet, usize)>,
    pub output: Output<Packet>,
}

impl Port {
    pub fn new(
        rate: f64,
        capacity: usize,
        capacity_unit: CapacityUnit,
        drop_strategy: DropStrategy,
    ) -> Port {
        let packet_drop = match drop_strategy {
            DropStrategy::TailDrop => TailDrop::new(capacity, capacity_unit),
            _ => unimplemented!(),
        };

        Port {
            scheduler_id: next_scheduler_id(),
            rate,
            drop_strategy: Box::new(packet_drop),
            packets_received: 0,
            packets_dropped: 0,
            bytes_in_queue: 0,
            queue: VecDeque::new(),
            busy_until: 0.0,
            output: Output::default(),
            sender: channel(100).0,
        }
    }

    pub fn id(&self) -> usize {
        self.scheduler_id
    }

    pub fn connect_sender(&mut self, sender: Sender<(Packet, usize)>) {
        self.sender = sender;
    }

    pub async fn packet_received(&mut self, packet: Packet, scheduler: &Scheduler<Self>) {
        let now = scheduler.time();
        let arrival_time = now.duration_since(MonotonicTime::EPOCH).as_secs_f64();

        // drops the packet if the buffer is full
        let should_drop_packet =
            self.drop_strategy
                .should_drop(packet.size, self.bytes_in_queue, self.queue.len());

        // the case that this packet will be dropped.
        if should_drop_packet {
            self.packets_dropped += 1;
            debug! {
                "Port {} dropped packet {} from flow {} at time {:.3}",
                self.scheduler_id,
                packet.packet_id,
                packet.flow_id,
                arrival_time
            }
            return;
        }

        // the case that this packet will not be dropped.
        self.packets_received += 1;
        self.queue.push_back(packet.clone());
        self.bytes_in_queue += packet.size;

        debug!(
            "Port {} received packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets received, {} packets in queue.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            arrival_time,
            self.packets_received,
            self.queue.len()
        );

        if arrival_time > self.busy_until {
            self.run((), scheduler).await;
        }
    }

    pub async fn send(&mut self, packet: Packet) {
        let _ = self.sender.send((packet.clone(), self.scheduler_id)).await;
        self.output.send(packet).await;
    }

    fn packet_sent(&mut self, now: f64, packet: Packet) {
        self.bytes_in_queue -= packet.size;
        self.busy_until = now;

        debug!(
            "Port {} will send packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets in queue.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            now,
            self.queue.len()
        );
    }

    pub fn run<'a>(
        &'a mut self,
        _: (),
        scheduler: &'a Scheduler<Self>,
    ) -> impl Future<Output = ()> + Send + 'a {
        async move {
            let current_time = scheduler.time().duration_since(MonotonicTime::EPOCH);
            let now = current_time.as_secs_f64();

            if let Some(mut packet) = self.queue.pop_front() {
                packet.send(now);

                if self.rate > 0.0 {
                    let timeout = packet.size as f64 * 8.0 / self.rate;

                    scheduler
                        .schedule_event(
                            Duration::from_secs_f64(timeout),
                            Self::send,
                            packet.clone(),
                        )
                        .unwrap();

                    scheduler
                        .schedule_event(Duration::from_secs_f64(timeout), Self::run, ())
                        .unwrap();

                    self.packet_sent(now + timeout, packet);
                } else {
                    let _ = self.sender.send((packet.clone(), self.scheduler_id)).await;
                    self.output.send(packet.clone()).await;
                    self.packet_sent(now, packet);
                }
            }
        }
    }
}

impl Model for Port {}
