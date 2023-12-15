//! Implements a Deficit Round Robin (DRR) scheduler.

use std::collections::VecDeque;
use std::sync::Arc;

use log::{debug, info};
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::flows::packet::Packet;
use crate::schedulers::drop::{CapacityUnit, DropStrategy, PacketDrop, TailDrop};
use crate::schedulers::Scheduler;
use crate::sim::{SimContext, Time};
use crate::{next_scheduler_id, Shared};

pub struct DRRServer {
    scheduler_id: usize,

    /// the bit rate of the server
    rate: f64,

    /// a closure that maps a flow_id to a class_id, used to implement
    /// class-based Deficit Round Robin. The default uses a packet's flow_id as
    /// its class_id, which is equivalent to flow-based DRR.
    pub flow_classes: Arc<dyn Fn(usize) -> usize>,

    /// a closure that determines whether an inbound packet should be dropped or not
    drop_strategy: Box<dyn PacketDrop>,

    /// deficit of classes, which are consecutive and start from 0
    deficit: Vec<usize>,
    /// quantum of classes, which are consecutive and start from 0
    quantum: Vec<usize>,

    /// the number of packets received, dropped, and in the queues waiting to be sent
    packets_received: usize,
    packets_dropped: usize,
    packets_waiting: usize,

    /// the number of bytes of classes, which are consecutive and start from 0
    byte_sizes: Vec<usize>,

    /// FIFO queues of classes, which are consecutive and start from 0
    queues: Vec<VecDeque<Packet>>,

    /// a sender for sending outbound packets to the downstream element
    pub sender: UnboundedSender<Packet>,
    /// a receiver for receiving inbound packets from upstream elements
    pub receiver: UnboundedReceiver<Packet>,
}

impl Scheduler for DRRServer {
    fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        self.sender = sender;
    }

    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        self.receiver = receiver;
    }
}

impl DRRServer {
    pub fn new(
        rate: f64,
        capacity: usize,
        capacity_unit: CapacityUnit,
        flow_classes: Arc<dyn Fn(usize) -> usize>,
        drop_strategy: DropStrategy,
        weights: Vec<usize>,
    ) -> DRRServer {
        let min_quantum = 1500;
        let mut deficit = Vec::new();
        let mut quantum = Vec::new();
        let mut byte_sizes = Vec::new();
        let mut queues = Vec::new();
        let (sender, receiver) = unbounded_channel();

        let min_weight = weights.iter().min().unwrap();

        for (class_id, _) in weights.iter().enumerate() {
            deficit.push(0);
            quantum.push(min_quantum * weights[class_id] / min_weight);
            byte_sizes.push(0);
            queues.push(VecDeque::new());
        }

        let packet_drop = match drop_strategy {
            DropStrategy::TailDrop => TailDrop::new(capacity, capacity_unit),
            _ => unimplemented!(),
        };

        DRRServer {
            scheduler_id: next_scheduler_id(),
            rate,
            flow_classes,
            drop_strategy: Box::new(packet_drop),
            deficit,
            quantum,
            packets_received: 0,
            packets_dropped: 0,
            packets_waiting: 0,
            byte_sizes,
            queues,
            sender,
            receiver,
        }
    }

    pub fn id(&self) -> usize {
        self.scheduler_id
    }

    fn packet_received(&mut self, packet: Packet, now: Time) {
        // drops the packet if the buffer is full
        let should_drop_packet = self.drop_strategy.should_drop(
            packet.size,
            self.byte_sizes.iter().sum(),
            self.queues.iter().map(|q| q.len()).sum(),
        );

        // the case that this packet will be dropped.
        if should_drop_packet {
            self.packets_dropped += 1;
            debug! {
                "Port {} dropped packet {} from flow {} at time {:.3}",
                self.scheduler_id,
                packet.packet_id,
                packet.flow_id,
                now
            }
            return;
        }

        self.packets_waiting += 1;
        self.packets_received += 1;

        let class_id = (self.flow_classes)(packet.flow_id);

        self.queues[class_id].push_back(packet.clone());
        self.byte_sizes[class_id] += packet.size;

        debug!(
            "DRRServer {} received packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets received, {} packet(s) in class queue {}.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            now,
            self.packets_received,
            self.queues[class_id].len(),
            class_id
        );
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        loop {
            // schedules packets by going through each queue
            for class_id in 0..self.queues.len() {
                // increases the deficit of the current queue if it is non-empty
                if !self.queues[class_id].is_empty() {
                    self.deficit[class_id] += self.quantum[class_id];
                } else {
                    // resets to zero if the queue is empty
                    self.deficit[class_id] = 0;
                }

                let mut current_deficit = self.deficit[class_id];

                while current_deficit > 0 && !self.queues[class_id].is_empty() {
                    let packet = self.queues[class_id].front().unwrap().clone();

                    if packet.size <= current_deficit {
                        // sends the packet out to the next element
                        self.byte_sizes[class_id] -= packet.size;

                        if self.rate > 0.0 {
                            sim.advance(packet.size as f64 * 8.0 / self.rate).await;
                        }

                        let mut outbound = self.queues[class_id].pop_front().unwrap();
                        outbound.departure_update(sim.now());
                        let _ = self.sender.send(outbound.clone());

                        self.packets_waiting -= 1;
                        current_deficit -= packet.size;

                        // polls for and receives all outstanding packets
                        // recently sent to DDRServer while sending the previous
                        // packet
                        while let Ok(packet) = self.receiver.try_recv() {
                            self.packet_received(packet, sim.now());
                        }

                        debug!(
                            "DRRServer {} sent packet {} ({} bytes) from flow {} at time {:.3}. \
                                    {} packets in the class queue.",
                            self.scheduler_id,
                            packet.packet_id,
                            packet.size,
                            packet.flow_id,
                            sim.now(),
                            self.queues[class_id].len(),
                        );
                    } else {
                        break;
                    }
                }

                self.deficit[class_id] = current_deficit;
            } // finishes going through each queue in one round

            // waits for inbound packets from the upstream element
            if self.packets_waiting == 0 {
                if let Some(packet) = self.receiver.recv().await {
                    self.packet_received(packet, sim.now());
                } else {
                    break;
                }
            }
        }
        info!(
            "DRRServer {} finished running at time {}.",
            self.scheduler_id,
            sim.now()
        );
    }
}
