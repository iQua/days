//! Implements a Virtual Clock scheduler.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use log::debug;

use asynchronix::model::{Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::flows::packet::Packet;
use crate::next_scheduler_id;
use crate::schedulers::drop::{CapacityUnit, DropStrategy, PacketDrop, TailDrop, RED};

pub struct VirtualClockServer {
    scheduler_id: usize,

    /// the bit rate of the server
    rate: f64,

    /// a closure that maps a flow_id to a class_id, used to implement
    /// class-based Virtual Clock. The default uses a packet's flow_id as
    /// its class_id, which is equivalent to flow-based Virtual Clock.
    pub flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,

    /// a closure that determines whether an inbound packet should be dropped or not
    drop_strategy: Box<dyn PacketDrop + Send + Sync>,

    /// the number of packets received and dropped
    packets_received: usize,
    packets_dropped: usize,

    /// the number of bytes of classes, which are consecutive and start from 0
    /// flow_class -> byte_size
    byte_sizes: HashMap<usize, usize>,

    /// FIFO queues of classes
    /// flow_class -> queue
    queues: BTreeMap<usize, VecDeque<Packet>>,

    /// flow_class -> inverse of the desired rates for the corresponding flows,
    /// in bits per second
    vticks: HashMap<usize, usize>,

    /// number of queued packets of each flow class
    flow_queue_count: HashMap<usize, usize>,

    aux_vc: HashMap<usize, usize>,
    v_clocks: HashMap<usize, usize>,

    /// The server is considered busy sending the current packet until this time
    busy_until: f64,

    pub output: Output<Packet>,
}

impl VirtualClockServer {
    pub fn new(
        rate: f64,
        capacity: usize,
        capacity_unit: CapacityUnit,
        flow_classes: Arc<dyn Fn(usize) -> usize + Send + Sync>,
        drop_strategy: DropStrategy,
        vticks: HashMap<usize, usize>,
    ) -> VirtualClockServer {
        let scheduler_id = next_scheduler_id();

        let packet_drop: Box<dyn PacketDrop + Send + Sync> = match drop_strategy {
            DropStrategy::TailDrop => Box::new(TailDrop::new(capacity, capacity_unit)),
            DropStrategy::RED => {
                Box::new(RED::new(capacity, capacity_unit, 2, 6, 0.8, scheduler_id))
            }
        };

        VirtualClockServer {
            scheduler_id,
            rate,
            flow_classes,
            drop_strategy: packet_drop,
            packets_received: 0,
            packets_dropped: 0,
            byte_sizes: HashMap::new(),
            queues: BTreeMap::new(),
            vticks,
            flow_queue_count: HashMap::new(),
            aux_vc: HashMap::new(),
            v_clocks: HashMap::new(),
            busy_until: 0.0,
            output: Output::default(),
        }
    }

    pub fn id(&self) -> usize {
        self.scheduler_id
    }

    pub async fn packet_received(&mut self, mut packet: Packet, scheduler: &Scheduler<Self>) {
        let now = scheduler.time();
        let arrival_time = now.duration_since(MonotonicTime::EPOCH).as_secs_f64();

        // drops the packet if the buffer is full
        let should_drop_packet = self.drop_strategy.should_drop(
            packet.size,
            self.byte_sizes.values().sum(),
            self.queues.values().map(|q| q.len()).sum(),
        );

        // the case that this packet will be dropped.
        if should_drop_packet {
            self.packets_dropped += 1;
            debug! {
                "VirtualClockServer {} dropped packet {} from flow {} at time {:.3}",
                self.scheduler_id,
                packet.packet_id,
                packet.flow_id,
                arrival_time
            }
            return;
        }

        self.packets_received += 1;
        packet.arrival_update(arrival_time);

        let class_id = (self.flow_classes)(packet.flow_id);

        let queue = self.queues.entry(class_id).or_insert(VecDeque::new());
        queue.push_back(packet.clone());

        let byte_size = self.byte_sizes.entry(class_id).or_insert(0);
        *byte_size += packet.size;

        debug!(
            "VirtualClockServer {} received packet {} ({} bytes) from flow {} belonging to class {} at time {:.3}. \
            {} packets received, {} packet(s) in flow class {}.",
            self.scheduler_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            class_id,
            arrival_time,
            self.packets_received,
            self.queues[&class_id].len(),
            class_id
        );

        if arrival_time > self.busy_until {
            self.run((), scheduler);
        }
    }

    pub async fn send(&mut self, packet: Packet) {
        self.output.send(packet).await;
    }

    pub fn run(&mut self, _: (), scheduler: &Scheduler<Self>) {
        let now = scheduler
            .time()
            .duration_since(MonotonicTime::EPOCH)
            .as_secs_f64();



            self.busy_until = now + timeout;

            debug!(
                "VirtualClockServer {} will send packet {} ({} bytes) from flow {} at time {:.3}. \
                        {} packets in the class queue.",
                self.scheduler_id,
                outbound.packet_id,
                outbound.size,
                outbound.flow_id,
                now + timeout,
                self.queues[].len(),
            );
        }
    }
}

impl Model for VirtualClockServer {}
