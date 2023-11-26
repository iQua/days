//! Implements a Weighted Fair Queueing (WFQ) scheduler.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;

use rand::distributions::weighted;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::packets::packet::Packet;
use crate::schedulers::drop::{CapacityUnit, DropStrategy, PacketDrop, TailDrop};
use crate::sim::SimContext;
use crate::{get_id, Element, Shared};

pub struct TaggedPacket {
    pub packet: Packet,
    /// tag is the finish time of the packet
    pub tag: f64,
}

impl Ord for TaggedPacket {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.tag > other.tag {
            Ordering::Greater;
        } else if self.tag < other.tag {
            Ordering::Less;
        }
        Ordering::Equal
    }
}

impl PartialOrd for TaggedPacket {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TaggedPacket {
    fn eq(&self, other: &Self) -> bool {
        self.tag == other.tag
    }
}

impl Eq for TaggedPacket {}

pub struct WFQServer {
    element_id: usize,
    /// the bit rate of the server
    rate: f64,

    /// a closure that maps a flow_id to a class_id, used to implement
    /// class-based WFQ. The default uses a packet's flow_id as its class_id,
    /// which is equivalent to flow-based WFQ.
    pub flow_classes: Arc<dyn Fn(usize) -> usize>,

    /// a closure that determines whether an inbound packet should be dropped or not
    drop_strategy: Box<dyn PacketDrop>,

    /// finish time of the last packet served in each class
    finish_times: Vec<f64>,

    vtime: f64,
    last_update: f64,

    /// the number of packets received, dropped, and in the queues waiting to be sent
    packets_received: usize,
    packets_dropped: usize,
    packets_waiting: usize,

    /// the number of bytes of classes, which are consecutive and start from 0
    byte_sizes: Vec<usize>,

    /// priority queues of classes, where packets are sorted according to their finish times
    queues: Vec<BinaryHeap<TaggedPacket>>,

    /// a sender for sending outbound packets to the downstream element
    pub sender: UnboundedSender<Packet>,
    /// a receiver for receiving inbound packets from upstream elements
    pub receiver: UnboundedReceiver<Packet>,
}

impl Element for WFQServer {
    fn id(&mut self) -> usize {
        self.element_id
    }

    fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        self.sender = sender;
    }

    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        self.receiver = receiver;
    }
}
