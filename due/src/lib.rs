use rand::rngs::SmallRng;
use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::packets::packet::Packet;
use crate::sim::{RandomVar, Time};

pub mod packets;
pub mod schedulers;
pub mod sim;
pub mod switches;
pub mod topos;

/// Globally shared data.
pub struct Shared {
    pub rng: RefCell<SmallRng>,
    pub queueing_delay: RandomVar,
    pub duration: Time,
}

/// EndPoint is a trait that defines the interface for all packet sources and sinks.
pub trait EndPoint {
    fn connect_sender(&mut self, sender: UnboundedSender<Packet>);
    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>);
}

/// Scheduler is a trait that defines the interface for all schedulers in packet
/// switches.
pub trait Scheduler {
    fn connect_sender(&mut self, sender: UnboundedSender<Packet>);
    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>);
}

/// Element is a trait that defines the interface for all elements in the network.
pub trait Element {
    fn id(&self) -> usize;
    fn get_sender(&self, element_id: usize) -> Option<UnboundedSender<Packet>>;
    fn connect_sender(&mut self, element_id: usize, sender: UnboundedSender<Packet>);
    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>);
}

pub fn get_id() -> usize {
    // the sequence of unique element_ids starts from 1
    // 0 is reserved for the endpoints
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}
