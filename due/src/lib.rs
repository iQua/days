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
    pub next_id: AtomicUsize,
}

/// Source is a trait that defines the interface for all packet sources.
pub trait Source {
    fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        println!("The sender: {:?}", sender);
    }

    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        println!("The receiver: {:?}", receiver)
    }
}

/// Sink is a trait that defines the interface for all packet sinks.
pub trait Sink {
    fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        println!("The sender: {:?}", sender);
    }

    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        println!("The receiver: {:?}", receiver)
    }
}

/// Scheduler is a trait that defines the interface for all schedulers in packet
/// switches.
pub trait Scheduler {
    fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        println!("The sender: {:?}", sender);
    }

    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        println!("The receiver: {:?}", receiver)
    }
}

/// Element is a trait that defines the interface for all elements in the network.
pub trait Element {
    fn id(&self) -> usize;

    fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        println!("The sender: {:?}", sender);
    }

    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        println!("The receiver: {:?}", receiver)
    }
}

pub fn get_id() -> usize {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub fn get_flow_id() -> usize {
    static FLOW_COUNTER: AtomicUsize = AtomicUsize::new(0);
    FLOW_COUNTER.fetch_add(1, Ordering::Relaxed)
}
