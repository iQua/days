use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};

use rand::rngs::SmallRng;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::packets::packet::Packet;
use crate::packets::sink::PacketSink;
use crate::packets::source::PacketSource;
use crate::packets::splitter::Splitter;
use crate::sim::{RandomVar, Time};
use crate::switches::switch::PacketSwitch;

pub mod packets;
pub mod schedulers;
pub mod sim;
pub mod switches;
pub mod topos;

pub enum Element {
    PacketSwitch(PacketSwitch),
    Splitter(Splitter),
}

pub enum EndPoint {
    PacketSource(PacketSource),
    PacketSink(PacketSink),
}

/// Globally shared data.
pub struct Shared {
    pub rng: RefCell<SmallRng>,
    pub queueing_delay: RandomVar,
    pub duration: Time,
}

/// Scheduler is a trait that defines the interface for all schedulers in packet
/// switches.
pub trait Scheduler {
    fn connect_sender(&mut self, sender: UnboundedSender<Packet>);
    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>);
}

pub fn get_id() -> usize {
    // the sequence of unique element_ids starts from 1
    // 0 is reserved for the endpoints
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}
