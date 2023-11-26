use std::cell::RefCell;
use std::sync::atomic::{AtomicUsize, Ordering};

use rand::rngs::SmallRng;

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

#[derive(Debug)]
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

pub fn next_element_id() -> usize {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub fn next_endpoint_id() -> usize {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub fn next_scheduler_id() -> usize {
    static COUNTER: AtomicUsize = AtomicUsize::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}
