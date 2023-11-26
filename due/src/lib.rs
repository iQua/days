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

impl EndPoint {
    pub fn id(&self) -> usize {
        match self {
            EndPoint::PacketSource(source) => source.id(),
            EndPoint::PacketSink(sink) => sink.id(),
        }
    }
}

/// Globally shared data.
pub struct Shared {
    pub rng: RefCell<SmallRng>,
    pub queueing_delay: RandomVar,
    pub duration: Time,
}

static NUM_ELEMENTS: AtomicUsize = AtomicUsize::new(0);
static ELEMENT_ID: AtomicUsize = AtomicUsize::new(0);
static ENDPOINT_ID: AtomicUsize = AtomicUsize::new(0);
static SCHEDULER_ID: AtomicUsize = AtomicUsize::new(0);

pub fn num_elements() -> usize {
    NUM_ELEMENTS.load(Ordering::Relaxed)
}

pub fn set_num_elements(num_elements: usize) {
    NUM_ELEMENTS.store(num_elements, Ordering::Relaxed);
    ENDPOINT_ID.store(num_elements, Ordering::Relaxed);
}

pub fn next_element_id() -> usize {
    ELEMENT_ID.fetch_add(1, Ordering::Relaxed)
}

pub fn next_endpoint_id() -> usize {
    ENDPOINT_ID.fetch_add(1, Ordering::Relaxed)
}

pub fn next_scheduler_id() -> usize {
    SCHEDULER_ID.fetch_add(1, Ordering::Relaxed)
}
