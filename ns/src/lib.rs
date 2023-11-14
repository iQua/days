use rand::rngs::SmallRng;
use sim::{RandomVar, Time};
use std::cell::RefCell;

pub mod packets;

/// Globally shared data.
pub struct Shared {
    pub rng: RefCell<SmallRng>,
    pub packet_size: RandomVar,
    pub duration: Time,
}
