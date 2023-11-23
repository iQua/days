use rand::rngs::SmallRng;
use std::cell::RefCell;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

pub mod packets;
pub mod schedulers;
pub mod sim;
pub mod switches;
pub mod topos;

use crate::packets::packet::Packet;
use crate::sim::{RandomVar, Time};

/// Globally shared data.
pub struct Shared {
    pub rng: RefCell<SmallRng>,
    pub queueing_delay: RandomVar,
    pub duration: Time,
}

/// Element is a trait that defines the interface for all elements in the network.
pub trait Element {
    fn id(&mut self) -> usize;

    fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        println!("The sender: {:?}", sender);
    }

    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        println!("The receiver: {:?}", receiver)
    }
}
