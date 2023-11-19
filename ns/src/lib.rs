use rand::rngs::SmallRng;
use sim::{RandomVar, Time};
use std::cell::RefCell;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub mod packets;
pub mod ports;
pub mod schedulers;
pub mod utils;

use crate::packets::packet::Packet;

/// Globally shared data.
pub struct Shared {
    pub rng: RefCell<SmallRng>,
    pub packet_size: RandomVar,
    pub duration: Time,
}

/// Element is a trait that defines the interface for all elements in the network.
pub trait Element {
    fn connect_sender(&mut self, sender: UnboundedSender<Packet>);
    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>);
}

/// connects a collection of upstream elements to a downstream element.
pub fn connect(upstream: &mut [impl Element], downstream: &mut impl Element) {
    let (sender, receiver) = unbounded_channel();

    for element in upstream {
        element.connect_sender(sender.clone());
    }

    (*downstream).connect_receiver(receiver);
}
