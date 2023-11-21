use rand::rngs::SmallRng;
use std::cell::RefCell;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub mod packets;
pub mod schedulers;
pub mod sim;
pub mod switches;

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
    fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        println!("The sender: {:?}", sender);
    }
    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        println!("The receiver: {:?}", receiver)
    }
    fn connect_senders(&mut self, senders: Vec<UnboundedSender<Packet>>) {
        println!("Length of the senders: {}", senders.len());
    }
}

/// connects a collection of upstream elements to a downstream element.
pub fn connect(upstream: &mut [impl Element], downstream: &mut impl Element) {
    let (sender, receiver) = unbounded_channel();

    for element in upstream {
        element.connect_sender(sender.clone());
    }

    (*downstream).connect_receiver(receiver);
}

pub fn connect_pair(upstream: &mut impl Element, downstream: &mut impl Element) {
    let (sender, receiver) = unbounded_channel();

    upstream.connect_sender(sender);
    downstream.connect_receiver(receiver);
}

/// connects an upstream element to a collection of downstream elemets.
pub fn connect_to_many(
    upstream: &mut impl Element,
    downstream: &mut [impl Element],
    senders: Vec<UnboundedSender<Packet>>,
    receivers: Vec<UnboundedReceiver<Packet>>,
) {
    assert_eq!(
        downstream.len(),
        receivers.len(),
        "The number of receivers is not equal to the number of downstream elements."
    );

    // connects senders and receivers
    upstream.connect_senders(senders);
    for (element, receiver) in downstream.iter_mut().zip(receivers.into_iter()) {
        element.connect_receiver(receiver);
    }
}
