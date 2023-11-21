use rand::rngs::SmallRng;
use statrs::statistics::Distribution;
use std::cell::RefCell;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub mod packets;
pub mod schedulers;
pub mod sim;
pub mod switches;

use crate::packets::dist_generator::DistPacketGenerator;
use crate::packets::packet::Packet;
use crate::packets::sink::PacketSink;
use crate::packets::splitter::Splitter;
use crate::packets::wire::Wire;
use crate::schedulers::drr::DRRServer;
use crate::schedulers::port::Port;
use crate::sim::{RandomVar, Time};
use crate::switches::fair::FairPacketSwitch;
use crate::switches::simple::SimplePacketSwitch;

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
}

pub enum ElementType<'a, A, B>
where
    A: Distribution<Time>,
    B: Distribution<f64>,
{
    DistPacketGenerator(&'a mut DistPacketGenerator<A, B>),
    PacketSink(&'a mut PacketSink),
    Port(&'a mut Port),
    Wire(&'a mut Wire<A>),
    DRRServer(&'a mut DRRServer),
    Splitter(&'a mut Splitter),
    SimplePacketSwitch(&'a mut SimplePacketSwitch),
    FairPacketSwitch(&'a mut FairPacketSwitch),
}

impl<'a, A, B> Element for ElementType<'a, A, B>
where
    A: Distribution<Time>,
    B: Distribution<f64>,
{
}

/// connects a collection of upstream elements to a downstream element.
pub fn connect_n_1(upstream: &mut [impl Element], downstream: &mut impl Element) {
    let (sender, receiver) = unbounded_channel();

    for element in upstream {
        element.connect_sender(sender.clone());
    }

    (*downstream).connect_receiver(receiver);
}

/// connects an upstream element to a downstream element.
pub fn connect_pair(upstream: &mut impl Element, downstream: &mut impl Element) {
    let (sender, receiver) = unbounded_channel();

    upstream.connect_sender(sender);
    downstream.connect_receiver(receiver);
}

/// connects an upstream element to a collection of downstream elements.
pub fn connect_1_n<A, B>(upstream: &mut impl Element, downstream: &mut [ElementType<A, B>])
where
    A: Distribution<Time>,
    B: Distribution<f64>,
{
    for element in downstream {
        let (sender, receiver) = unbounded_channel();
        upstream.connect_sender(sender);
        element.connect_receiver(receiver);
    }
}
