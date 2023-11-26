pub mod drop;
pub mod drr;
pub mod port;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::packets::packet::Packet;

/// Scheduler is a trait that defines the interface for all schedulers in packet
/// switches.
pub trait Scheduler {
    fn connect_sender(&mut self, sender: UnboundedSender<Packet>);
    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>);
}
