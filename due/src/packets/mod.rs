pub mod packet;
pub mod sink;
pub mod source;
pub mod wire;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::packets::packet::Packet;
use crate::packets::sink::PacketSink;
use crate::packets::source::PacketSource;
use crate::sim::SimContext;
use crate::Shared;

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

    pub fn connect_switch(
        &mut self,
        sender: UnboundedSender<Packet>,
        receiver: UnboundedReceiver<Packet>,
    ) {
        match self {
            EndPoint::PacketSource(source) => source.connect_switch(sender, receiver),
            EndPoint::PacketSink(sink) => sink.connect_switch(sender, receiver),
        }
    }

    pub fn activate(self, sim: SimContext<'_, Shared>) {
        match self {
            EndPoint::PacketSource(source) => sim.activate(source.run(sim)),
            EndPoint::PacketSink(sink) => sim.activate(sink.run(sim)),
        }
    }
}
