pub mod flow;
pub mod packet;
pub mod route;
pub mod sink;
pub mod source;
pub mod wire;

use std::sync::Arc;

use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::flows::packet::Packet;
use crate::flows::sink::PacketSink;
use crate::flows::source::PacketSource;
use crate::sim::Simulator;
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

    pub async fn activate(self, sim: Arc<Simulator<Shared>>) {
        match self {
            EndPoint::PacketSource(source) => sim.activate(source.run(Arc::clone(&sim))).await,
            EndPoint::PacketSink(sink) => sim.activate(sink.run(Arc::clone(&sim))).await,
        }
    }
}
