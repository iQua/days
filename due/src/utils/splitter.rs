//! A splitter is a utility element that forwards packets to two downstream elements.

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::packets::packet::Packet;

pub struct Splitter {
    element_id: u32,
    pub sender_1: UnboundedSender<Packet>,
    pub sender_2: UnboundedSender<Packet>,
    pub receiver: UnboundedReceiver<Packet>,
}

impl Splitter {
    pub fn new(element_id: u32) -> Splitter {
        Splitter {
            element_id,
            sender_1: unbounded_channel().0,
            sender_2: unbounded_channel().0,
            receiver: unbounded_channel().1,
        }
    }

    pub async fn run(mut self) {
        while let Some(packet) = self.receiver.recv().await {
            println!(
                "Splitter {} forwarded packet {} ({} bytes).",
                self.element_id, packet.packet_id, packet.size,
            );

            let _ = self.sender_1.send(packet.clone());
            let _ = self.sender_2.send(packet.clone());
        }
    }
}
