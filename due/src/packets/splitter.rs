//! A splitter is a utility element that forwards packets to two downstream elements.

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::{packets::packet::Packet, Element};

impl Element for Splitter {
    fn id(&mut self) -> usize {
        self.element_id
    }

    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        self.receiver = receiver;
    }

    fn connect_sender(&mut self, sender: UnboundedSender<Packet>) {
        self.senders.push(sender.clone());
    }
}

pub struct Splitter {
    element_id: usize,
    senders: Vec<UnboundedSender<Packet>>,
    receiver: UnboundedReceiver<Packet>,
}

impl Splitter {
    pub fn new(element_id: usize) -> Splitter {
        Splitter {
            element_id,
            senders: Vec::new(),
            receiver: unbounded_channel().1,
        }
    }

    pub async fn run(mut self) {
        while let Some(packet) = self.receiver.recv().await {
            println!(
                "Splitter {} forwarded packet {} ({} bytes).",
                self.element_id, packet.packet_id, packet.size,
            );

            for sender in self.senders.iter() {
                let _ = sender.send(packet.clone());
            }
        }

        println!("Splitter {} finished running.", self.element_id);
    }
}
