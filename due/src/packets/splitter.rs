//! A splitter is a utility element that forwards packets to two downstream elements.

use std::collections::HashMap;

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::{get_id, packets::packet::Packet, Element};

impl Element for Splitter {
    fn id(&self) -> usize {
        self.element_id
    }

    fn get_sender(&self, element_id: usize) -> Option<UnboundedSender<Packet>> {
        if let Some(sender) = self.senders.get(&element_id) {
            return Some(sender.clone());
        }

        None
    }

    fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        self.receiver = receiver;
    }

    fn connect_sender(&mut self, element_id: usize, sender: UnboundedSender<Packet>) {
        self.senders.insert(element_id, sender.clone());
    }
}

impl Default for Splitter {
    fn default() -> Self {
        Splitter {
            element_id: get_id(),
            senders: HashMap::new(),
            receiver: unbounded_channel().1,
        }
    }
}

pub struct Splitter {
    element_id: usize,
    senders: HashMap<usize, UnboundedSender<Packet>>,
    receiver: UnboundedReceiver<Packet>,
}

impl Splitter {
    pub fn new() -> Splitter {
        Default::default()
    }

    pub async fn run(mut self) {
        while let Some(packet) = self.receiver.recv().await {
            println!(
                "Splitter {} forwarded packet {} ({} bytes).",
                self.element_id, packet.packet_id, packet.size,
            );

            for (_, sender) in &self.senders {
                let _ = sender.send(packet.clone());
            }
        }

        println!("Splitter {} finished running.", self.element_id);
    }
}
