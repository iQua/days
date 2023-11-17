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
        loop {
            if let Some(packet) = self.receiver.recv().await {
                let _ = self.sender_1.send(packet.clone()).unwrap();
                let _ = self.sender_2.send(packet.clone()).unwrap();
            } else {
                break;
            }
        }
    }
}
