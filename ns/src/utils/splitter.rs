//! TODO
use crate::packets::packet::Packet;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub struct Splitter {
    pub sender_1: UnboundedSender<Packet>,
    pub sender_2: UnboundedSender<Packet>,
    pub receiver: UnboundedReceiver<Packet>,
}

impl Splitter {
    pub fn new() -> Splitter {
        Splitter {
            sender_1: unbounded_channel().0,
            sender_2: unbounded_channel().0,
            receiver: unbounded_channel().1,
        }
    }

    pub async fn run(mut self) {
        loop {
            let packet = self.receiver.recv().await.unwrap();
            self.sender_1.send(packet.clone()).unwrap();
            self.sender_2.send(packet).unwrap();
        }
    }
}