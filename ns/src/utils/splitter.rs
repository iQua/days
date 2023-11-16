//! TODO
use crate::packets::packet::Packet;
use sim::{channel, Sender, Receiver};

pub struct Splitter {
    pub sender_1: Sender<Packet>,
    pub sender_2: Sender<Packet>,
    pub receiver: Receiver<Packet>,
}

impl Splitter {
    pub fn new() -> Splitter {
        Splitter {
            sender_1: channel().0,
            sender_2: channel().0,
            receiver: channel().1,
        }
    }

    pub async fn run(self) {
        loop {
            let packet = self.receiver.recv().await.unwrap();
            self.sender_1.send(packet.clone()).await.expect("no receiver 1");
            self.sender_2.send(packet).await.expect("no receiver 2");
        }
    }
}