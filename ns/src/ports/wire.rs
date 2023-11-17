//! A simple wire component.

use rand_distr::Distribution;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

pub struct Wire<A>
where
    A: Distribution<Time>,
{
    element_id: u32,
    /// the packet delay distribution
    delay_dist: Box<dyn Fn() -> A>,
    /// the packet queue of the wire
    queue: VecDeque<(Packet, Time)>,
    /// a sender for sending packets
    pub sender: UnboundedSender<Packet>,
    /// a receiver for receiving incoming packets
    pub receiver: UnboundedReceiver <Packet>,
}

impl<A> Wire<A>
where
    A: Distribution<Time>,
{
    pub fn new(element_id: u32, delay_dist: Box<dyn Fn() -> A>) -> Wire<A> {
        Wire {
            element_id,
            delay_dist,
            queue: VecDeque::new(),
            sender: unbounded_channel().0,
            receiver: unbounded_channel().1,
        }
    }

    fn packet_received(&mut self, packet: Packet, sim: SimContext<'_, Shared>) {
        self.queue.push_back((packet.clone(), sim.now()));

        println!(
            "Wire {} received packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets in queue.",
            self.element_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            sim.now(),
        );
    }

    fn packet_sent(&mut self, packet: Packet, sim: SimContext<'_, Shared>) {
        println!(
            "Wire {} sent packet {} ({} bytes) from flow {} at time {:.3}.",
            self.element_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            sim.now(),
        );
    }

    pub async fn run(mut self, sim: SimContext<'_, Shared>) {
        loop {
            let receive_action = self.receiver.recv();
            let send_action = async {
                if let Some((packet, arrival_time)) = self.queue.front() {
                    let delay = (self.delay_dist)().sample(&mut *sim.shared().rng.borrow_mut());
                    let wait_time = arrival_time + delay - sim.now();
                    sim.advance(wait_time).await;
                } else {
                    sim.advance(1.0).await;
                }
                None
            };
            match select(sim, receive_action, send_action).await {
                Some(packet) => {
                    self.packet_received(packet, sim);
                }
                None => {
                    if let Some((packet, _)) = self.queue.pop_front() {
                        self.sender
                            .send(packet.clone())
                            .unwrap();
                        self.packet_sent(packet, sim);
                    }
                }
            }
        }
    }
}
