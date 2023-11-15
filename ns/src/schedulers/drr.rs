//! Implements a Deficit Round Robin (DRR) server.

use crate::packets::packet::Packet;
use crate::Shared;
use sim::{channel, select, until, Control, Receiver, Sender, SimContext, Time};
use std::{collections::{HashMap, VecDeque}, cell::RefCell};
use futures::future::join;

pub struct DRRServer {
    element_id: u32,
    /// the bit rate of the port
    rate: f64,

    /// a closure that maps a flow_id to a class_id, used to implement
    /// class-based Deficit Round Robin. The default uses a packet's flow_id as
    /// its class_id, which is equivalent to flow-based DRR.
    pub flow_classes: Box<dyn Fn(u32) -> u32>,

    /// class_id -> deficit
    deficit: RefCell<HashMap<u32, u32>>,
    /// class_id -> the number of packets in its queue
    flow_queue_count: RefCell<HashMap<u32, u32>>,
    /// class_id -> quantum
    quantum: RefCell<HashMap<u32, u32>>,
    /// class_id -> the head-of-line packet in its queue
    head_of_line: RefCell<HashMap<u32, Packet>>,

    /// the number of packets received
    packets_received: RefCell<u32>,
    // the total number of packets in the server
    total_packets: RefCell<Control<u32>>,
    /// the current packet being sent to the downstream element with its sending
    /// time, if any
    current_packet: RefCell<Option<(Packet, Time)>>,
    /// class_id -> the number of bytes in its queue
    byte_sizes: RefCell<HashMap<u32, u32>>,

    /// class_id -> its FIFO queue
    queues: RefCell<HashMap<u32, VecDeque<(Packet, Time)>>>,

    /// a sender for sending packets to downstream elements
    pub sender: Sender<Packet>,
    /// a receiver for receiving incoming packets from upstream elements
    pub receiver: Receiver<Packet>,
}

impl DRRServer {
    pub fn new(element_id: u32, rate: f64, weights: HashMap<u32, u32>) -> DRRServer {
        let min_quantum = 1500;
        let deficit = RefCell::new(HashMap::new());
        let quantum = RefCell::new(HashMap::new());
        let flow_queue_count = RefCell::new(HashMap::new());
        let byte_sizes = RefCell::new(HashMap::new());

        let min_weight = weights.values().min().unwrap();
        for (class_id, weight) in &weights {
            deficit.borrow_mut().insert(*class_id, 0);
            quantum.borrow_mut().insert(*class_id, min_quantum * weight / min_weight);
            flow_queue_count.borrow_mut().insert(*class_id, 0);
            byte_sizes.borrow_mut().insert(*class_id, 0);
        }

        println!("weights: {:?};\nquantum: {:?}", weights, quantum.borrow());
        // TODO: add the usage of flow_classes!
        DRRServer {
            element_id,
            rate,
            flow_classes: Box::new(|flow_id| flow_id),
            deficit,
            flow_queue_count,
            quantum,
            head_of_line: RefCell::new(HashMap::new()),
            packets_received: RefCell::new(0),
            total_packets: RefCell::new(Control::default()),
            current_packet: RefCell::new(None),
            byte_sizes,
            queues: RefCell::new(HashMap::new()),
            sender: channel().0,
            receiver: channel().1,
        }
        // Q: do we need packet_available?
    }

    fn packet_received(&self, packet: Packet, sim: SimContext<'_, Shared>) {
        self.queues
            .borrow_mut()
            .entry(packet.flow_id)
            .or_insert_with(VecDeque::new)
            .push_back((packet.clone(), sim.now()));

        *self.packets_received.borrow_mut() += 1;
        self.total_packets.borrow_mut().set(self.total_packets.borrow().get() + 1);

        self.byte_sizes
            .borrow_mut()
            .entry(packet.flow_id)
            .and_modify(|byte_size| *byte_size += packet.size);
        self.flow_queue_count
            .borrow_mut()
            .entry(packet.flow_id)
            .and_modify(|flow_id| *flow_id += 1);

        println!(
            "DRRServer {} received packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets received, {} packets in the flow queue, {} packets in all flow queues",
            self.element_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            sim.now(),
            self.packets_received.borrow(),
            self.flow_queue_count.borrow().get(&packet.flow_id).unwrap(),
            self.total_packets.borrow().get()
        );
    }

    fn packet_sent(&self, packet: Packet, sim: SimContext<'_, Shared>) {
        self.total_packets.borrow_mut().set(self.total_packets.borrow().get() - 1);

        self.byte_sizes
            .borrow_mut()
            .entry(packet.flow_id)
            .and_modify(|byte_size| {
                *byte_size -= packet.size;
            });
        self.flow_queue_count
            .borrow_mut()
            .entry(packet.flow_id)
            .and_modify(|packet_count| *packet_count -= 1);

        self.deficit
            .borrow_mut()
            .entry(packet.flow_id)
            .and_modify(|deficit| *deficit -= packet.size);

        if *self.flow_queue_count.borrow().get(&packet.flow_id).unwrap() == 0 {
            self.deficit
                .borrow_mut()
                .entry(packet.flow_id)
                .and_modify(|deficit| *deficit = 0);
        }

        println!(
            "DRRServer {} sent packet {} ({} bytes) from flow {} at time {:.3}. \
            {} packets in the flow queue, {} packets in all flow queues.",
            self.element_id,
            packet.packet_id,
            packet.size,
            packet.flow_id,
            sim.now(),
            self.flow_queue_count.borrow().get(&packet.flow_id).unwrap(),
            self.total_packets.borrow().get()
        );
    }

    pub async fn fetch_packet(&self, sim: SimContext<'_, Shared>) {
        println!("Inside fetch_packet function.");
        loop {
            // wait until there exist packets
            // until(&（self.total_packets.borrow()）, |counts| counts.get() >
            // 0).await;

            until(&(*self.total_packets.borrow()), |counts| counts.get() > 0).await;

            for (flow_id, &count) in self.flow_queue_count.borrow().iter() {
                if count > 0 {
                    self.deficit.borrow_mut().entry(*flow_id).and_modify(|deficit| {
                        *deficit += self.quantum.borrow_mut().get(flow_id).unwrap();
                    });
                }

                while *self.deficit.borrow().get(flow_id).unwrap() > 0
                    && *self.flow_queue_count.borrow().get(flow_id).unwrap() > 0
                {
                    let packet; // Do we need to store the arrival time here?
                    if let Some(head_packet) = self.head_of_line.borrow_mut().remove(flow_id) {
                        packet = head_packet;
                    } else {
                        packet = self.queues.borrow_mut().get_mut(flow_id).unwrap().pop_front().unwrap().0;
                    }

                    if packet.size < *self.deficit.borrow().get(flow_id).unwrap() {
                        let wait_time = (packet.size as f64) * 8.0 / self.rate;
                        *self.current_packet.borrow_mut() = Some((packet, wait_time));
                        sim.advance(wait_time).await;

                        // updates stats will be done in run funtion
                    } else {
                        self.head_of_line.borrow_mut().insert(*flow_id, packet);
                    }
                }
            }
        }
    }

    pub async fn schedule_packets(&self, sim: SimContext<'_, Shared>) {
        println!("Inside schedule_packet function.");
        loop {
            let receive_action = self.receiver.recv();
            let send_action = async {
                if let Some((_, wait_time)) = self.current_packet.borrow().clone() {
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
                    if let Some((packet, _)) = self.current_packet.borrow_mut().take() {
                        self.sender
                            .send(packet.clone())
                            .await
                            .expect("no receiving element in the simulation");
                        // update stats here
                        self.packet_sent(packet, sim);
                    }
                }
            }
        }
    }

    pub async fn run(self, sim: SimContext<'_, Shared>) {
        println!("Inside run function.");
        let fetch_task = self.fetch_packet(sim);
        let schedule_task = self.schedule_packets(sim);
        let _ = join(fetch_task, schedule_task).await;
    }
}
