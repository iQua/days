pub mod splitter;
pub mod switch;

use serde;
use serde::Deserialize;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};

use crate::flows::packet::Packet;
use crate::flows::EndPoint;
use crate::sim_new::Simulator;
use crate::switches::splitter::Splitter;
use crate::switches::switch::PacketSwitch;
use crate::Shared;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename = "UPPERCASE")]
pub enum SchedulingDiscipline {
    DRR,
    FIFO,
}

pub enum Element {
    PacketSwitch(PacketSwitch),
    Splitter(Splitter),
}

impl Element {
    pub fn id(&self) -> usize {
        match self {
            Element::PacketSwitch(switch) => switch.id(),
            Element::Splitter(splitter) => splitter.id(),
        }
    }

    pub fn connect_sender(&mut self, endpoint_id: usize, sender: UnboundedSender<Packet>) {
        match self {
            Element::PacketSwitch(switch) => switch.connect_sender(endpoint_id, sender),
            Element::Splitter(splitter) => splitter.connect_sender(endpoint_id, sender),
        }
    }

    pub fn connect_receiver(&mut self, receiver: UnboundedReceiver<Packet>) {
        match self {
            Element::PacketSwitch(switch) => switch.connect_receiver(receiver),
            Element::Splitter(splitter) => splitter.connect_receiver(receiver),
        }
    }

    /// assists a downstream neighbour element (with host_id) to connect to an
    /// endpoint, reusing the same sender in this element.
    pub fn connect_neighbour_to_endpoint(
        &mut self,
        endpoint: &mut EndPoint,
        downlink_receiver: UnboundedReceiver<Packet>,
        neighbour_id: usize,
    ) {
        match self {
            Element::PacketSwitch(switch) => {
                let uplink_sender = switch.get_sender(neighbour_id).unwrap();
                endpoint.connect_switch(uplink_sender, downlink_receiver);
            }
            Element::Splitter(splitter) => {
                let uplink_sender = splitter.get_sender(neighbour_id).unwrap();
                endpoint.connect_switch(uplink_sender, downlink_receiver);
            }
        }
    }

    /// activates the element.
    pub fn activate(self, sim: Simulator<Shared>) {
        match self {
            Element::PacketSwitch(switch) => {
                sim.activate(switch.run(sim.clone()));
            }
            Element::Splitter(splitter) => {
                sim.activate(splitter.run());
            }
        }
    }
}
