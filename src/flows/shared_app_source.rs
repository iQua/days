use crate::flows::packet::Packet;
use crate::flows::traffic::TrafficPattern;
use rand::rngs::SmallRng;

/// A pre-generated sequence of packets that can be shared among multiple flows.
#[derive(Debug, Clone)]
pub struct SharedAppDataSource {
    packets: Vec<Packet>,
}

impl SharedAppDataSource {
    /// Generates a shared sequence of packets using the same logic as AppDataSource.
    ///
    /// This enables multiple flows to reuse the same underlying packet data, e.g. for broadcast.
    pub fn new(flow_id: usize, traffic: TrafficPattern, rng: SmallRng) -> Self {
        let packets = traffic.generate_packets(flow_id, rng);
        SharedAppDataSource { packets }
    }

    /// Clones the packet sequence so each flow can use an independent copy.
    pub fn clone_packets(&self) -> Vec<Packet> {
        self.packets.clone()
    }
}
