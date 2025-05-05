use crate::flows::dist_source::DistPacketSource;
use crate::flows::packet::Packet;
use crate::flows::TrafficCharacteristics;
use rand::rngs::SmallRng;

/// A pre-generated sequence of packets that can be shared among multiple flows.
#[derive(Debug, Clone)]
pub struct SharedAppDataSource {
    packets: Vec<Packet>,
}

impl SharedAppDataSource {
    /// Generates a shared sequence of packets using DistPacketSource logic,
    /// replicating AppDataSource::new() behavior but returning a packet vector.
    pub fn new(flow_id: usize, traffic: TrafficCharacteristics, rng: SmallRng) -> Self {
        // Construct an internal DistPacketSource (used in AppDataSource)
        let mut source = DistPacketSource::new(flow_id, vec![], traffic.clone(), rng);
        source.flow_start_time = 0.0;

        let mut packets = Vec::new();
        let mut now = 0.0;

        // Generate packets based on traffic model until traffic is exceeded
        while !source.traffic_exceeded(now) {
            let (packet, interval) = source.produce_packet(now);
            source.packet_sent(&packet, now);
            now += interval;
            packets.push(packet);
        }

        SharedAppDataSource { packets }
    }

    /// Clones the packet sequence so each flow can use an independent copy.
    pub fn clone_packets(&self) -> Vec<Packet> {
        self.packets.clone()
    }
}
