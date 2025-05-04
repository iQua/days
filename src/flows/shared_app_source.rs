use crate::flows::packet::Packet;

/// A pre-generated sequence of packets that can be shared among multiple flows.
#[derive(Debug, Clone)]
pub struct SharedAppDataSource {
    packets: Vec<Packet>,
}

impl SharedAppDataSource {
    pub fn new(
        packet_size: usize,
        num_packets: usize,
        flow_id: usize,
        start_time: f64,
        interval: f64,
    ) -> Self {
        let mut packets = Vec::with_capacity(num_packets);
        for i in 0..num_packets {
            let p = Packet::new(
                packet_size,
                i * packet_size,
                flow_id,
                start_time + i as f64 * interval,
            );
            packets.push(p);
        }

        SharedAppDataSource { packets }
    }

    /// Clone packets for a new flow to use.
    pub fn clone_packets(&self) -> Vec<Packet> {
        self.packets.clone()
    }
}
