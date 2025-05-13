use crate::flows::packet::Packet;

#[derive(Debug, Clone)]
pub struct BufferedAppDataSource {
    total_size: usize,
    packets: Vec<Packet>,
}

impl BufferedAppDataSource {
    /// Create a buffered app source from a given packet vector.
    pub fn new(packets: Vec<Packet>) -> Self {
        let total_size = packets.iter().map(|p| p.size).sum();
        BufferedAppDataSource {
            total_size,
            packets,
        }
    }

    /// Clone all packets for a flow (to ensure independent ownership).
    pub fn clone_packets(&self) -> Vec<Packet> {
        self.packets.clone()
    }

    /// Return the total size of all packets (in bytes).
    pub fn total_size(&self) -> usize {
        self.total_size
    }
}
