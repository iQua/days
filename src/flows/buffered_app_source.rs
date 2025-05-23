use crate::flows::packet::Packet;

/// A unified trait for any application-level data source used by TCP or other flows.
pub trait AppSource {
    /// Produce packets based on the application's sending logic at time `now`.
    fn produce_data(&mut self, now: f64) -> Vec<Packet>;

    /// Return the total data size the app will generate (in bytes).
    fn total_size(&self) -> usize;

    /// Optionally set the flow's start time (no-op for buffered source).
    fn set_flow_start_time(&mut self, _t: f64) {}
}

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

impl AppSource for BufferedAppDataSource {
    fn produce_data(&mut self, _now: f64) -> Vec<Packet> {
        self.clone_packets()
    }

    fn total_size(&self) -> usize {
        self.total_size()
    }

    fn set_flow_start_time(&mut self, _t: f64) {
        // Do nothing for buffered source
    }
}
