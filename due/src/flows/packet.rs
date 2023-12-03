//! A very simple struct that represents a packet.

use crate::sim_new::Time;

#[derive(Debug, Clone)]
pub struct Packet {
    /// Packets in ns.rs are typically created by packet generators, and runs
    /// through a sequence of network elements such as traffic shapers,
    /// packet-forwarding switches, or splitters. It may be entered into a queue
    /// at an output port on each of these network elements.

    /// Key fields include: creation time, size, packet id, flow_id, source,
    /// and destination. We do not model upper layer protocols, i.e., packets do
    /// not contain a payload. The size (in bytes) field is used to determine
    /// its transmission time.

    /// # Example
    /// ```
    /// use due::packets::packet::Packet;
    ///
    /// let mut packet = Packet::new(
    ///     1024, // packet size
    ///     0, // packet id
    ///     "source".to_string(),
    ///     "destination".to_string(),
    ///     0, // flow_id
    ///     0.0, // creation time
    /// );
    ///
    /// println!("{:?}", packet);
    /// ```
    /// the time when the packet is sent through a channel to the next element
    pub time: Time,
    /// the time when the packet is originally generated
    pub creation_time: Time,
    /// the size of the packet in bytes
    pub size: usize,
    /// a unique identifier
    pub packet_id: usize,
    /// identifiers for the source
    pub src: String,
    /// identifiers for the destination
    pub dst: String,
    /// the flow identifier that the packet belongs to
    pub flow_id: usize,
    /// the queueing delay experienced by the packet so far
    pub queueing_delay: Time,
}

impl Packet {
    /// creates a new packet.
    pub fn new(
        size: usize,
        packet_id: usize,
        src: String,
        dst: String,
        flow_id: usize,
        creation_time: Time,
    ) -> Packet {
        Packet {
            time: creation_time,
            size,
            packet_id,
            src,
            dst,
            flow_id,
            creation_time,
            queueing_delay: 0.0,
        }
    }

    /// updates the queueing delay of the packet.
    pub fn send(&mut self, time: f64) {
        self.queueing_delay += time - self.time;
        self.time = time;
    }
}

impl std::fmt::Display for Packet {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "id: {}, src: {}, creation time: {}, size: {}, queueing delay: {}",
            self.packet_id, self.src, self.creation_time, self.size, self.queueing_delay
        )
    }
}
