//! A very simple struct that represents a packet.

#[derive(Debug, Clone)]
pub struct Packet {
    /// Packets in ns.rs are typically created by packet generators, and runs
    /// through a sequence of network elements such as traffic shapers,
    /// packet-forwarding switches or splitters. It may be entered into a queue
    /// at an output port on each of these network elements.

    /// Key fields include: generation time, size, packet id, flow_id, source,
    /// and destination. We do not model upper layer protocols, i.e., packets do
    /// not contain a payload. The size (in bytes) field is used to determine
    /// its transmission time.

    /// # Example
    /// ```
    /// let packet = Packet {
    ///     time: 0.0,
    ///     size: 2,
    ///     packet_id: 0,
    ///     src: "source".to_string(),
    ///     dst: "destination".to_string(),
    ///     flow_id: 0,
    /// };

    /// println!("{:?}", packet);
    /// ```
    /// the time when the packet is sent through a channel to the next element
    pub time: f64,
    /// the time when the packet is originally generated
    pub production_time: f64,
    /// the size of the packet in bytes
    pub size: u32,
    /// a unique identifier
    pub packet_id: u32,
    /// identifiers for the source
    pub src: String,
    /// identifiers for the destination
    pub dst: String,
    /// the flow identifier that the packet belongs to
    pub flow_id: u32,
}

/// usage: println!("{packet}");
impl std::fmt::Display for Packet {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "id: {}, src: {}, time: {}, size: {}",
            self.packet_id, self.src, self.time, self.size
        )
    }
}
