//! A very simple struct that represents a packet.

#[derive(Debug, Copy, Clone)]
pub struct TCPAck {
    pub sequence_num: usize,
    pub acknowledged_size: usize,
}

#[derive(Debug, Clone)]
pub struct Packet {
    /// Packets in Days are typically created by packet sources, and run through
    /// a sequence of packet-forwarding switches. It may be entered into a queue
    /// at an output port on each of these switches.
    ///
    /// Key fields include: creation time, size, packet id, flow_id, source, and
    /// destination. We do not model upper layer protocols, i.e., packets do not
    /// contain a payload. The size (in bytes) field is used to determine its
    /// transmission time.
    ///
    /// # Example
    /// ```
    /// use days::flows::packet::Packet;
    ///
    /// let mut packet = Packet::new(
    ///     1024, // packet size
    ///     0, // packet id
    ///     0, // flow_id
    ///     0.0, // creation time
    /// );
    ///
    /// println!("{:?}", packet);
    /// ```
    /// the time when the packet is sent through a channel to the next element
    pub time: f64,
    /// the time when the packet is originally generated
    pub creation_time: f64,
    /// the size of the packet in bytes
    pub size: usize,
    /// a unique identifier
    pub packet_id: usize,
    /// the flow identifier that the packet belongs to
    pub flow_id: usize,
    /// the queueing delay experienced by the packet so far
    pub queueing_delay: f64,
    /// whether this is the last packet of the flow
    pub last_packet: bool,
    /// used by TCPPacketSource and TCPPacketSink
    pub ack: Option<TCPAck>,
}

impl Packet {
    /// Creates a new packet.
    pub fn new(size: usize, packet_id: usize, flow_id: usize, creation_time: f64) -> Packet {
        Packet {
            time: creation_time,
            size,
            packet_id,
            flow_id,
            creation_time,
            queueing_delay: 0.0,
            last_packet: false,
            ack: None,
        }
    }

    /// Updates the queueing delay of the packet when it departs from a scheduler.
    pub fn queueing_delay_update(&mut self, time: f64) {
        self.queueing_delay += time - self.time;
    }

    /// Records the current simulation time when a packet departs from a component.
    pub fn departure_update(&mut self, time: f64) {
        self.time = time;
    }
}

impl std::fmt::Display for Packet {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "id: {}, flow_id: {}, creation time: {}, size: {}, queueing delay: {}",
            self.packet_id, self.flow_id, self.creation_time, self.size, self.queueing_delay
        )
    }
}
