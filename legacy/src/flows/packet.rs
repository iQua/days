//! A very simple struct that represents a packet.

#[derive(Debug, Copy, Clone, serde::Serialize, serde::Deserialize)]
pub struct TCPAck {
    pub sequence_num: usize,
    pub acknowledged_size: usize,
    pub ece: bool,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ControlPacket {
    DcqcnCnp,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EcnField {
    NotEct,
    Ect0,
    Ect1,
    Ce,
}

impl EcnField {
    pub fn is_ect(self) -> bool {
        matches!(self, EcnField::Ect0 | EcnField::Ect1)
    }
}

#[cfg(feature = "lean")]
impl From<EcnField> for days::utils::logger::AqmLoggedEcnField {
    fn from(field: EcnField) -> Self {
        match field {
            EcnField::NotEct => Self::NotEct,
            EcnField::Ect0 => Self::Ect0,
            EcnField::Ect1 => Self::Ect1,
            EcnField::Ce => Self::Ce,
        }
    }
}

#[cfg(all(feature = "lean", feature = "dcqcn"))]
impl From<EcnField> for days::utils::logger::DcqcnLoggedEcnField {
    fn from(field: EcnField) -> Self {
        match field {
            EcnField::NotEct => Self::NotEct,
            EcnField::Ect0 => Self::Ect0,
            EcnField::Ect1 => Self::Ect1,
            EcnField::Ce => Self::Ce,
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
    /// use days_legacy::flows::packet::Packet;
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
    /// 802.1Q priority code point (0-7). Default is 0 (best-effort).
    pub priority: u8,
    /// whether this is the last packet of the flow
    pub last_packet: bool,
    /// used by TCPPacketSource and TCPPacketSink
    pub ack: Option<TCPAck>,
    /// Control plane marker for non-data packets (e.g., DCQCN CNP).
    pub control: Option<ControlPacket>,
    /// Whether this packet was ECN-marked by the network.
    pub ecn: EcnField,
    /// Whether this packet has the TCP CWR (Congestion Window Reduced) flag set.
    pub cwr: bool,
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
            priority: 0,
            ack: None,
            control: None,
            ecn: EcnField::NotEct,
            cwr: false,
        }
    }

    /// Sets the packet priority (0-7).
    pub fn set_priority(&mut self, priority: u8) {
        assert!(
            priority <= 7,
            "priority must be within 0..=7, got {}",
            priority
        );
        self.priority = priority;
    }

    /// Updates the queueing delay of the packet when it departs from a scheduler.
    pub fn queueing_delay_update(&mut self, time: f64) {
        self.queueing_delay += time - self.time;
    }

    /// Records the current simulation time when a packet departs from a component.
    pub fn departure_update(&mut self, time: f64) {
        self.time = time;
    }

    /// Marks this packet as CE if it is ECN-capable. Returns true if the packet can be forwarded.
    pub fn mark_ce(&mut self) -> bool {
        if self.ecn == EcnField::NotEct {
            return false;
        }
        if self.ecn.is_ect() {
            self.ecn = EcnField::Ce;
        }
        true
    }
}

impl std::fmt::Display for Packet {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "id: {}, flow_id: {}, creation time: {}, size: {}, queueing delay: {}, priority: {}, ecn: {:?}, cwr: {}",
            self.packet_id,
            self.flow_id,
            self.creation_time,
            self.size,
            self.queueing_delay,
            self.priority,
            self.ecn,
            self.cwr
        )
    }
}
