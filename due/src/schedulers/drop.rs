/// capacity unit for the packet drop strategy.
pub enum CapacityUnit {
    Bytes,
    Packets,
}

/// the packet drop strategy.
pub enum DropStrategy {
    TailDrop,
    RED,
}

/// defines the interface for all packet drop strategies.
pub trait PacketDrop {
    fn should_drop(&mut self, packet_size: usize, byte_size: usize, queue_length: usize) -> bool;
}

// TailDrop is a packet drop strategy that drops packets when the buffer is full.
pub struct TailDrop {
    capacity: usize, // 0 for unlimited
    capacity_unit: CapacityUnit,
}

impl TailDrop {
    pub fn new(capacity: usize, capacity_unit: CapacityUnit) -> TailDrop {
        TailDrop {
            capacity,
            capacity_unit,
        }
    }
}

impl PacketDrop for TailDrop {
    fn should_drop(&mut self, packet_size: usize, byte_size: usize, queue_length: usize) -> bool {
        match self.capacity_unit {
            CapacityUnit::Bytes => self.capacity > 0 && byte_size + packet_size > self.capacity,
            CapacityUnit::Packets => self.capacity > 0 && queue_length + 1 > self.capacity,
        }
    }
}
