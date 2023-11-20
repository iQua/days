pub enum CapacityUnit {
    Bytes,
    Packets,
}

pub enum DropStrategy {
    TailDrop,
    RED,
}
pub trait PacketDrop {
    fn should_drop(&mut self, byte_size: usize, queue_length: usize) -> bool;
}

pub struct TailDrop {
    capacity: usize,
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
    fn should_drop(&mut self, byte_size: usize, queue_length: usize) -> bool {
        match self.capacity_unit {
            CapacityUnit::Bytes => byte_size > self.capacity,
            CapacityUnit::Packets => queue_length > self.capacity,
        }
    }
}
