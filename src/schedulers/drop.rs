//! Implements packet drop strategies for the scheduler.

use rand::SeedableRng;
use rand::distr::Distribution;
use rand::distr::Uniform;
use rand::rngs::SmallRng;
use serde::Deserialize;

use crate::get_seed;

/// Capacity unit for the packet drop strategy.
#[derive(Clone, Copy, Debug)]
pub enum CapacityUnit {
    Bytes,
    Packets,
}

/// The packet drop strategy.
#[derive(Clone, Debug, Deserialize)]
pub enum DropStrategy {
    TailDrop,
    RED,
    #[serde(rename = "RED_ECN")]
    RedEcn,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DropAction {
    Enqueue,
    Drop,
    MarkEcn,
}

/// Defines the interface for all packet drop strategies.
pub trait PacketDrop {
    fn action(&mut self, packet_size: usize, byte_size: usize, queue_length: usize) -> DropAction;
}

/// TailDrop is a packet drop strategy that drops packets when the buffer is full.
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
    fn action(&mut self, packet_size: usize, byte_size: usize, queue_length: usize) -> DropAction {
        let overflow = match self.capacity_unit {
            CapacityUnit::Bytes => self.capacity > 0 && byte_size + packet_size > self.capacity,
            CapacityUnit::Packets => self.capacity > 0 && queue_length + 1 > self.capacity,
        };

        if overflow {
            DropAction::Drop
        } else {
            DropAction::Enqueue
        }
    }
}

/// Random Early Detection, as defined in RFC 2309.
pub struct RED {
    capacity: usize, // 0 for unlimited
    capacity_unit: CapacityUnit,
    min_threshold: f64,
    max_threshold: f64,
    max_probability: f64,
    weight_factor: u32,
    avg_queue_length: usize,
    rng: SmallRng,
    ecn: bool,
}

impl RED {
    pub fn new(
        capacity: usize,
        capacity_unit: CapacityUnit,
        min_threshold: f64,
        max_threshold: f64,
        max_probability: f64,
        seed: usize,
        ecn: bool,
    ) -> RED {
        let global_seed = get_seed();
        let rng = match global_seed {
            1.. => SmallRng::seed_from_u64((global_seed + seed) as u64),
            _ => SmallRng::from_os_rng(),
        };

        RED {
            capacity,
            capacity_unit,
            min_threshold,
            max_threshold,
            max_probability,
            weight_factor: 9,
            avg_queue_length: 0,
            rng,
            ecn,
        }
    }
}

impl PacketDrop for RED {
    fn action(&mut self, packet_size: usize, byte_size: usize, queue_length: usize) -> DropAction {
        if self.capacity == 0 {
            return DropAction::Enqueue; // unlimited
        }

        let alpha = 1 / usize::pow(2, self.weight_factor);
        self.avg_queue_length = self.avg_queue_length * (1 - alpha) + queue_length * alpha;

        // drops the packet if the capacity of the queue is exceeded
        let queue_overflow = match self.capacity_unit {
            CapacityUnit::Bytes => self.capacity > 0 && byte_size + packet_size > self.capacity,
            CapacityUnit::Packets => self.capacity > 0 && queue_length + 1 > self.capacity,
        };

        // drops the packet if the average queue length exceeds the max_threshold
        let threshold_overflow = match self.capacity_unit {
            CapacityUnit::Bytes => {
                if byte_size + packet_size
                    > (self.max_threshold * self.capacity as f64).floor() as usize
                {
                    let drop_probability = Uniform::new(0.0, 1.0).unwrap().sample(&mut self.rng);

                    drop_probability <= self.max_probability
                } else {
                    false
                }
            }
            CapacityUnit::Packets => {
                if queue_length + 1 > (self.max_threshold * self.capacity as f64).floor() as usize {
                    let drop_probability = Uniform::new(0.0, 1.0).unwrap().sample(&mut self.rng);

                    drop_probability <= self.max_probability
                } else {
                    false
                }
            }
        };

        let threshold_normal = match self.capacity_unit {
            CapacityUnit::Bytes => {
                if byte_size + packet_size
                    > (self.min_threshold * self.capacity as f64).floor() as usize
                {
                    let probability = f64::max(
                        0.0,
                        self.avg_queue_length as f64 - self.min_threshold * self.capacity as f64,
                    ) / (self.max_threshold - self.min_threshold)
                        * self.capacity as f64
                        * self.max_probability;
                    let drop_probability = Uniform::new(0.0, 1.0).unwrap().sample(&mut self.rng);

                    drop_probability <= probability
                } else {
                    false
                }
            }
            CapacityUnit::Packets => {
                if queue_length + 1 > (self.min_threshold * self.capacity as f64).floor() as usize {
                    let probability = f64::max(
                        0.0,
                        self.avg_queue_length as f64 - self.min_threshold * self.capacity as f64,
                    ) / (self.max_threshold - self.min_threshold)
                        * self.capacity as f64
                        * self.max_probability;
                    let drop_probability = Uniform::new(0.0, 1.0).unwrap().sample(&mut self.rng);

                    drop_probability <= probability
                } else {
                    false
                }
            }
        };

        if queue_overflow {
            DropAction::Drop
        } else if threshold_overflow || threshold_normal {
            if self.ecn {
                DropAction::MarkEcn
            } else {
                DropAction::Drop
            }
        } else {
            DropAction::Enqueue
        }
    }
}
