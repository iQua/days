//! Implements packet drop strategies for the scheduler.

use rand::SeedableRng;
use rand::distr::Distribution;
use rand::distr::Uniform;
use rand::rngs::SmallRng;
use serde::{Deserialize, Serialize};

use crate::get_seed;

/// Capacity unit for the packet drop strategy.
#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
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
    #[serde(rename = "ECN_THRESHOLD")]
    EcnThreshold,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DropAction {
    Enqueue,
    Drop,
    MarkEcn,
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DropStrategyKind {
    TailDrop,
    Red,
    RedEcn,
    EcnThreshold,
}

#[derive(Clone, Debug)]
pub struct DropWitness {
    pub strategy: DropStrategyKind,
    pub capacity: usize,
    pub capacity_unit: CapacityUnit,
    pub queue_length: usize,
    pub byte_length: usize,
    pub ecn_threshold_ppb: Option<u64>,
    pub red_min_threshold_ppb: Option<u64>,
    pub red_max_threshold_ppb: Option<u64>,
    pub red_max_probability_ppb: Option<u64>,
    pub red_avg_queue_length: Option<usize>,
    pub red_rand_max_ppb: Option<u64>,
    pub red_rand_min_ppb: Option<u64>,
}

#[derive(Clone, Debug)]
pub struct DropDecision {
    pub action: DropAction,
    pub witness: DropWitness,
}

pub const DEFAULT_ECN_THRESHOLD: f64 = 0.8;

/// Defines the interface for all packet drop strategies.
pub trait PacketDrop {
    fn decision(&mut self, packet_size: usize, byte_size: usize, queue_length: usize)
        -> DropDecision;

    fn action(&mut self, packet_size: usize, byte_size: usize, queue_length: usize) -> DropAction {
        self.decision(packet_size, byte_size, queue_length).action
    }
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
    fn decision(
        &mut self,
        packet_size: usize,
        byte_size: usize,
        queue_length: usize,
    ) -> DropDecision {
        let overflow = match self.capacity_unit {
            CapacityUnit::Bytes => self.capacity > 0 && byte_size + packet_size > self.capacity,
            CapacityUnit::Packets => self.capacity > 0 && queue_length + 1 > self.capacity,
        };

        let action = if overflow {
            DropAction::Drop
        } else {
            DropAction::Enqueue
        };

        DropDecision {
            action,
            witness: DropWitness {
                strategy: DropStrategyKind::TailDrop,
                capacity: self.capacity,
                capacity_unit: self.capacity_unit,
                queue_length,
                byte_length: byte_size,
                ecn_threshold_ppb: None,
                red_min_threshold_ppb: None,
                red_max_threshold_ppb: None,
                red_max_probability_ppb: None,
                red_avg_queue_length: None,
                red_rand_max_ppb: None,
                red_rand_min_ppb: None,
            },
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

/// ECN threshold marking. Marks CE when queue occupancy exceeds a threshold.
pub struct EcnThreshold {
    capacity: usize,
    capacity_unit: CapacityUnit,
    threshold: f64,
}

impl EcnThreshold {
    pub fn new(capacity: usize, capacity_unit: CapacityUnit, threshold: f64) -> EcnThreshold {
        EcnThreshold {
            capacity,
            capacity_unit,
            threshold,
        }
    }
}

impl PacketDrop for EcnThreshold {
    fn decision(
        &mut self,
        packet_size: usize,
        byte_size: usize,
        queue_length: usize,
    ) -> DropDecision {
        if self.capacity == 0 {
            return DropDecision {
                action: DropAction::Enqueue,
                witness: DropWitness {
                    strategy: DropStrategyKind::EcnThreshold,
                    capacity: self.capacity,
                    capacity_unit: self.capacity_unit,
                    queue_length,
                    byte_length: byte_size,
                    ecn_threshold_ppb: Some(to_ppb(self.threshold)),
                    red_min_threshold_ppb: None,
                    red_max_threshold_ppb: None,
                    red_max_probability_ppb: None,
                    red_avg_queue_length: None,
                    red_rand_max_ppb: None,
                    red_rand_min_ppb: None,
                },
            };
        }

        let threshold = self.threshold.clamp(0.0, 1.0);

        let queue_overflow = match self.capacity_unit {
            CapacityUnit::Bytes => self.capacity > 0 && byte_size + packet_size > self.capacity,
            CapacityUnit::Packets => self.capacity > 0 && queue_length + 1 > self.capacity,
        };

        if queue_overflow {
            return DropDecision {
                action: DropAction::Drop,
                witness: DropWitness {
                    strategy: DropStrategyKind::EcnThreshold,
                    capacity: self.capacity,
                    capacity_unit: self.capacity_unit,
                    queue_length,
                    byte_length: byte_size,
                    ecn_threshold_ppb: Some(to_ppb(threshold)),
                    red_min_threshold_ppb: None,
                    red_max_threshold_ppb: None,
                    red_max_probability_ppb: None,
                    red_avg_queue_length: None,
                    red_rand_max_ppb: None,
                    red_rand_min_ppb: None,
                },
            };
        }

        let threshold_exceeded = match self.capacity_unit {
            CapacityUnit::Bytes => {
                byte_size + packet_size > (threshold * self.capacity as f64).floor() as usize
            }
            CapacityUnit::Packets => {
                queue_length + 1 > (threshold * self.capacity as f64).floor() as usize
            }
        };

        let action = if threshold_exceeded {
            DropAction::MarkEcn
        } else {
            DropAction::Enqueue
        };

        DropDecision {
            action,
            witness: DropWitness {
                strategy: DropStrategyKind::EcnThreshold,
                capacity: self.capacity,
                capacity_unit: self.capacity_unit,
                queue_length,
                byte_length: byte_size,
                ecn_threshold_ppb: Some(to_ppb(threshold)),
                red_min_threshold_ppb: None,
                red_max_threshold_ppb: None,
                red_max_probability_ppb: None,
                red_avg_queue_length: None,
                red_rand_max_ppb: None,
                red_rand_min_ppb: None,
            },
        }
    }
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
    fn decision(
        &mut self,
        packet_size: usize,
        byte_size: usize,
        queue_length: usize,
    ) -> DropDecision {
        if self.capacity == 0 {
            return DropDecision {
                action: DropAction::Enqueue,
                witness: DropWitness {
                    strategy: if self.ecn {
                        DropStrategyKind::RedEcn
                    } else {
                        DropStrategyKind::Red
                    },
                    capacity: self.capacity,
                    capacity_unit: self.capacity_unit,
                    queue_length,
                    byte_length: byte_size,
                    ecn_threshold_ppb: None,
                    red_min_threshold_ppb: Some(to_ppb(self.min_threshold)),
                    red_max_threshold_ppb: Some(to_ppb(self.max_threshold)),
                    red_max_probability_ppb: Some(to_ppb(self.max_probability)),
                    red_avg_queue_length: Some(self.avg_queue_length),
                    red_rand_max_ppb: None,
                    red_rand_min_ppb: None,
                },
            };
        }

        let alpha = 1 / usize::pow(2, self.weight_factor);
        self.avg_queue_length = self.avg_queue_length * (1 - alpha) + queue_length * alpha;

        // drops the packet if the capacity of the queue is exceeded
        let queue_overflow = match self.capacity_unit {
            CapacityUnit::Bytes => self.capacity > 0 && byte_size + packet_size > self.capacity,
            CapacityUnit::Packets => self.capacity > 0 && queue_length + 1 > self.capacity,
        };

        // drops the packet if the average queue length exceeds the max_threshold
        let mut red_rand_max_ppb = None;
        let mut red_rand_min_ppb = None;
        let threshold_overflow = match self.capacity_unit {
            CapacityUnit::Bytes => {
                if byte_size + packet_size
                    > (self.max_threshold * self.capacity as f64).floor() as usize
                {
                    let drop_probability = Uniform::new(0.0, 1.0).unwrap().sample(&mut self.rng);
                    red_rand_max_ppb = Some(to_ppb(drop_probability));

                    drop_probability <= self.max_probability
                } else {
                    false
                }
            }
            CapacityUnit::Packets => {
                if queue_length + 1 > (self.max_threshold * self.capacity as f64).floor() as usize {
                    let drop_probability = Uniform::new(0.0, 1.0).unwrap().sample(&mut self.rng);
                    red_rand_max_ppb = Some(to_ppb(drop_probability));

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
                    red_rand_min_ppb = Some(to_ppb(drop_probability));

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
                    red_rand_min_ppb = Some(to_ppb(drop_probability));

                    drop_probability <= probability
                } else {
                    false
                }
            }
        };

        let action = if queue_overflow {
            DropAction::Drop
        } else if threshold_overflow || threshold_normal {
            if self.ecn {
                DropAction::MarkEcn
            } else {
                DropAction::Drop
            }
        } else {
            DropAction::Enqueue
        };

        DropDecision {
            action,
            witness: DropWitness {
                strategy: if self.ecn {
                    DropStrategyKind::RedEcn
                } else {
                    DropStrategyKind::Red
                },
                capacity: self.capacity,
                capacity_unit: self.capacity_unit,
                queue_length,
                byte_length: byte_size,
                ecn_threshold_ppb: None,
                red_min_threshold_ppb: Some(to_ppb(self.min_threshold)),
                red_max_threshold_ppb: Some(to_ppb(self.max_threshold)),
                red_max_probability_ppb: Some(to_ppb(self.max_probability)),
                red_avg_queue_length: Some(self.avg_queue_length),
                red_rand_max_ppb,
                red_rand_min_ppb,
            },
        }
    }
}

fn to_ppb(v: f64) -> u64 {
    (v.max(0.0).min(1.0) * 1e9).round() as u64
}
