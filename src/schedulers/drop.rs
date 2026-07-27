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
    fn decision(
        &mut self,
        packet_size: usize,
        byte_size: usize,
        queue_length: usize,
    ) -> DropDecision;

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
    avg_queue_length: f64,
    count: i64,
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
/// Implements Floyd-Jacobson RED with the following arithmetic:
///
/// 1. The EWMA retains fractional state and updates as
///    `avg += (sample - avg) * 2^-9`.
/// 2. The normal-region base probability is
///    `p_b = max_p * (avg - min_abs) / (max_abs - min_abs)`.
/// 3. `avg`, rather than instantaneous occupancy, selects the threshold region.
/// 4. An average at or above `max_abs` produces a certain action without a draw.
/// 5. Byte-capacity RED averages byte occupancy; packet-capacity RED averages
///    packet occupancy.
///
/// In the normal region, the final probability is
/// `p_a = p_b / (1 - count * p_b)`. At or beyond the singularity
/// `count * p_b >= 1`, `p_a` is 1.
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
            _ => {
                let mut rng = rand::rng();
                SmallRng::from_rng(&mut rng)
            }
        };

        RED {
            capacity,
            capacity_unit,
            min_threshold,
            max_threshold,
            max_probability,
            weight_factor: 9,
            avg_queue_length: 0.0,
            count: -1,
            rng,
            ecn,
        }
    }

    fn count_adjusted_probability(&self, base_probability: f64) -> f64 {
        let count_times_probability = self.count.max(0) as f64 * base_probability;
        if count_times_probability >= 1.0 {
            1.0
        } else {
            (base_probability / (1.0 - count_times_probability)).clamp(0.0, 1.0)
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
                    red_avg_queue_length: Some(self.avg_queue_length as usize),
                    red_rand_max_ppb: None,
                    red_rand_min_ppb: None,
                },
            };
        }

        let queue_sample = match self.capacity_unit {
            CapacityUnit::Bytes => byte_size,
            CapacityUnit::Packets => queue_length,
        };
        let weight = 2.0_f64.powi(-(self.weight_factor as i32));
        self.avg_queue_length += (queue_sample as f64 - self.avg_queue_length) * weight;

        // drops the packet if the capacity of the queue is exceeded
        let queue_overflow = match self.capacity_unit {
            CapacityUnit::Bytes => self.capacity > 0 && byte_size + packet_size > self.capacity,
            CapacityUnit::Packets => self.capacity > 0 && queue_length + 1 > self.capacity,
        };

        // drops the packet if the average queue length exceeds the max_threshold
        let red_rand_max_ppb = None;
        let mut red_rand_min_ppb = None;
        let min_threshold = self.min_threshold * self.capacity as f64;
        let max_threshold = self.max_threshold * self.capacity as f64;
        let threshold_action = if self.avg_queue_length >= max_threshold {
            self.count = 0;
            true
        } else if self.avg_queue_length >= min_threshold {
            self.count = self.count.saturating_add(1);
            let base_probability = self.max_probability * (self.avg_queue_length - min_threshold)
                / (max_threshold - min_threshold);
            let probability = self.count_adjusted_probability(base_probability);
            let draw = Uniform::new(0.0, 1.0).unwrap().sample(&mut self.rng);
            red_rand_min_ppb = Some(to_ppb(draw));

            if draw <= probability {
                self.count = 0;
                true
            } else {
                false
            }
        } else {
            self.count = -1;
            false
        };

        let action = if queue_overflow {
            self.count = 0;
            DropAction::Drop
        } else if threshold_action {
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
                red_avg_queue_length: Some(self.avg_queue_length as usize),
                red_rand_max_ppb,
                red_rand_min_ppb,
            },
        }
    }
}

fn to_ppb(v: f64) -> u64 {
    (v.clamp(0.0, 1.0) * 1e9).round() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn red(capacity_unit: CapacityUnit) -> RED {
        RED::new(1_000, capacity_unit, 0.2, 0.8, 0.5, 0, false)
    }

    fn rng_with_first_draw_above(threshold: f64) -> SmallRng {
        for seed in 0..1_000 {
            let mut candidate = SmallRng::seed_from_u64(seed);
            let draw = Uniform::new(0.0, 1.0).unwrap().sample(&mut candidate);
            if draw > threshold {
                return SmallRng::seed_from_u64(seed);
            }
        }
        panic!("failed to find a deterministic RNG seed above {threshold}");
    }

    #[test]
    fn red_ewma_applies_weight_drains_and_uses_capacity_units() {
        let mut packets = RED::new(2_000, CapacityUnit::Packets, 0.7, 0.9, 0.8, 0, false);

        for _ in 0..512 {
            packets.decision(1, 1, 100);
        }
        let expected = 100.0 * (1.0 - (511.0_f64 / 512.0).powi(512));
        assert!(
            (packets.avg_queue_length - expected).abs() < 1e-12,
            "the packet EWMA must apply w = 2^-9 to packet occupancy"
        );

        packets.avg_queue_length = 2.0;
        packets.decision(1, 0, 0);
        assert_eq!(packets.avg_queue_length, 2.0 * 511.0 / 512.0);

        let mut bytes = RED::new(2_000, CapacityUnit::Bytes, 0.7, 0.9, 0.8, 0, false);
        for _ in 0..512 {
            bytes.decision(0, 100, 1);
        }
        assert!(
            (bytes.avg_queue_length - expected).abs() < 1e-12,
            "the byte EWMA must sample byte occupancy rather than packet count"
        );
    }

    #[test]
    fn red_normal_probability_scales_by_capacity_once_in_both_units() {
        for capacity_unit in [CapacityUnit::Bytes, CapacityUnit::Packets] {
            let mut red = red(capacity_unit);
            red.avg_queue_length = 500.0;
            red.rng = rng_with_first_draw_above(0.25);

            let decision = red.decision(0, 500, 500);

            assert_eq!(
                decision.action,
                DropAction::Enqueue,
                "the first midpoint probability is 0.5 * (500 - 200) / (800 - 200) = 0.25"
            );
            assert!(decision.witness.red_rand_min_ppb.is_some());
        }
    }

    #[test]
    fn red_regions_are_selected_by_average_in_both_units() {
        for capacity_unit in [CapacityUnit::Bytes, CapacityUnit::Packets] {
            let mut above = red(capacity_unit);
            above.avg_queue_length = 500.0;
            let above_decision = above.decision(0, 100, 100);
            assert!(
                above_decision.witness.red_rand_min_ppb.is_some(),
                "an average above min_th must enter the normal region"
            );

            let mut below = red(capacity_unit);
            below.avg_queue_length = 100.0;
            let below_decision = below.decision(0, 500, 500);
            assert!(
                below_decision.witness.red_rand_min_ppb.is_none(),
                "an average below min_th must remain outside the normal region"
            );
        }
    }

    #[test]
    fn red_above_max_drops_without_random_draw_in_both_units() {
        for capacity_unit in [CapacityUnit::Bytes, CapacityUnit::Packets] {
            let mut red = red(capacity_unit);
            red.avg_queue_length = 900.0;
            let (packet_size, queue_length, byte_size) = match capacity_unit {
                CapacityUnit::Bytes => (0, 0, 900),
                CapacityUnit::Packets => (0, 900, 0),
            };

            let decision = red.decision(packet_size, queue_length, byte_size);

            assert_eq!(decision.action, DropAction::Drop);
            assert_eq!(red.count, 0);
            assert!(
                decision.witness.red_rand_max_ppb.is_none(),
                "the above-max region must not draw for its certain drop"
            );
            assert!(
                decision.witness.red_rand_min_ppb.is_none(),
                "RED regions must be mutually exclusive"
            );
        }
    }

    #[test]
    fn red_count_spacing_resets_and_clamps_at_the_singularity() {
        let mut red = red(CapacityUnit::Packets);

        red.count = 1;
        assert_eq!(red.count_adjusted_probability(0.25), 1.0 / 3.0);
        red.count = 4;
        assert_eq!(red.count_adjusted_probability(0.25), 1.0);
        red.count = 5;
        assert_eq!(red.count_adjusted_probability(0.25), 1.0);

        red.avg_queue_length = 500.0;
        red.count = 4;
        let decision = red.decision(0, 500, 500);
        assert_eq!(decision.action, DropAction::Drop);
        assert_eq!(red.count, 0);

        red.avg_queue_length = 100.0;
        let decision = red.decision(0, 100, 100);
        assert_eq!(decision.action, DropAction::Enqueue);
        assert_eq!(red.count, -1);
    }
}
