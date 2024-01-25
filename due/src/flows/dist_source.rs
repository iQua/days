//! Implements a packet source that simulates the sending of packets with
//! specific distributions of inter-arrival times and packet sizes.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use log::{debug, info};
use rand::distributions::Distribution;
use rand::rngs::SmallRng;
use rand::SeedableRng;
use statrs::distribution::{DiscreteUniform, Exp, Uniform};

use asynchronix::model::{InitializedModel, Model, Output};
use asynchronix::time::{MonotonicTime, Scheduler};

use crate::flows::packet::Packet;
use crate::flows::{DistributionInfo, TrafficCharacteristics};
use crate::{get_seed, next_endpoint_id};

#[derive(Debug)]
pub struct DistPacketSource {
    endpoint_id: usize,
    flow_id: usize,
    traffic: TrafficCharacteristics,
    packets_sent: usize,
    sent_size: usize,
    rng: SmallRng,

    pub output: Output<Packet>,
}

impl DistPacketSource {
    pub fn new(flow_id: usize, traffic: TrafficCharacteristics, seed: usize) -> DistPacketSource {
        let global_seed = get_seed();
        let rng = match global_seed {
            1.. => SmallRng::seed_from_u64((global_seed + seed) as u64),
            _ => SmallRng::from_entropy(),
        };

        DistPacketSource {
            endpoint_id: next_endpoint_id(),
            flow_id,
            traffic,
            packets_sent: 0,
            sent_size: 0,
            rng,
            output: Output::default(),
        }
    }
}
