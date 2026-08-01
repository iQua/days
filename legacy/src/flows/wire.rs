//! Implements a wire element that adds a propagation delay to packets.

use std::collections::VecDeque;
use std::time::Duration;

use log::debug;
use rand::SeedableRng;
use rand::distr::Distribution;
use rand::distr::Uniform;
use rand::rngs::SmallRng;
use rand_distr::Exp;
use tracing::instrument;

use nexosim::model::{BuildContext, Context, Model, ModelRegistry, ProtoModel, SchedulableId};
use nexosim::ports::Output;
#[cfg(feature = "test")]
use nexosim::time::MonotonicTime;

use crate::flows::DistributionInfo;
use crate::flows::packet::Packet;
use crate::get_seed;
use crate::utils::time::{quantize_after, quantize_time};

#[derive(Debug)]
pub struct Wire {
    wire_id: usize,
    delay_dist: DistributionInfo,
    rng: SmallRng,

    pub output: Output<Packet>,
    scheduled_departures: VecDeque<Packet>,
}

impl Wire {
    const FORWARD_SCHEDULED_SID: SchedulableId<Self, ()> = SchedulableId::__from_decorated(0);

    pub fn new(wire_id: usize, delay_dist: DistributionInfo) -> Wire {
        let seed = get_seed();
        let rng = match seed {
            1.. => SmallRng::seed_from_u64(seed as u64),
            _ => {
                let mut rng = rand::rng();
                SmallRng::from_rng(&mut rng)
            }
        };

        Wire {
            wire_id,
            delay_dist,
            rng,
            output: Output::default(),
            scheduled_departures: VecDeque::new(),
        }
    }

    #[instrument(skip(self, cx))]
    pub async fn packet_received(&mut self, mut packet: Packet, cx: &Context<Self>) {
        #[cfg(feature = "test")]
        {
            let global_time = cx.time().duration_since(MonotonicTime::EPOCH).as_secs_f64();

            // makes sure that the current simulation time can be correctly retrieved from
            // the packet itself
            assert!(
                packet.time <= global_time + 1e-7,
                "Timing mismatch: packet.time = {}, global_time = {}",
                packet.time,
                global_time
            );
        }

        let mut now = packet.time;

        debug!(
            "Wire {} received packet {} ({} bytes) from flow {} at time {:.3}.",
            self.wire_id, packet.packet_id, packet.size, packet.flow_id, now,
        );

        now = quantize_time(now);
        packet.departure_update(now);

        let delay = match self.delay_dist {
            DistributionInfo::DiscreteUniform { low, high } => {
                let dist = Uniform::new_inclusive(low, high).unwrap();
                dist.sample(&mut self.rng) as f64
            }
            DistributionInfo::Exp { lambda } => Exp::new(lambda).unwrap().sample(&mut self.rng),
            DistributionInfo::Uniform { low, high } => {
                if low == high {
                    low
                } else {
                    Uniform::new(low, high).unwrap().sample(&mut self.rng)
                }
            }
        };

        let arrival_time = quantize_after(now, delay);
        packet.departure_update(arrival_time);

        if arrival_time > now {
            let delay = (arrival_time - now).max(0.0);
            self.scheduled_departures.push_back(packet);
            cx.schedule_event_fast(
                Duration::from_secs_f64(delay),
                &Self::FORWARD_SCHEDULED_SID,
                Self::forward_scheduled,
                (),
            )
            .unwrap();
        } else {
            self.forward_packet(packet).await;
        }
    }

    #[instrument(skip(self))]
    async fn forward_packet(&mut self, packet: Packet) {
        debug!(
            "Wire {} sent packet {} ({} bytes) from flow {} at time {:.3}.",
            self.wire_id, packet.packet_id, packet.size, packet.flow_id, packet.time,
        );

        self.output.send(packet).await;
    }

    async fn forward_scheduled(&mut self, _: (), _: &Context<Self>) {
        let Some(packet) = self.scheduled_departures.pop_front() else {
            debug_assert!(
                false,
                "Wire {} scheduled departure queue underflow",
                self.wire_id
            );
            return;
        };
        self.forward_packet(packet).await;
    }
}

impl Model for Wire {
    type Env = ();
    fn register_schedulables(
        cx: &mut BuildContext<impl ProtoModel<Model = Self>>,
    ) -> ModelRegistry {
        let mut registry = ModelRegistry::default();
        registry.add(cx.register_schedulable(Self::forward_scheduled));
        registry
    }
}
