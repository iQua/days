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

use crate::flows::DistributionInfo;
use crate::flows::packet::Packet;
use crate::get_seed;
use crate::utils::exact_time::{behavior_delay_ns, clock_ns, seconds_view};
use nexosim::model::{BuildContext, Context, Model, ModelRegistry, ProtoModel, SchedulableId};
use nexosim::ports::Output;

#[derive(Debug)]
enum WireDelay {
    Distribution(DistributionInfo),
    FixedNs(u64),
}

#[derive(Debug)]
pub struct Wire {
    wire_id: usize,
    delay: WireDelay,
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
            delay: WireDelay::Distribution(delay_dist),
            rng,
            output: Output::default(),
            scheduled_departures: VecDeque::new(),
        }
    }

    pub fn with_propagation_ns(wire_id: usize, propagation_ns: u64) -> Wire {
        let mut wire = Self::new(
            wire_id,
            DistributionInfo::DiscreteUniform { low: 0, high: 0 },
        );
        wire.delay = WireDelay::FixedNs(propagation_ns);
        wire
    }

    #[instrument(skip(self, cx))]
    pub async fn packet_received(&mut self, mut packet: Packet, cx: &Context<Self>) {
        let now_ns = clock_ns(cx.time());
        let now = seconds_view(now_ns);

        #[cfg(feature = "test")]
        {
            let global_time = now;

            // makes sure that the current simulation time can be correctly retrieved from
            // the packet itself
            assert!(
                packet.time <= global_time + 1e-7,
                "Timing mismatch: packet.time = {}, global_time = {}",
                packet.time,
                global_time
            );
        }

        debug!(
            "Wire {} received packet {} ({} bytes) from flow {} at time {:.3}.",
            self.wire_id, packet.packet_id, packet.size, packet.flow_id, now,
        );

        packet.departure_update(now);

        let sampled_seconds = match &self.delay {
            WireDelay::FixedNs(_) => None,
            WireDelay::Distribution(DistributionInfo::DiscreteUniform { low, high }) => {
                let dist = Uniform::new_inclusive(low, high).unwrap();
                Some(dist.sample(&mut self.rng) as f64)
            }
            WireDelay::Distribution(DistributionInfo::Exp { lambda }) => {
                Some(Exp::new(*lambda).unwrap().sample(&mut self.rng))
            }
            WireDelay::Distribution(DistributionInfo::Uniform { low, high }) => {
                if low == high {
                    Some(*low)
                } else {
                    Some(Uniform::new(*low, *high).unwrap().sample(&mut self.rng))
                }
            }
        };
        let delay_ns = match (&self.delay, sampled_seconds) {
            (WireDelay::FixedNs(delay_ns), _) => *delay_ns,
            (_, Some(0.0)) => 0,
            (_, Some(delay)) => behavior_delay_ns(delay, "wire delay sample")
                .expect("wire delay distributions must produce finite nonnegative delays"),
            (_, None) => unreachable!(),
        };
        let arrival_ns = now_ns
            .checked_add(delay_ns)
            .expect("wire arrival time exceeds the u64 nanosecond clock range");
        let arrival_time = seconds_view(arrival_ns);

        packet.departure_update(arrival_time);

        if delay_ns > 0 {
            self.scheduled_departures.push_back(packet);
            cx.schedule_event_fast(
                Duration::from_nanos(delay_ns),
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
