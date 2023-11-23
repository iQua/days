use statrs::statistics::Distribution;

use crate::packets::dist_generator::DistPacketGenerator;
use crate::packets::sink::PacketSink;
use crate::sim::{SimContext, Time};
use crate::switches::switch::PacketSwitch;
use crate::topos::{connect_n_1_hetero, connect_pair};
use crate::{elements_to_agg, elements_to_core, elements_to_edge, Element, Shared};

pub struct FatTree<A, B>
where
    A: Distribution<Time> + 'static,
    B: Distribution<f64> + 'static,
{
    k: usize,
    /// packet generators in hosts
    generators: Vec<DistPacketGenerator<A, B>>,
    /// packet sinks of in hosts
    sinks: Vec<PacketSink>,
    /// core-layer switches
    core_switches: Vec<PacketSwitch>,
    /// aggregation-layer switches
    agg_switches: Vec<PacketSwitch>,
    /// edge-layer switches
    edge_switches: Vec<PacketSwitch>,
}

impl<A, B> FatTree<A, B>
where
    A: Distribution<Time> + 'static,
    B: Distribution<f64> + 'static,
{
    pub fn new(
        k: usize,
        generators: Vec<DistPacketGenerator<A, B>>,
        sinks: Vec<PacketSink>,
        core_switches: Vec<PacketSwitch>,
        agg_switches: Vec<PacketSwitch>,
        edge_switches: Vec<PacketSwitch>,
    ) -> FatTree<A, B> {
        assert!(k > 0 && k % 2 == 0, "Invalid k!");
        FatTree {
            k,
            generators,
            sinks,
            core_switches,
            agg_switches,
            edge_switches,
        }
    }

    pub fn connect(&mut self) {
        // connects edge-layer switches to sinks
        for (sink_id, sink) in self.sinks.iter_mut().enumerate() {
            let switch_id = sink_id / 2;
            connect_pair(self.edge_switches.get_mut(switch_id).unwrap(), sink);
        }

        // connects elements that send packets to edge-layer switches
        for edge_switch in self.edge_switches.iter_mut() {
            let mut upstreams: Vec<Box<&mut dyn Element>> = Vec::new();
            let (agg_ids, generator_ids) = elements_to_edge(self.k, edge_switch.id());

            for switch in &mut self.agg_switches {
                if agg_ids.contains(&switch.id()) {
                    upstreams.push(Box::new(switch as &mut dyn Element));
                }
            }

            for generator in &mut self.generators {
                if generator_ids.contains(&generator.id()) {
                    upstreams.push(Box::new(generator as &mut dyn Element));
                }
            }

            connect_n_1_hetero(&mut upstreams, edge_switch);
        }

        // connects elements that send packets to aggregation-layer switches
        for agg_switch in self.agg_switches.iter_mut() {
            let mut upstreams: Vec<Box<&mut dyn Element>> = Vec::new();
            let (core_ids, edge_ids) = elements_to_agg(self.k, agg_switch.id());

            for switch in &mut self.core_switches {
                if core_ids.contains(&switch.id()) {
                    upstreams.push(Box::new(switch as &mut dyn Element));
                }
            }

            for switch in &mut self.edge_switches {
                if edge_ids.contains(&switch.id()) {
                    upstreams.push(Box::new(switch as &mut dyn Element));
                }
            }

            connect_n_1_hetero(&mut upstreams, agg_switch);
        }

        // connects aggregation-layer switches and core-layer switches
        for core_switch in self.core_switches.iter_mut() {
            let mut upstreams: Vec<Box<&mut dyn Element>> = Vec::new();
            let agg_ids = elements_to_core(self.k, core_switch.id());

            for switch in &mut self.agg_switches {
                if agg_ids.contains(&switch.id()) {
                    upstreams.push(Box::new(switch as &mut dyn Element));
                }
            }

            connect_n_1_hetero(&mut upstreams, core_switch);
        }
    }

    pub async fn run(self, sim: SimContext<'_, Shared>) {
        for generator in self.generators {
            sim.activate(generator.run(sim));
        }
        for sink in self.sinks {
            sim.activate(sink.run(sim));
        }
        for switch in self.core_switches {
            sim.activate(switch.run(sim));
        }
        for switch in self.agg_switches {
            sim.activate(switch.run(sim));
        }
        for switch in self.edge_switches {
            sim.activate(switch.run(sim));
        }

        // waits for the end of this simulation
        sim.advance(sim.shared().duration + 100.).await;
    }
}
