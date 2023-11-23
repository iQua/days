

use statrs::statistics::Distribution;

use crate::packets::dist_generator::DistPacketGenerator;
use crate::packets::sink::PacketSink;
use crate::sim::{SimContext, Time};
use crate::switches::switch::PacketSwitch;
use crate::Shared;

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
    pub fn new(k: usize, generators: Vec<DistPacketGenerator<A, B>>, sinks: Vec<PacketSink>, core_switches: Vec<PacketSwitch>, agg_switches: Vec<PacketSwitch>, edge_switches: Vec<PacketSwitch>) -> FatTree<A, B> {
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
