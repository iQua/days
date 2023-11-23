use std::io::Sink;

use statrs::statistics::Distribution;

use crate::packets::dist_generator::DistPacketGenerator;
use crate::packets::sink::PacketSink;
use crate::sim::{SimContext, Time};
use crate::switches::switch::PacketSwitch;

pub struct FatTree<A, B>
where
    A: Distribution<Time>,
    B: Distribution<f64>,
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
    A: Distribution<Time>,
    B: Distribution<f64>,
{
    pub fn new(k: usize) -> FatTree<A, B> {
        FatTree {
            k,
            generators: Vec::new(),
            sinks: Vec::new(),
            core_switches: Vec::new(),
            agg_switches: Vec::new(),
            edge_switches: Vec::new(),
        }
    }
}
