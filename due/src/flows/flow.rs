use std::fs;

use petgraph::graph::{DiGraph, NodeIndex, UnGraph};
use petgraph::visit::EdgeRef;
use serde::Deserialize;

use crate::sim_new::{Simulator, Time};
use crate::{next_flow_id, Shared};

use super::route::{RandomSimplePath, RoutingProtocol};
use super::sink::PacketSink;
use super::source::PacketSource;
use super::EndPoint;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename = "UPPERCASE")]
pub enum FlowType {
    PacketDistribution,
    TCP,
}

#[derive(Deserialize, Debug)]
struct TomlFlow {
    flow_type: FlowType,
    graph: Vec<(u32, u32)>,
    initial_delay: Time,
    arr_dist: DistributionInfo,
    pkt_size_dist: DistributionInfo,
}

#[derive(Deserialize, Debug)]
struct FlowConfig {
    flows: Vec<TomlFlow>,
}

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(tag = "type")]
pub enum DistributionInfo {
    Exp { lambda: f64 },
    Uniform { low: i64, high: i64 },
}

#[derive(Debug)]
pub struct Flow {
    pub id: usize,
    pub flow_type: FlowType,
    pub graph: DiGraph<usize, ()>,
    pub initial_delay: Time,
    pub arr_dist: DistributionInfo,
    pub pkt_size_dist: DistributionInfo,
    pub endpoints: Vec<EndPoint>,
    pub routing: RandomSimplePath,
}

impl Flow {
    pub fn new(
        id: usize,
        flow_type: FlowType,
        graph: DiGraph<usize, ()>,
        initial_delay: Time,
        arr_dist: DistributionInfo,
        pkt_size_dist: DistributionInfo,
    ) -> Flow {
        let routing = RandomSimplePath::new(UnGraph::<usize, ()>::new_undirected().clone());
        Flow {
            id,
            flow_type,
            graph,
            initial_delay,
            arr_dist,
            pkt_size_dist,
            endpoints: Vec::new(),
            routing,
        }
    }

    // Initializes flows from a vector of directed graphs.
    pub fn flows_from_graph(graphs: Vec<Vec<(u32, u32)>>) -> Vec<Flow> {
        let mut flows = Vec::new();

        for graph in graphs {
            let flow_graph = DiGraph::<usize, ()>::from_edges(&graph);

            flows.push(Flow::new(
                next_flow_id(),
                FlowType::PacketDistribution,
                flow_graph,
                0.,
                DistributionInfo::Exp { lambda: 1. },
                DistributionInfo::Uniform {
                    low: 1000,
                    high: 1000,
                },
            ));
        }

        flows
    }

    // Initializes flows from a configuration file.
    pub fn flows_from_config(file_path: &str) -> Vec<Flow> {
        let content = fs::read_to_string(file_path).expect("The configuration is not valid");

        let config: FlowConfig =
            toml::from_str(&content).expect("Failed to deserialize the configuration");

        let mut flows = Vec::new();

        for flow in config.flows {
            let graph = DiGraph::<usize, ()>::from_edges(flow.graph);

            flows.push(Flow::new(
                next_flow_id(),
                flow.flow_type,
                graph,
                flow.initial_delay,
                flow.arr_dist,
                flow.pkt_size_dist,
            ));
        }

        flows
    }

    // Gets the simple paths for all edges of the flow
    pub async fn compute_paths(
        &mut self,
        graph: UnGraph<usize, ()>,
        sim: Simulator<Shared>,
    ) -> Vec<Vec<NodeIndex>> {
        // sets the routing protocol
        self.routing = RandomSimplePath::new(graph);

        let mut paths = Vec::new();

        let rng = sim.get_rng().await;

        for (idx, edge) in self.graph.edge_references().enumerate() {
            let mut path = self
                .routing
                .compute_route(edge.source(), edge.target(), rng.clone());
            let sink_id = self
                .endpoints
                .iter()
                .filter_map(|endpoint| match endpoint {
                    EndPoint::PacketSink(sink) => Some(sink),
                    _ => None,
                })
                .nth(idx)
                .unwrap()
                .id();
            path.push(NodeIndex::new(sink_id));
            paths.push(path);
        }
        paths
    }

    // Gets the hosts ids that endpoints should attach to
    pub fn get_hosts(&self) -> Vec<NodeIndex> {
        let mut attach_to = Vec::new();
        for edge in self.graph.edge_references() {
            attach_to.push(edge.source());
            attach_to.push(edge.target());
        }

        attach_to
    }

    // Initializes endpoints for the flow
    pub fn init_endpoints(&mut self) {
        for _ in 0..self.graph.edge_count() {
            self.endpoints
                .push(EndPoint::PacketSource(PacketSource::new(
                    self.id,
                    self.initial_delay,
                    self.arr_dist,
                    self.pkt_size_dist,
                )));
            self.endpoints
                .push(EndPoint::PacketSink(PacketSink::new(self.id)));
        }
    }

    pub async fn run(self, sim: Simulator<Shared>) {
        for endpoint in self.endpoints {
            match endpoint {
                EndPoint::PacketSource(source) => {
                    sim.activate(source.run(sim.clone()));
                }
                EndPoint::PacketSink(sink) => {
                    sim.activate(sink.run(sim.clone()));
                }
            }
        }
    }
}
