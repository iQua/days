//! This example shows how to create a basic network where two packet generators
//! send packets to a wire that adds propagation delays according to a random
//! distribution, and then to a packet sink.

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use petgraph::graph::{NodeIndex, UnGraph};
use rand::{rngs::SmallRng, SeedableRng};
use statrs::distribution::{DiscreteUniform, Exp, Uniform};

use due::packets::sink::Sink;
use due::packets::source::Source;
use due::packets::wire::Wire;
use due::sim::{simulation, Process, RandomVar, SimContext};
use due::topos::connect_n_1_hetero;
use due::{Element, NodeData, NodeType, Shared};

const SEED: u64 = 1000;

async fn network_sim(sim: SimContext<'_, Shared>) {
    let mut g = UnGraph::<NodeData, ()>::new_undirected();

    let mut elements: HashMap<usize, Box<dyn Element>> = HashMap::new();

    let arr_interval_dist = Arc::new(|| Exp::new(1.0).unwrap());
    let packet_size_dist = Arc::new(|| DiscreteUniform::new(1000, 1500).unwrap());

    // creates a collection of packet generators
    for _ in 0..2 {
        let mut generator = Source::new(1.0, arr_interval_dist.clone(), packet_size_dist.clone());
        g.add_node(NodeData {
            id: generator.id(),
            node_type: NodeType::Source,
        });
        elements.insert(generator.id(), Box::new(generator));
    }

    // creates a sink
    let mut sink = Sink::default();
    g.add_node(NodeData {
        id: sink.id(),
        node_type: NodeType::Sink,
    });
    elements.insert(sink.id(), Box::new(sink));

    // creates a wire
    let mut wire = Wire::new(Box::new(|| Uniform::new(2.0, 2.0).unwrap()));
    g.add_node(NodeData {
        id: wire.id(),
        node_type: NodeType::Edge,
    });
    elements.insert(wire.id(), Box::new(wire));

    // finds the wire
    let edge_node_idxs: Vec<NodeIndex> = g
        .node_indices()
        .filter(|&n| match g[n].node_type {
            NodeType::Edge => true,
            _ => false,
        })
        .collect();

    println!("{:?}", edge_node_idxs);

    // connects sources and the sink to the wire in the graph
    for node_index in g.node_indices() {
        match g[node_index].node_type {
            NodeType::Source | NodeType::Sink => {
                for &edge_node in &edge_node_idxs {
                    g.add_edge(node_index, edge_node, ());
                }
            }
            _ => {}
        }
    }

    // connects neighbor nodes through connect function
    for node_idx in g.node_indices() {
        match g[node_idx].node_type {
            NodeType::Edge | NodeType::Sink => {
                let mut upstream_ids = Vec::new();
                let mut upstreams: Vec<Box<&mut dyn Element>> = Vec::new();
                for neighbor_idx in g.neighbors_undirected(node_idx) {
                    upstream_ids.push(g[neighbor_idx].id);
                }
                for upstream_id in upstream_ids {
                    if let Some(element) = elements.get_mut(&upstream_id) {
                        upstreams.push(Box::new(element.as_mut()));
                    }
                }
                let downstream = elements.get_mut(&g[node_idx].id).unwrap();
                connect_n_1_hetero(&mut upstreams, downstream.as_mut());
            }
            _ => {
                continue;
            }
        }
    }

    // todo: activate all nodes
    // for node_idx in g.node_indices() {
    //     // activate
    // }

    // waits for the end of this simulation
    sim.advance(sim.shared().duration + 100.).await;
}

fn main() {
    let outcome = simulation(
        Shared {
            rng: RefCell::new(SmallRng::seed_from_u64(SEED)),
            queueing_delay: RandomVar::new(),
            duration: 10.,
            next_id: (0..3).map(|_| AtomicUsize::new(0)).collect(),
        },
        |sim| Process::new(sim, network_sim(sim)),
    );

    println!(
        "Statistics on queueing delay in this simulation: {:#.3}",
        outcome.queueing_delay
    );
}
