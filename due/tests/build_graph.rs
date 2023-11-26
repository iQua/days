use petgraph::algo;
use petgraph::graph::UnGraph;

use due::topos::build::build_graph;

#[test]
fn test_build_graph() {
    let simple_graph = build_graph("tests/build_graph.toml");

    let ground_truth = UnGraph::<usize, ()>::from_edges(&[(0, 1)]);
    assert!(algo::is_isomorphic(&simple_graph, &ground_truth));

    println!("The simple graph is:\n{:?}", simple_graph);
}
