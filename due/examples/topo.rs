use due::topos::build::{build_fattree, build_graph};

fn main() {
    let simple_graph = build_graph("configs/simple.toml");
    let (fattree_graph, fattree_hosts) = build_fattree("configs/fattree.toml");
    println!("The simple graph is:\n{:?}", simple_graph);
    println!("The fat tree graph is:\n{:?}", fattree_graph);
    println!("The fat tree hosts are:\n{:?}", fattree_hosts);
}
