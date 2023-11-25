use due::topos::builders::{build, build_fattree};

fn main() {
    let simple_graph = build("configs/simple.toml");
    let fattree_graph = build_fattree("configs/fattree.toml");

    println!("The simple graph is:\n{:?}", simple_graph);
    println!("The fattree graph is:\n{:?}", fattree_graph);
}
