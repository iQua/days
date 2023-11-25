use due::topos::builder::build;


fn main() {
    let graph = build("./examples/topo.toml");
    println!("{:?}", graph);
}