# Due: a Performant Discrete-Event Simulator for Network Simulations

Developed with the Rust programming language, **Due** is designed as a performant discrete-event simulator for network simulations. 

The following command is used to run the network simulation:
```
cargo run -- configs/simple.toml
```

In more details, three examples are provided in `due/examples/`:
```
cargo run --example fattree
```

Besided, to run the simulation with logging information:
```
RUST_LOG=debug cargo run -- configs/simple.toml
```
where the level of RUST_LOG can be warn, info, and debug.