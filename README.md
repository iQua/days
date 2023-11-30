# Due: a Performant Discrete-Event Simulator for Network Simulations

Developed with the Rust programming language, **Due** is designed as a performant discrete-event simulator for network simulations. 

To run a network simulation session using a configuration file:

```
cargo run -- configs/simple.toml
```

Three examples have also been provided in `due/examples/`:

```
cargo run --example fattree
```

To run the simulation with logging information with configurable logging levels:

```
RUST_LOG=debug cargo run -- configs/simple.toml
```

where the level of RUST_LOG can be warn, info, and debug.
