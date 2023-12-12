# Due: a Performant Discrete-Event Simulator for Network Simulations

Developed with the Rust programming language, **Due** is designed as a performant discrete-event simulator for network simulations. 

## Running Instructions

To run a network simulation session using a configuration file:

```
cargo run -- configs/simple.toml
```

Five examples have also been provided in `due/examples/`:

```
cargo run --example switch
```

Especailly, several fattree configuration files have been provide：

```
cargo run --example fattree configs/fattree_32.toml
```

To run the simulation with logging information with configurable logging levels:

```
RUST_LOG=debug cargo run -- configs/simple.toml
```

where the level of RUST_LOG can be warn, info, and debug.

Besides, the following command can be used to evaluate the total running time:

```
time cargo run -- configs/simple.toml
```

## Configuration Settings

To set the number of threads for the simulation, there are two ways to create
the builder `SimInit`:

```
let mut sim = SimInit::new()
```

or

```
let mut sim = SimInit::with_num_threads(1)
```

where `new()` creates a builder for a multithreaded simulation running on all
available logical threads, while `with_num_thread()` specifies the number of
threads.

To set the simulation time, there are two parts to modify. First, if
configuration files are used, modify the `duration` of each flow. Otherwise,
modify the `duration` of the PacketSource directly. 

Second, modify the total
simulation time:

```
sim.step_by(Duration::from_secs(20));
```

where this `step_by()` function will either in the example file or in the `/due/src/topos/topo.rs`.