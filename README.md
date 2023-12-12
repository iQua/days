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

Several configuration files with the FatTree topology have been provided as well:

```
cargo run --example fattree configs/fattree_32.toml
```

To run the simulation with logging information with configurable logging levels, use the `RUST_LOG` environment variable:

```
RUST_LOG=debug cargo run -- configs/simple.toml
```

where `RUST_LOG` levels can be `error`, `warn`, `info`, `debug`, and `trace`.

The `time` command in UNIX can be used to measure the total running time of a run:

```
time cargo run -- configs/simple.toml
```

## Setting the number of threads and the duration of each flow

```
let mut sim = SimInit::new()
```

is the default as it creates a multi-threaded simulation, with one thread per CPU core. 

```
let mut sim = SimInit::with_num_threads(1)
```

specifies the number of threads explicitly.

The `duration` of each flow can be specified in the configuration file.

```
sim.step_by(Duration::from_secs(20));
```

can also limit the total duration of the simulation.