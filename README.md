# Due: a Performant Discrete-Event Simulator for Network Simulations

Developed with the Rust programming language, **Due** is designed as a performant discrete-event simulator for network simulations, using a multi-threaded executor that oversees stackless coroutines. It is designed based on the actor model, where each actor can only interact with its counterparts using message passing. A different design with a single-threaded executor, called **Dew**, has also been included for information only, and is no longer maintained continuously.

To run a network simulation session using a configuration file:

```
RUST_LOG=debug cargo run -- configs/simple.toml
```

where `RUST_LOG` levels can be `error`, `warn`, `info`, `debug`, and `trace`. Five examples have also been provided in `due/examples/`. One can run each of these example using:

```
RUST_LOG=debug cargo run --example switch
```

The `time` command in UNIX can be used to measure the total running time of a run without any logs:

```
time cargo run -- configs/simple.toml
```