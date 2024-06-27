# Day: a Multi-Threaded Performant Discrete-Event Simulator for Network Simulations

Developed with the Rust programming language, **Day** is designed as a highly performant discrete-event simulator for network simulations, using a **multi-threaded** executor that oversees stackless coroutines. It is designed based on the actor model, where each actor can only interact with its counterparts using message passing.

To run a network simulation session using a configuration file:

```
RUST_LOG=debug day configs/simple.toml
```

where `RUST_LOG` levels can be `error`, `warn`, `info`, `debug`, and `trace`.
