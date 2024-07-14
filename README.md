# Day: a Multi-Threaded Performant Discrete-Event Simulator for Network Simulations

Developed with the Rust programming language, **Day** is designed as a highly performant discrete-event simulator for network simulations, using a **multi-threaded** executor that oversees stackless coroutines. It is designed based on the actor model, where each actor can only interact with its counterparts using message passing.

To run a network simulation session using a configuration file:

```
RUST_LOG=debug day configs/simple.toml
```

where `RUST_LOG` levels can be `error`, `warn`, `info`, `debug`, and `trace`.

## Configuration Settings

In **Day**, all configuration settings are read from a configuration file when a simulation session starts, and the configuration file follows the TOML format for the sake of simplicity and readability. The following introduces all the possible settings in the configuration file.

### General

#### seed
The seed for the random number generator to ensure reproducibility of the simulation results.

- **Valid value**: Integer
- **Required**: No
- **Default**: 42
- **Example**:

  ```toml
  seed = 1000
  ```

#### edges

#### hosts

#### progress

#### duration
The total duration of the simulation in seconds.

#### log_path

#### log_interval



### Switch

#### port_rate

#### capacity

#### weights

#### discipline

#### drop 


### Flow

#### flow_type

#### graph

#### initial_delay

#### size

#### duration

#### arr_dist

#### pkt\_size\_dist


### Collective

#### collective_type

#### graph

#### initial_delay

#### size

#### duration

#### arr_dist

#### pkt\_size\_dist