# Day: a Multi-Threaded Performant Discrete-Event Simulator for Network Simulations

Developed with the Rust programming language, **Day** is designed as a highly performant discrete-event simulator for network simulations, using a **multi-threaded** executor that oversees stackless coroutines. It is designed based on the actor model, where each actor can only interact with its counterparts using message passing.

To run a network simulation session using a configuration file:

```
RUST_LOG=debug day configs/simple.toml
```

where `RUST_LOG` levels can be `error`, `warn`, `info`, `debug`, and `trace`.

## Configuration Settings

In **Day**, all configuration settings are read from a configuration file when a simulation session starts, and the configuration file follows the `TOML` format for the sake of simplicity and readability. The following introduces all the possible settings in the configuration file.

### General

#### seed
The seed for the random number generator to ensure reproducibility of the simulation results.

- **Valid value**: Integer
- **Required**: Yes
- **Example**:

  ```toml
  seed = 1000
  ```

#### duration
The total duration of the simulation in seconds.

- **Valid value**: Floating point number
- **Required**: No
- **Default**: 1500.0
- **Example**:

  ```toml
  duration = 20.0
  ```
 
#### progress
**Day** provides a progress bar to visualize the progression of a simulation session. This `progress` element specifies the progress interval, which is the time interval to advance the position of the progress bar.

- **Valid value**: Floating point number
- **Required**: No
- **Default**: `duration` / 100
- **Example**:

  ```toml
  progress = 1.0
  ```

#### log_path
**Day** generates three CSV files, `sources.csv`, `sinks.csv`, and `switches.csv`, containing statistics of a simulation session. `log_path` specifies th directory of the three CSV files.

- **Valid value**: String
- **Required**: No
- **Default**: `./output`
- **Example**:

  ```toml
  log_path = "./test"
  ```
  
#### log_interval
In the three CSV files, each row contains statistics in a time interval. `log_interval` specifies the length of a time interval in seconds.

- **Valid value**: Floating point number
- **Required**: No
- **Default**: Value of `progress`
- **Example**:

  ```toml
  log_interval = "1.0"
  ```

#### topology
**Day** supports arbitrary topologies. Besides widely-used topologies `FatTree` and `Torus`, any topology that can be specified as an undirected graph can be supported as well.

- `FatTree`
  
	To specify the `FatTree` topology, it is required to specific `k`. `k` is a multiple of 2.
  
	**Example**:
	
	```toml
  [topology]
	category = "FatTree"

	[topology.torus]
    	k = 8
  ```

- `Torus`

	To specify the `Torus` topology, it is required to specific dimension `dim`, and node per dimension `n`. Only 1D, 2D, and 3D Torus topologies are supported. That is, valid values of `dim` are 1, 2, 3.

  **Example**:

  ```toml
  [topology]
	category = "Torus"

	[topology.torus]
    	dim = 2
    	n = 3
  ```
 
- Custom topology

	Use `edges` to specify an undirected graph as the topology, and `hosts` to specify hosts in the topology.
	
	`edges` is a vector of [integer, integer]. An [integer, integer] pair presents an edge in the undirected graph. 
	
	`hosts` is a vector of integers

	**Example**:

	```toml
	edges = [[0, 1], [0, 2]]
	hosts = [0, 1, 2]
	```




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