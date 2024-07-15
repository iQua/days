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
In **Day**, all switches use the same setting which can be specified with the following attributes.

#### port_rate
The bit rate of each outbound port.

- **Valid value**: Floating point number
- **Required**: Yes
- **Example**:

  ```toml
  port_rate = 8000
  ```

#### capacity
The capacity (buffer size) of each outbound port.

- **Valid value**: Integer
- **Required**: Yes
- **Example**:

  ```toml
  capacity = 100
  ```

#### drop
The packet drop strategy that drops packets when the buffer is full.

- **Valid value**: 
	
	|   Value  |  Meaning |
	|----------|----------|
	|`TailDrop`| Dropping packets at the tail of the queue |
	|   `RED`  | Random Early Detection |
	 
- **Required**: Yes
- **Example**:

  ```toml
  capacity = 100
  ```

#### discipline
The scheduling discipline.

- **Valid value**: 

	|   Value  |  Meaning |   Notes  |
	|----------|----------|----------|
	|  `FIFO`  | First In First Out |
	|  `DRR`   | Deficit Round Robin | Required to specify `weights` |
	|  `WFQ`   | Weighted Fair Queueing | Required to specify `weights` |
	|  `SP`    | Static Priority | Required to specify `priorities` |
	|  `VC`    | Virtual Clock | Required to specify `vticks` |
	
- **Required**: Yes
- **Example**:

  ```toml
  discipline = "FIFO"
  ```

#### weights

- **Valid value**: Vector of integers
- **Required**: Yes if `dispcipline = "DRR"` or `dispcipline = "WFQ"`
- **Example**:

  ```toml
  weights = [1, 2, 3]
  ```

#### priorities
- **Valid value**: Vector of (integer, integer), where the first integer is the flow class and the second integer is the priority of this flow class
- **Required**: Yes if `dispcipline = "SP"`
- **Example**:

  ```toml
  priorities = [(0, 2), (1, 1)]
  ```

#### vticks
- **Valid value**: Vector of (integer, integer), where the first integer is the flow class and the second integer is the inverse of the desired rates for the corresponding flows, in bits per second
- **Required**: Yes if `dispcipline = "VC"`
- **Example**:

  ```toml
  vticks = [(0, 2), (1, 1)]
  ```


### Flow

In **Day**, flows can be specified one by one:

```toml
[[flow]]
flow_id = 2
starts_before = [3]
starts_after = [1]
flow_type = "PacketDistribution"
graph = [[0, 1]]
[flow.traffic]
    initial_delay = 1.0
    duration = 2.0
    arr_dist = {type = "Uniform", low = 1, high = 1}
    pkt_size_dist = {type = "Uniform", low = 1000, high = 1500}
```

or by sets:

```toml
[[flow_set]]
first_flow_id = 10
flow_type = "PacketDistribution"
flow_count = 10
[flow_set.traffic]
    initial_delay = 0.0
    duration = 10.0
    arr_dist = {type = "Uniform", low = 0.0008, high = 0.0008}  # 10Mbps
    pkt_size_dist = {type = "Uniform", low = 1024, high = 1024}
``` 

The following table lists required, optional, or not supported attributes of a flow or a flow set.

|     Attribute   |      Meaning    |   flow   | flow_set |
|-----------------|-----------------|:--------:|:--------:|
|    `flow_id`    | The id of the flow| optional |    no    |
| `first_flow_id` | The smallest flow id of the flow set |    no    | optional |
| `starts_before` | The ids of flows that cannot start until this flow / flow set ends| optional | optional |
| `starts_after`  | The ids of flows that this flow / flow set must wait for them to end before it starts | optional | optional |
|   `flow_type`   | The type of the flow or flows of the flow_set | required | required |
|   `flow_count`  | The number of flows in the flow set |    no    | required |
|     `graph`     | The pair of source host and sink host | required | required |
|     `path`      | The path of the flow | optional |    no    |
|    `traffic`    | The traffic of the flow / flow set| required | required |

#### flow_type

- **Valid value**: 
	
	|   Value  |  Meaning |
	|----------|----------|
	|`PacketDistribution`| A flow whose packet source sends packets with specific distributions of inter-arrival times and packet sizes |
	|   `TCP`  | A flow whose packet source simulate the TCP protocol |
	 
- **Required**: Yes
- **Example**:

  ```toml
  flow_type = "PacketDistribution"
  ```
  
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