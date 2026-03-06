use days::flows::collective::Collective;
use days::{next_flow_id, update_next_flow_id};
use std::io::Write;
use std::sync::{Mutex, OnceLock};
use tempfile::NamedTempFile;

fn write_config(contents: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().expect("create temp config");
    write!(file, "{contents}").expect("write temp config");
    file
}

fn flow_id_test_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

struct FlowIdReset {
    original: usize,
}

impl FlowIdReset {
    fn capture() -> Self {
        let original = next_flow_id();
        update_next_flow_id(original);
        Self { original }
    }
}

impl Drop for FlowIdReset {
    fn drop(&mut self) {
        update_next_flow_id(self.original);
    }
}

#[test]
fn ring_collective_reserves_expanded_flow_range_for_following_collective() {
    let _guard = flow_id_test_lock().lock().unwrap();
    let _reset = FlowIdReset::capture();
    let config = r#"
seed = 1

[[collective]]
collective_type = "RingAllReduce"
first_flow_id = 100000
flow_type = "PacketDistribution"
flow_count = 4
sources = [0, 1, 2, 3]
sinks = [1, 2, 3, 0]
[collective.traffic]
initial_delay = 0.0
size = 4096
arr_dist = { type = "Uniform", low = 1.0, high = 1.0 }
pkt_size_dist = { type = "DiscreteUniform", low = 512, high = 512 }

[[collective]]
collective_type = "Broadcast"
flow_type = "PacketDistribution"
flow_count = 1
sources = [0]
sinks = [1]
[collective.traffic]
initial_delay = 0.0
size = 512
arr_dist = { type = "Uniform", low = 1.0, high = 1.0 }
pkt_size_dist = { type = "DiscreteUniform", low = 512, high = 512 }
"#;

    let file = write_config(config);
    let hosts = vec![0, 1, 2, 3];
    let collectives = Collective::collectives_from_config(file.path().to_str().unwrap(), &hosts);

    assert_eq!(collectives.len(), 2);
    assert_eq!(collectives[0].first_flow_id, 100000);
    assert_eq!(collectives[0].flow_count, 4);
    assert_eq!(collectives[1].first_flow_id, 100024);
}

#[test]
fn ring_collective_set_uses_expanded_stride_and_reserves_for_following_set() {
    let _guard = flow_id_test_lock().lock().unwrap();
    let _reset = FlowIdReset::capture();
    let config = r#"
seed = 1

[[collective_set]]
collective_type = "RingAllReduce"
collective_count = 2
first_flow_id = 200000
flow_type = "PacketDistribution"
flow_count = 4
sources = [[0, 1, 2, 3], [4, 5, 6, 7]]
sinks = [[1, 2, 3, 0], [5, 6, 7, 4]]
[collective_set.traffic]
initial_delay = 0.0
size = 4096
arr_dist = { type = "Uniform", low = 1.0, high = 1.0 }
pkt_size_dist = { type = "DiscreteUniform", low = 512, high = 512 }

[[collective_set]]
collective_type = "Broadcast"
collective_count = 1
flow_type = "PacketDistribution"
flow_count = 1
sources = [[0]]
sinks = [[1]]
[collective_set.traffic]
initial_delay = 0.0
size = 512
arr_dist = { type = "Uniform", low = 1.0, high = 1.0 }
pkt_size_dist = { type = "DiscreteUniform", low = 512, high = 512 }
"#;

    let file = write_config(config);
    let hosts = vec![0, 1, 2, 3, 4, 5, 6, 7];
    let collectives = Collective::collectives_from_config(file.path().to_str().unwrap(), &hosts);

    assert_eq!(collectives.len(), 3);
    assert_eq!(collectives[0].first_flow_id, 200000);
    assert_eq!(collectives[1].first_flow_id, 200024);
    assert_eq!(collectives[2].first_flow_id, 200048);
}

#[test]
fn ring_collectives_from_graph_reserve_expanded_flow_ranges() {
    let _guard = flow_id_test_lock().lock().unwrap();
    let _reset = FlowIdReset::capture();
    update_next_flow_id(300000);

    let collectives = Collective::collectives_from_graph(
        days::flows::collective::CollectiveType::RingAllReduce,
        vec![
            vec![(0, 1), (1, 2), (2, 3), (3, 0)],
            vec![(4, 5), (5, 6), (6, 7), (7, 4)],
        ],
        None,
        vec![vec![0, 1, 2, 3], vec![4, 5, 6, 7]],
        vec![vec![1, 2, 3, 0], vec![5, 6, 7, 4]],
    );

    assert_eq!(collectives.len(), 2);
    assert_eq!(collectives[0].first_flow_id, 300000);
    assert_eq!(collectives[1].first_flow_id, 300024);
}
