//! P18 cpu-levers W2: every batch buffer of the static CPU path comes back to its sender, so the
//! buffers a run allocates do not grow with its rounds, however the threads are scheduled.
//!
//! W2 keeps one outbound buffer per owner and reuses the buffers owners hand back. The first
//! version handed a buffer back only inside the owner's next batch to that sender, so on one-way
//! traffic none came back: the sender allocated a fresh buffer every round and the owner dropped
//! the one it held (about a third of the batches on dp8 and b4 at 16 workers; review F2). Its
//! test counted every allocation of the run and depended on thread scheduling (review F4).
//!
//! **What is counted** (`planner-test-hooks`): batch buffers allocated because none had come back,
//! and returned buffers dropped. Each sender-owner pair keeps a few buffers in circulation (one
//! being filled, one in flight, one on its way back), so both counts are bounded by a constant
//! per pair, whatever the scheduling and the round count. The cap is four buffers per pair.
//!
//! Two scenarios, each at two exclusive horizons a millisecond apart (about 1,000 more rounds):
//! the k = 4 E1-long fabric at 2 workers (traffic both ways), and a one-way chain (host 0 to a
//! switch port to host 2) at 4 workers with modulo placement, where no owner ever sends back.
//!
//! Run: `cargo test -p days --features test --test p18_cpu_worker_allocations`.
#![cfg(feature = "test")]

use std::fs;
use std::path::{Path, PathBuf};

use days::scenario::compile_config_with_route_workers;
use days::topos::route::RouteWorkers;
use days_executor::{
    CpuConfig, SimulationImage, StaticPartitionPolicy, run_cpu,
    take_cpu_batch_buffer_counts_for_testing,
};

const E1LONG_FIXTURE: &str = "configs/benchmarks/evaluation/e1long_open_k32_load_30.toml";

const ONE_WAY_CHAIN: &str = r#"
seed = 18
edges = [[0, 1], [1, 2]]
hosts = [0, 2]
duration = 0.004
threading = "single"
log_path = "logs/p18/one_way_chain"

[switch]
port_rate = 10000000000
capacity = 1000
weights = [1]
discipline = "FIFO"
drop = "TailDrop"

[link]
propagation_ns = 1000

[[flow]]
flow_type = "PacketDistribution"
graph = [[0, 2]]

[flow.traffic]
initial_delay = 0.0
size = 100000000
arr_dist = { type = "Uniform", low = 0.000001, high = 0.000001 }
pkt_size_dist = { type = "DiscreteUniform", low = 512, high = 512 }
"#;

fn write_scenario(name: &str, text: &str) -> PathBuf {
    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "p18_cpu_worker_allocations_{name}_{}.toml",
        std::process::id()
    ));
    fs::write(&path, text).expect("write the scenario");
    path
}

fn lower(name: &str, text: &str) -> SimulationImage {
    let path = write_scenario(name, text);
    let image = compile_config_with_route_workers(&path, RouteWorkers::serial())
        .expect("the scenario lowers");
    let _ = fs::remove_file(&path);
    image
}

fn e1long_k4() -> SimulationImage {
    let text = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join(E1LONG_FIXTURE))
        .expect("read the E1-long fixture");
    let text = [
        ("\nk = 32\n", "\nk = 4\n"),
        ("\nhosts_per_edge = 16\n", "\nhosts_per_edge = 2\n"),
        ("\nflow_count = 8192\n", "\nflow_count = 16\n"),
    ]
    .iter()
    .fold(text, |text, (from, to)| {
        assert_eq!(text.matches(from).count(), 1, "{from:?}");
        text.replace(from, to)
    });
    lower("e1long_k4", &text)
}

/// Runs to two horizons and checks that neither the fresh nor the dropped batch buffers exceed
/// four per sender-owner pair.
fn check(label: &str, image: &SimulationImage, config: CpuConfig, short_ns: u64) {
    let cap = 4 * (config.workers * config.workers) as u64;
    for horizon_ns in [short_ns, short_ns + 1_000_000] {
        let _ = take_cpu_batch_buffer_counts_for_testing();
        run_cpu(image, Some(horizon_ns), config).expect("the CPU run succeeds");
        let (fresh, dropped) = take_cpu_batch_buffer_counts_for_testing();
        println!("{label} to {horizon_ns} ns: fresh batch buffers {fresh}, dropped {dropped}");
        assert!(
            fresh <= cap && dropped <= cap,
            "{label} to {horizon_ns} ns: {fresh} fresh and {dropped} dropped batch buffers, cap {cap}"
        );
    }
}

#[test]
fn batch_buffers_come_back_to_their_senders() {
    check(
        "e1long k4, 2 workers",
        &e1long_k4(),
        CpuConfig {
            workers: 2,
            ..CpuConfig::default()
        },
        1_000_000,
    );
    check(
        "one-way chain, 4 workers",
        &lower("one_way_chain", ONE_WAY_CHAIN),
        CpuConfig {
            workers: 4,
            static_partition: StaticPartitionPolicy::Modulo,
            ..CpuConfig::default()
        },
        1_000_000,
    );
}
