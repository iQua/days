//! P18 cpu-levers W1: a CPU worker merges each inbound remote envelope in one visit.
//!
//! Until W1 an owner radix-sorted every inbound batch on up to 34 key bytes (target, time, phase,
//! origin node, origin sequence), two passes over the envelopes per varying byte, only to group
//! the batch by target. That sort was 8 to 12% of a CPU-1 worker's instructions on every corpus
//! member (`days-gpu/evidence/P18/dp1-profile`). The order it produced reaches no output: each LP
//! queue pops in unique-key order whatever its insertion history, the packet store has no
//! iteration API, and batches from different sources already arrive in thread-timing order.
//!
//! **What is counted** (`planner-test-hooks`): the envelopes merged into owner queues, and the
//! envelope visits the merges made (finding the varying bytes, each sorting pass's count and
//! scatter, the grouped insert). The cap: one visit per envelope.
//!
//! Run: `cargo test -p days --features test --test p18_cpu_inbound_merge_pass`.
#![cfg(feature = "test")]

use std::fs;
use std::path::{Path, PathBuf};

use days::scenario::compile_config_with_route_workers;
use days::topos::route::RouteWorkers;
use days_executor::{CpuConfig, run_cpu, take_cpu_inbound_merge_counts_for_testing};

const E1LONG_FIXTURE: &str = "configs/benchmarks/evaluation/e1long_open_k32_load_30.toml";

fn e1long_k4() -> PathBuf {
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
    let path = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "p18_cpu_inbound_merge_pass_k4_{}.toml",
        std::process::id()
    ));
    fs::write(&path, text).expect("write the derived scenario");
    path
}

#[test]
fn an_inbound_envelope_is_merged_in_one_visit() {
    let path = e1long_k4();
    let image = compile_config_with_route_workers(&path, RouteWorkers::serial())
        .expect("the derived scenario lowers");
    let _ = fs::remove_file(&path);
    let _ = take_cpu_inbound_merge_counts_for_testing();
    let config = CpuConfig {
        workers: 2,
        ..CpuConfig::default()
    };
    run_cpu(&image, Some(300_000), config).expect("the CPU run succeeds");
    let (envelopes, visits) = take_cpu_inbound_merge_counts_for_testing();
    println!("inbound envelopes {envelopes}, envelope visits {visits}");
    assert!(envelopes > 1_000, "the run must exchange remote events");
    assert_eq!(
        visits, envelopes,
        "each inbound envelope must be visited once"
    );
}
