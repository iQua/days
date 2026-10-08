//! P16 a2aset: a `[[collective_set]]` lowers each member exactly as the same `[[collective]]`
//! block would (the W2 docs lane's finding: an all-to-all set lowered without the all-to-all's
//! chunk policy and failed validation, "collective policy or phase is inconsistent with its
//! algorithm"). Every fixture writes its collectives once as a set and once as individual blocks;
//! the two images are equal, the set runs to completion on Scalar, and it equals Scalar on CPU
//! (1, 2 and 4 workers), Metal and CUDA under Full and Summary observation, at the stop, at stop/2
//! and at four checkpoints. A set's members have no name and no `after`, so an all-to-all set's
//! stages are all ungated roots: its progress certificate has no rows and LeanGuard has nothing to
//! check.

use std::sync::atomic::{AtomicU64, Ordering};

use days::scenario::compile_config;
use days_executor::{
    CpuConfig, ObservationMode, RunResult, SimulationImage, run_cpu_with_observations,
    run_scalar_with_observations,
};

fn lower(label: &str, config: &str) -> SimulationImage {
    static ID: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "days-p16-a2aset-{label}-{}-{}.toml",
        std::process::id(),
        ID.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&path, config).expect("write fixture");
    let image = compile_config(&path);
    std::fs::remove_file(&path).expect("remove fixture");
    image.unwrap_or_else(|error| panic!("{label} must lower: {error}"))
}

/// `hosts` hosts on one switch; `body` holds the collectives.
fn star(hosts: u64, body: &str) -> String {
    let edges = (0..hosts)
        .map(|host| format!("[{host}, {hosts}]"))
        .collect::<Vec<_>>()
        .join(", ");
    let list = (0..hosts)
        .map(|host| host.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        r#"
seed = 26
edges = [{edges}]
hosts = [{list}]
duration = 0.05

[switch]
port_rate = 8000000000
capacity = 200
discipline = "FIFO"
drop = "TailDrop"
{body}"#
    )
}

fn transport(flow: &str) -> &'static str {
    if flow == "TCP" {
        "\n[TABLE.traffic.tcp]\ncc_algorithm = \"TCPReno\"\n"
    } else {
        "\n[TABLE.traffic.dcqcn]\nmax_rate_gbps = 8.0\npacing_interval_ns = 500\n\n[TABLE.traffic.roce]\nretransmit_timeout_ns = 1000000\n"
    }
}

fn traffic(table: &str, flow: &str, size: u64) -> String {
    format!(
        "\n[{table}.traffic]\ninitial_delay = 0.0\nsize = {size}\narr_dist = {{ type = \"Uniform\", low = 1, high = 1 }}\npkt_size_dist = {{ type = \"DiscreteUniform\", low = 500, high = 500 }}\n{}",
        transport(flow).replace("TABLE", table)
    )
}

fn list(hosts: &[u64]) -> String {
    hosts
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

/// A ring's sinks: each rank's next rank.
fn ring_sinks(hosts: &[u64]) -> Vec<u64> {
    (0..hosts.len())
        .map(|rank| hosts[(rank + 1) % hosts.len()])
        .collect()
}

/// The members as one `[[collective_set]]`.
fn as_set(kind: &str, flow: &str, groups: &[Vec<u64>], size: u64) -> String {
    let sources = groups
        .iter()
        .map(|group| format!("[{}]", list(group)))
        .collect::<Vec<_>>()
        .join(", ");
    let sinks = if kind == "AllToAll" || kind == "SendRecv" {
        String::new()
    } else {
        let sinks = groups
            .iter()
            .map(|group| format!("[{}]", list(&ring_sinks(group))))
            .collect::<Vec<_>>()
            .join(", ");
        format!("sinks = [{sinks}]\n")
    };
    format!(
        "\n[[collective_set]]\ncollective_type = \"{kind}\"\ncollective_count = {}\nflow_type = \"{flow}\"\nflow_count = {}\nsources = [{sources}]\n{sinks}{}",
        groups.len(),
        groups[0].len(),
        traffic("collective_set", flow, size)
    )
}

/// The same members as individual `[[collective]]` blocks.
fn as_blocks(kind: &str, flow: &str, groups: &[Vec<u64>], size: u64) -> String {
    groups
        .iter()
        .map(|group| {
            let sinks = if kind == "AllToAll" || kind == "SendRecv" {
                String::new()
            } else {
                format!("sinks = [{}]\n", list(&ring_sinks(group)))
            };
            format!(
                "\n[[collective]]\ncollective_type = \"{kind}\"\nflow_type = \"{flow}\"\nflow_count = {}\nsources = [{}]\n{sinks}{}",
                group.len(),
                list(group),
                traffic("collective", flow, size)
            )
        })
        .collect()
}

/// A fixture: `(label, hosts, kind, transport, member groups, size)`.
type Fixture = (
    &'static str,
    u64,
    &'static str,
    &'static str,
    Vec<Vec<u64>>,
    u64,
);

fn fixtures() -> Vec<Fixture> {
    vec![
        // The W2 docs lane's reproducer: two 2-rank TCP all-to-alls.
        (
            "a2a-set-tcp",
            4,
            "AllToAll",
            "TCP",
            vec![vec![0, 1], vec![2, 3]],
            8_000,
        ),
        // Two 4-rank RoCE all-to-alls, one member in rank order and one permuted.
        (
            "a2a-set-roce",
            8,
            "AllToAll",
            "RoCE",
            vec![vec![0, 1, 2, 3], vec![7, 5, 6, 4]],
            12_000,
        ),
        // A ring set, which lowered before: its image must not change.
        (
            "ring-set-tcp",
            4,
            "RingAllReduce",
            "TCP",
            vec![vec![0, 1], vec![2, 3]],
            8_000,
        ),
    ]
}

fn images() -> Vec<(&'static str, SimulationImage)> {
    fixtures()
        .into_iter()
        .map(|(label, hosts, kind, flow, groups, size)| {
            (
                label,
                lower(label, &star(hosts, &as_set(kind, flow, &groups, size))),
            )
        })
        .collect()
}

/// A set's image is the image of the same collectives written as individual blocks.
#[test]
fn a_set_lowers_as_its_members_written_one_by_one() {
    for (label, hosts, kind, flow, groups, size) in fixtures() {
        let set = lower(label, &star(hosts, &as_set(kind, flow, &groups, size)));
        let blocks = lower(label, &star(hosts, &as_blocks(kind, flow, &groups, size)));
        assert_eq!(set, blocks, "{label}");
    }
}

fn scalar(image: &SimulationImage, horizon: Option<u64>, mode: ObservationMode) -> RunResult {
    run_scalar_with_observations(image, horizon, mode).expect("the Scalar oracle runs")
}

#[cfg(any(feature = "cuda", all(feature = "metal", target_vendor = "apple")))]
fn without_diagnostics(mut result: RunResult) -> RunResult {
    result.diagnostics = None;
    result
}

fn checkpoint_image(original: &SimulationImage, checkpoint: &RunResult) -> SimulationImage {
    let mut image = original.clone();
    image.host_states.clone_from(&checkpoint.host_states);
    image.switch_states.clone_from(&checkpoint.switch_states);
    image
        .initial_packets
        .clone_from(&checkpoint.resident_packets);
    image.initial_events.clone_from(&checkpoint.pending_events);
    image
}

/// The fixtures, then checkpoints of each at four horizons over its departures.
fn suite_images() -> Vec<(String, SimulationImage)> {
    let mut all = Vec::new();
    for (label, image) in images() {
        let full = scalar(&image, None, ObservationMode::Full);
        let end = full
            .departures
            .iter()
            .map(|departure| departure.time_ns)
            .max()
            .unwrap_or(1);
        for step in 1..=4 {
            let horizon = end * step / 5 + 1;
            let prefix = scalar(&image, Some(horizon), ObservationMode::Full);
            all.push((
                format!("{label}@{horizon}"),
                checkpoint_image(&image, &prefix),
            ));
        }
        all.push((label.to_owned(), image));
    }
    all
}

#[test]
fn every_fixture_runs_to_completion_on_scalar() {
    for (label, image) in images() {
        let result = scalar(&image, None, ObservationMode::Summary);
        let mut stages = 0;
        for state in &result.host_states {
            for (generator, stage) in state.generators_with_stages() {
                if stage.is_some() {
                    stages += 1;
                    assert_eq!(
                        generator.next_emission.status,
                        days_executor::GeneratorStatus::Finished,
                        "{label}: flow {:?}",
                        generator.flow
                    );
                }
            }
        }
        assert!(stages > 0, "{label}: the set lowers to stages");
    }
}

#[test]
fn cpu_matches_scalar_on_every_fixture_and_checkpoint() {
    for (label, image) in suite_images() {
        days_executor::validate(&image, days_executor::Backend::Scalar)
            .unwrap_or_else(|error| panic!("{label}: {error}"));
        for mode in [ObservationMode::Full, ObservationMode::Summary] {
            let expected = scalar(&image, None, mode);
            for workers in [1, 2, 4] {
                let actual = run_cpu_with_observations(
                    &image,
                    None,
                    CpuConfig {
                        workers,
                        ..CpuConfig::default()
                    },
                    mode,
                )
                .unwrap_or_else(|error| panic!("{label}: {error}"))
                .result;
                assert_eq!(actual, expected, "{label} {mode:?} workers={workers}");
            }
        }
    }
}

#[cfg(feature = "cuda")]
mod cuda {
    use days_executor::{CudaConfig, ObservationMode, run_cuda_with_observations};

    use super::{scalar, suite_images, without_diagnostics};

    #[test]
    fn cuda_matches_scalar_on_every_fixture_and_checkpoint() {
        for (label, image) in suite_images() {
            for horizon in [None, Some(image.stop_time_ns / 2)] {
                for mode in [ObservationMode::Full, ObservationMode::Summary] {
                    let expected = without_diagnostics(scalar(&image, horizon, mode));
                    let actual =
                        run_cuda_with_observations(&image, horizon, CudaConfig::default(), mode)
                            .unwrap_or_else(|error| panic!("{label}: {error}"))
                            .result;
                    assert_eq!(actual, expected, "{label} horizon={horizon:?} {mode:?}");
                }
            }
        }
    }
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
mod metal {
    use days_executor::{MetalConfig, ObservationMode, run_metal_with_observations};

    use super::{scalar, suite_images, without_diagnostics};

    #[test]
    fn metal_matches_scalar_on_every_fixture_and_checkpoint() {
        for (label, image) in suite_images() {
            for horizon in [None, Some(image.stop_time_ns / 2)] {
                for mode in [ObservationMode::Full, ObservationMode::Summary] {
                    let expected = without_diagnostics(scalar(&image, horizon, mode));
                    let actual =
                        run_metal_with_observations(&image, horizon, MetalConfig::default(), mode)
                            .unwrap_or_else(|error| panic!("{label}: {error}"))
                            .result;
                    assert_eq!(actual, expected, "{label} horizon={horizon:?} {mode:?}");
                }
            }
        }
    }
}
