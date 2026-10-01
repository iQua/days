//! P14 Lane B: PFC per-priority link pause on the device backends, byte-identical to the Scalar
//! oracle.
//!
//! The images are:
//! - the PFC-carrying `configs/p14/` fixtures, including the zero-XOFF DCQCN fixtures whose PFC
//!   state is present but inert;
//! - an in-test incast that pauses two priorities under every scheduling discipline while a third,
//!   non-PFC priority keeps being served;
//! - checkpoints of the active images, taken mid-pause.

#![cfg(any(
    feature = "cuda",
    feature = "cuda-planner-test",
    all(feature = "metal", target_vendor = "apple")
))]

use std::fs;
use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    Backend, MechanismTransitionRecord, ObservationMode, RunResult, SimulationImage,
    run_scalar_with_observations, validate,
};

fn lower(name: &str) -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/p14")
        .join(name);
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
}

const DISCIPLINES: [&str; 5] = ["FIFO", "SP", "WFQ", "DRR", "WRR"];

/// Three sources converge on switch 2, whose egress to switch 3 is the bottleneck. Priorities 3
/// and 1 are lossless (nonzero XOFF); priority 0 is not PFC-controlled, so its packets stay
/// eligible while the others are paused.
fn incast(discipline: &str) -> SimulationImage {
    let directory = tempfile::TempDir::new().expect("temporary directory");
    let path = directory.path().join("pfc_incast.toml");
    let flow = |source: u32, priority: u8, size: u64, delay: &str| {
        format!(
            r#"
[[flow]]
flow_type = "PacketDistribution"
priority = {priority}
graph = [[{source}, 3]]

[flow.traffic]
initial_delay = {delay}
size = {size}
arr_dist = {{ type = "Uniform", low = 0.000001, high = 0.000001 }}
pkt_size_dist = {{ type = "DiscreteUniform", low = 1000, high = 1000 }}
"#
        )
    };
    let config = format!(
        r#"
seed = 14
edges = [[0, 2], [1, 2], [2, 3]]
hosts = [0, 1, 2, 3]
duration = 0.001

[switch]
port_rate = 1_000_000_000
capacity = 1000
weights = [3, 1, 2]
priorities = [3, 2, 1]
discipline = "{discipline}"
drop = "TailDrop"

[link]
mode = "Pfc"

[link.pfc]
xoff = [0, 3000, 0, 3000, 0, 0, 0, 0]
xon = [0, 1500, 0, 1500, 0, 0, 0, 0]
pause_quanta = [1, 1, 1, 1, 1, 1, 1, 1]
buffer_capacity = [0, 8000, 0, 8000, 0, 0, 0, 0]
{}{}{}{}"#,
        flow(0, 3, 100_000, "0.0"),
        flow(1, 1, 100_000, "0.000002"),
        flow(0, 0, 60_000, "0.000001"),
        flow(2, 3, 60_000, "0.000003"),
    );
    fs::write(&path, config).expect("scenario must be written");
    compile_config(&path).unwrap_or_else(|error| panic!("{discipline} incast must lower: {error}"))
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

#[cfg(any(feature = "cuda", all(feature = "metal", target_vendor = "apple")))]
fn scalar(image: &SimulationImage, horizon: Option<u64>) -> RunResult {
    let mut expected = run_scalar_with_observations(image, horizon, ObservationMode::Full)
        .expect("scalar oracle must run");
    assert!(expected.diagnostics.is_some());
    expected.diagnostics = None;
    expected
}

fn pfc_controls(image: &SimulationImage) -> usize {
    run_scalar_with_observations(image, None, ObservationMode::Full)
        .expect("scalar oracle must run")
        .diagnostics
        .expect("full observation")
        .mechanism_transitions
        .iter()
        .filter(|record| matches!(record, MechanismTransitionRecord::PfcControl(_)))
        .count()
}

/// Every PFC image the device tests run, with checkpoints of the active ones taken while pause
/// state is live.
fn pfc_images() -> Vec<(String, SimulationImage)> {
    let mut images = Vec::new();
    for name in [
        "dcqcn_t26_pfc.toml",
        "leanguard_pfc_executable.toml",
        "dcqcn_simple_zero_xoff.toml",
        "dcqcn_multi_zero_xoff.toml",
        "leanguard_dcqcn_zero_xoff.toml",
    ] {
        images.push((name.to_owned(), lower(name)));
    }
    for discipline in DISCIPLINES {
        images.push((format!("incast {discipline}"), incast(discipline)));
    }
    let active = images
        .iter()
        .filter(|(name, _)| name.starts_with("incast") || name == "dcqcn_t26_pfc.toml")
        .cloned()
        .collect::<Vec<_>>();
    for (name, image) in active {
        for step in 1..8 {
            let horizon = image.stop_time_ns / 8 * step;
            let prefix = run_scalar_with_observations(&image, Some(horizon), ObservationMode::Full)
                .expect("checkpoint prefix must run");
            let paused = prefix
                .switch_states
                .iter()
                .flat_map(|state| &state.queues)
                .filter_map(|queue| queue.pfc.as_ref())
                .any(|pfc| pfc.paused_by_controller.iter().any(|set| !set.is_empty()));
            if paused || step % 2 == 0 {
                images.push((
                    format!("{name}@{horizon}"),
                    checkpoint_image(&image, &prefix),
                ));
            }
        }
    }
    images
}

#[test]
fn the_incast_pauses_two_priorities_under_every_discipline() {
    for discipline in DISCIPLINES {
        let image = incast(discipline);
        validate(&image, Backend::Scalar)
            .unwrap_or_else(|error| panic!("{discipline} incast must validate: {error}"));
        let full = run_scalar_with_observations(&image, None, ObservationMode::Full)
            .expect("scalar oracle must run");
        let mut paused_priorities = std::collections::BTreeSet::new();
        for record in &full.diagnostics.as_ref().unwrap().mechanism_transitions {
            if let MechanismTransitionRecord::PfcControl(control) = record {
                paused_priorities.insert(control.priority);
            }
        }
        assert_eq!(
            paused_priorities,
            [1_u8, 3].into_iter().collect(),
            "{discipline}: both lossless priorities must pause"
        );
        assert!(pfc_controls(&image) >= 3, "{discipline}");
        assert_eq!(
            full.summary.dropped_packets, 0,
            "{discipline}: PFC is lossless"
        );
    }
    let checkpoints = pfc_images()
        .into_iter()
        .filter(|(name, _)| name.contains('@'))
        .count();
    assert!(checkpoints >= 20, "only {checkpoints} checkpoints");
    for (name, image) in pfc_images() {
        validate(&image, Backend::Scalar)
            .unwrap_or_else(|error| panic!("{name} must validate: {error}"));
    }
}

#[cfg(feature = "cuda")]
mod cuda {
    use days_executor::{CudaConfig, ObservationMode, run_cuda_with_observations};

    use super::{pfc_images, scalar};

    #[test]
    fn cuda_pfc_fixtures_incasts_and_checkpoints_match_scalar() {
        for (name, image) in pfc_images() {
            for horizon in [None, Some(image.stop_time_ns / 3)] {
                let expected = scalar(&image, horizon);
                for (streams_enabled, round_threads_per_block) in
                    [(true, 256), (true, 32), (false, 32)]
                {
                    let actual = run_cuda_with_observations(
                        &image,
                        horizon,
                        CudaConfig {
                            streams_enabled,
                            round_threads_per_block,
                            ..CudaConfig::default()
                        },
                        ObservationMode::Full,
                    )
                    .unwrap_or_else(|error| {
                        panic!(
                            "{name} horizon={horizon:?} streams={streams_enabled} \
                             threads={round_threads_per_block}: {error}"
                        )
                    });
                    assert_eq!(
                        actual.result, expected,
                        "{name} horizon={horizon:?} streams={streams_enabled} \
                         threads={round_threads_per_block}"
                    );
                }
            }
        }
    }
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
mod metal {
    use days_executor::{MetalConfig, ObservationMode, run_metal_with_observations};

    use super::{pfc_images, scalar};

    #[test]
    fn metal_pfc_fixtures_incasts_and_checkpoints_match_scalar() {
        for (name, image) in pfc_images() {
            for horizon in [None, Some(image.stop_time_ns / 3)] {
                let expected = scalar(&image, horizon);
                for (streams_enabled, round_threads_per_threadgroup) in
                    [(true, 256), (true, 32), (false, 32)]
                {
                    let actual = run_metal_with_observations(
                        &image,
                        horizon,
                        MetalConfig {
                            streams_enabled,
                            round_threads_per_threadgroup,
                            ..MetalConfig::default()
                        },
                        ObservationMode::Full,
                    )
                    .unwrap_or_else(|error| {
                        panic!(
                            "{name} horizon={horizon:?} streams={streams_enabled} \
                             threads={round_threads_per_threadgroup}: {error}"
                        )
                    });
                    assert_eq!(
                        actual.result, expected,
                        "{name} horizon={horizon:?} streams={streams_enabled} \
                         threads={round_threads_per_threadgroup}"
                    );
                }
            }
        }
    }
}
