//! P15 lane R4: RoCE queue pairs and host-link PFC on the device backends, byte-identical to the
//! Scalar oracle (`evidence/P15/device-design.md`).
//!
//! The images are the `configs/p15/` fixtures and checkpoints taken from them, so the devices
//! resume from mid-run queue-pair states: a pending retransmission timeout, a parked pacer, a
//! class paused at a host with pause-parked pairs. Identity is the complete state under full
//! observation with Scalar's diagnostics stripped (ruling D1): devices produce no transition
//! records.

#![cfg(any(
    feature = "cuda",
    feature = "cuda-planner-test",
    all(feature = "metal", target_vendor = "apple")
))]

use std::path::PathBuf;

use days::scenario::compile_config;
use days_executor::{
    EventKind, FlowGeneratorKind, ObservationMode, RunResult, SimulationImage,
    run_scalar_with_observations,
};

/// Every fixture except the release-only HPCC incast, which has its own tests below.
const FIXTURES: &[&str] = &[
    "roce_lossless_pfc.toml",
    "roce_gbn_lossy.toml",
    "roce_timeout.toml",
    "roce_nack_only.toml",
    "roce_cnp_under_pfc.toml",
    "roce_feedback_priority.toml",
    "roce_mixed_tcp.toml",
    "hostpfc_incast_lossless.toml",
    "hostpfc_multi_qp_tcp.toml",
];

fn lower(name: &str) -> SimulationImage {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("configs/p15")
        .join(name);
    compile_config(&path).unwrap_or_else(|error| panic!("{} must lower: {error}", path.display()))
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

fn scalar(image: &SimulationImage, horizon: Option<u64>) -> RunResult {
    let mut expected = run_scalar_with_observations(image, horizon, ObservationMode::Full)
        .expect("scalar oracle must run");
    assert!(expected.diagnostics.is_some());
    expected.diagnostics = None;
    expected
}

/// Checkpoints of `name` at every `step` ns up to `until` ns.
fn checkpoints(name: &str, step: u64, until: u64) -> Vec<(String, SimulationImage)> {
    let image = lower(name);
    let stem = name.trim_end_matches(".toml");
    let mut images = Vec::new();
    let mut horizon = step;
    while horizon <= until.min(image.stop_time_ns - 1) {
        let prefix = run_scalar_with_observations(&image, Some(horizon), ObservationMode::Full)
            .expect("checkpoint prefix must run");
        images.push((
            format!("{stem}@{horizon}"),
            checkpoint_image(&image, &prefix),
        ));
        horizon += step;
    }
    images
}

/// The fixtures, then checkpoints of the timeout fixture (pending queue-pair timeouts) and of the
/// multi-pair host-PFC fixture (mid-pause parked pairs and restart windows).
fn qp_images() -> Vec<(String, SimulationImage)> {
    let mut images = FIXTURES
        .iter()
        .map(|name| (name.trim_end_matches(".toml").to_owned(), lower(name)))
        .collect::<Vec<_>>();
    images.extend(checkpoints("roce_timeout.toml", 2_500_000, 20_000_000));
    images.extend(checkpoints(
        "hostpfc_multi_qp_tcp.toml",
        1_000_000,
        10_000_000,
    ));
    images
}

fn queue_pairs(
    state: &[days_executor::HostState],
) -> impl Iterator<Item = (u64, &days_executor::RoceGenerator)> {
    state
        .iter()
        .flat_map(|host| &host.generators)
        .filter_map(|generator| match &generator.kind {
            FlowGeneratorKind::Roce(roce) => Some((generator.flow.0, roce)),
            _ => None,
        })
}

/// The images exercise what the identity tests rely on: a stalled pair whose pacing token has no
/// live event (token pinning, design note F3), checkpoints with a pending queue-pair timeout, and
/// checkpoints taken while a class is paused at a host with pause-parked pairs (ruling D3).
#[test]
fn qp_images_exercise_token_pinning_timeouts_and_parked_pairs() {
    let stalled = scalar(&lower("roce_nack_only.toml"), None);
    let unpinned_token = queue_pairs(&stalled.host_states).any(|(_, roce)| {
        !roce.pacer_armed
            && roce.snd_una < roce.pacer.total_bytes
            && stalled
                .pending_events
                .iter()
                .all(|event| event.payload != roce.pacing_timer_payload)
            && stalled
                .resident_packets
                .iter()
                .any(|packet| packet.id == roce.pacing_timer_payload)
    });
    assert!(
        unpinned_token,
        "roce_nack_only must end with a stalled pair whose token has no event"
    );

    let images = qp_images();
    let pending_timeout = images.iter().any(|(_, image)| {
        queue_pairs(&image.host_states).any(|(_, roce)| {
            image.initial_events.iter().any(|event| {
                event.kind == EventKind::RetransmissionTimeout
                    && event.payload == roce.pacing_timer_payload
            })
        })
    });
    assert!(
        pending_timeout,
        "some checkpoint must carry a pending queue-pair timeout"
    );
    let mid_pause = images.iter().any(|(_, image)| {
        image.host_states.iter().any(|host| {
            host.pfc.as_deref().is_some_and(|pfc| {
                (0..8).any(|class| pfc.is_paused(class) && !pfc.pause_parked[class].is_empty())
            })
        })
    });
    assert!(
        mid_pause,
        "some checkpoint must hold a paused class with pause-parked pairs"
    );
}

#[cfg(feature = "cuda")]
mod cuda {
    use days_executor::{CudaConfig, ObservationMode, run_cuda_with_observations};

    use super::{lower, qp_images, scalar};

    fn assert_matches(
        name: &str,
        image: &days_executor::SimulationImage,
        configs: &[(bool, usize)],
    ) {
        for horizon in [None, Some(image.stop_time_ns / 2)] {
            let expected = scalar(image, horizon);
            for &(streams_enabled, round_threads_per_block) in configs {
                let actual = run_cuda_with_observations(
                    image,
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

    #[test]
    fn cuda_qp_and_host_pfc_fixtures_and_checkpoints_match_scalar() {
        for (name, image) in qp_images() {
            let configs: &[(bool, usize)] = if name.contains('@') {
                &[(true, 256), (false, 32)]
            } else {
                &[(true, 256), (true, 32), (false, 32)]
            };
            assert_matches(&name, &image, configs);
        }
    }

    #[test]
    #[ignore = "release-only: 64 queue pairs on a 390-host Dragonfly embedding"]
    fn cuda_hpcc_incast_matches_scalar() {
        let image = lower("hpcc_incast64_dragonfly.toml");
        assert_matches("hpcc_incast64_dragonfly", &image, &[(true, 256)]);
    }
}

#[cfg(all(feature = "metal", target_vendor = "apple"))]
mod metal {
    use days_executor::{MetalConfig, ObservationMode, run_metal_with_observations};

    use super::{lower, qp_images, scalar};

    fn assert_matches(
        name: &str,
        image: &days_executor::SimulationImage,
        configs: &[(bool, usize)],
    ) {
        for horizon in [None, Some(image.stop_time_ns / 2)] {
            let expected = scalar(image, horizon);
            for &(streams_enabled, round_threads_per_threadgroup) in configs {
                let actual = run_metal_with_observations(
                    image,
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

    #[test]
    fn metal_qp_and_host_pfc_fixtures_and_checkpoints_match_scalar() {
        for (name, image) in qp_images() {
            let configs: &[(bool, usize)] = if name.contains('@') {
                &[(true, 256), (false, 32)]
            } else {
                &[(true, 256), (true, 32), (false, 32)]
            };
            assert_matches(&name, &image, configs);
        }
    }

    #[test]
    #[ignore = "release-only: 64 queue pairs on a 390-host Dragonfly embedding"]
    fn metal_hpcc_incast_matches_scalar() {
        let image = lower("hpcc_incast64_dragonfly.toml");
        assert_matches("hpcc_incast64_dragonfly", &image, &[(true, 256)]);
    }
}
